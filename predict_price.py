import struct
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Define schema and parsing logic (shared from parser)
combined_schema = StructType([
    StructField("msg_type", StringType(), True),
    StructField("stock_locate", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("stock", StringType(), True),
    StructField("shares", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_ref", LongType(), True),
    StructField("side", StringType(), True),
    StructField("market_cat", StringType(), True)
])

def parse_itch_chunk(binary_data):
    offset = 0
    records = []
    while offset + 2 <= len(binary_data):
        msg_len = struct.unpack('>H', binary_data[offset:offset+2])[0]
        offset += 2
        if offset + msg_len > len(binary_data):
            break 
        msg = binary_data[offset:offset+msg_len]
        offset += msg_len
        msg_type = chr(msg[0])
        try:
            stock_locate = struct.unpack('>H', msg[1:3])[0]
            timestamp = int.from_bytes(msg[5:11], 'big')
            if msg_type == 'R':
                stock = msg[11:19].decode().strip()
                market_cat = chr(msg[19])
                records.append(('R', stock_locate, timestamp, stock, None, None, None, None, market_cat))
            elif msg_type == 'A':
                order_ref = struct.unpack('>Q', msg[11:19])[0]
                side = chr(msg[19])
                shares = struct.unpack('>I', msg[20:24])[0]
                stock = msg[24:32].decode().strip()
                price = struct.unpack('>I', msg[32:36])[0] / 10000.0
                records.append(('A', stock_locate, timestamp, stock, shares, price, order_ref, side, None))
            elif msg_type == 'P':
                side = chr(msg[19]) 
                shares = struct.unpack('>I', msg[20:24])[0]
                stock = msg[24:32].decode().strip()
                price = struct.unpack('>I', msg[32:36])[0] / 10000.0
                records.append(('P', stock_locate, timestamp, stock, shares, price, None, side, None))
        except Exception:
            continue
    return records
def main():
    spark = SparkSession.builder \
        .appName("HFT_Price_Prediction") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading and parsing raw data in memory...")
    raw_rdd = spark.sparkContext.binaryFiles("data/sample_itch_100mb.bin")
    parsed_rdd = raw_rdd.flatMap(lambda x: parse_itch_chunk(x[1]))
    full_df = spark.createDataFrame(parsed_rdd, schema=combined_schema)

    # Convert timestamp to seconds for window functions
    full_df = full_df.withColumn("timestamp_sec", (F.col("timestamp") / 1e9).cast("long"))

    # Extract top stocks to keep computation manageable (e.g. top 5 by volume)
    top_stocks_df = full_df.filter("msg_type = 'P'").groupBy("stock").agg(F.sum("shares").alias("vol")).orderBy(F.desc("vol")).limit(5)
    top_stocks = [r.stock for r in top_stocks_df.collect() if r.stock]

    if not top_stocks:
        print("No trades found in the sample.")
        return

    # Filter for top stocks
    stock_df = full_df.filter(F.col("stock").isin(top_stocks))
    
    # 2. Extract Trades & Add Orders
    trades_df = stock_df.filter("msg_type = 'P'").select("stock", "timestamp_sec", "price", "shares")
    add_orders_df = stock_df.filter("msg_type = 'A'").select("stock", "timestamp_sec", "shares", "side")

    # 3. Feature Engineering - OBI (Minute bucketed)
    print("Calculating OBI...")
    add_orders_min = add_orders_df.withColumn("minute_bucket", (F.col("timestamp_sec") / 60).cast("long") * 60)
    obi_df = add_orders_min.groupBy("stock", "minute_bucket").agg(
        F.sum(F.when(F.col("side") == 'B', F.col("shares")).otherwise(0)).alias("Buy_Vol"),
        F.sum(F.when(F.col("side") == 'S', F.col("shares")).otherwise(0)).alias("Sell_Vol")
    ).withColumn("OBI", (F.col("Buy_Vol") - F.col("Sell_Vol")) / (F.col("Buy_Vol") + F.col("Sell_Vol") + 1e-9))
    
    # Join OBI with trades based on minute buckets
    trades_ob = trades_df.withColumn("minute_bucket", (F.col("timestamp_sec") / 60).cast("long") * 60)
    features_df = trades_ob.join(obi_df, ["stock", "minute_bucket"], "left").fillna({"OBI": 0.0})

    # Rolling Windows for Trade stats (1 minute = 60 seconds)
    print("Calculating Rolling Features...")
    window_60s = Window.partitionBy("stock").orderBy("timestamp_sec").rangeBetween(-60, 0)
    
    features_df = features_df.withColumn("Rolling_Volatility", F.stddev("price").over(window_60s))
    features_df = features_df.withColumn("Trade_Count_Per_Min", F.count("price").over(window_60s))
    
    # VWAP Change
    features_df = features_df.withColumn("vwap_num", F.sum(F.col("price") * F.col("shares")).over(window_60s))
    features_df = features_df.withColumn("vwap_den", F.sum("shares").over(window_60s))
    features_df = features_df.withColumn("VWAP", F.col("vwap_num") / F.col("vwap_den"))
    
    window_prev = Window.partitionBy("stock").orderBy("timestamp_sec")
    features_df = features_df.withColumn("VWAP_prev", F.lag("VWAP").over(window_prev))
    features_df = features_df.withColumn("VWAP_Change", F.col("VWAP") - F.col("VWAP_prev"))

    # 4. Label Generation (Future Price up in next 30 seconds -> 1 else 0)
    print("Generating Labels...")
    window_fwd_30s = Window.partitionBy("stock").orderBy("timestamp_sec").rangeBetween(1, 30)
    
    # Max price in next 30s
    features_df = features_df.withColumn("Future_Max_Price", F.max("price").over(window_fwd_30s))
    features_df = features_df.withColumn("price_direction", F.when(F.col("Future_Max_Price") > F.col("price"), 1.0).otherwise(0.0))

    # Clean up nulls
    clean_df = features_df.select(
        "stock", "timestamp_sec",
        "OBI", "Rolling_Volatility", "Trade_Count_Per_Min", "VWAP_Change", "price_direction"
    ).dropna()

    if clean_df.count() == 0:
        print("Not enough data to train (after dropna).")
        return

    # 5. ML Data Preparation
    print("Preparing Data for ML...")
    assembler = VectorAssembler(
        inputCols=["OBI", "Rolling_Volatility", "Trade_Count_Per_Min", "VWAP_Change"],
        outputCol="features",
        handleInvalid="skip"
    )

    ml_data = assembler.transform(clean_df)
    
    # Chronological Split (Train: 80%, Test: 20%)
    fraction_train = 0.8
    quantiles = ml_data.approxQuantile("timestamp_sec", [fraction_train], 0.01)
    split_time = quantiles[0]
    
    train_df = ml_data.filter(F.col("timestamp_sec") <= split_time)
    test_df = ml_data.filter(F.col("timestamp_sec") > split_time)

    # 6. Model Training
    print("Training Random Forest Classifier...")
    rf = RandomForestClassifier(featuresCol="features", labelCol="price_direction", numTrees=20, maxDepth=5)
    rf_model = rf.fit(train_df)

    # 7. Evaluation
    print("Evaluating Model...")
    predictions = rf_model.transform(test_df)
    
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="price_direction", predictionCol="prediction", metricName="accuracy")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="price_direction", predictionCol="prediction", metricName="weightedPrecision")
    
    accuracy = evaluator_acc.evaluate(predictions)
    precision = evaluator_prec.evaluate(predictions)
    
    print("\n" + "="*40)
    print("             MODEL RESULTS")
    print("="*40)
    print(f"Test Accuracy  : {accuracy * 100:.2f}%")
    print(f"Test Precision : {precision * 100:.2f}%")
    print("="*40)

if __name__ == "__main__":
    main()
