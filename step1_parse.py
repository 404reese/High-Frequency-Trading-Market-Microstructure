import struct
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

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

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NASDAQ_HFT_Parser") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    raw_rdd = spark.sparkContext.binaryFiles("data/sample_itch_100mb.bin")
    parsed_rdd = raw_rdd.flatMap(lambda x: parse_itch_chunk(x[1]))
    full_df = spark.createDataFrame(parsed_rdd, schema=combined_schema)

    full_df = full_df.withColumn("readable_time", F.to_timestamp(F.col("timestamp") / 1e9)) \
                     .withColumn("hour", F.hour("readable_time"))

    directory_df = full_df.filter("msg_type = 'R'").select("stock_locate", "stock", "market_cat").distinct()
    trades_df = full_df.filter("msg_type = 'P'").select("stock", "timestamp", "shares", "price", "msg_type", "hour")

    enriched_trades = trades_df.join(directory_df, "stock")

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    print("Saving full_df to Parquet...")
    full_df.write.mode("overwrite") \
        .partitionBy("stock", "msg_type", "hour") \
        .option("compression", "snappy") \
        .parquet(str(data_dir / "full_df_parquet"))

    print("Saving enriched_trades to Parquet...")
    enriched_trades.write.mode("overwrite") \
        .partitionBy("stock", "msg_type", "hour") \
        .option("compression", "snappy") \
        .parquet(str(data_dir / "enriched_trades_parquet"))
    
    print("Done!")
