from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NASDAQ_HFT_Signals") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    data_dir = Path("data")
    hft_dir = data_dir / "HFT_Signals"
    hft_dir.mkdir(exist_ok=True, parents=True)

    print("Loading Parquet files...")
    full_df = spark.read.parquet(str(data_dir / "full_df_parquet"))
    enriched_trades = spark.read.parquet(str(data_dir / "enriched_trades_parquet"))

    print("Computing Dashboard Summaries...")
    dashboard_summary_df = enriched_trades.withColumn(
        "minute_bucket",
        (F.col("timestamp") / F.lit(60_000_000_000)).cast("long")
    ).groupBy("stock", "minute_bucket", "market_cat").agg(
        F.sum("shares").alias("volume"),
        (F.sum(F.col("price") * F.col("shares")) / F.sum("shares")).alias("vwap"),
        F.avg("price").alias("avg_price"),
        F.count("*").alias("trade_count")
    ).orderBy("stock", "minute_bucket")

    stock_summary_df = enriched_trades.groupBy("stock", "market_cat").agg(
        F.sum("shares").alias("total_volume"),
        F.avg("price").alias("avg_price"),
        F.count("*").alias("trade_count")
    ).orderBy(F.desc("total_volume"))

    dashboard_summary_df.toPandas().to_csv(data_dir / "dashboard_summary.csv", index=False)
    stock_summary_df.toPandas().to_csv(data_dir / "stock_summary.csv", index=False)
    print("Dashboard summaries saved to CSV.")

    print("Computing Order Book Imbalance (OBI)...")
    add_orders = full_df.filter("msg_type = 'A'")
    add_orders_min = add_orders.withColumn(
        "minute_bucket",
        (F.col("timestamp") / F.lit(60_000_000_000)).cast("long")
    )
    obi_df = add_orders_min.groupBy("stock", "minute_bucket").agg(
        F.sum(F.when(F.col("side") == 'B', F.col("shares")).otherwise(0)).alias("Buy_Vol"),
        F.sum(F.when(F.col("side") == 'S', F.col("shares")).otherwise(0)).alias("Sell_Vol")
    ).withColumn(
        "OBI",
        (F.col("Buy_Vol") - F.col("Sell_Vol")) / (F.col("Buy_Vol") + F.col("Sell_Vol"))
    )
    obi_df.write.mode("overwrite").parquet(str(hft_dir / "obi"))
    print("OBI saved.")

    print("Computing Market Impact...")
    a_p_df = full_df.filter(F.col("msg_type").isin('A', 'P'))
    window_impact = Window.partitionBy("stock_locate").orderBy("timestamp")
    a_p_df = a_p_df.withColumn("a_price", F.when(F.col("msg_type") == 'A', F.col("price")))
    a_p_df = a_p_df.withColumn("last_a_price", F.last("a_price", ignorenulls=True).over(window_impact))
    market_impact_df = a_p_df.filter("msg_type = 'P'").select(
        "stock", "stock_locate", "timestamp", "price", "last_a_price", "msg_type", "hour"
    ).withColumn(
        "market_impact", F.col("price") - F.col("last_a_price")
    )
    market_impact_df.write.mode("overwrite").parquet(str(hft_dir / "market_impact"))
    print("Market Impact saved.")

    print("Computing Volatility...")
    trades_df = full_df.filter("msg_type = 'P'")
    top_10 = trades_df.groupBy("stock").agg(F.sum("shares").alias("vol")).orderBy(F.desc("vol")).limit(10)
    top_10_list = [row.stock for row in top_10.collect()]
    top_trades = trades_df.filter(F.col("stock").isin(top_10_list))

    vol_window = Window.partitionBy("stock").orderBy("timestamp").rangeBetween(-300_000_000_000, Window.currentRow)
    volatility_df = top_trades.withColumn(
        "volatility", F.stddev("price").over(vol_window)
    )
    volatility_df.write.mode("overwrite").parquet(str(hft_dir / "volatility"))
    print("Volatility saved.")
    
    print("Done computing all signals!")