import struct
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

import os

os.environ["PYSPARK_PYTHON"] = r"E:\coding ground\High-Frequency-Trading-Market-Microstructure\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"E:\coding ground\High-Frequency-Trading-Market-Microstructure\venv\Scripts\python.exe"

# We define a combined schema to handle the stream, then split them later
# This is a common pattern in Big Data for handling multiplexed binary streams
combined_schema = StructType([
    StructField("msg_type", StringType(), True),
    StructField("stock_locate", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("stock", StringType(), True),
    StructField("shares", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_ref", LongType(), True),      # For 'A' messages
    StructField("side", StringType(), True),           # For 'A' and 'P' messages
    StructField("market_cat", StringType(), True)      # For 'R' messages
])

def parse_itch_chunk(binary_data):
    offset = 0
    records = []

    while offset + 2 <= len(binary_data):
        # 1. Read message length (2 bytes, big-endian)
        msg_len = struct.unpack('>H', binary_data[offset:offset+2])[0]
        offset += 2

        if offset + msg_len > len(binary_data):
            break 

        msg = binary_data[offset:offset+msg_len]
        offset += msg_len
        msg_type = chr(msg[0])

        try:
            # Common fields for most ITCH messages: Locate (1:3), Timestamp (5:11)
            stock_locate = struct.unpack('>H', msg[1:3])[0] # [cite: 69, 149, 205]
            timestamp = int.from_bytes(msg[5:11], 'big')   # [cite: 60, 69, 149]

            # TYPE R: Stock Directory 
            if msg_type == 'R':
                stock = msg[11:19].decode().strip()
                market_cat = chr(msg[19])
                # (type, locate, time, stock, shares, price, ref, side, cat)
                records.append(('R', stock_locate, timestamp, stock, None, None, None, None, market_cat))

            # TYPE A: Add Order (No MPID) 
            elif msg_type == 'A':
                order_ref = struct.unpack('>Q', msg[11:19])[0]
                side = chr(msg[19])
                shares = struct.unpack('>I', msg[20:24])[0]
                stock = msg[24:32].decode().strip()
                price = struct.unpack('>I', msg[32:36])[0] / 10000.0 # Price(4) [cite: 52]
                records.append(('A', stock_locate, timestamp, stock, shares, price, order_ref, side, None))

            # TYPE P: Trade 
            elif msg_type == 'P':
                # Order Reference is 0/null for binary P messages [cite: 205, 208]
                side = chr(msg[19]) 
                shares = struct.unpack('>I', msg[20:24])[0]
                stock = msg[24:32].decode().strip()
                price = struct.unpack('>I', msg[32:36])[0] / 10000.0
                records.append(('P', stock_locate, timestamp, stock, shares, price, None, side, None))

        except Exception:
            continue

    return records

# Initialize Spark
spark = SparkSession.builder \
    .appName("NASDAQ_HFT_Analyzer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Load and Parse
raw_rdd = spark.sparkContext.binaryFiles("data/sample_itch_100mb.bin")
parsed_rdd = raw_rdd.flatMap(lambda x: parse_itch_chunk(x[1]))
full_df = spark.createDataFrame(parsed_rdd, schema=combined_schema)

# --- PROJECT MILESTONE: Data Separation ---
# Filter out the Stock Directory to create a mapping table
directory_df = full_df.filter("msg_type = 'R'").select("stock_locate", "stock", "market_cat").distinct()

# Filter out the Trades
trades_df = full_df.filter("msg_type = 'P'").select("stock", "timestamp", "shares", "price")

# --- ANALYTICS: Joining Metadata (The BDA Flex) ---
# This shows you can join a lookup table with a high-volume stream
enriched_trades = trades_df.join(directory_df, "stock")

print("Enriched Trade Data (Trade + Market Category):")
enriched_trades.show(5)

# --- ANALYTICS: VWAP Calculation ---
vwap_df = enriched_trades.groupBy("stock", "market_cat").agg(
    (F.sum(F.col("price") * F.col("shares")) / F.sum("shares")).alias("vwap"),
    F.sum("shares").alias("total_volume")
).orderBy(F.desc("total_volume"))

print("Market-Wide VWAP Analytics:")
vwap_df.show(10)

# Export a compact dashboard dataset so Streamlit stays fast on a single laptop.
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

data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

dashboard_summary_df.toPandas().to_csv(data_dir / "dashboard_summary.csv", index=False)
stock_summary_df.toPandas().to_csv(data_dir / "stock_summary.csv", index=False)