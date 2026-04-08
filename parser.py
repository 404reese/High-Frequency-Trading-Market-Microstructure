import struct
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define the schema for the output (What recruiters want to see)
trade_schema = StructType([
    StructField("stock_locate", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("shares", LongType(), True),
    StructField("stock", StringType(), True),
    StructField("price", DoubleType(), True)
])

def parse_itch_chunk(binary_data):
    offset = 0
    records = []

    while offset + 2 <= len(binary_data):
        # Read message length
        msg_len = struct.unpack('>H', binary_data[offset:offset+2])[0]
        offset += 2

        if offset + msg_len > len(binary_data):
            break  # incomplete message

        msg = binary_data[offset:offset+msg_len]
        offset += msg_len

        msg_type = chr(msg[0])

        if msg_type == 'P':
            try:
                # Correct ITCH P message parsing
                stock_locate = struct.unpack('>H', msg[1:3])[0]
                timestamp = int.from_bytes(msg[5:11], 'big')
                shares = struct.unpack('>I', msg[20:24])[0]
                stock = msg[24:32].decode().strip()
                price = struct.unpack('>I', msg[32:36])[0] / 10000

                records.append((stock_locate, timestamp, shares, stock, price))

            except Exception:
                continue

    return records

# Start Spark
spark = SparkSession.builder.appName("HFT_Parser").getOrCreate()

# Load the binary file as an RDD
raw_rdd = spark.sparkContext.binaryFiles("data/sample_itch_100mb.bin")
# Process and convert to DataFrame
parsed_rdd = raw_rdd.flatMap(lambda x: parse_itch_chunk(x[1]))
trade_df = spark.createDataFrame(parsed_rdd, schema=trade_schema)

trade_df.show(5)