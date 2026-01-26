from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

# create Spark Session
spark = SparkSession \
        .builder \
        .appName("CalculateUSD") \
        .getOrCreate()

# ===== CREATE AND CLEAN STREAMS =====

# link spark to kafka topics
raw_donations_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "raw_donations") \
        .load()

raw_currency_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "raw_currency") \
        .load()

# define the schema for the streams
donations_schema = StructType([
    StructField("donation_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("amount_ars", DoubleType(), False),
    StructField("donor_type", StringType(), False),
    StructField("timestamp", DoubleType(), False)
])

currency_schema = StructType([
    StructField("timestamp", DoubleType(), False),
    StructField("provider", StringType(), False),
    StructField("rates", StructType([
        StructField("blue", DoubleType(), False),
        StructField("mep", DoubleType(), False),
    ]), False)
])

# cast the 'value' column from binary to string
donations_string = raw_donations_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
currency_string = raw_currency_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# parse json payload using structured columns using defined schema
donations_struct = donations_string.select(sf.from_json("value", donations_schema).alias("data"))
currency_struct = currency_string.select(sf.from_json("value", currency_schema).alias("data"))

# flatten nested structures 
donations_df = donations_struct.select("data.*")
currency_df = currency_struct.select("data.*").select("timestamp","provider","rates.*")

# standarize timestamps columns to spark's format
donations_df = donations_df.withColumn("timestamp",sf.timestamp_seconds(sf.col("timestamp")))
currency_df = currency_df.withColumn("timestamp",sf.timestamp_seconds(sf.col("timestamp")))

# ===== JOIN STREAMS =====

# add watermark to streams in preparation for join()
donations_df_wtmrk = donations_df.withWatermark("timestamp", "5 minutes")
currency_df_wtmrk = currency_df.withWatermark("timestamp", "10 minutes")

# join donation with last currency update
actual_usd_df = donations_df_wtmrk.join(
    currency_df_wtmrk,
    donations_df_wtmrk.timestamp >= currency_df_wtmrk.timestamp,
    "inner"
)

# calculate the actual amount of usd from the donation based on current price of dollar 'blue'
actual_usd_df = actual_usd_df.withColumn("amount_usd", sf.col("amount_ars")/sf.col("blue")) \
        .select(donations_df_wtmrk.timestamp, donations_df_wtmrk.donation_id, sf.col("amount_usd"))
 
# stream output to new table
query = actual_usd_df.writeStream.format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/bitnami/spark/processing/checkpoints/donations_usd") \
        .toTable("donations_usd")

query.awaitTermination()
