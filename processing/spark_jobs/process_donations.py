from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

# creates the Spark Session
spark = SparkSession \
        .builder \
        .appName("DonationsProcesser") \
        .getOrCreate()

# links spark to the kafka topic 'raw_donations'
raw_donations_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "raw_donations") \
        .load()

# defines the schema for the donations
donations_schema = StructType([
    StructField("donation_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("amount_ars", DoubleType(), False),
    StructField("donor_type", StringType(), False),
    StructField("timestamp", DoubleType(), False)
])

# uses .selectExpr() to cast the 'value' column from binary (from kafka) to string
donations_string = raw_donations_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
 
# turns the json-string and the defined schema and turns it into a spark struct
# the struct is a single column named 'data'
donations_struct = donations_string.select(sf.from_json("value", donations_schema).alias("data"))

# turns the single struct column into the individual defined columns (donation_id, campaign_id, etc)
# donations_df is a standard dataframe
donations_df = donations_struct.select("data.*")

# convers timestamp column to epoch numbers to timestamps
donations_df = donations_df.withColumn("timestamp",sf.timestamp_seconds(sf.col("timestamp")))

donations_df.printSchema()

query = donations_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()
