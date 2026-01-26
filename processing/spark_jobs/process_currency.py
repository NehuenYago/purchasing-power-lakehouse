from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

# creates the Spark Session
spark = SparkSession \
        .builder \
        .appName("CurrencyProcesser") \
        .getOrCreate()

# links spark to the kafka topic 'raw_currency'
raw_currency_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "raw_currency") \
        .load()

# defines the schema for the currency 
currency_schema = StructType([
    StructField("timestamp", DoubleType(), False),
    StructField("provider", StringType(), False),
    StructField("rates", StructType([
        StructField("blue", DoubleType(), False),
        StructField("mep", DoubleType(), False),
    ]), False)
])

# uses .selectExpr() to cast the 'value' column from binary (from kafka) to string
currency_string = raw_currency_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
 
# turns the json-string and the defined schema and turns it into a spark struct
# the struct is a single column named 'data'
currency_struct = currency_string.select(sf.from_json("value", currency_schema).alias("data"))

# turns the single struct column into the individual defined columns (timestamp, provider, rates)
# turns rates columns into blue and mep columns
# currency_df is a standard dataframe
currency_df = currency_struct \
        .select("data.*") \
        .select("timestamp","provider","rates.*")

# convers timestamp column to epoch numbers to timestamps
currency_df = currency_df.withColumn("timestamp",sf.timestamp_seconds(sf.col("timestamp")))

currency_df.printSchema()

query = currency_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()

