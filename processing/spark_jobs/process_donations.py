from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
        .builder \
        .appName("DonationsProcesser") \
        .getOrCreate()

donations_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "raw_donations") \
        .load()

query = donations_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

query.awaitTermination()
