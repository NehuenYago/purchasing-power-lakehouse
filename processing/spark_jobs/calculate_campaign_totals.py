from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import *
import os
import logging

DB_USER = str(os.getenv("DB_USER"))
DB_PASSWORD = str(os.getenv("DB_PASSWORD"))
DB_NAME = str(os.getenv("DB_NAME"))

# create Spark Session
spark = SparkSession \
        .builder \
        .appName("CampaingTotals") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

donations_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("donation_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("amount_ars", DoubleType(), False),
    StructField("blue", DoubleType(), False),
    StructField("amount_usd", DoubleType(), False)
])

campaigns_df = spark.readStream \
        .schema(donations_schema) \
        .parquet("/opt/bitnami/spark/processing/data/actual_donations_usd") 

campaigns_df = campaigns_df \
        .groupBy(sf.col("campaign_id")) \
        .agg(
            sf.count("donation_id").alias("total_donations"),
            sf.sum("amount_ars").alias("total_donated_ars"),
            sf.sum("amount_usd").alias("total_donated_usd"),
            sf.avg("blue").alias("avg_exchange_rate"),
            sf.median("amount_usd").alias("median_donation_usd")
        ) \
        .select(sf.col("campaign_id"), sf.col("total_donated_ars"),
                sf.col("total_donated_usd"), sf.col("avg_exchange_rate"),
                sf.col("median_donation_usd")
        )

def write_to_db (df, batchId):
    if not df.isEmpty():
        df.write.jdbc(url="jdbc:postgresql://db:5432/" + DB_NAME, 
                      table=DB_NAME, 
                      mode="overwrite", 
                      properties={
                          "user": DB_USER, 
                          "password": DB_PASSWORD,
                          "driver": "org.postgresql.Driver"
                          }
                      )

query = campaigns_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_db) \
    .option("checkpointLocation", "/opt/bitnami/spark/processing/checkpoints/campaigns_total_donations") \
    .start() \
    .awaitTermination()
