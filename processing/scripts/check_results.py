from pyspark.sql import SparkSession

# to read the output table created by spark

spark = SparkSession.builder.appName("Checker").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_parquet = spark.read.parquet("/opt/bitnami/spark/processing/data/actual_donations_usd")
df_parquet.show()

print(f"Total de registros procesados: {df_parquet.count()}")
