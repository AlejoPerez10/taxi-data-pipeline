from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("taxi_transform").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/raw/yellow_tripdata_2025-01.parquet")

df.printSchema()
df.show(10)
df.describe().show()
