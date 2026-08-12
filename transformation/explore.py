from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("taxi_explore").getOrCreate()
df = spark.read.parquet("data/raw/yellow_tripdata_2025-01.parquet")
df.printSchema()
df.show(5)
print("Filas: ", df.count())
