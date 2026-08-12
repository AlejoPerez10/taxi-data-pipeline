from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reado_parquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/processed/summary_by_day_hour/part-00000-ca2a6f62-9f26-4adc-a395-7e38352f7d7c-c000.snappy.parquet")

df.printSchema()
df.show(10)
df.describe()