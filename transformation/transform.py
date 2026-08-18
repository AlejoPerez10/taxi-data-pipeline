import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, date_format, hour, month, round as spark_round, unix_timestamp
)

BASE_PATH = os.environ.get("DATA_BASE_PATH", ".")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:4566")

# Elimina http:// o https:// si vienen en la variable de entorno
S3_ENDPOINT_CLEAN = S3_ENDPOINT.replace("http://", "").replace("https://", "")

# 1. Crear el bucket en LocalStack PRIMERO
try:
    req = urllib.request.Request(f"{S3_ENDPOINT}/nyc-taxi-data-lake", method="PUT")
    urllib.request.urlopen(req)
    print("Bucket creado o ya existente.")
except Exception as e:
    print(f"Aviso al crear bucket: {e}")

# 2. Sesión de Spark
spark = SparkSession.builder \
    .appName("taxi_transform") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_CLEAN) \
    .config("spark.hadoop.fs.s3a.access.key", "mock_access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "mock_secret_key") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.paging.maximum", "1000") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Carga y Limpieza
df = spark.read.parquet(f"{BASE_PATH}/data/raw/")

df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("passenger_count") > 0) &
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull())
)

# 4. Columnas derivadas
df_enriched = df_clean \
    .withColumn("pickup_month", month("tpep_pickup_datetime")) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_day", date_format("tpep_pickup_datetime", "EEEE")) \
    .withColumn(
        "trip_duration_min",
        spark_round(
            (
                unix_timestamp("tpep_dropoff_datetime")
                - unix_timestamp("tpep_pickup_datetime")
            ) / 60, 2
        )
    )

# 5. Agregación (sin .show())
summary = df_enriched.groupBy("pickup_month", "pickup_day", "pickup_hour").agg(
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip"),
    spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
    count("*").alias("total_trips")
).orderBy("pickup_month", "pickup_day", "pickup_hour")

# 6. Escrituras directas
summary.write.mode("overwrite").parquet(f"{BASE_PATH}/data/processed/summary_by_month")
print("Escritura local completada.")

summary.write.mode("overwrite").parquet("s3a://nyc-taxi-data-lake/summary_by_month")
print("Escritura a S3 completada.")