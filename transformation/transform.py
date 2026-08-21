import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, date_format, hour, month, round as spark_round, unix_timestamp
)

#Variables de entorno
BASE_PATH = os.environ.get("DATA_BASE_PATH", ".")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:4566")
PROCESS_MONTH = os.environ.get("PROCESS_MONTH")

# Elimina http:// o https:// si vienen en la variable de entorno
S3_ENDPOINT_CLEAN = S3_ENDPOINT.replace("http://", "").replace("https://", "")

# 1. Crear el bucket en LocalStack
try:
    req = urllib.request.Request(f"{S3_ENDPOINT}/nyc-taxi-data-lake", method="PUT")
    urllib.request.urlopen(req)
    print("Bucket creado o ya existente.")
except Exception as e:
    print(f"Aviso al crear bucket: {e}")

# 2. Sesión de Spark
spark = (
    SparkSession.builder 
    .appName("taxi_transform") 
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") 
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_CLEAN) # Dónde está S3
    .config("spark.hadoop.fs.s3a.access.key", "mock_access_key") # Credenciales ficticias para LocalStack
    .config("spark.hadoop.fs.s3a.secret.key", "mock_secret_key") # Credenciales ficticias para LocalStack
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # Le indica a Hadoop que utilice S3A para comunicarse con S3.
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") 
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") 
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") 
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") 
    .config("spark.hadoop.fs.s3a.paging.maximum", "1000") 
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") 
    .getOrCreate() # Crea la sesión o utiliza una existente
)
spark.sparkContext.setLogLevel("ERROR") # Solamente muestra los logs de erroes y no toda la información de ejecución 

# 3. Carga
df = spark.read.parquet( # Lee los datos RAW
    f"{BASE_PATH}/data/raw/yellow_tripdata_{PROCESS_MONTH}.parquet"
)

# 4. Liempieza
df_clean = df.filter( # Eliminar los registros que no cumplen las siguiente condiciones
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("passenger_count") > 0) &
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull())
)

# 5. Crear nuevas columnas
df_enriched = df_clean \
    .withColumn("pickup_month", month("tpep_pickup_datetime")) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_day", date_format("tpep_pickup_datetime", "EEEE")) \
    .withColumn("trip_duration_min",
        spark_round(
            (
                unix_timestamp("tpep_dropoff_datetime")
                - unix_timestamp("tpep_pickup_datetime")
            ) / 60, 2 # Convierte a minutos y redondea a 2 decimales
        )
    )

# 6. Agrupa los viajes
summary = df_enriched.groupBy("pickup_month", "pickup_day", "pickup_hour").agg(
    spark_round(avg("fare_amount"), 2).alias("avg_fare"), # Tarifa promedio
    spark_round(avg("tip_amount"), 2).alias("avg_tip"), # Propina promedio
    spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"), # Duración promedio (en minutos)
    count("*").alias("total_trips") # Total de viajes
).orderBy("pickup_month", "pickup_day", "pickup_hour")

# 7. Guarda el resultado localmente
output_path = f"{BASE_PATH}/data/processed/summary_by_month/{PROCESS_MONTH}" # Ruta para guardar el parquet ya procesado.
summary.write.mode("overwrite").parquet(output_path) # Para que se reescriba lo que ya tiene guardado cada vez que se haga.
print(f"Escritura local completada: {PROCESS_MONTH}") # Imprimir en consola el check al completarse exitosamente.

# 8. Guarda el resultado en S3/LocalStack
s3_output_path = f"s3a://nyc-taxi-data-lake/summary_by_month/{PROCESS_MONTH}"
summary.write.mode("overwrite").parquet(s3_output_path)
print(f"Escritura a S3 completada: {PROCESS_MONTH}")