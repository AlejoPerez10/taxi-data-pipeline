from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, month, date_format, round as spark_round, avg, count
)
from pyspark.sql.functions import unix_timestamp

spark = SparkSession.builder.appName("taxi_transform").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#------------------- CARGA (los 12 meses) -------------------
df = spark.read.parquet("/opt/airflow/data/raw/")

#------------------- LIMPIEZA -------------------
df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("passenger_count") > 0) &
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull())
)

#------------------- COLUMNAS DERIVADAS -------------------
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

#------------------- VALIDACIÓN -------------------
df_enriched.select(
    "pickup_hour",
    "pickup_day",
    "trip_duration_min",
    "fare_amount",
    "tip_amount"
).show(10)

# --- AGREGACIÓN: ingresos, propinas y duración por hora y día ---
summary = df_enriched.groupBy("pickup_month", "pickup_day", "pickup_hour").agg(
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip"),
    spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
    count("*").alias("total_trips")
).orderBy("pickup_month", "pickup_day", "pickup_hour")

summary.show(20)

print("Filas originales: ", df.count())
print("Filas después de la limpieza: ", df_clean.count())

# --- ESCRITURA: guardar resultado como Parquet particionado ---
summary.write.mode("overwrite").parquet("/opt/airflow/data/processed/summary_by_month")
print("Escritura completada.")