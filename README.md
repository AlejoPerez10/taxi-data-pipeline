# NYC Taxi Data Pipeline

Pipeline de ingeniería de datos end-to-end construido sobre el dataset público **NYC TLC Yellow Taxi Trip Records** (12 meses de 2025, +40 millones de registros). El proyecto simula un flujo de datos real de producción: ingesta, transformación distribuida, orquestación automatizada e infraestructura como código.

## Pregunta de negocio

¿Cómo varían los ingresos, las propinas y la duración de los viajes según la hora del día y el día de la semana a lo largo del año?

El pipeline procesa los datos crudos y genera un resumen agregado por mes, día de la semana y hora, permitiendo identificar patrones de demanda y comportamiento de tarifas.

## Arquitectura

```
data/raw/ (Parquet mensuales)
        │
        ▼
   PySpark (transform.py)
   limpieza → columnas derivadas → agregación
        │
        ├──► data/processed/ (Parquet local, por mes)
        │
        └──► S3 / LocalStack (nyc-taxi-data-lake)
        │
   Orquestado por Airflow (DAG mensual, backfill por mes)
        │
   Infraestructura provisionada con Terraform
```

Todo el stack corre containerizado con Docker, incluyendo una imagen custom de Airflow con PySpark y Java 17 instalados.

## Stack técnico

| Tecnología | Uso en el proyecto |
|---|---|
| **PySpark** | Limpieza, transformación y agregación distribuida de +40M de filas |
| **Apache Airflow** | Orquestación del pipeline con ejecución mensual (`data_interval_start` como parámetro de partición) |
| **Docker / Docker Compose** | Containerización de Airflow y de la infraestructura simulada de nube |
| **Terraform** | Infraestructura como código (provisión de bucket S3) |
| **LocalStack** | Simulación local de AWS S3 |
| **Python 3.11** | Lógica de ingesta y scripts de soporte |

## Cómo ejecutarlo

### Requisitos
- Docker Desktop
- Terraform
- Java 17 y PySpark instalados localmente (para ejecución fuera de Docker)

### 1. Levantar Airflow
```bash
docker-compose build
docker-compose up -d
```
Dashboard disponible en `http://localhost:8080` (usuario/contraseña: `airflow`)

### 2. Levantar la infraestructura simulada (LocalStack)
```bash
cd infra
docker-compose -f docker-compose.localstack.yaml up -d
```

### 3. Provisionar el bucket S3 con Terraform
```bash
cd infra
terraform init
terraform apply
```

### 4. Ejecutar el pipeline
Desde el dashboard de Airflow, dispara el DAG `taxi_data_pipeline` (manual o por backfill). Cada ejecución procesa un mes, definido por la fecha lógica de la corrida.

### Variables de entorno usadas por `transform.py`

| Variable | Propósito | Valor local | Valor en Airflow |
|---|---|---|---|
| `DATA_BASE_PATH` | Ruta base de datos | `.` | `/opt/airflow` |
| `S3_ENDPOINT` | Endpoint de S3/LocalStack | `http://localhost:4566` | `http://localstack:4566` |
| `PROCESS_MONTH` | Mes a procesar (`YYYY-MM`) | Definido manualmente | Inyectado por Airflow |

## Estructura del repositorio

```
taxi-data-pipeline/
├── dags/                          # DAG de Airflow
├── transformation/                # Script de transformación PySpark
├── infra/                         # Terraform + LocalStack
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── data/                          # Datos crudos y procesados (ignorado en git)
├── Dockerfile                     # Imagen custom de Airflow (Java 17 + PySpark)
├── docker-compose.yaml            # Orquestación de Airflow
└── README.md
```

## Resultado

El pipeline genera un Parquet agregado con las siguientes columnas:

`pickup_month | pickup_day | pickup_hour | avg_fare | avg_tip | avg_duration_min | total_trips`

Procesado exitosamente para los 12 meses de 2025, tanto en almacenamiento local como en el bucket S3 simulado.

## Nota sobre la infraestructura cloud

Terraform apunta a **LocalStack** en lugar de una cuenta real de AWS o GCP, debido a un error persistente de facturación al vincular tarjetas internacionales en ambos proveedores. El código Terraform es funcionalmente idéntico al que se usaría contra un proveedor real — solo cambia el `endpoint` del provider — por lo que es directamente portable a una cuenta de nube real sin modificar la lógica de infraestructura.

## Posibles mejoras futuras

- Migrar de LocalStack a un proveedor cloud real (AWS/GCP)
- Agregar tests automatizados (pytest) para las transformaciones
- Particionar la escritura en S3 por año/mes para optimizar lecturas futuras
- Agregar validaciones de calidad de datos (Great Expectations o similar)

## Autor

Alejandro Perez Jaramillo — Ingeniero de Sistemas.<br>`En búsqueda de rol como Data Engineer Junior.`