from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "dylan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def extraer_datos(**context):
    """Extrae datos de Chicago Taxi desde BigQuery Public Datasets."""
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project="savvy-kit-494301-m6")

    # Fecha de ejecución del DAG para ingesta incremental
    execution_date = context["data_interval_start"]
    start_date = execution_date.strftime("%Y-%m-%d")
    end_date = (execution_date + timedelta(days=31)).strftime("%Y-%m-%d")

    logger.info(f"Extrayendo datos del {start_date} al {end_date}")

    query = f"""
        SELECT
            unique_key,
            taxi_id,
            trip_start_timestamp,
            trip_end_timestamp,
            trip_seconds,
            trip_miles,
            pickup_community_area,
            dropoff_community_area,
            fare,
            tips,
            tolls,
            extras,
            trip_total,
            payment_type,
            company
        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
        WHERE DATE(trip_start_timestamp) >= '{start_date}'
            AND DATE(trip_start_timestamp) < '{end_date}'
            AND fare > 0
            AND trip_miles > 0
            AND trip_seconds > 0
    """

    logger.info("Ejecutando query en BigQuery...")
    df = client.query(query).to_dataframe()
    logger.info(f"Datos extraídos: {len(df)} filas")

    df.to_csv("/tmp/taxi_raw.csv", index=False)
    logger.info("Datos guardados en /tmp/taxi_raw.csv")
    return len(df)


def validar_datos(**context):
    """Valida calidad de los datos extraídos."""
    import pandas as pd

    df = pd.read_csv("/tmp/taxi_raw.csv")
    logger.info(f"Validando {len(df)} filas...")

    assert len(df) > 0, "Dataset vacío"
    assert (df["fare"] > 0).all(), "Hay tarifas negativas o cero"
    assert (df["trip_miles"] > 0).all(), "Hay distancias negativas o cero"
    assert df["trip_start_timestamp"].notnull().all(), "Hay timestamps nulos"

    # Duplicados — warning en lugar de error
    duplicados = df["unique_key"].duplicated().sum()
    if duplicados > 0:
        logger.warning(f"⚠ {duplicados} duplicados en unique_key — se deduplicará")
        df = df.drop_duplicates(subset=["unique_key"])
        logger.info(f"Filas después de deduplicar: {len(df)}")

    df.to_csv("/tmp/taxi_raw.csv", index=False)
    logger.info(f"Nulos por columna:\n{df.isnull().sum()}")
    logger.info("✅ Validaciones completadas")
    return True


def cargar_bigquery(**context):
    """Carga los datos procesados en BigQuery usando MERGE para evitar duplicados."""
    import pandas as pd
    from google.cloud import bigquery

    df = pd.read_csv("/tmp/taxi_raw.csv")
    df["trip_start_timestamp"] = pd.to_datetime(df["trip_start_timestamp"])
    df["trip_end_timestamp"] = pd.to_datetime(df["trip_end_timestamp"])

    client = bigquery.Client(project="savvy-kit-494301-m6")

    dataset_id = "savvy-kit-494301-m6.taxi_pipeline"
    table_id = f"{dataset_id}.raw_taxi_trips"
    staging_id = f"{dataset_id}.staging_taxi_trips"

    # Crear dataset si no existe
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info("Dataset creado")

    schema = [
        bigquery.SchemaField("unique_key", "STRING"),
        bigquery.SchemaField("taxi_id", "STRING"),
        bigquery.SchemaField("trip_start_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("trip_end_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("trip_seconds", "INTEGER"),
        bigquery.SchemaField("trip_miles", "FLOAT"),
        bigquery.SchemaField("pickup_community_area", "FLOAT"),
        bigquery.SchemaField("dropoff_community_area", "FLOAT"),
        bigquery.SchemaField("fare", "FLOAT"),
        bigquery.SchemaField("tips", "FLOAT"),
        bigquery.SchemaField("tolls", "FLOAT"),
        bigquery.SchemaField("extras", "FLOAT"),
        bigquery.SchemaField("trip_total", "FLOAT"),
        bigquery.SchemaField("payment_type", "STRING"),
        bigquery.SchemaField("company", "STRING"),
    ]

    # Paso 1 — Cargar a tabla staging temporal
    job_config_staging = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(df, staging_id, job_config=job_config_staging)
    job.result()
    logger.info(f"Staging cargado: {len(df)} filas")

    # Paso 2 — Crear tabla final particionada si no existe
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="trip_start_timestamp",
        )
        client.create_table(table)
        logger.info("Tabla final creada con particionamiento")

    # Paso 3 — MERGE para insertar solo registros nuevos
    merge_query = f"""
        MERGE `{table_id}` T
        USING `{staging_id}` S
        ON T.unique_key = S.unique_key
        WHEN NOT MATCHED THEN
            INSERT (
                unique_key, taxi_id, trip_start_timestamp, trip_end_timestamp,
                trip_seconds, trip_miles, pickup_community_area, dropoff_community_area,
                fare, tips, tolls, extras, trip_total, payment_type, company
            )
            VALUES (
                S.unique_key, S.taxi_id, S.trip_start_timestamp, S.trip_end_timestamp,
                S.trip_seconds, S.trip_miles, S.pickup_community_area, S.dropoff_community_area,
                S.fare, S.tips, S.tolls, S.extras, S.trip_total, S.payment_type, S.company
            )
    """

    merge_job = client.query(merge_query)
    merge_job.result()

    # Limpiar staging
    client.delete_table(staging_id)
    logger.info("Staging eliminado")

    tabla = client.get_table(table_id)
    logger.info(f"✅ Tabla final: {tabla.num_rows} filas")
    logger.info(f"✅ MERGE completado sin duplicados")


with DAG(
    dag_id="taxi_pipeline",
    default_args=default_args,
    description="Chicago Taxi Pipeline — Incremental load to BigQuery partitioned table",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 6, 1),
    catchup=True,
    tags=["taxi", "bigquery", "gcp"],
) as dag:

    tarea_extraer = PythonOperator(
        task_id="extraer_datos",
        python_callable=extraer_datos,
    )

    tarea_validar = PythonOperator(
        task_id="validar_datos",
        python_callable=validar_datos,
    )

    tarea_cargar = PythonOperator(
        task_id="cargar_bigquery",
        python_callable=cargar_bigquery,
    )

    tarea_extraer >> tarea_validar >> tarea_cargar