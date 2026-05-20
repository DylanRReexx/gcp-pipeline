from airflow import DAG
from airflow.operators.python import PythonOperator
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

    client = bigquery.Client(project="savvy-kit-494301-m6")

    query = """
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
        WHERE trip_start_timestamp >= '2023-01-01'
            AND trip_start_timestamp < '2023-02-01'
            AND fare > 0
            AND trip_miles > 0
            AND trip_seconds > 0
        LIMIT 100000
    """

    logger.info("Ejecutando query en BigQuery...")
    df = client.query(query).to_dataframe()
    logger.info(f"Datos extraídos: {len(df)} filas")

    # Guardar temporalmente
    df.to_csv("/tmp/taxi_raw.csv", index=False)
    logger.info("Datos guardados en /tmp/taxi_raw.csv")
    return len(df)


def validar_datos(**context):
    """Valida calidad de los datos extraídos."""
    import pandas as pd

    df = pd.read_csv("/tmp/taxi_raw.csv")
    logger.info(f"Validando {len(df)} filas...")

    # Validaciones
    assert len(df) > 0, "Dataset vacío"
    assert df["unique_key"].nunique() == len(df), "Hay duplicados en unique_key"
    assert (df["fare"] > 0).all(), "Hay tarifas negativas o cero"
    assert (df["trip_miles"] > 0).all(), "Hay distancias negativas o cero"
    assert df["trip_start_timestamp"].notnull().all(), "Hay timestamps nulos"

    logger.info(f"Nulos por columna:\n{df.isnull().sum()}")
    logger.info("✅ Todas las validaciones pasaron")
    return True


def cargar_bigquery(**context):
    """Carga los datos procesados en BigQuery."""
    import pandas as pd
    from google.cloud import bigquery

    df = pd.read_csv("/tmp/taxi_raw.csv")
    client = bigquery.Client(project="savvy-kit-494301-m6")

    # Crear dataset si no existe
    dataset_id = "savvy-kit-494301-m6.taxi_pipeline"
    try:
        client.get_dataset(dataset_id)
        logger.info("Dataset ya existe")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info("Dataset creado")

    # Cargar tabla
    table_id = f"{dataset_id}.raw_taxi_trips"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    tabla = client.get_table(table_id)
    logger.info(f"✅ Cargadas {tabla.num_rows} filas en {table_id}")


with DAG(
    dag_id="taxi_pipeline",
    default_args=default_args,
    description="Chicago Taxi Pipeline — Extract, Validate, Load to BigQuery",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
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