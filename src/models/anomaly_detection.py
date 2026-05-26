import sys
import os
sys.path.insert(0, "/mnt/c/Users/valen/Portafolio/Projects/gcp-pipeline")

from google.cloud import bigquery
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import shap
import pickle
from utils.logger import get_logger

logger = get_logger("anomaly_detection")

PROJECT_ID = "savvy-kit-494301-m6"
TABLE_FEATURES = f"{PROJECT_ID}.taxi_pipeline_marts.mart_anomaly_features"
TABLE_RESULTS = f"{PROJECT_ID}.taxi_pipeline_marts.anomaly_results"


def cargar_features() -> pd.DataFrame:
    """Carga los features desde BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            unique_key,
            trip_date,
            trip_hour,
            day_of_week,
            trip_month,
            fare,
            trip_miles,
            trip_seconds,
            tip_percentage,
            avg_speed_mph,
            fare_per_mile,
            fare_zscore,
            miles_zscore,
            seconds_zscore,
            speed_zscore,
            fare_per_mile_zscore,
            payment_type,
            company,
            pickup_community_area,
            dropoff_community_area
        FROM `{TABLE_FEATURES}`
        WHERE fare_zscore IS NOT NULL
            AND speed_zscore IS NOT NULL
    """

    logger.info("Cargando features desde BigQuery...")
    df = client.query(query).to_dataframe()
    logger.info(f"Features cargados: {len(df)} filas")
    return df


def entrenar_modelo(df: pd.DataFrame):
    """Entrena Isolation Forest para detección de anomalías."""

    features = [
        "fare_zscore",
        "miles_zscore",
        "seconds_zscore",
        "speed_zscore",
        "fare_per_mile_zscore",
        "trip_hour",
        "day_of_week"
    ]

    X = df[features].fillna(0)

    logger.info("Entrenando Isolation Forest...")
    modelo = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
        n_jobs=-1
    )
    modelo.fit(X)

    # Predicciones
    df["anomaly_score"] = modelo.decision_function(X)
    df["is_anomaly"] = modelo.predict(X)
    df["is_anomaly"] = df["is_anomaly"].map({1: 0, -1: 1})

    total_anomalias = df["is_anomaly"].sum()
    pct = round(total_anomalias / len(df) * 100, 2)
    logger.info(f"Anomalías detectadas: {total_anomalias} ({pct}%)")

    return modelo, df, features, X


def analizar_shap(modelo, X, features):
    """Analiza importancia de features con SHAP."""
    logger.info("Calculando SHAP values...")

    # Muestra para SHAP (costoso con 2.5M filas)
    muestra = X.sample(min(10000, len(X)), random_state=42)

    explainer = shap.TreeExplainer(modelo)
    shap_values = explainer.shap_values(muestra)

    importancia = pd.DataFrame({
        "feature": features,
        "importancia": np.abs(shap_values).mean(axis=0)
    }).sort_values("importancia", ascending=False)

    logger.info("\nImportancia de features (SHAP):")
    for _, row in importancia.iterrows():
        logger.info(f"  {row['feature']}: {round(row['importancia'], 4)}")

    return importancia


def guardar_resultados(df: pd.DataFrame):
    """Guarda los resultados en BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    resultados = df[[
        "unique_key", "trip_date", "fare", "trip_miles",
        "avg_speed_mph", "fare_per_mile", "anomaly_score",
        "is_anomaly", "payment_type", "company",
        "pickup_community_area", "dropoff_community_area"
    ]].copy()

    resultados["trip_date"] = pd.to_datetime(resultados["trip_date"])

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        resultados, TABLE_RESULTS, job_config=job_config
    )
    job.result()
    logger.info(f"✅ Resultados guardados en {TABLE_RESULTS}")


if __name__ == "__main__":
    # Instalar shap si no está
    try:
        import shap
    except ImportError:
        os.system("pip install shap")
        import shap

    df = cargar_features()
    modelo, df_results, features, X = entrenar_modelo(df)
    importancia = analizar_shap(modelo, X, features)
    guardar_resultados(df_results)

    logger.info("✅ Detección de anomalías completada")
    logger.info(f"Total viajes analizados: {len(df_results)}")
    logger.info(f"Anomalías encontradas: {df_results['is_anomaly'].sum()}")