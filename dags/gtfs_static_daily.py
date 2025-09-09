from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os, requests, zipfile, logging

# Dossiers montés par docker-compose (volume)
DATA_DIR = "/opt/airflow/data/static"     # données source (GTFS statique)

logger = logging.getLogger(__name__)

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def download_gtfs_static_zip():
    """Télécharge l'archive GTFS statique dans data/static/gtfs_static.zip."""
    ensure_dirs()
    url = os.environ.get("GTFS_STATIC_URL")
    if not url:
        raise RuntimeError("GTFS_STATIC_URL manquante (variable d'environnement)")
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    r = requests.get(url, timeout=30); r.raise_for_status()
    with open(zip_path, "wb") as f:
        f.write(r.content)
    logger.info("Téléchargé : %s", zip_path)

def unzip_gtfs_static_zip():
    """Dézippe le GTFS statique dans data/static/ (stops.txt, routes.txt, trips.txt, ...)."""
    ensure_dirs()
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Archive manquante : {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)
    logger.info("Dézippé dans : %s", DATA_DIR)

SNOWFLAKE_CONN_ID = "snowflake_conn"

with DAG(
    dag_id="gtfs_static_daily",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",          
    catchup=False,
    tags=["GTFS", "static"],
    default_args={"retries": 0},
    description="Téléchargement + unzip du GTFS statique (quotidien)",
) as dag:

    t_static_dl = PythonOperator(
        task_id="download_gtfs_static_zip",
        python_callable=download_gtfs_static_zip,
    )

    t_static_unzip = PythonOperator(
        task_id="unzip_gtfs_static_zip",
        python_callable=unzip_gtfs_static_zip,
    )

    snowflake_check = SQLExecuteQueryOperator(
    task_id="snowflake_check",
    conn_id=SNOWFLAKE_CONN_ID,
    sql="SELECT 1;",
    do_xcom_push=False,
    )

    create_static_tables = SQLExecuteQueryOperator(
    task_id="create_static_tables",
    conn_id=SNOWFLAKE_CONN_ID,
    sql=[
        "CREATE DATABASE  IF NOT EXISTS GTFS_DB;",
        "CREATE SCHEMA    IF NOT EXISTS GTFS_DB.BRONZE_STATIC;",

        """
        CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.routes_static (
          route_id STRING,
          route_short_name STRING,
          route_long_name STRING
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.trips_static (
          trip_id STRING,
          route_id STRING,
          service_id STRING,
          direction_id NUMBER,
          trip_headsign STRING
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.stops_static (
          stop_id STRING,
          stop_name STRING,
          stop_lat FLOAT,
          stop_lon FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.stop_times_static (
          trip_id STRING,
          arrival_time STRING,
          departure_time STRING,
          stop_id STRING,
          stop_sequence NUMBER
        );
        """
    ],
    do_xcom_push=False,
    )

    # Ordre de dépendances
    t_static_dl >> t_static_unzip >> snowflake_check >> create_static_tables