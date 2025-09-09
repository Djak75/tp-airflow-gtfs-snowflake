from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os, requests, zipfile, logging

# Dossiers montés par docker-compose (volume)
DATA_DIR = "/opt/airflow/data/static"     # données source (GTFS statique)
SNOWFLAKE_CONN_ID = "snowflake_conn"

# Config projet
DB      = "GTFS_DB"
SCHEMA  = "BRONZE"                
STAGE   = "stage_gtfs_static"             

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

# Tables statiques
create_static_tables_sql = [
    f"CREATE DATABASE IF NOT EXISTS {DB};",
    f"CREATE SCHEMA   IF NOT EXISTS {DB}.{SCHEMA};",

    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.routes_static (
      route_id STRING,
      agency_id STRING,
      route_short_name STRING,
      route_long_name STRING,
      route_type NUMBER,
      route_url STRING,
      route_color STRING,
      route_text_color STRING
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.trips_static (
      route_id STRING,
      service_id STRING,
      trip_id STRING,
      trip_headsign STRING,
      trip_short_name STRING,
      direction_id NUMBER,
      shape_id STRING,
      wheelchair_accessible NUMBER,
      bike_allowed NUMBER
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.stops_static (
      stop_id STRING,
      stop_code STRING,
      stop_name STRING,
      stop_lat FLOAT,
      stop_lon FLOAT,
      zone_id STRING,
      location_type NUMBER,
      parent_station STRING,
      stop_timezone STRING,
      wheelchair_boarding NUMBER
    );
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.stop_times_static (
      trip_id STRING,
      arrival_time STRING,
      departure_time STRING,
      stop_id STRING,
      stop_sequence NUMBER,
      pickup_type NUMBER,
      drop_off_type NUMBER
    );
    """,
]

# Stage pour les fichiers TXT
create_stage_sql = f"CREATE STAGE IF NOT EXISTS {DB}.{SCHEMA}.{STAGE};"

# PUT des fichiers dézippés vers le stage
put_files_sql = [
    f"PUT file://{DATA_DIR}/routes.txt @{DB}.{SCHEMA}.{STAGE} OVERWRITE=TRUE;",
    f"PUT file://{DATA_DIR}/trips.txt @{DB}.{SCHEMA}.{STAGE} OVERWRITE=TRUE;",
    f"PUT file://{DATA_DIR}/stops.txt @{DB}.{SCHEMA}.{STAGE} OVERWRITE=TRUE;",
    f"PUT file://{DATA_DIR}/stop_times.txt @{DB}.{SCHEMA}.{STAGE} OVERWRITE=TRUE;",
]

# COPY INTO : charger les données du stage vers les tables
copy_into_static_tables = [
    f"""
    COPY INTO {DB}.{SCHEMA}.routes_static
    FROM @{DB}.{SCHEMA}.{STAGE}/routes.txt
    FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE';
    """,
    f"""
    COPY INTO {DB}.{SCHEMA}.trips_static
    FROM @{DB}.{SCHEMA}.{STAGE}/trips.txt
    FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE';
    """,
    f"""
    COPY INTO {DB}.{SCHEMA}.stops_static
    FROM @{DB}.{SCHEMA}.{STAGE}/stops.txt
    FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE';
    """,
    f"""
    COPY INTO {DB}.{SCHEMA}.stop_times_static
    FROM @{DB}.{SCHEMA}.{STAGE}/stop_times.txt
    FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE';
    """,
]

with DAG(
    dag_id="gtfs_static_daily",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",
    catchup=False,
    tags=["GTFS", "static"],
    default_args={"retries": 0},
    description="Téléchargement + unzip + stage + copy Snowflake (quotidien)",
) as dag:
    # Téléchargement du fichier ZIP
    t_static_dl = PythonOperator(
        task_id="download_gtfs_static_zip",
        python_callable=download_gtfs_static_zip,
    )

    # Décompression du fichier ZIP
    t_static_unzip = PythonOperator(
        task_id="unzip_gtfs_static_zip",
        python_callable=unzip_gtfs_static_zip,
    )

    # Vérification de la connexion Snowflake
    snowflake_check = SQLExecuteQueryOperator(
        task_id="snowflake_check",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT 1;",
        do_xcom_push=False,
    )

    # Création des tables statiques
    create_static_tables = SQLExecuteQueryOperator(
        task_id="create_static_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_static_tables_sql,
        do_xcom_push=False,
    )

    # Création du stage
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_stage_sql,
        do_xcom_push=False,
    )

    # PUT des fichiers dézippés vers le stage
    put_files_to_stage = SQLExecuteQueryOperator(
        task_id="put_files_to_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=put_files_sql,
        do_xcom_push=False,
    )

    # COPY INTO : charger les données du stage vers les tables
    copy_into_tables = SQLExecuteQueryOperator(
        task_id="copy_into_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=copy_into_static_tables,
        do_xcom_push=False,
    )

    # Ordre des tâches
    t_static_dl >> t_static_unzip >> snowflake_check >> create_static_tables >> create_stage >> put_files_to_stage >> copy_into_tables