from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os, requests, zipfile, logging
from google.transit import gtfs_realtime_pb2

# Dossiers montés par docker-compose (volume)
DATA_DIR = "/opt/airflow/data/static"     # données source (GTFS statique)
EXPORTS_DIR = "/opt/airflow/exports"      # sorties

logger = logging.getLogger(__name__)

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)

# --------- Tâches "statiques" découpées : download -> unzip ---------

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
    """Dézippe l'archive GTFS statique dans data/static/ (stops.txt, routes.txt, trips.txt, ...)."""
    ensure_dirs()
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Archive manquante : {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)
    logger.info("Dézippé dans : %s", DATA_DIR)

# --------- Tâches RT  ---------

def export_trip_updates():
    """Télécharge le flux TripUpdates (protobuf), le décode et écrit un texte lisible."""
    ensure_dirs()
    url = os.environ.get("TRIP_UPD_URL")
    if not url:
        raise RuntimeError("TRIP_UPD_URL manquante (variable d'environnement)")
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)
    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update)); f.write("\n")
    logger.info("Écrit : %s (entités=%d)", out_txt, len(feed.entity))

def export_vehicle_positions():
    """Télécharge le flux VehiclePositions (protobuf), le décode et écrit un texte lisible."""
    ensure_dirs()
    url = os.environ.get("VEH_POS_URL")
    if not url:
        raise RuntimeError("VEH_POS_URL manquante (variable d'environnement)")
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)
    out_txt = os.path.join(EXPORTS_DIR, "vehicle_positions.txt")
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle)); f.write("\n")
    logger.info("Écrit : %s (entités=%d)", out_txt, len(feed.entity))

# --------- Test Snowflake ---------

# Connexion Airflow vers Snowflake
SNOWFLAKE_CONN_ID = "snowflake_conn"

with DAG(
    dag_id="simple_gtfs_dag",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",
    catchup=False,
    tags=["GTFS", "demo"],
    default_args={"retries": 0},
    description="MVP: Téléchargement static, unzip, export RT et ping Snowflake"
) as dag:

    t_static_dl = PythonOperator(
        task_id="download_gtfs_static_zip",
        python_callable=download_gtfs_static_zip,
    )

    t_static_unzip = PythonOperator(
        task_id="unzip_gtfs_static_zip",
        python_callable=unzip_gtfs_static_zip,
    )

    t_tu = PythonOperator(
        task_id="export_trip_updates",
        python_callable=export_trip_updates,
    )

    t_vp = PythonOperator(
        task_id="export_vehicle_positions",
        python_callable=export_vehicle_positions,
    )

    # Petit ping SQL pour valider la connexion Snowflake (si la Connection existe)
    snowflake_ping = SQLExecuteQueryOperator(
        task_id="snowflake_ping",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT
              CURRENT_ACCOUNT(),
              CURRENT_REGION(),
              CURRENT_ROLE(),
              CURRENT_WAREHOUSE(),
              CURRENT_DATABASE(),
              CURRENT_SCHEMA();
        """,
        do_xcom_push=False,   # on ne récupère pas le resulat
    )

    # Ordre : download -> unzip -> RT -> test ping
    t_static_dl >> t_static_unzip >> [t_tu, t_vp] >> snowflake_ping
    
