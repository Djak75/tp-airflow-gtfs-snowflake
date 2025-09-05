from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import os, requests, logging
from google.transit import gtfs_realtime_pb2

EXPORTS_DIR = "/opt/airflow/exports"
SNOWFLAKE_CONN_ID = "snowflake_conn"

logger = logging.getLogger(__name__)

def ensure_dirs():
    os.makedirs(EXPORTS_DIR, exist_ok=True)

# --- 1) Export snapshots RT en local ---

def export_trip_updates_snapshot():
    """Télécharge TripUpdates (protobuf) et écrit un texte lisible."""
    ensure_dirs()
    url = os.environ.get("TRIP_UPD_URL")
    if not url:
        raise RuntimeError("TRIP_UPD_URL manquante (variable d'environnement)")
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)

    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")  # simple pour la démo
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update)); f.write("\n")
    logger.info("Snapshot TripUpdates écrit : %s (entités=%d)", out_txt, len(feed.entity))

def export_vehicle_positions_snapshot():
    """Télécharge VehiclePositions (protobuf) et écrit un texte lisible."""
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
    logger.info("Snapshot VehiclePositions écrit : %s (entités=%d)", out_txt, len(feed.entity))

# --- 2) Snowflake : check + tables bronze RT ---

create_bronze_rt_tables_sql = [

    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trip_updates_raw (
        event_ts TIMESTAMP_NTZ,
        trip_id STRING,
        route_id STRING,
        stop_id STRING,
        stop_sequence NUMBER,
        arrival_delay_sec NUMBER,
        departure_delay_sec NUMBER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.vehicle_positions_raw (
        event_ts TIMESTAMP_NTZ,
        vehicle_id STRING,
        trip_id STRING,
        route_id STRING,
        lat FLOAT,
        lon FLOAT,
        bearing FLOAT,
        speed FLOAT,
        stop_id STRING
    );
    """
]

with DAG(
    dag_id="gtfs_rt_minutely",
    start_date=datetime(2025, 9, 3),
    schedule="*/2 * * * *",    # toutes les 2 minutes
    catchup=False,
    tags=["GTFS", "RT"],
    default_args={"retries": 0},
    description="Export snapshots TripUpdates/VehiclePositions + check Snowflake (minutely)",
) as dag:

    # 1) Snapshots locaux (dépendent pas de Snowflake)
    t_tu_snapshot = PythonOperator(
        task_id="export_trip_updates_snapshot",
        python_callable=export_trip_updates_snapshot,
    )
    t_vp_snapshot = PythonOperator(
        task_id="export_vehicle_positions_snapshot",
        python_callable=export_vehicle_positions_snapshot,
    )

    # 2) Test connexion Snowflake
    snowflake_connection_check = SQLExecuteQueryOperator(
        task_id="snowflake_connection_check",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT 1;",
        do_xcom_push=False,
    )

    # 3) Créer les tables bronze RT
    create_bronze_rt_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_rt_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_bronze_rt_tables_sql,
        do_xcom_push=False,
    )

    # Ordre
    [t_tu_snapshot, t_vp_snapshot] >> snowflake_connection_check >> create_bronze_rt_tables