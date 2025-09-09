from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import os, requests, logging, pendulum
import pandas as pd
from google.transit import gtfs_realtime_pb2

EXPORTS_DIR = "/opt/airflow/exports"
SNOWFLAKE_CONN_ID = "snowflake_conn"

logger = logging.getLogger(__name__)

def ensure_dirs():
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    os.makedirs(os.path.join(EXPORTS_DIR, "rt"), exist_ok=True)

def map_to_midnight(execution_date, **_):
    # On veut que le run du jour attende le DAG statique
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)

def current_minute_stamp():
    # heure Paris
    return pendulum.now("Europe/Paris").strftime("%Y%m%d_%H%M")

# --- 1) Export snapshots RT en local (texte lisible) ---

def export_trip_updates_snapshot():
    """Télécharge TripUpdates (protobuf) et écrit un texte lisible."""
    ensure_dirs()
    url = os.environ.get("TRIP_UPD_URL")
    if not url:
        raise RuntimeError("TRIP_UPD_URL manquante (variable d'environnement)")
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)

    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")  
    written = 0
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update)); f.write("\n")
                written += 1
    logger.info("Snapshot TripUpdates écrit : %s (entités=%d)", out_txt, written)

def export_vehicle_positions_snapshot():
    """Télécharge VehiclePositions (protobuf) et écrit un texte lisible."""
    ensure_dirs()
    url = os.environ.get("VEH_POS_URL")
    if not url:
        raise RuntimeError("VEH_POS_URL manquante (variable d'environnement)")
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)

    out_txt = os.path.join(EXPORTS_DIR, "vehicle_positions.txt")
    written = 0
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle)); f.write("\n")
                written += 1
    logger.info("Snapshot VehiclePositions écrit : %s (entités=%d)", out_txt, written)

# --- 1/ RT -> pandas DataFrame -> CSV horodaté (prêt pour Snowflake) ---

def export_trip_updates_csv():
    """Lit TripUpdates (protobuf) -> 2 DataFrames -> 2 CSV :
       - exports/rt/trip_updates_trips_YYYYMMDD_HHMM.csv   (pour BRONZE.trip_updates_raw)
       - exports/rt/trip_updates_stop_times_YYYYMMDD_HHMM.csv (pour BRONZE.trip_stop_times)
    """
    ensure_dirs()
    url = os.environ["TRIP_UPD_URL"]
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)

    # 1) "header trip" : une ligne par trip
    rows_trips = []  # (trip_id, route_id, direction_id)
    seen_trips = set()

    # 2) stop_time_update : une ligne par arrêt
    rows_stop_times = []  # (trip_id, stop_sequence, stop_id, arrival_time, departure_time)

    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue

        tu = ent.trip_update
        trip_id = tu.trip.trip_id or None
        route_id = tu.trip.route_id or None
        direction_id = tu.trip.direction_id if tu.trip.HasField("direction_id") else None

        # header trip (déduplication par trip_id pendant ce run)
        if trip_id and trip_id not in seen_trips:
            rows_trips.append((trip_id, route_id, direction_id))
            seen_trips.add(trip_id)

        # stop_time_update
        for stu in tu.stop_time_update:
            stop_sequence = getattr(stu, "stop_sequence", None)
            stop_id = getattr(stu, "stop_id", None)
            arrival_time = stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None
            departure_time = stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None

            rows_stop_times.append((trip_id, stop_sequence, stop_id, arrival_time, departure_time))

    stamp = current_minute_stamp()
    out_trips = os.path.join(EXPORTS_DIR, "rt", f"trip_updates_trips_{stamp}.csv")
    out_stops = os.path.join(EXPORTS_DIR, "rt", f"trip_updates_stop_times_{stamp}.csv")

    # DataFrames (colonnes alignées avec tes tables BRONZE)
    df_trips = pd.DataFrame(rows_trips, columns=["trip_id","route_id","direction_id"])
    df_stops = pd.DataFrame(rows_stop_times, columns=["trip_id","stop_sequence","stop_id","arrival_time","departure_time"])

    # CAST Int64 pour avoir un nombre entier
    df_trips["direction_id"]  = pd.to_numeric(df_trips["direction_id"], errors="coerce").astype("Int64")
    df_stops["stop_sequence"] = pd.to_numeric(df_stops["stop_sequence"], errors="coerce").astype("Int64")
    df_stops["arrival_time"]  = pd.to_numeric(df_stops["arrival_time"],  errors="coerce").astype("Int64")
    df_stops["departure_time"]= pd.to_numeric(df_stops["departure_time"], errors="coerce").astype("Int64")

    # Écriture CSV
    df_trips.to_csv(out_trips, index=False)
    df_stops.to_csv(out_stops, index=False)

    logger.info("CSV TripUpdates écrits : %s (trips=%d) ; %s (stop_times=%d)",
                out_trips, len(df_trips), out_stops, len(df_stops))

def export_vehicle_positions_csv():
    """Lit VehiclePositions (protobuf) -> DataFrame -> CSV 'exports/rt/vehicle_positions_YYYYMMDD_HHMM.csv'."""
    ensure_dirs()
    url = os.environ["VEH_POS_URL"]
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)

    rows = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        vp = ent.vehicle
        trip_id = vp.trip.trip_id if vp.HasField("trip") else None
        route_id = vp.trip.route_id if vp.HasField("trip") else None
        vehicle_id = vp.vehicle.id if vp.HasField("vehicle") else None
        lat = vp.position.latitude  if vp.HasField("position") and vp.position.HasField("latitude")  else None
        lon = vp.position.longitude if vp.HasField("position") and vp.position.HasField("longitude") else None
        bearing = vp.position.bearing if vp.HasField("position") and vp.position.HasField("bearing") else None
        stop_id = vp.stop_id if vp.HasField("stop_id") else None
        ts_epoch = vp.timestamp if vp.HasField("timestamp") else None

        rows.append({
            "trip_id": trip_id,
            "route_id": route_id,
            "vehicle_id": vehicle_id,
            "latitude": lat,
            "longitude": lon,
            "bearing": bearing,
            "stop_id": stop_id,
            "timestamp_epoch": ts_epoch
        })

    stamp = current_minute_stamp()
    out_csv = os.path.join(EXPORTS_DIR, "rt", f"vehicle_positions_{stamp}.csv")
    df = pd.DataFrame(rows, columns=[
        "trip_id","route_id","vehicle_id",
        "latitude","longitude","bearing","stop_id","timestamp_epoch"
    ])

    # CAST Int64 : nombre entier
    df["bearing"] = pd.to_numeric(df["bearing"], errors="coerce").round().astype("Int64")
    df["timestamp_epoch"] = pd.to_numeric(df["timestamp_epoch"], errors="coerce").astype("Int64")

    df.to_csv(out_csv, index=False)
    logger.info("CSV VehiclePositions écrit : %s (lignes=%d)", out_csv, len(df))

# --- 2/ Snowflake : check + tables BRONZE RT ---

create_bronze_rt_tables_sql = [
    "CREATE DATABASE IF NOT EXISTS GTFS_DB;",
    "CREATE SCHEMA   IF NOT EXISTS GTFS_DB.BRONZE;",

    # TripUpdates
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trip_updates_raw (
        trip_id STRING,
        route_id STRING,
        direction_id NUMBER
    );
    """,

    # TripUpdates - stop_time_update (une ligne par arrêt)
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trip_stop_times (
        trip_id STRING,
        stop_sequence NUMBER,
        stop_id STRING,
        arrival_time NUMBER,
        departure_time NUMBER
    );
    """,

    # VehiclePositions - une ligne par message
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.vehicle_positions_raw (
        trip_id STRING,
        route_id STRING,
        vehicle_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        stop_id STRING,
        timestamp_epoch NUMBER
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
    description="Attente statique du jour + export snapshots RT + check Snowflake + CSV RT (minutely)",
) as dag:

    # On attend que le statique du jour ait fini l'unzip
    wait_for_static_today = ExternalTaskSensor(
        task_id="wait_for_static_today",
        external_dag_id="gtfs_static_daily",
        external_task_id="unzip_gtfs_static_zip",
        execution_date_fn=map_to_midnight,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=60*60,
        mode="reschedule",
    )

    # Snapshots locaux (texte lisible)
    t_tu_snapshot = PythonOperator(
        task_id="export_trip_updates_snapshot",
        python_callable=export_trip_updates_snapshot,
    )
    t_vp_snapshot = PythonOperator(
        task_id="export_vehicle_positions_snapshot",
        python_callable=export_vehicle_positions_snapshot,
    )

    # CSV RT (pour Snowflake)
    t_tu_csv = PythonOperator(
        task_id="export_trip_updates_csv",
        python_callable=export_trip_updates_csv,
    )
    t_vp_csv = PythonOperator(
        task_id="export_vehicle_positions_csv",
        python_callable=export_vehicle_positions_csv,
    )

    # Test connexion Snowflake
    snowflake_connection_check = SQLExecuteQueryOperator(
        task_id="snowflake_connection_check",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT 1;",
        do_xcom_push=False,
    )

    # Créer les tables bronze RT
    create_bronze_rt_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_rt_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_bronze_rt_tables_sql,
        do_xcom_push=False,
    )

     # Ordre : d'abord on s'assure que le statique du jour est prêt
    wait_for_static_today >> t_tu_snapshot >> t_vp_snapshot >> [t_tu_csv, t_vp_csv] >> snowflake_connection_check >> create_bronze_rt_tables