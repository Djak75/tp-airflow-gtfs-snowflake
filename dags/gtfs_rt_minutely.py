from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import os, requests, logging, pendulum
import pandas as pd
from google.transit import gtfs_realtime_pb2

# Dossiers montés par docker-compose (volume)
EXPORTS_DIR = "/opt/airflow/exports"
SNOWFLAKE_CONN_ID = "snowflake_conn"

# Config projet
DB      = "GTFS_DB"
SCHEMA  = "BRONZE"
RT_STAGE   = "stage_gtfs_rt"

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

# Export snapshots RT en local (texte lisible)
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

# RT -> pandas DataFrame -> CSV horodaté (prêt pour Snowflake) 
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

        # header trip
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

# Snowflake : check + tables BRONZE RT 
create_bronze_rt_tables_sql = [
    f"CREATE DATABASE IF NOT EXISTS {DB};",
    f"CREATE SCHEMA   IF NOT EXISTS {DB}.{SCHEMA};",

    # TripUpdates
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.trip_updates_raw (
        trip_id STRING,
        route_id STRING,
        direction_id NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,

    # TripUpdates - stop_time_update (une ligne par arrêt)
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.trip_stop_times (
        trip_id STRING,
        stop_sequence NUMBER,
        stop_id STRING,
        arrival_time NUMBER,
        departure_time NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,

    # VehiclePositions - une ligne par message
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.vehicle_positions_raw (
        trip_id STRING,
        route_id STRING,
        vehicle_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        stop_id STRING,
        timestamp_epoch NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """
]

# Stage pour les fichiers CSV
create_rt_stage_sql = f"CREATE STAGE IF NOT EXISTS {DB}.{SCHEMA}.{RT_STAGE};"

# PUT des CSV RT vers le stage
put_rt_to_stage_sql = [
    f"PUT file://{EXPORTS_DIR}/rt/trip_updates_trips_*.csv @{DB}.{SCHEMA}.{RT_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;",
    f"PUT file://{EXPORTS_DIR}/rt/trip_updates_stop_times_*.csv @{DB}.{SCHEMA}.{RT_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;",
    f"PUT file://{EXPORTS_DIR}/rt/vehicle_positions_*.csv @{DB}.{SCHEMA}.{RT_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;",
]

# COPY INTO : charger les données du stage vers les tables
copy_rt_sql = [
    f"""
    COPY INTO {DB}.{SCHEMA}.trip_updates_raw (trip_id, route_id, direction_id)
    FROM @{DB}.{SCHEMA}.{RT_STAGE}
    PATTERN='.*trip_updates_trips_.*\\.csv'
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE'
    PURGE=TRUE;
    """,
    f"""
    COPY INTO {DB}.{SCHEMA}.trip_stop_times (trip_id, stop_sequence, stop_id, arrival_time, departure_time)
    FROM @{DB}.{SCHEMA}.{RT_STAGE}
    PATTERN='.*trip_updates_stop_times_.*\\.csv'
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE'
    PURGE=TRUE;
    """,
    f"""
    COPY INTO {DB}.{SCHEMA}.vehicle_positions_raw (trip_id, route_id, vehicle_id, latitude, longitude, bearing, stop_id, timestamp_epoch)
    FROM @{DB}.{SCHEMA}.{RT_STAGE}
    PATTERN='.*vehicle_positions_.*\\.csv'
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=('','NULL','null'))
    ON_ERROR='CONTINUE'
    PURGE=TRUE;
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

    # Créer le stage RT
    create_rt_stage = SQLExecuteQueryOperator(
        task_id="create_rt_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_rt_stage_sql,
        do_xcom_push=False,
    )

    # PUT des CSV RT vers le stage
    put_rt_to_stage = SQLExecuteQueryOperator(
        task_id="put_rt_to_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=put_rt_to_stage_sql,
        do_xcom_push=False,
    )

    # Lister les fichiers dans le stage pour debug
    list_rt_stage = SQLExecuteQueryOperator(
        task_id="list_rt_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"LIST @{DB}.{SCHEMA}.{RT_STAGE};",
        do_xcom_push=False,
    )

    # COPY INTO : charger les données du stage vers les tables
    copy_rt_into_tables = SQLExecuteQueryOperator(
        task_id="copy_rt_into_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=copy_rt_sql,
        do_xcom_push=False,
    )

    # Ordre d'exécution
    wait_for_static_today >> t_tu_snapshot >> t_vp_snapshot >> [t_tu_csv, t_vp_csv] >> snowflake_connection_check >> create_bronze_rt_tables >> create_rt_stage >> put_rt_to_stage >> list_rt_stage >> copy_rt_into_tables