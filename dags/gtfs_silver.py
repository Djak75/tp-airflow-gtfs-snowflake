from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

# Connexion Snowflake (définie dans Airflow UI)
SNOWFLAKE_CONN_ID = "snowflake_conn"

# Noms des bases et schémas
DB = "GTFS_DB"
BRONZE = "BRONZE"
SILVER = "SILVER"

# Fuseau horaire Paris pour les timestamps
PARIS_DEFAULT_TSZ = "CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)"

def map_to_midnight(execution_date, **_):
    """Mappe une date d'exécution à minuit le même jour."""
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)

# Base et schéma SILVER
create_silver_schema_sql = [
    f"CREATE DATABASE IF NOT EXISTS {DB};",
    f"CREATE SCHEMA IF NOT EXISTS {DB}.{SILVER};",
]

# Tables SILVER normalisées
create_silver_tables_sql = [

    # TABLES STATICS
    # route_static_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.routes_static_silver (
        route_id STRING,
        agency_id STRING,
        route_long_name STRING,
        route_type INT,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # trip_static_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.trips_static_silver (
        route_id STRING,
        service_id STRING,
        trip_id STRING,
        trip_headsign STRING,
        direction_id INT,
        shape_id STRING,
        wheelchair_accessible NUMBER,
        bike_allowed NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # stop_static_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.stops_static_silver (
        stop_id STRING,
        stop_code STRING,
        stop_name STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        parent_station STRING,
        wheelchair_boarding NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # stop_times_static_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.stop_times_static_silver (
        trip_id STRING,
        stop_id STRING,
        stop_sequence NUMBER,
        pickup_type NUMBER,
        drop_off_type NUMBER,
        intermediate_stop STRING,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # TABLES RT
    # trip_updates_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.trip_updates_silver (
        trip_id STRING,
        route_id STRING,
        direction_id STRING,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # trip_stop_times_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.trip_stop_times_silver (
        trip_id STRING,
        stop_sequence NUMBER,
        stop_id STRING,
        intermediate_stop STRING,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,

    # vehicle_positions_silver
    f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SILVER}.vehicle_positions_silver (
        trip_id STRING,
        route_id STRING,
        vehicle_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        stop_id STRING,
        timestamp_epoch NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT {PARIS_DEFAULT_TSZ}
    );
    """,
]

# Chargement BRONZE -> SILVER (On prend seulement ce qui est arrivé après le dernier insert en SILVER)

# STATICS
load_routes_static_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.routes_static_silver (route_id, agency_id, route_long_name, route_type)
    SELECT
        r.route_id,
        r.agency_id,
        r.route_long_name,
        r.route_type
    FROM {DB}.{BRONZE}.routes_static r
    WHERE r.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.routes_static_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

load_trips_static_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.trips_static_silver (route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible, bike_allowed)
    SELECT
        t.route_id,
        t.service_id,
        t.trip_id,
        t.trip_headsign,
        t.direction_id,
        t.shape_id,
        t.wheelchair_accessible,
        t.bike_allowed
    FROM {DB}.{BRONZE}.trips_static t
    WHERE t.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.trips_static_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

load_stops_static_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.stops_static_silver (stop_id, stop_code, stop_name, stop_lat, stop_lon, parent_station, wheelchair_boarding)
    SELECT
        s.stop_id,
        s.stop_code,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        s.parent_station,
        s.wheelchair_boarding
    FROM {DB}.{BRONZE}.stops_static s
    WHERE s.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.stops_static_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

load_stop_times_static_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.stop_times_static_silver (trip_id, stop_id, stop_sequence, pickup_type, drop_off_type, intermediate_stop)
    SELECT
        st.trip_id,
        st.stop_id,
        st.stop_sequence,
        st.pickup_type,
        st.drop_off_type,
        COALESCE(st.arrival_time, st.departure_time) AS intermediate_stop
    FROM {DB}.{BRONZE}.stop_times_static st
    WHERE st.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.stop_times_static_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

# RT
load_trip_updates_rt_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.trip_updates_silver (trip_id, route_id, direction_id)
    SELECT
        tu.trip_id,
        tu.route_id,
        CASE WHEN tu.direction_id IS NULL THEN 'in experimentation' ELSE TO_VARCHAR(tu.direction_id) END AS direction_id
    FROM {DB}.{BRONZE}.trip_updates_raw tu
    WHERE tu.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.trip_updates_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

load_trip_stop_times_rt_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.trip_stop_times_silver (trip_id, stop_sequence, stop_id, intermediate_stop)
    SELECT
        tst.trip_id,
        tst.stop_sequence,
        tst.stop_id,
        COALESCE(tst.arrival_time, tst.departure_time) AS intermediate_stop
    FROM {DB}.{BRONZE}.trip_stop_times tst
    WHERE tst.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.trip_stop_times_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

load_vehicle_positions_rt_silver_sql = f"""
    INSERT INTO {DB}.{SILVER}.vehicle_positions_silver (trip_id, route_id, vehicle_id, latitude, longitude, bearing, stop_id, timestamp_epoch)
    SELECT
        vp.trip_id,
        vp.route_id,
        vp.vehicle_id,
        vp.latitude,
        vp.longitude,
        vp.bearing,
        vp.stop_id,
        vp.timestamp_epoch
    FROM {DB}.{BRONZE}.vehicle_positions_raw vp
    WHERE vp.insert_date > COALESCE((SELECT MAX(insert_date) FROM {DB}.{SILVER}.vehicle_positions_silver), '1900-01-01' ::TIMESTAMP_NTZ);
"""

# DAG 
with DAG(
    dag_id="gtfs_silver",
    start_date=datetime(2025, 9, 3),
    schedule="*/5 * * * *",  # toutes les 5 minutes
    catchup=True,
    tags=["GTFS", "silver"],
    default_args={"retries": 0},
    description="Normalisation des données BRONZE vers SILVER (statics et RT)",
) as dag:

    # On attend que le DAG daily ait chargé le statique BRONZE
    wait_static_daily = ExternalTaskSensor(
        task_id="wait_static_daily",
        external_dag_id="gtfs_static_daily",
        external_task_id="copy_into_tables",
        execution_date_fn=map_to_midnight,  # on attend le run du jour à minuit
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=60*60,
        mode="reschedule",
    )

    # Création du schéma SILVER
    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_silver_schema_sql,
        do_xcom_push=False,
    )

    # Création des tables SILVER
    create_silver_tables = SQLExecuteQueryOperator(
        task_id="create_silver_tables",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=create_silver_tables_sql,
        do_xcom_push=False,
    )

    # Chargement des tables statics SILVER
    load_routes_static_silver = SQLExecuteQueryOperator(
        task_id="load_routes_static_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_routes_static_silver_sql,
        do_xcom_push=False,
    )

    load_trips_static_silver = SQLExecuteQueryOperator(
        task_id="load_trips_static_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_trips_static_silver_sql,
        do_xcom_push=False,
    )

    load_stops_static_silver = SQLExecuteQueryOperator(
        task_id="load_stops_static_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_stops_static_silver_sql,
        do_xcom_push=False,
    )

    load_stop_times_static_silver = SQLExecuteQueryOperator(
        task_id="load_stop_times_static_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_stop_times_static_silver_sql,
        do_xcom_push=False,
    )

    # Chargement des tables RT SILVER
    load_trip_updates_rt_silver = SQLExecuteQueryOperator(
        task_id="load_trip_updates_rt_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_trip_updates_rt_silver_sql,
        do_xcom_push=False,
    )

    load_trip_stop_times_rt_silver = SQLExecuteQueryOperator(
        task_id="load_trip_stop_times_rt_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_trip_stop_times_rt_silver_sql,
        do_xcom_push=False,
    )

    load_vehicle_positions_rt_silver = SQLExecuteQueryOperator(
        task_id="load_vehicle_positions_rt_silver",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=load_vehicle_positions_rt_silver_sql,
        do_xcom_push=False,
    )

    # Ordre des dépendances
    wait_static_daily >> create_silver_schema >> create_silver_tables >> [
        load_routes_static_silver,
        load_trips_static_silver,
        load_stops_static_silver,
        load_stop_times_static_silver,
        load_trip_updates_rt_silver,
        load_trip_stop_times_rt_silver,
        load_vehicle_positions_rt_silver,
    ]