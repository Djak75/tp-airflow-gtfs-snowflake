# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from datetime import datetime
# import os, requests, zipfile, logging

# # Dossiers montés par docker-compose (volume)
# DATA_DIR = "/opt/airflow/data/static"     # données source (GTFS statique)
# SNOWFLAKE_CONN_ID = "snowflake_conn"

# DB = "GTFS_DB"
# SCHEMA = "BRONZE_STATIC"
# STAGE = "GTFS_STATIC"
# ROLE = "ETL_ROLE"
# WAREHOUSE = "ETL_WH" 

# logger = logging.getLogger(__name__)

# def ensure_dirs():
#     os.makedirs(DATA_DIR, exist_ok=True)

# def download_gtfs_static_zip():
#     """Télécharge l'archive GTFS statique dans data/static/gtfs_static.zip."""
#     ensure_dirs()
#     url = os.environ.get("GTFS_STATIC_URL")
#     if not url:
#         raise RuntimeError("GTFS_STATIC_URL manquante (variable d'environnement)")
#     zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
#     r = requests.get(url, timeout=30); r.raise_for_status()
#     with open(zip_path, "wb") as f:
#         f.write(r.content)
#     logger.info("Téléchargé : %s", zip_path)

# def unzip_gtfs_static_zip():
#     """Dézippe le GTFS statique dans data/static/ (stops.txt, routes.txt, trips.txt, ...)."""
#     ensure_dirs()
#     zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
#     if not os.path.exists(zip_path):
#         raise FileNotFoundError(f"Archive manquante : {zip_path}")
#     with zipfile.ZipFile(zip_path, "r") as z:
#         z.extractall(DATA_DIR)
#     logger.info("Dézippé dans : %s", DATA_DIR)

# # Tables statiques
# create_static_tables = [
#     "CREATE DATABASE IF NOT EXISTS GTFS_DB;",
#     "CREATE SCHEMA   IF NOT EXISTS GTFS_DB.BRONZE_STATIC;",

#     """
#     CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.routes_static (
#       route_id STRING,
#       agency_id STRING,
#       route_short_name STRING,
#       route_long_name STRING,
#       route_type NUMBER,
#       route_url STRING,
#       route_color STRING,
#       route_text_color STRING
#     );
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.trips_static (
#       route_id STRING,
#       service_id STRING,
#       trip_id STRING,
#       trip_headsign STRING,
#       trip_short_name STRING,
#       direction_id NUMBER,
#       shape_id STRING,
#       wheelchair_accessible NUMBER,
#       bike_allowed NUMBER
#     );
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.stops_static (
#       stop_id STRING,
#       stop_code STRING,
#       stop_name STRING,
#       stop_lat FLOAT,
#       stop_lon FLOAT,
#       zone_id STRING,
#       location_type NUMBER,
#       parent_station STRING,
#       stop_timezone STRING,
#       wheelchair_boarding NUMBER
#     );
#     """,
#     """
#     CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.stop_times_static (
#       trip_id STRING,
#       arrival_time STRING,
#       departure_time STRING,
#       stop_id STRING,
#       stop_sequence NUMBER,
#       pickup_type NUMBER,
#       drop_off_type NUMBER
#     );
#     """,
# ]

# # COPY INTO : charger les données du stage vers les tables 
# copy_into_static_tables = [
#     """
#     COPY INTO GTFS_DB.BRONZE_STATIC.routes_static
#     FROM @GTFS_DB.BRONZE_STATIC.GTFS_STATIC/routes.txt
#     ON_ERROR = 'CONTINUE';
#     """,
#     """
#     COPY INTO GTFS_DB.BRONZE_STATIC.trips_static
#     FROM @GTFS_DB.BRONZE_STATIC.GTFS_STATIC/trips.txt
#     ON_ERROR = 'CONTINUE';
#     """,
#     """
#     COPY INTO GTFS_DB.BRONZE_STATIC.stops_static
#     FROM @GTFS_DB.BRONZE_STATIC.GTFS_STATIC/stops.txt
#     ON_ERROR = 'CONTINUE';
#     """,
#     """
#     COPY INTO GTFS_DB.BRONZE_STATIC.stop_times_static
#     FROM @GTFS_DB.BRONZE_STATIC.GTFS_STATIC/stop_times.txt
#     ON_ERROR = 'CONTINUE';
#     """,
# ]

# # Checks simples après chargement
# rowcount_checks = [
#     """
#     SELECT COUNT(*) FROM GTFS_DB.BRONZE_STATIC.routes_static;
#     """,
#     """
#     SELECT COUNT(*) FROM GTFS_DB.BRONZE_STATIC.trips_static;
#     """,
#     """
#     SELECT COUNT(*) FROM GTFS_DB.BRONZE_STATIC.stops_static;
#     """,
#     """
#     SELECT COUNT(*) FROM GTFS_DB.BRONZE_STATIC.stop_times_static;
#     """,
# ]

# with DAG(
#     dag_id="gtfs_static_daily",
#     start_date=datetime(2025, 9, 3),
#     schedule="@daily",          
#     catchup=False,
#     tags=["GTFS", "static"],
#     default_args={"retries": 0},
#     description="Téléchargement + unzip du GTFS statique (quotidien)",
# ) as dag:
#     # Téléchargement du fichier ZIP
#     t_static_dl = PythonOperator(
#         task_id="download_gtfs_static_zip",
#         python_callable=download_gtfs_static_zip,
#     )

#     # Décompression du fichier ZIP
#     t_static_unzip = PythonOperator(
#         task_id="unzip_gtfs_static_zip",
#         python_callable=unzip_gtfs_static_zip,
#     )

#     # Vérification de la connexion Snowflake
#     snowflake_check = SQLExecuteQueryOperator(
#     task_id="snowflake_check",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql="SELECT 1;",
#     do_xcom_push=False,
#     )

#     # Création des tables statiques
#     create_static_tables = SQLExecuteQueryOperator(
#     task_id="create_static_tables",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql=create_static_tables,
#     do_xcom_push=False,
#     )

#     # Création du format de fichier et du stage
#     create_stage = SQLExecuteQueryOperator(
#     task_id="create_stage",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql=""" CREATE STAGE IF NOT EXISTS GTFS_DB.BRONZE_STATIC.stage_gtfs_static; """,
#     do_xcom_push=False,
#     )

#     # PUT des fichiers dézippés vers le stage
#     put_files_to_stage = SQLExecuteQueryOperator(
#     task_id="put_files_to_stage",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql=[
#         f"PUT file://{DATA_DIR}/routes.txt @GTFS_DB.BRONZE_STATIC.stage_gtfs_static AUTO_COMPRESS=TRUE OVERWRITE=TRUE;",
#         f"PUT file://{DATA_DIR}/trips.txt @GTFS_DB.BRONZE_STATIC.stage_gtfs_static AUTO_COMPRESS=TRUE OVERWRITE=TRUE;",
#         f"PUT file://{DATA_DIR}/stops.txt @GTFS_DB.BRONZE_STATIC.stage_gtfs_static AUTO_COMPRESS=TRUE OVERWRITE=TRUE;",
#         f"PUT file://{DATA_DIR}/stop_times.txt @GTFS_DB.BRONZE_STATIC.stage_gtfs_static AUTO_COMPRESS=TRUE OVERWRITE=TRUE;",
#         ],
#     do_xcom_push=False,
#     )

#     # COPY INTO : charger les données du stage vers les tables
#     copy_into_tables = SQLExecuteQueryOperator(
#     task_id="copy_into_tables",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql=copy_into_static_tables,
#     do_xcom_push=False,
#     )

#     # Check des données
#     rowcount_checks = SQLExecuteQueryOperator(
#     task_id="rowcount_checks",
#     conn_id=SNOWFLAKE_CONN_ID,
#     sql=rowcount_checks,
#     do_xcom_push=False,
#     )

#     # Ordre de dépendances
#     t_static_dl >> t_static_unzip >> snowflake_check >> create_static_tables >> create_stage >> put_files_to_stage >> copy_into_tables >> rowcount_checks