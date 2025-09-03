from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, requests, zipfile
from google.transit import gtfs_realtime_pb2

DATA_DIR = "/opt/airflow/data/static"
EXPORTS_DIR = "/opt/airflow/exports"

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)

def download_gtfs_static():
    ensure_dirs()
    url = os.environ["GTFS_STATIC_URL"]
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    r = requests.get(url, timeout=30); r.raise_for_status()
    with open(zip_path, "wb") as f:
        f.write(r.content)
    # dézipper dans data/static/
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)
    print(f"Static GTFS téléchargé et extrait dans {DATA_DIR}")

def export_trip_updates():
    ensure_dirs()
    url = os.environ["TRIP_UPD_URL"]
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)
    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update)); f.write("\n")
    print(f"Écrit {out_txt}")

def export_vehicle_positions():
    ensure_dirs()
    url = os.environ["VEH_POS_URL"]
    r = requests.get(url, timeout=20); r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage(); feed.ParseFromString(r.content)
    out_txt = os.path.join(EXPORTS_DIR, "vehicle_positions.txt")
    with open(out_txt, "w") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle)); f.write("\n")
    print(f"Écrit {out_txt}")

with DAG(
    dag_id="simple_gtfs_dag",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",   # déclenchement quotidien 
    catchup=False,
    tags=["GTFS", "demo"],
) as dag:

    t_static = PythonOperator(
        task_id="download_gtfs_static",
        python_callable=download_gtfs_static,
    )

    t_tu = PythonOperator(
        task_id="export_trip_updates",
        python_callable=export_trip_updates,
    )

    t_vp = PythonOperator(
        task_id="export_vehicle_positions",
        python_callable=export_vehicle_positions,
    )

    # ordre : on télécharge le statique une fois/jour, puis on fait les RT
    t_static >> [t_tu, t_vp]