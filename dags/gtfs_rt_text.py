from datetime import datetime
import os, requests
from google.transit import gtfs_realtime_pb2

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- fonctions appelées par les tasks ---

def trip_updates_to_text():
    # 1) lire l'URL depuis l'env du container
    url = os.environ["TRIP_UPD_URL"]
    # 2) télécharger le flux protobuf
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    # 3) décoder en FeedMessage
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    # 4) écrire un dump texte (lisible humain)
    out_path = "/opt/airflow/exports/trip_updates.txt"
    with open(out_path, "w") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update))
                f.write("\n")

def vehicle_positions_to_text():
    url = os.environ["VEH_POS_URL"]
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    out_path = "/opt/airflow/exports/vehicle_positions.txt"
    with open(out_path, "w") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle))
                f.write("\n")

# --- définition du DAG ---

with DAG(
    dag_id="gtfs_rt_text",
    start_date=datetime(2025, 9, 1),
    schedule=None,          # déclenchement manuel pour la démo
    catchup=False,
    default_args={"retries": 0},
    description="MVP: export texte TripUpdates + VehiclePositions",
) as dag:

    t_trip_updates = PythonOperator(
        task_id="trip_updates_to_text",
        python_callable=trip_updates_to_text,
    )

    t_vehicle_positions = PythonOperator(
        task_id="vehicle_positions_to_text",
        python_callable=vehicle_positions_to_text,
    )

    # ordre : d'abord trip updates, puis positions 
    t_trip_updates >> t_vehicle_positions