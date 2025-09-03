# On veut télécharger les 2 flux GTFS-RT (Trip Updates + Vehicle Positions)
#  et écrire 2 fichiers texte dans ./exports/

import os
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2  
import requests

# Chargement des URL depuis .env
load_dotenv()
TRIP_UPD_URL = os.getenv("TRIP_UPD_URL")
VEH_POS_URL  = os.getenv("VEH_POS_URL")
if not TRIP_UPD_URL or not VEH_POS_URL:
    raise ValueError("Renseigne TRIP_UPD_URL et VEH_POS_URL dans .env")

# Fonction qui télécharge et décode protobuf -> FeedMessage
def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    r = requests.get(url, timeout=30)
    r.raise_for_status()  # stoppe si 4xx/5xx
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)  # décode le binaire protobuf
    return feed

# Création du dossier de sortie
os.makedirs("exports", exist_ok=True)

# Trip Updates -> texte
tu_feed = fetch_feed(TRIP_UPD_URL)
with open(os.path.join("exports", "trip_updates.txt"), "w") as f:
    for ent in tu_feed.entity:
        if ent.HasField("trip_update"):
            # on écrit la représentation texte du message
            f.write(str(ent.trip_update))
            f.write("\n")  # séparation entre entrées
print(" export trip_updates.txt prêt dans exports/")

# Vehicle Positions -> texte
vp_feed = fetch_feed(VEH_POS_URL)
with open(os.path.join("exports", "vehicle_positions.txt"), "w") as f:
    for ent in vp_feed.entity:
        if ent.HasField("vehicle"):
            f.write(str(ent.vehicle))
            f.write("\n")
print(" export vehicle_positions.txt prêt dans exports/")

