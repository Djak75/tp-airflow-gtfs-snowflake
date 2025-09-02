import os, pandas as pd

BASE = "data/gtfs_static_2025-09-01"
required = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
missing = [f for f in required if not os.path.exists(os.path.join(BASE, f))]
assert not missing, f"Fichiers manquants: {missing}"

def load(name):
    return pd.read_csv(os.path.join(BASE, name), dtype=str)

stops = load("stops.txt")
routes = load("routes.txt")
trips  = load("trips.txt")
stop_times = load("stop_times.txt")

print(" Lecture OK")
print("stops:", stops.shape)
print("routes:", routes.shape)
print("trips:", trips.shape)
print("stop_times:", stop_times.shape)
