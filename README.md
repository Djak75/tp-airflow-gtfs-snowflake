# Pipeline ETL GTFS & GTFS-RT — Lignes d'Azur

Pipeline de données temps réel pour le suivi des retards du réseau de transport urbain de Nice, construit avec Airflow, Snowflake et Power BI.

## Contexte

La métropole de Nice met à disposition en open data les flux GTFS (statiques et temps réel) du réseau Lignes d'Azur. L'objectif de ce projet est de construire un MVP capable de suivre les retards en quasi temps réel afin d'aider les équipes opérationnelles à prendre des décisions rapides. Le livrable principal est un dashboard Power BI Desktop.

## Sources de données

**GTFS statique** (offre théorique du réseau)
- `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`

**GTFS-RT** (état réel du trafic)
- Trip Updates — horaires observés, base de calcul des retards
- Vehicle Positions — positions GPS en temps réel

## Architecture

Les données transitent par deux schémas Snowflake :

- **BRONZE** — données brutes, ingérées telles quelles
- **SILVER** — données nettoyées, prêtes pour l'analyse

Il n'y a pas de couche GOLD dans ce MVP. Les KPI sont calculés directement depuis SILVER et exposés à Power BI.

## Orchestration

Trois DAGs Airflow gèrent l'ensemble du pipeline :

| DAG | Rôle | Fréquence |
|---|---|---|
| `gtfs_static_daily` | Ingestion du GTFS statique | 1x/jour |
| `gtfs_rt_minutely` | Ingestion des flux temps réel | Toutes les 2 min |
| `gtfs_silver` | Normalisation BRONZE → SILVER | Toutes les 5 min |

## Structure du projet
```
tp-airflow-gtfs-snowflake/
├── dags/
│   ├── gtfs_rt_minutely.py
│   ├── gtfs_static_daily.py
│   └── gtfs_silver.py
├── scripts/
│   ├── check_gtfs_static.py
│   ├── export_trip_updates.py
│   └── export_vehicle_positions.py
├── data/static/
├── exports/rt/
├── logs/
├── warehouse/
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Prérequis

- Docker Desktop
- Compte Snowflake (essai suffisant)
- Power BI Desktop
- URLs des flux GTFS statique et GTFS-RT
- Fichier `.env` configuré (voir `.env.example`)

## Configuration

Créer un fichier `.env` à la racine du projet :
```env
GTFS_STATIC_URL=https://...
TRIP_UPD_URL=https://...
VEH_POS_URL=https://...

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=GTFS_DB
```

## Lancement
```bash
docker compose up -d
```

Accéder à l'interface Airflow en local, puis activer les trois DAGs dans l'ordre :
1. `gtfs_static_daily`
2. `gtfs_rt_minutely`
3. `gtfs_silver`

## Fonctionnement des DAGs

### gtfs_static_daily

Télécharge le fichier ZIP GTFS statique, le décompresse, crée les tables et le stage Snowflake, puis charge les fichiers via `COPY INTO`.

### gtfs_rt_minutely

Attend le chargement statique du jour, télécharge les flux Trip Updates et Vehicle Positions, génère des snapshots CSV horodatés, puis les charge dans les tables BRONZE.

### gtfs_silver

Attend le chargement statique, crée le schéma SILVER et ses tables, puis effectue les chargements incrémentaux à partir du champ `insert_date`.

## Modèle de données Snowflake

**BRONZE**
- `routes_static`, `trips_static`, `stops_static`, `stop_times_static`
- `trip_updates_raw`, `trip_stop_times`, `vehicle_positions_raw`

**SILVER**
- `routes_static_silver`, `trips_static_silver`, `stops_static_silver`, `stop_times_static_silver`
- `trip_updates_silver`, `trip_stop_times_silver`, `vehicle_positions_silver`

## KPI Power BI

Le dashboard couvre les indicateurs suivants :

- Retard moyen dans le temps
- Taux de ponctualité
- Lignes les plus en retard
- Top arrêts problématiques
- Heatmap heures × jours
- Distribution des retards
- Temps de parcours réel vs théorique
- Carte des bus en temps réel
- Carte des arrêts avec état de service
- Évolution du retard par arrêt

## Connexion Power BI

Power BI Desktop se connecte directement à Snowflake. La logique métier (calcul des KPI) reste côté Snowflake via des vues ou requêtes dédiées. Power BI ne sert qu'à la visualisation.

## Limites connues

- Les flux GTFS-RT peuvent être incomplets selon les moments de la journée
- Certains arrêts ou séquences peuvent manquer dans les mises à jour temps réel
- Les performances Power BI se dégradent si les requêtes ne sont pas filtrées sur une fenêtre temporelle raisonnable

## Auteur

Jawad Berrhili — projet réalisé dans le cadre d'un brief Data Engineering (Simplon).

Les données utilisées proviennent de sources ouvertes liées au réseau Lignes d'Azur (data.gouv.fr)



