# Pipeline ETL GTFS & GTFS-RT (Nice – Lignes d’Azur) avec Airflow + Snowflake + Power BI

**Objectif**  
MVP temps réel : récupérer les données GTFS (statique) et GTFS-RT (Trip Updates / Vehicle Positions) de Nice, calculer les retards, exporter en Parquet/CSV et construire un Dashboard Power BI.

## Architecture (MVP)
Extract → (Python) télécharge GTFS (zip) + GTFS-RT (protobuf)  
Transform → (Snowflake) jointures / calculs de retard / agrégats (silver/gold)  
Load → Exports Parquet/CSV consommés par Power BI Desktop
