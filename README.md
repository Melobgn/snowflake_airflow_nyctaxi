# NYC Taxi Data Pipeline — Snowflake + Airflow

## Contexte

Hudson Cab Partners souhaite mieux comprendre la demande de taxis jaunes à New York afin d’identifier :

- les zones où la demande est la plus forte ;
- les créneaux horaires les plus actifs ;
- le revenu moyen généré par trajet ;
- les différences observées selon les modes de paiement.

Le projet consiste à construire un pipeline de données complet à partir des données NYC TLC Yellow Taxi pour les mois de janvier, février et mars 2025.

Le pipeline couvre :

- l’ingestion des fichiers Parquet ;
- le stockage dans Snowflake ;
- la transformation des données ;
- les contrôles de qualité ;
- l’orchestration avec Apache Airflow ;
- la production de marts analytiques ;
- la sécurisation des accès ;
- la vérification de l’idempotence ;
- le suivi de la consommation Snowflake.

---

# Architecture

Le pipeline suit une architecture en plusieurs couches :

```text
NYC TLC
   |
   | Yellow Taxi Parquet files
   v
Python ingestion
   |
   v
Snowflake internal stage
   |
   v
RAW
   |
   v
STAGING
   |
   v
INTERMEDIATE
   |
   v
MARTS
   |
   v
Business analysis
```

Apache Airflow orchestre l’ensemble du processus mensuellement.

---

# Stack technique

Le projet utilise :

- Python 3.12
- Snowflake
- Apache Airflow 3
- Astronomer Astro CLI
- Docker
- Snowflake Python Connector
- Snowflake Airflow Provider
- SQL
- Git / GitHub

---

# Structure du projet

```text
.
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_monthly.py
│   ├── include/
│   │   └── sql/
│   │       ├── 00_tables.sql
│   │       ├── staging/
│   │       ├── intermediate/
│   │       ├── marts/
│   │       └── controles/
│   ├── plugins/
│   ├── tests/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .env.example
│
├── ingestion/
│   ├── load_month.py
│   ├── load_zones.py
│   └── requirements.txt
│
├── snowflake/
│   ├── 01_infrastructure.sql
│   └── 02_raw.sql
│
├── docs/
│   ├── fiche_trajets.md
│   ├── REPONSE.md
│   └── screenshots/
│
├── data/
│
├── .gitignore
└── README.md
```

---

# Données utilisées

## Yellow Taxi Trip Records

Source :

NYC Taxi & Limousine Commission.

Les fichiers utilisés sont :

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
```

Le pipeline contient au total :

```text
11 198 026 trajets bruts
```

## Taxi Zone Lookup

Le fichier de référence des zones NYC TLC contient :

```text
265 zones
```

Il permet d’associer les identifiants `PULocationID` et `DOLocationID` à une zone et à un borough.

---

# Architecture Snowflake

## Database

```text
NYC_TAXI
```

## Schemas

Quatre couches principales sont utilisées :

```text
RAW
STAGING
INTERMEDIATE
MARTS
```

### RAW

Contient les données telles qu’elles sont chargées depuis les fichiers source.

Principales tables :

```text
RAW.YELLOW_TRIPDATA
RAW.TAXI_ZONE_LOOKUP
```

### STAGING

Cette couche prépare et normalise les données brutes.

Principaux objets :

```text
STG_TLC__YELLOW_TRIPS
STG_TLC__TAXI_ZONES
PAYMENT_TYPE_CODES
RATE_CODE_CODES
VENDOR_CODES
```

### INTERMEDIATE

Cette couche applique les règles de qualité, les rejets, la déduplication et les enrichissements.

Principales tables :

```text
INT_TRIPS__FLAGGED
INT_TRIPS__ENRICHED
```

### MARTS

Cette couche contient les tables analytiques finales.

Principales tables :

```text
FCT_TRIPS
DIM_DATE
DIM_ZONE
DIM_PAYMENT_TYPE
DIM_RATE_CODE
DIM_VENDOR
MART_DAILY_REVENUE
MART_ZONE_HOURLY_DEMAND
MART_DATA_QUALITY
```

---

# Warehouse Snowflake

Le warehouse utilisé est :

```text
NYC_TAXI_WH
```

Configuration :

```text
SIZE = XSMALL
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
```

Cette configuration permet de limiter la consommation de crédits en suspendant automatiquement le warehouse après 60 secondes d’inactivité.

---

# Sécurité

Le pipeline ne se connecte pas avec `ACCOUNTADMIN`.

Un rôle dédié a été créé :

```text
TRANSFORMER
```

Il dispose uniquement des privilèges nécessaires pour :

- utiliser le warehouse ;
- accéder à la base `NYC_TAXI` ;
- créer et manipuler les objets nécessaires ;
- charger les fichiers ;
- exécuter les transformations.

Un utilisateur technique est utilisé par Airflow :

```text
AIRFLOW_SVC
```

L’authentification repose sur une paire de clés RSA.

La clé privée n’est jamais versionnée dans Git.

Les fichiers sensibles sont exclus via `.gitignore`, notamment :

```text
.env
*.p8
*.pem
rsa_key*
```

---

# Installation Snowflake

Les objets principaux sont créés à partir des scripts présents dans :

```text
snowflake/
```

Exécuter d’abord :

```text
snowflake/01_infrastructure.sql
```

Puis :

```text
snowflake/02_raw.sql
```

Ces scripts créent notamment :

- le warehouse ;
- la base ;
- les schemas ;
- le rôle ;
- le user de service ;
- les file formats ;
- le stage ;
- les tables RAW.

---

# Format Parquet

Le format Parquet utilise la prise en charge des types logiques afin de charger correctement les timestamps présents dans les fichiers TLC.

Cette configuration est importante car les colonnes :

```text
tpep_pickup_datetime
tpep_dropoff_datetime
```

sont stockées dans Parquet avec un type logique timestamp.

Une mauvaise interprétation de ces valeurs peut entraîner des durées de trajet incohérentes.

---

# Ingestion Python

L’ingestion est assurée par :

```text
ingestion/load_month.py
```

Le script :

1. télécharge le fichier Parquet depuis NYC TLC ;
2. l’enregistre temporairement localement ;
3. l’upload dans le stage Snowflake ;
4. exécute un `COPY INTO` vers `RAW.YELLOW_TRIPDATA` ;
5. ajoute les métadonnées techniques du fichier chargé.

Les mois pris en charge sont :

```text
2025-01
2025-02
2025-03
```

Exemple :

```bash
python ingestion/load_month.py 2025-01
```

Le fichier de zones est chargé avec :

```bash
python ingestion/load_zones.py
```

---

# Idempotence de l’ingestion

Le chargement RAW est idempotent.

Snowflake conserve l’historique des fichiers chargés avec `COPY INTO`.

Si le même fichier est rejoué, Snowflake retourne un statut équivalent à :

```text
LOAD_SKIPPED
```

et ne recharge pas les lignes.

Cela évite la création de doublons dans la couche RAW.

---

# Airflow

Apache Airflow 3 est utilisé avec Astro CLI.

Le DAG principal est :

```text
nyc_taxi_monthly
```

Fichier :

```text
airflow/dags/nyc_taxi_monthly.py
```

---

# Configuration Airflow

Le DAG est configuré avec une exécution mensuelle.

Principaux paramètres :

```text
schedule = @monthly
catchup = True
max_active_runs = 1
```

La période couverte est :

```text
2025-01-01
2025-02-01
2025-03-01
```

Grâce à `catchup=True`, Airflow génère automatiquement les trois exécutions historiques.

---

# Date logique

Le pipeline utilise la date logique Airflow pour déterminer le mois à traiter.

Par exemple :

```text
2025-01-01
```

correspond au fichier :

```text
yellow_tripdata_2025-01.parquet
```

Les transformations utilisent également cette date afin de limiter les traitements au mois concerné.

---

# Connexion Airflow vers Snowflake

La connexion Airflow utilise :

```text
snowflake_nyc_taxi
```

Elle est définie via la variable d’environnement :

```text
AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI
```

La connexion utilise :

```text
user      = AIRFLOW_SVC
role      = TRANSFORMER
warehouse = NYC_TAXI_WH
database  = NYC_TAXI
schema    = RAW
```

avec authentification par clé privée RSA.

Le fichier `.env` contenant les secrets n’est pas versionné.

---

# Lancer Airflow

Depuis le dossier Airflow :

```bash
cd airflow
```

Démarrer l’environnement :

```bash
astro dev start
```

L’interface Airflow est ensuite disponible localement.

Pour arrêter l’environnement :

```bash
astro dev stop
```

---

# Orchestration du DAG

Le DAG suit globalement cette séquence :

```text
check source file
       |
download + upload
       |
COPY INTO RAW
       |
initialize tables
       |
check RAW month
       |
STAGING
       |
INTERMEDIATE
       |
quality checks
       |
MARTS
```

Les traitements SQL sont regroupés par couche via des `TaskGroup`.

Chaque fichier SQL correspond à une tâche Airflow distincte.

---

# Transformations STAGING

La couche STAGING :

- renomme les colonnes ;
- caste les types ;
- crée les vues métier ;
- charge les tables de codes TLC ;
- extrait le mois du fichier source.

---

# Contrôles qualité

Le pipeline applique plusieurs règles de qualité.

Un trajet peut être rejeté pour les raisons suivantes :

```text
timestamp_null
duration_non_positive
duration_too_long
pickup_outside_file_month
distance_out_of_range
amount_non_positive
zone_null
```

Les seuils principaux sont configurés dans Airflow :

```text
max_trip_distance_miles = 100
max_trip_duration_min = 180
```

---

# Contrôles Airflow

Trois contrôles principaux sont exécutés.

## Vérification du mois RAW

Le pipeline vérifie que des lignes existent bien pour le mois traité avant de poursuivre.

## Taux de rejet

Le contrôle vérifie que le pourcentage de trajets rejetés reste inférieur au seuil configuré.

```text
max_rejection_pct = 20
```

Si le contrôle échoue, les tâches suivantes sont bloquées.

## Absence de doublons

Le pipeline vérifie que `trip_sk` est unique dans `FCT_TRIPS`.

---

# Blocage du pipeline

Les contrôles sont bloquants.

Lorsqu’un contrôle échoue :

```text
check_rejection_rate = failed
```

les traitements dépendants passent en :

```text
upstream_failed
```

Cela empêche des données invalides de se propager vers les marts.

Une capture de cette situation est disponible dans :

```text
docs/screenshots/
```

---

# Déduplication

Les trajets considérés comme valides sont dédupliqués avec une fenêtre SQL utilisant :

```sql
ROW_NUMBER()
```

Une clé technique est ensuite générée :

```text
trip_sk
```

à partir des principales caractéristiques du trajet.

---

# Idempotence des transformations

Les transformations mensuelles sont rejouables.

Le principe utilisé est :

```sql
DELETE FROM target_table
WHERE source_file_month = '{{ ds }}'::date;

INSERT INTO target_table
SELECT ...
```

Le mois est donc supprimé avant d’être reconstruit.

Cela permet de relancer une exécution Airflow sans dupliquer les données.

Un test complet a été réalisé sur février 2025.

Les volumes observés avant et après relance sont restés identiques.

---

# Résultats attendus

Après exécution complète des trois mois :

| Table | Nombre de lignes |
|---|---:|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `MARTS.FCT_TRIPS` | 10 382 378 |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `MARTS.MART_DATA_QUALITY` | 18 |

Ces valeurs permettent de vérifier que le pipeline a été exécuté correctement.

---

# Vérification des doublons

La table de faits peut être contrôlée avec :

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT trip_sk) AS distinct_trip_sk,
    COUNT(*) - COUNT(DISTINCT trip_sk) AS duplicate_rows
FROM NYC_TAXI.MARTS.FCT_TRIPS;
```

Résultat attendu :

```text
duplicate_rows = 0
```

---

# Analyse métier

La question posée par Hudson Cab Partners est :

> Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

Les résultats complets sont disponibles dans :

```text
docs/REPONSE.md
```

---

# Principaux résultats métier

Sur janvier à mars 2025, la demande la plus forte est principalement observée à Manhattan.

Deux comportements ressortent :

- East Village et West Village sont particulièrement actifs pendant les nuits de week-end ;
- Midtown Center concentre une forte activité en fin de journée les jours de semaine.

East Village atteint notamment :

```text
738 trajets moyens par jour à minuit le week-end
```

La carte bancaire est le mode de paiement majoritaire :

```text
7 397 954 trajets
```

avec un revenu moyen de :

```text
28,32 $ par trajet
```

contre :

```text
23,70 $ par trajet
```

pour les paiements en espèces.

---

# Data Quality Mart

`MART_DATA_QUALITY` fournit une synthèse mensuelle des trajets valides et rejetés.

Il permet notamment de suivre :

- le nombre de lignes par statut ;
- les différentes raisons de rejet ;
- le pourcentage associé à chaque catégorie ;
- l’évolution de la qualité des fichiers source.

---

# Contrôle des droits

Les permissions du rôle peuvent être vérifiées avec :

```sql
SHOW GRANTS TO ROLE TRANSFORMER;
```

Le rôle peut créer les objets nécessaires dans son périmètre, mais ne dispose pas des privilèges permettant de créer une nouvelle base.

Par exemple :

```sql
USE ROLE TRANSFORMER;

CREATE DATABASE SHOULD_FAIL;
```

doit échouer avec une erreur de privilèges.

Cela permet de démontrer l’application du principe du moindre privilège.

---

# Consommation Snowflake

La consommation du warehouse peut être contrôlée via :

```sql
SELECT
    warehouse_name,
    SUM(credits_used) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'NYC_TAXI_WH'
GROUP BY warehouse_name;
```

Le warehouse utilisé est volontairement dimensionné en :

```text
XSMALL
```

avec :

```text
AUTO_SUSPEND = 60
```

afin de limiter les coûts lorsque le pipeline ne travaille pas.

---

# Screenshots

Les preuves d’exécution sont stockées dans :

```text
docs/screenshots/
```

Elles comprennent notamment :

```text
airflow_three_runs_success.png
airflow_dag_graph.png
airflow_control_failure.png
snowflake_copy_history.png
snowflake_transformer_grants.png
snowflake_credits.png
snowflake_data_quality.png
snowflake_final_query.png
```

---

# Rejouer le projet

## 1. Configurer Snowflake

Exécuter :

```text
snowflake/01_infrastructure.sql
snowflake/02_raw.sql
```

## 2. Installer les dépendances Python

```bash
pip install -r ingestion/requirements.txt
```

## 3. Définir les variables Snowflake

Exemple :

```bash
export SNOWFLAKE_ACCOUNT="..."
```

La clé privée RSA doit être disponible localement.

## 4. Charger les zones

```bash
python ingestion/load_zones.py
```

## 5. Charger un mois manuellement si nécessaire

```bash
python ingestion/load_month.py 2025-01
```

## 6. Configurer Airflow

Créer :

```text
airflow/.env
```

à partir de :

```text
airflow/.env.example
```

Puis renseigner :

```text
AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI
```

## 7. Démarrer Airflow

```bash
cd airflow
astro dev start
```

## 8. Activer le DAG

Activer :

```text
nyc_taxi_monthly
```

Airflow exécutera automatiquement les runs historiques grâce à :

```text
catchup=True
```

---

# Tests réalisés

Les principaux tests effectués sont :

- connexion Python vers Snowflake ;
- connexion Airflow vers Snowflake ;
- chargement du fichier zones ;
- chargement du fichier Parquet janvier ;
- rejet du second chargement du même fichier ;
- chargement des trois mois ;
- vérification des timestamps ;
- exécution complète des trois runs Airflow ;
- validation des volumes finaux ;
- test de contrôle bloquant ;
- test d’idempotence sur février ;
- vérification des doublons dans `FCT_TRIPS` ;
- vérification des permissions `TRANSFORMER` ;
- mesure de la consommation du warehouse.

---

# Limites

Le projet présente plusieurs limites :

- seules les données de janvier à mars 2025 sont utilisées ;
- l’analyse ne couvre donc pas une année complète ;
- certains trajets sont écartés par les règles de qualité ;
- les coûts réels d’exploitation des taxis ne sont pas disponibles ;
- les revenus analysés correspondent aux montants présents dans les données TLC, et non à la marge nette de Hudson Cab Partners ;
- les résultats décrivent la demande observée et ne démontrent pas directement l’impact financier qu’aurait un changement de positionnement de la flotte.

---

# Conclusion

Ce projet met en place un pipeline de données complet et rejouable pour les données NYC Yellow Taxi.

Snowflake fournit les différentes couches de stockage et de transformation, tandis qu’Apache Airflow orchestre les traitements mensuels.

Le pipeline intègre :

- ingestion incrémentale ;
- contrôles de qualité ;
- déduplication ;
- transformations SQL ;
- marts analytiques ;
- authentification sécurisée ;
- gestion des droits ;
- idempotence ;
- contrôle de consommation.

Les résultats permettent à Hudson Cab Partners d’identifier les zones et créneaux horaires où la demande observée est la plus forte et de comparer les revenus générés selon le contexte des trajets et les modes de paiement.