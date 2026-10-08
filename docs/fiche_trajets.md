# Fiche — NYC Yellow Taxi Trip Records

## 1. Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | NYC Yellow Taxi Trip Records |
| Producteur des données | New York City Taxi & Limousine Commission (TLC) |
| Adresse (URL) | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| URL du fichier | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet |
| Accès | Public, sans authentification |
| Format du fichier | Apache Parquet |
| Fréquence de publication | Mensuelle |
| Délai de publication | Généralement environ 2 mois après la période couverte (selon la documentation TLC) |
| Périmètre du projet | Janvier à mars 2025 |
| Fichier analysé | `yellow_tripdata_2025-01.parquet` |
| Destination prévue | `NYC_TAXI.RAW.YELLOW_TRIPDATA` |

## 2. Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outils utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59 158 238 octets (56,42 MiB) | 3 475 226 | 20 | DuckDB, `stat`, `ls` |

**Commandes utilisées :**

```bash
stat -c '%s bytes' data/yellow_tripdata_2025-01.parquet
ls -lh data/yellow_tripdata_2025-01.parquet
```

```python
import duckdb

con = duckdb.connect()

con.execute("""
    SELECT COUNT(*)
    FROM 'data/yellow_tripdata_2025-01.parquet'
""").fetchall()

con.execute("""
    DESCRIBE SELECT *
    FROM 'data/yellow_tripdata_2025-01.parquet'
""").fetchall()
```

Le volume mesuré correspond aux **3 475 226 lignes attendues** dans le brief.

## 3. Colonnes

Le fichier contient 20 colonnes. Les types ci-dessous ont été relevés avec DuckDB (`DESCRIBE`).

Les exemples proviennent des cinq premières lignes affichées lors de l'exploration, lorsque ces valeurs étaient visibles. Les autres exemples restent à relever.

| Colonne | Type | Signification | Exemple observé |
|---|---|---|---|
| `VendorID` | INTEGER | Identifiant du fournisseur ayant transmis le trajet | `1` |
| `tpep_pickup_datetime` | TIMESTAMP | Date et heure de prise en charge | `2025-01-01 00:18:38` |
| `tpep_dropoff_datetime` | TIMESTAMP | Date et heure de fin de trajet | `2025-01-01 00:26:59` |
| `passenger_count` | BIGINT | Nombre de passagers | `1` |
| `trip_distance` | DOUBLE | Distance du trajet en miles | Non relevé |
| `RatecodeID` | BIGINT | Code du tarif appliqué | Non relevé |
| `store_and_fwd_flag` | VARCHAR | Indique si le trajet a été stocké temporairement avant transmission | Non relevé |
| `PULocationID` | INTEGER | Identifiant de la zone de départ | Non relevé |
| `DOLocationID` | INTEGER | Identifiant de la zone d'arrivée | Non relevé |
| `payment_type` | BIGINT | Mode de paiement | Non relevé |
| `fare_amount` | DOUBLE | Prix calculé par le compteur | Non relevé |
| `extra` | DOUBLE | Suppléments divers | Non relevé |
| `mta_tax` | DOUBLE | Taxe MTA | Non relevé |
| `tip_amount` | DOUBLE | Pourboire enregistré (hors pourboires en espèces) | Non relevé |
| `tolls_amount` | DOUBLE | Montant des péages | Non relevé |
| `improvement_surcharge` | DOUBLE | Supplément d'amélioration du service | Non relevé |
| `total_amount` | DOUBLE | Montant total facturé (hors pourboires en espèces) | `18.00` |
| `congestion_surcharge` | DOUBLE | Supplément lié à la congestion | `2.5` |
| `Airport_fee` | DOUBLE | Supplément de prise en charge aux aéroports JFK et LaGuardia | `0.0` |
| `cbd_congestion_fee` | DOUBLE | Supplément lié à la Congestion Relief Zone (depuis janvier 2025) | `0.0` |

Les 20 colonnes autorisent les valeurs nulles selon les métadonnées de DuckDB. Cela ne signifie pas qu'elles contiennent toutes des valeurs manquantes.

## 4. Codes

Signification des codes issue du dictionnaire officiel TLC *Yellow Taxi Trip Records*, daté du 18 mars 2025.

### VendorID — Fournisseur

| Valeur | Signification |
|---|---|
| `1` | Creative Mobile Technologies, LLC |
| `2` | Curb Mobility, LLC |
| `6` | Myle Technologies Inc |
| `7` | Helix |

### RatecodeID — Type de tarif

| Valeur | Signification |
|---|---|
| `1` | Tarif standard |
| `2` | JFK Airport |
| `3` | Newark Airport |
| `4` | Nassau ou Westchester |
| `5` | Tarif négocié |
| `6` | Trajet collectif |
| `99` | Tarif inconnu / non renseigné |

### payment_type — Mode de paiement

| Valeur | Signification |
|---|---|
| `0` | Flex Fare |
| `1` | Carte bancaire |
| `2` | Espèces |
| `3` | Trajet non facturé |
| `4` | Litige |
| `5` | Inconnu |
| `6` | Trajet annulé |

### store_and_fwd_flag — Mode de transmission

| Valeur | Signification |
|---|---|
| `Y` | Enregistrement stocké avant transmission |
| `N` | Enregistrement transmis sans stockage préalable |

Les champs `PULocationID` et `DOLocationID` correspondent aux identifiants des zones TLC. Le fichier de référence `taxi_zone_lookup.csv` permettra d'obtenir les noms des zones.

## 5. Qualité des données

Une première analyse exploratoire a été réalisée avec DuckDB sur les 3 475 226 trajets de janvier 2025.

### Valeurs manquantes

| Contrôle | Nombre de lignes |
|---|---:|
| `passenger_count IS NULL` | 540 149 |
| `trip_distance IS NULL` | 0 |
| `total_amount IS NULL` | 0 |
| `PULocationID IS NULL` | 0 |

Environ **15,54 %** des enregistrements ne renseignent pas le nombre de passagers.

### Anomalies détectées

| Contrôle | Nombre de lignes |
|---|---:|
| Distance nulle ou négative (`trip_distance <= 0`) | 90 893 |
| Montant total négatif (`total_amount < 0`) | 63 037 |
| Date de fin antérieure à la date de début | 124 |
| Date de départ hors janvier 2025 | 22 |

Ces contrôles sont indépendants : un même trajet peut présenter plusieurs anomalies. Leurs résultats ne doivent donc pas être additionnés pour déterminer le nombre de trajets invalides.

### Statistiques descriptives

| Indicateur | Valeur |
|---|---|
| Première prise en charge | `2024-12-31 20:47:55` |
| Dernière prise en charge | `2025-02-01 00:00:44` |
| Distance minimale | 0 mile |
| Distance maximale | 276 423,57 miles |
| Distance moyenne | 5,86 miles |
| Montant total minimal | -901,00 $ |
| Montant total maximal | 863 380,37 $ |
| Montant total moyen | 25,61 $ |

La moyenne est calculée sur les données brutes, avant nettoyage et exclusion des anomalies.

## 6. Ce qui a surpris

L'exploration révèle des distances et des montants manifestement aberrants, notamment un trajet de plus de 276 000 miles et un montant supérieur à 863 000 $. On relève également 63 037 montants négatifs et 540 149 enregistrements dont le nombre de passagers est absent.

Le fichier mensuel contient aussi 22 trajets dont la prise en charge est datée hors janvier 2025, ainsi que 124 trajets dont la date de fin est antérieure à celle du départ.

Ces résultats justifient la mise en place de contrôles de qualité avant les analyses métier.

## 7. Intégration dans le pipeline

Le fichier sera chargé dans Snowflake selon l'architecture du projet :

`SOURCE → RAW → STAGING → INTERMEDIATE → MARTS`

- **RAW** : conservation des données d'origine, sans suppression des anomalies.
- **STAGING** : harmonisation des noms, des types et des codes.
- **INTERMEDIATE** : application des règles de contrôle, identification des trajets invalides et enrichissement.
- **MARTS** : production des tables destinées à l'analyse de la demande, des revenus et de la qualité des données.

Le chargement ajoutera deux colonnes techniques :

| Colonne | Utilité |
|---|---|
| `_source_file` | Identification exacte du fichier chargé |
| `_loaded_at` | Date et heure du chargement |

L'ingestion devra être idempotente : rejouer le chargement d'un même mois ne devra pas créer de doublons.

Les transformations SQL fournies dans le starter-kit seront réutilisées sans modification.

## 8. Références

- [TLC — Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [TLC — Yellow Taxi Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Fichier Parquet — Janvier 2025](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet)
- `CONTRAT_RAW.md` — Contrat technique fourni dans le starter-kit.
- `ETAPES.md` — Consignes du projet.

**Méthode :** exploration avec DuckDB, comptage SQL, statistiques descriptives et mesure de taille sous Ubuntu. Les chiffres présentés dans cette fiche ont été mesurés sur le fichier de janvier 2025, sauf les informations générales issues de la documentation officielle TLC.