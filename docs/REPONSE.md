# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

La demande est analysée à partir du mart `MART_ZONE_HOURLY_DEMAND`, qui agrège les trajets par zone de prise en charge, heure et type de jour.

~~~sql
SELECT
    pickup_zone_name,
    pickup_borough,
    pickup_hour,
    is_weekend,
    nb_trips,
    avg_trips_per_day,
    avg_revenue_per_trip,
    revenue_per_hour_driven
FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
WHERE pickup_zone_name IS NOT NULL
ORDER BY avg_trips_per_day DESC
LIMIT 10;
~~~

Pour compléter l'analyse avec le mode de paiement :

~~~sql
SELECT
    payment_type_label,
    SUM(nb_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(
        SUM(total_revenue) / NULLIF(SUM(nb_trips), 0),
        2
    ) AS avg_revenue_per_trip
FROM NYC_TAXI.MARTS.MART_DAILY_REVENUE
GROUP BY payment_type_label
ORDER BY total_trips DESC;
~~~

## Le résultat : les 10 premières lignes

| Zone | Borough | Heure | Week-end | Nb trajets | Moy. trajets / jour | Revenu moyen / trajet ($) | Revenu / heure conduite ($) |
|---|---|---:|---|---:|---:|---:|---:|
| East Village | Manhattan | 0 | Oui | 19 187 | 738.0 | 22.84 | 113.75 |
| East Village | Manhattan | 1 | Oui | 18 869 | 725.7 | 22.11 | 109.36 |
| Midtown Center | Manhattan | 18 | Non | 38 170 | 596.4 | 24.98 | 106.77 |
| Midtown Center | Manhattan | 17 | Non | 36 602 | 571.9 | 30.06 | 115.24 |
| West Village | Manhattan | 0 | Oui | 14 536 | 559.1 | 23.14 | 108.21 |
| Midtown Center | Manhattan | 20 | Non | 34 207 | 534.5 | 22.81 | 107.93 |
| West Village | Manhattan | 1 | Oui | 13 505 | 519.4 | 22.57 | 105.77 |
| East Village | Manhattan | 2 | Oui | 12 976 | 519.0 | 22.00 | 117.25 |
| Midtown Center | Manhattan | 19 | Non | 32 773 | 512.1 | 24.14 | 113.12 |
| Midtown Center | Manhattan | 21 | Non | 31 636 | 494.3 | 23.01 | 108.84 |

> La valeur `revenue_per_hour_driven` de Midtown Center à 19h doit être recopiée directement depuis Snowflake, car elle n'était pas entièrement visible sur la capture d'écran.

## Résultats par mode de paiement

| Mode de paiement | Nombre de trajets | Revenu total ($) | Revenu moyen / trajet ($) |
|---|---:|---:|---:|
| Credit card | 7 397 954 | 209 535 888.79 | 28.32 |
| Flex Fare trip | 1 767 528 | 43 034 783.81 | 24.35 |
| Cash | 1 063 448 | 25 208 170.14 | 23.70 |
| Dispute | 115 468 | 4 065 063.99 | 35.21 |
| No charge | 37 980 | 1 038 569.87 | 27.35 |

## Ce qu'il faut en retenir

1. La demande la plus forte se concentre principalement à Manhattan, avec deux profils distincts : East Village et West Village connaissent une forte activité nocturne le week-end, tandis que Midtown Center concentre davantage de demande en fin de journée les jours de semaine.

2. East Village atteint environ 738 trajets par jour à minuit le week-end, ce qui constitue le créneau présentant la plus forte demande quotidienne moyenne dans le classement. Midtown Center affiche cependant des volumes absolus très importants en fin de journée, avec plus de 38 000 trajets observés à 18h sur l'ensemble de la période étudiée.

3. La carte bancaire constitue de très loin le principal mode de paiement avec près de 7,4 millions de trajets. Ces trajets représentent environ 209,5 millions de dollars de revenu cumulé et un revenu moyen de 28,32 dollars par trajet, contre 23,70 dollars pour les paiements en espèces.

## Interprétation métier

Les résultats mettent en évidence des périodes de demande différentes selon les zones de Manhattan.

East Village et West Village apparaissent particulièrement actifs pendant les nuits de week-end. East Village atteint notamment une moyenne de 738 trajets par jour à minuit et de 725,7 trajets à 1h du matin. West Village présente également une forte demande autour de minuit et 1h.

Midtown Center présente un profil différent. La demande y est particulièrement importante pendant les jours de semaine, entre 17h et 21h. Le créneau de 18h totalise 38 170 trajets sur la période analysée, tandis que celui de 17h présente un revenu moyen particulièrement élevé de 30,06 dollars par trajet.

Pour Hudson Cab Partners, ces résultats suggèrent qu'un positionnement plus important de la flotte dans East Village et West Village pendant les nuits de week-end, ainsi que dans Midtown Center pendant les heures de pointe de fin de journée en semaine, pourrait permettre de mieux répondre aux périodes de forte demande observées.

## Analyse des modes de paiement

La carte bancaire représente le mode de paiement largement dominant :

- 7 397 954 trajets ;
- 209 535 888,79 dollars de revenu cumulé ;
- 28,32 dollars de revenu moyen par trajet.

Les paiements en espèces représentent 1 063 448 trajets, pour un revenu moyen inférieur de 23,70 dollars par trajet.

Les trajets classés `Flex Fare trip` représentent également une part importante du dataset avec 1 767 528 trajets et un revenu moyen de 24,35 dollars.

Les catégories `Dispute` et `No charge` doivent être interprétées avec prudence. Ces catégories correspondent aux codes fournis par la TLC et certaines lignes peuvent néanmoins contenir un `total_amount` positif dans les données sources.

## Qualité des données

Les résultats présentés sont calculés après application des règles de qualité du pipeline.

Les données brutes sont conservées dans la couche `RAW`, sans suppression. Les trajets sont ensuite contrôlés dans la couche `INTERMEDIATE`, où chaque ligne peut recevoir une raison de rejet.

Les règles de contrôle portent notamment sur :

- les dates de prise en charge et de dépose manquantes ;
- les durées de trajet nulles, négatives ou excessivement longues ;
- les trajets dont la date ne correspond pas au mois du fichier source ;
- les distances nulles, négatives ou supérieures au seuil retenu ;
- les montants négatifs ou nuls ;
- les zones de départ ou d'arrivée absentes.

Un trajet ne reçoit qu'une seule raison de rejet : la première règle invalide rencontrée dans le `CASE` du fichier `int_trips__flagged.sql`.

Les trajets valides sont ensuite dédupliqués et enrichis avant leur intégration dans la table de faits `FCT_TRIPS`.

Après traitement des trois mois, la table de faits contient :

**10 382 378 trajets valides.**

La table RAW conserve quant à elle :

**11 198 026 trajets bruts.**

Cette différence correspond aux trajets rejetés par les règles de qualité ainsi qu'aux éventuels doublons éliminés lors de la construction des données analytiques.

## Rejouabilité du pipeline

Le pipeline a été conçu pour être rejouable sans créer de doublons.

Au niveau de l'ingestion, Snowflake conserve l'historique des fichiers déjà chargés et empêche leur rechargement par `COPY INTO` lorsqu'ils ont déjà été traités.

Au niveau des transformations mensuelles, les tables principales utilisent une stratégie :

~~~sql
DELETE FROM ...
WHERE source_file_month = '{{ ds }}'::date;

INSERT INTO ...
~~~

Le mois concerné est donc supprimé puis reconstruit lors de chaque nouvelle exécution.

Une relance complète du mois de février a été effectuée. Les nombres de lignes observés avant et après la relance sont restés identiques, confirmant l'idempotence du pipeline.

La table `FCT_TRIPS` ne contient également aucun doublon sur la clé `trip_sk`.

## Orchestration

Apache Airflow 3 orchestre le pipeline avec un DAG mensuel.

Le DAG utilise :

- une planification mensuelle ;
- `catchup=True` afin de rejouer janvier, février et mars 2025 ;
- la date logique de l'exécution pour déterminer le fichier à traiter ;
- `max_active_runs=1` afin de traiter les mois successivement ;
- des contrôles SQL bloquants afin d'empêcher la propagation de données incorrectes.

Les trois exécutions historiques de janvier, février et mars 2025 ont été exécutées avec succès.

Un contrôle de qualité a également été observé en échec lors du développement. Les tâches situées en aval sont alors passées en état `upstream_failed`, ce qui confirme que les contrôles peuvent effectivement arrêter le pipeline avant la propagation de données invalides.

## Sécurité

Les outils ne se connectent pas à Snowflake avec le rôle `ACCOUNTADMIN`.

Un rôle dédié, `TRANSFORMER`, a été créé selon le principe du moindre privilège. Il dispose uniquement des autorisations nécessaires pour :

- utiliser le warehouse ;
- accéder à la base `NYC_TAXI` ;
- créer et manipuler les objets nécessaires dans les schémas du pipeline.

L'utilisateur technique `AIRFLOW_SVC` utilise ce rôle.

L'authentification de Python et d'Airflow repose sur une paire de clés RSA. La clé privée n'est ni stockée dans Git ni intégrée dans l'image Docker.

## Les limites

Cette analyse présente plusieurs limites.

- La période étudiée couvre uniquement janvier, février et mars 2025. Elle ne permet donc pas d'observer la saisonnalité sur une année complète.

- Les résultats sont calculés uniquement à partir des trajets considérés comme valides après application des règles de qualité.

- Les zones inconnues ou non renseignées ne sont pas utilisées dans le classement principal.

- Le nombre de trajets observé dans une combinaison `zone × heure` ne représente pas nécessairement une demande quotidienne constante. La métrique `avg_trips_per_day` a donc été utilisée pour comparer les créneaux.

- Le revenu analysé correspond aux montants enregistrés dans les données TLC. Il ne représente pas le bénéfice net de Hudson Cab Partners, car les coûts liés aux véhicules, aux chauffeurs, au carburant, à l'entretien ou aux assurances ne sont pas disponibles.

- Les résultats montrent des associations entre zone, horaire, volume de trajets et revenus observés. Ils ne permettent pas à eux seuls d'établir qu'une modification du positionnement de la flotte entraînerait automatiquement une hausse du chiffre d'affaires.

## Conclusion

Sur la période de janvier à mars 2025, les zones présentant les niveaux de demande les plus élevés sont principalement situées à Manhattan.

East Village et West Village concentrent une forte activité pendant les nuits de week-end, alors que Midtown Center connaît une demande particulièrement importante en fin d'après-midi et en début de soirée les jours de semaine.

Ces résultats fournissent à Hudson Cab Partners des éléments permettant d'identifier les zones et les créneaux horaires où la demande observée est la plus importante.

Ils montrent également que les paiements par carte bancaire constituent la majorité des trajets analysés et représentent la principale source de revenu du dataset.

L'architecture Snowflake et Airflow mise en place permet enfin de reproduire automatiquement cette analyse chaque mois tout en conservant la traçabilité des données, des contrôles de qualité et des chargements.