-- Check trip uniqueness in the fact table.
-- Every trip_sk must be non-null and unique.

SELECT
    COUNT(*) > 0
    AND COUNT(*) = COUNT(trip_sk)
    AND COUNT(*) = COUNT(DISTINCT trip_sk)
FROM NYC_TAXI.MARTS.FCT_TRIPS
WHERE source_file_month = '{{ ds }}'::date;
