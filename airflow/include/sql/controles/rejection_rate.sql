-- Check the rejection rate for the processed month.
-- Fail if the month is empty or rejection rate exceeds threshold.

SELECT
    COUNT(*) > 0
    AND
    (
        COUNT_IF(rejection_reason IS NOT NULL)
        * 100.0
        / NULLIF(COUNT(*), 0)
    ) <= {{ params.max_rejection_pct }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
