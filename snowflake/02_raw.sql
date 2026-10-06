-- ============================================
-- NYC TAXI - RAW LAYER
-- ============================================

-- Use the service role
USE ROLE TRANSFORMER;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;


-- 1. File formats
CREATE FILE FORMAT IF NOT EXISTS PARQUET_FF
    TYPE = PARQUET;

CREATE FILE FORMAT IF NOT EXISTS CSV_FF
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- 2. Internal stage
CREATE STAGE IF NOT EXISTS TLC_STAGE
    COMMENT = 'TLC files for RAW ingestion';


-- 3. Yellow taxi trips
CREATE TABLE IF NOT EXISTS YELLOW_TRIPDATA (
    vendorid NUMBER,
    tpep_pickup_datetime TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count NUMBER,
    trip_distance FLOAT,
    ratecodeid NUMBER,
    store_and_fwd_flag VARCHAR,
    pulocationid NUMBER,
    dolocationid NUMBER,
    payment_type NUMBER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT,

    -- Technical metadata
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);


-- 4. Taxi zone lookup
CREATE TABLE IF NOT EXISTS TAXI_ZONE_LOOKUP (
    locationid NUMBER,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR,

    -- Technical metadata
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);
