"""NYC Yellow Taxi monthly ingestion and transformation pipeline."""

from pathlib import Path

import pendulum
import requests

from airflow.sdk import dag, task, get_current_context, TaskGroup
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

CONN_ID = "snowflake_nyc_taxi"

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)

STAGE = "NYC_TAXI.RAW.TLC_STAGE"
RAW_TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"

SQL_PATH = "/usr/local/airflow/include/sql"


# --------------------------------------------------
# DAG definition
# --------------------------------------------------

@dag(
    dag_id="nyc_taxi_monthly",
    description="Monthly NYC Yellow Taxi ELT pipeline",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    template_searchpath=SQL_PATH,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    params={
        "max_trip_distance_miles": 100,
        "max_trip_duration_min": 180,
        "start_month": "2025-01-01",
        "end_month": "2025-04-01",
        "max_rejection_pct": 20,
    },
    tags=["nyc", "taxi", "snowflake", "elt"],
)
def nyc_taxi_monthly():

    # ----------------------------------------------
    # 1. INGESTION
    # ----------------------------------------------

    @task
    def get_filename() -> str:
        """Get the filename from the logical date."""

        context = get_current_context()

        month = context[
            "data_interval_start"
        ].strftime("%Y-%m")

        filename = f"yellow_tripdata_{month}.parquet"

        print(f"Processing file: {filename}")

        return filename

    @task
    def check_file(filename: str) -> str:
        """Verify that the TLC file is available."""

        url = f"{BASE_URL}/{filename}"

        response = requests.head(
            url,
            timeout=30,
            allow_redirects=True,
        )

        response.raise_for_status()

        print(f"File available: {url}")

        return url

    @task
    def download_and_upload(url: str) -> str:
        """Download and upload the file to Snowflake."""

        filepath = Path("/tmp") / url.rsplit("/", 1)[-1]

        temporary = filepath.with_suffix(
            ".parquet.part"
        )

        try:
            # Download in chunks
            with requests.get(
                url,
                stream=True,
                timeout=120,
            ) as response:

                response.raise_for_status()

                with temporary.open("wb") as file:
                    for chunk in response.iter_content(
                        chunk_size=8 * 1024 * 1024
                    ):
                        if chunk:
                            file.write(chunk)

            temporary.replace(filepath)

            # Upload to Snowflake internal stage
            hook = SnowflakeHook(
                snowflake_conn_id=CONN_ID
            )

            hook.run(
                f"PUT 'file://{filepath}' "
                f"@{STAGE} "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=FALSE"
            )

            print(f"Uploaded: {filepath.name}")

            return filepath.name

        finally:
            filepath.unlink(missing_ok=True)
            temporary.unlink(missing_ok=True)

    @task
    def copy_into_raw(filename: str):
        """Load the staged Parquet into RAW."""

        hook = SnowflakeHook(
            snowflake_conn_id=CONN_ID
        )

        hook.run(f"""
            COPY INTO {RAW_TABLE}
            FROM @{STAGE}
            FILES = ('{filename}')
            FILE_FORMAT = (
                FORMAT_NAME = {FILE_FORMAT}
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
        """)

        print(f"COPY INTO completed: {filename}")

    filename = get_filename()
    url = check_file(filename)
    staged_file = download_and_upload(url)
    loaded = copy_into_raw(staged_file)

    # ----------------------------------------------
    # 2. INITIALIZATION
    # ----------------------------------------------

    init_tables = SQLExecuteQueryOperator(
        task_id="init_tables",
        conn_id=CONN_ID,
        sql="00_tables.sql",
        split_statements=True,
    )

    # ----------------------------------------------
    # 3. RAW QUALITY CHECK
    # ----------------------------------------------

    raw_check = SQLCheckOperator(
        task_id="check_raw_month",
        conn_id=CONN_ID,
        sql="controles/raw_mois_charge.sql",
        retries=0,
    )

    # ----------------------------------------------
    # 4. STAGING
    # ----------------------------------------------

    with TaskGroup(group_id="staging"):

        stg_trips = SQLExecuteQueryOperator(
            task_id="stg_yellow_trips",
            conn_id=CONN_ID,
            sql="staging/stg_tlc__yellow_trips.sql",
        )

        stg_zones = SQLExecuteQueryOperator(
            task_id="stg_taxi_zones",
            conn_id=CONN_ID,
            sql="staging/stg_tlc__taxi_zones.sql",
        )

        codes = SQLExecuteQueryOperator(
            task_id="codes_tlc",
            conn_id=CONN_ID,
            sql="staging/codes_tlc.sql",
            split_statements=True,
        )

    # ----------------------------------------------
    # 5. INTERMEDIATE
    # ----------------------------------------------

    with TaskGroup(group_id="intermediate"):

        flagged = SQLExecuteQueryOperator(
            task_id="flag_trips",
            conn_id=CONN_ID,
            sql="intermediate/int_trips__flagged.sql",
            split_statements=True,
        )

        rejection_check = SQLCheckOperator(
            task_id="check_rejection_rate",
            conn_id=CONN_ID,
            sql="controles/rejection_rate.sql",
            retries=0,
        )

        enriched = SQLExecuteQueryOperator(
            task_id="enrich_trips",
            conn_id=CONN_ID,
            sql="intermediate/int_trips__enriched.sql",
            split_statements=True,
        )

        flagged >> rejection_check >> enriched

    # ----------------------------------------------
    # 6. MARTS
    # ----------------------------------------------

    with TaskGroup(group_id="marts"):

        # Dimensions
        dimensions = [
            SQLExecuteQueryOperator(
                task_id="dim_date",
                conn_id=CONN_ID,
                sql="marts/dim_date.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="dim_payment_type",
                conn_id=CONN_ID,
                sql="marts/dim_payment_type.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="dim_rate_code",
                conn_id=CONN_ID,
                sql="marts/dim_rate_code.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="dim_vendor",
                conn_id=CONN_ID,
                sql="marts/dim_vendor.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="dim_zone",
                conn_id=CONN_ID,
                sql="marts/dim_zone.sql",
            ),
        ]

        # Fact table
        fact_trips = SQLExecuteQueryOperator(
            task_id="fct_trips",
            conn_id=CONN_ID,
            sql="marts/fct_trips.sql",
            split_statements=True,
        )

        # Duplicate quality check
        duplicate_check = SQLCheckOperator(
            task_id="check_duplicates",
            conn_id=CONN_ID,
            sql="controles/no_duplicate_trips.sql",
            retries=0,
        )

        # Analytical marts
        marts = [
            SQLExecuteQueryOperator(
                task_id="mart_daily_revenue",
                conn_id=CONN_ID,
                sql="marts/mart_daily_revenue.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="mart_zone_hourly_demand",
                conn_id=CONN_ID,
                sql="marts/mart_zone_hourly_demand.sql",
            ),
            SQLExecuteQueryOperator(
                task_id="mart_data_quality",
                conn_id=CONN_ID,
                sql="marts/mart_data_quality.sql",
            ),
        ]

        # Dimensions before fact table
        for dim in dimensions:
            dim >> fact_trips

        # Check fact quality before aggregation
        fact_trips >> duplicate_check

        for mart in marts:
            duplicate_check >> mart

    # ----------------------------------------------
    # 7. MAIN DEPENDENCIES
    # ----------------------------------------------

    loaded >> init_tables >> raw_check

    # Execute each staging task after RAW check
    for staging_task in [stg_trips, stg_zones, codes]:
        raw_check >> staging_task

    # All staging tasks must finish before
    # intermediate transformations begin
    for staging_task in [stg_trips, stg_zones, codes]:
        staging_task >> flagged

    # Enriched trips must exist before
    # building analytical dimensions and facts
    for dim in dimensions:
        enriched >> dim


nyc_taxi_monthly()
