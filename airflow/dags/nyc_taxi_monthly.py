"""Monthly NYC Yellow Taxi RAW ingestion pipeline."""

from pathlib import Path

import pendulum
import requests

from airflow.sdk import dag, task, get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


CONN_ID = "snowflake_nyc_taxi"

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)

STAGE = "NYC_TAXI.RAW.TLC_STAGE"
TABLE = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
FILE_FORMAT = "NYC_TAXI.RAW.PARQUET_FF"


@dag(
    dag_id="nyc_taxi_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["nyc", "taxi", "ingestion"],
)
def nyc_taxi_monthly():

    @task
    def get_filename() -> str:
        """Determine the file from the logical interval."""

        context = get_current_context()
        month = context["data_interval_start"].strftime(
            "%Y-%m"
        )

        return f"yellow_tripdata_{month}.parquet"

    @task
    def check_file(filename: str) -> str:
        """Check the availability of the remote file."""

        url = f"{BASE_URL}/{filename}"

        response = requests.head(
            url,
            timeout=30,
            allow_redirects=True,
        )
        response.raise_for_status()

        return url

    @task
    def download_and_upload(url: str) -> str:
        """Download the file and upload it to Snowflake."""

        filepath = Path("/tmp") / url.rsplit("/", 1)[-1]
        temporary = filepath.with_suffix(".parquet.part")

        try:
            with requests.get(
                url, stream=True, timeout=120
            ) as response:
                response.raise_for_status()

                with temporary.open("wb") as file:
                    for chunk in response.iter_content(
                        chunk_size=8 * 1024 * 1024
                    ):
                        if chunk:
                            file.write(chunk)

            temporary.replace(filepath)

            hook = SnowflakeHook(
                snowflake_conn_id=CONN_ID
            )

            hook.run(
                f"PUT 'file://{filepath}' "
                f"@{STAGE} "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=FALSE"
            )

            return filepath.name

        finally:
            filepath.unlink(missing_ok=True)
            temporary.unlink(missing_ok=True)

    @task
    def copy_into_raw(filename: str):
        """Load the staged file into the RAW table."""

        hook = SnowflakeHook(
            snowflake_conn_id=CONN_ID
        )

        hook.run(f"""
            COPY INTO {TABLE}
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

    filename = get_filename()
    url = check_file(filename)
    staged_file = download_and_upload(url)
    copy_into_raw(staged_file)


nyc_taxi_monthly()
