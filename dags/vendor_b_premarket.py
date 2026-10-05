from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task

from market_pipeline.vendor_b import ingest_vendor_b


VENDOR_B_BASE_URL = (
    "https://mock-market.duckdns.org/sentiment"
)

MARKET_TIMEZONE = ZoneInfo("America/New_York")


@dag(
    dag_id="vendor_b_premarket",
    schedule="5 8 * * *",
    start_date=datetime(
        2026,
        10,
        1,
        tzinfo=MARKET_TIMEZONE,
    ),
    catchup=False,
    tags=["vendor-b", "ingestion"],
)
def vendor_b_premarket():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    def ingest():
        market_date = datetime.now(
            MARKET_TIMEZONE
        ).date().isoformat()

        url = (
            f"{VENDOR_B_BASE_URL}/"
            f"sentiment-{market_date}-premarket.json"
        )

        return ingest_vendor_b(
            url=url,
            market_date=market_date,
            batch_type="PREMARKET",
            data_root=Path("/opt/airflow/data"),
        )

    ingest()


vendor_b_premarket()