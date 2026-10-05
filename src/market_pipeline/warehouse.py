import json
import os
from pathlib import Path

import psycopg2


CREATE_RAW_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.vendor_b_batch (
    batch_id           BIGSERIAL PRIMARY KEY,
    schema_version     TEXT NOT NULL,
    market_date        DATE NOT NULL,
    batch_type         TEXT NOT NULL,
    window_start       TIMESTAMPTZ NOT NULL,
    window_end         TIMESTAMPTZ NOT NULL,
    observation_count  INTEGER NOT NULL,
    source_url          TEXT NOT NULL,
    retrieved_at_utc    TIMESTAMPTZ NOT NULL,
    sha256              TEXT NOT NULL,

    UNIQUE (market_date, batch_type),
    UNIQUE (sha256)
);

CREATE TABLE IF NOT EXISTS raw.vendor_b_observation (
    observation_id     UUID PRIMARY KEY,
    batch_id           BIGINT NOT NULL
                           REFERENCES raw.vendor_b_batch(batch_id),
    schema_version     TEXT NOT NULL,
    observed_at        TIMESTAMPTZ NOT NULL,
    company            TEXT NOT NULL,
    channel            TEXT NOT NULL,
    sentiment          TEXT NOT NULL,
    engagement         INTEGER NOT NULL
);
"""


def connect_market_db():
    return psycopg2.connect(
        host=os.environ["MARKET_DB_HOST"],
        port=os.environ["MARKET_DB_PORT"],
        dbname=os.environ["MARKET_DB_NAME"],
        user=os.environ["MARKET_DB_USER"],
        password=os.environ["MARKET_DB_PASSWORD"],
    )


def load_vendor_b_artifact(
    artifact_path: Path,
    metadata_path: Path,
) -> dict:
    payload = json.loads(
        artifact_path.read_text(encoding="utf-8")
    )
    metadata = json.loads(
        metadata_path.read_text(encoding="utf-8")
    )

    with connect_market_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_RAW_SCHEMA_SQL)

            cursor.execute(
                """
                INSERT INTO raw.vendor_b_batch (
                    schema_version,
                    market_date,
                    batch_type,
                    window_start,
                    window_end,
                    observation_count,
                    source_url,
                    retrieved_at_utc,
                    sha256
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_date, batch_type)
                DO NOTHING
                RETURNING batch_id;
                """,
                (
                    payload["schema_version"],
                    payload["market_date"],
                    payload["batch_type"],
                    payload["window_start"],
                    payload["window_end"],
                    payload["observation_count"],
                    metadata["source_url"],
                    metadata["retrieved_at_utc"],
                    metadata["sha256"],
                ),
            )

            inserted = cursor.fetchone()

            if inserted is not None:
                batch_id = inserted[0]
                batch_result = "created"
            else:
                cursor.execute(
                    """
                    SELECT batch_id, sha256
                    FROM raw.vendor_b_batch
                    WHERE market_date = %s
                      AND batch_type = %s;
                    """,
                    (
                        payload["market_date"],
                        payload["batch_type"],
                    ),
                )

                existing = cursor.fetchone()

                if existing is None:
                    raise RuntimeError(
                        "Batch conflict occurred but existing batch "
                        "could not be found."
                    )

                batch_id, existing_hash = existing

                if existing_hash != metadata["sha256"]:
                    raise RuntimeError(
                        "Existing warehouse batch has different content "
                        f"for {payload['market_date']} "
                        f"{payload['batch_type']}."
                    )

                batch_result = "unchanged"

            for observation in payload["observations"]:
                cursor.execute(
                    """
                    INSERT INTO raw.vendor_b_observation (
                        observation_id,
                        batch_id,
                        schema_version,
                        observed_at,
                        company,
                        channel,
                        sentiment,
                        engagement
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (observation_id)
                    DO NOTHING;
                    """,
                    (
                        observation["observation_id"],
                        batch_id,
                        observation["schema_version"],
                        observation["observed_at"],
                        observation["company"],
                        observation["channel"],
                        observation["sentiment"],
                        observation["engagement"],
                    ),
                )

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM raw.vendor_b_observation
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

            loaded_count = cursor.fetchone()[0]

            if loaded_count != payload["observation_count"]:
                raise RuntimeError(
                    "Observation count mismatch: "
                    f"vendor={payload['observation_count']} "
                    f"warehouse={loaded_count}"
                )

    return {
        "batch_id": batch_id,
        "batch_result": batch_result,
        "market_date": payload["market_date"],
        "batch_type": payload["batch_type"],
        "observation_count": loaded_count,
        "sha256": metadata["sha256"],
    }