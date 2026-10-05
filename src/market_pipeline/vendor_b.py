import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen


EXPECTED_SCHEMA = "company.sentiment.batch/v1"


def fetch_vendor_b(url: str) -> tuple[bytes, int]:
    with urlopen(url, timeout=15) as response:
        return response.read(), response.status


def validate_payload(
    raw_bytes: bytes,
    expected_market_date: str,
    expected_batch_type: str,
) -> dict:
    payload = json.loads(raw_bytes)

    if payload.get("schema_version") != EXPECTED_SCHEMA:
        raise ValueError(
            f"Unexpected schema_version: {payload.get('schema_version')}"
        )

    if payload.get("market_date") != expected_market_date:
        raise ValueError(
            f"Unexpected market_date: {payload.get('market_date')}"
        )

    if payload.get("batch_type") != expected_batch_type:
        raise ValueError(
            f"Unexpected batch_type: {payload.get('batch_type')}"
        )

    return payload


def sha256_bytes(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


def write_immutable(path: Path, raw_bytes: bytes) -> str:
    incoming_hash = sha256_bytes(raw_bytes)

    if path.exists():
        existing_hash = sha256_bytes(path.read_bytes())

        if existing_hash == incoming_hash:
            return "unchanged"

        raise RuntimeError(
            f"Immutable raw artifact already exists with different content: {path}"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw_bytes)

    return "created"


def ingest_vendor_b(
    url: str,
    market_date: str,
    batch_type: str,
    data_root: Path,
) -> Path:
    raw_bytes, status = fetch_vendor_b(url)

    if status != 200:
        raise RuntimeError(f"Vendor B returned HTTP {status}")

    payload = validate_payload(
        raw_bytes,
        expected_market_date=market_date,
        expected_batch_type=batch_type,
    )

    year, month, day = market_date.split("/" if "/" in market_date else "-")

    landing_dir = (
        data_root
        / "raw"
        / "vendor_b"
        / year
        / month
        / day
        / batch_type.lower()
    )

    artifact_path = landing_dir / "sentiment.json"

    write_result = write_immutable(
        artifact_path,
        raw_bytes,
    )

    metadata = {
        "source": "vendor_b",
        "source_url": url,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "http_status": status,
        "sha256": sha256_bytes(raw_bytes),
        "market_date": payload["market_date"],
        "batch_type": payload["batch_type"],
        "schema_version": payload["schema_version"],
        "write_result": write_result,
    }

    metadata_path = landing_dir / "metadata.json"

    if write_result == "created":
        metadata_path.write_text(
            json.dumps(metadata, indent=2) + "\n",
            encoding="utf-8",
        )

    return {
        "artifact_path": str(artifact_path),
        "write_result": write_result,
        "sha256": metadata["sha256"],
        "market_date": payload["market_date"],
        "batch_type": payload["batch_type"],
        "schema_version": payload["schema_version"],
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest a Vendor B sentiment artifact."
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--market-date", required=True)
    parser.add_argument(
        "--batch-type",
        required=True,
        choices=["PREMARKET", "POSTMARKET"],
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )

    args = parser.parse_args()

    result = ingest_vendor_b(
        url=args.url,
        market_date=args.market_date,
        batch_type=args.batch_type,
        data_root=args.data_root,
    )

    print(json.dumps(result, indent=2))