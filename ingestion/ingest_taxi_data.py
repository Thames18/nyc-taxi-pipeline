"""
ingestion/ingest_taxi_data.py
==============================
Downloads NYC Yellow Taxi trip data from the TLC public dataset """

import os
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

DOWNLOAD_YEAR = 2024
DOWNLOAD_MONTH = 1

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
RAW_DATA_DIR = Path("data/raw")


class TaxiDataIngestor:
    """
    Downloads TLC Yellow Taxi trip data,     performs light cleaning, and writes it as a CSV to the raw layer.

    Usage:
        ingestor = TaxiDataIngestor(execution_date="2024-01-15")
        result = ingestor.run()
    """

    def __init__(self, execution_date: str):
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        self.year = dt.year
        self.month = dt.month
        self.execution_date = execution_date
        self.output_dir = RAW_DATA_DIR / execution_date
        self.output_path = self.output_dir / "taxi_trips.csv"

    def _build_url(self) -> str:
        return BASE_URL.format(year=self.year, month=self.month)

    def _download_parquet(self, url: str, local_path: Path) -> None:
        logger.info(f"Downloading: {url}")
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded to {local_path}")

    def _filter_to_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only rows matching the execution date."""
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )
        mask = df["tpep_pickup_datetime"].dt.date == pd.Timestamp(
            self.execution_date
        ).date()
        return df[mask].copy()

    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename and keep only schema-aligned columns."""
        rename_map = {
            "VendorID": "vendor_id",
            "RatecodeID": "rate_code_id",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
        }
        df = df.rename(columns=rename_map)

        keep_cols = [
            "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "rate_code_id",
            "fare_amount", "tip_amount", "total_amount", "payment_type",
            "pickup_location_id", "dropoff_location_id",
        ]
        available = [c for c in keep_cols if c in df.columns]
        return df[available]

    def run(self) -> dict:
        """Execute ingestion. Returns dict with row_count and output_path."""
        url = self._build_url()
        temp_parquet = RAW_DATA_DIR / f"_tmp_{self.year}_{self.month:02d}.parquet"

        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        if not temp_parquet.exists():
            self._download_parquet(url, temp_parquet)
        else:
            logger.info(f"Cache hit: {temp_parquet}")

        logger.info("Reading parquet and filtering to execution date...")
        df = pd.read_parquet(temp_parquet)
        df = self._filter_to_date(df)
        df = self._clean_columns(df)

        if df.empty:
            raise ValueError(
                f"No rows found for {self.execution_date}. "
                "Check that the month file contains this date."
            )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        row_count = len(df)
        logger.info(f"Wrote {row_count:,} rows to {self.output_path}")

        return {
            "row_count": row_count,
            "output_path": str(self.output_path),
        }


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="Ingest NYC Taxi trip data")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    args = parser.parse_args()

    ingestor = TaxiDataIngestor(execution_date="2024-01-15")
    result = ingestor.run()
    print(f"\nDone: {result}")
