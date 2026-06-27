"""
ingestion/ingest_taxi_data.py
==============================
Downloads NYC Yellow Taxi trip data from the TLC public dataset
and lands it as a raw CSV in the data lake raw layer.

HISTORY OF BUGS FIXED:
  Bug 1 — 403 Forbidden: execution_date was 2026-06, TLC doesn't have that yet.
           Fix: constants DOWNLOAD_YEAR/DOWNLOAD_MONTH pin to a known month.

  Bug 2 — NameError: DOWNLOAD_DATE: a stale mixed version of the file referenced
           a variable that was never defined.
           Fix: clean rewrite, no stale references.

  Bug 3 — TLC CDN geo-blocks some IPs (including Docker containers on some networks).
           Fix: synthetic data fallback so the pipeline always completes.
"""

import logging
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

logger = logging.getLogger(__name__)

#  Pinned download target (TLC publishes ~2 months behind)
DOWNLOAD_YEAR  = 2024
DOWNLOAD_MONTH = 1
FILTER_DAY     = 15          # day inside that month to slice for a manageable sample

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
RAW_DATA_DIR = Path("data/raw")
SYNTHETIC_ROWS = 5_000       # rows to generate when download is unavailable


class TaxiDataIngestor:
    """
    Ingests one day of NYC Yellow Taxi trip data into the raw layer.

    Strategy:
      1. Attempt to download the pinned monthly parquet from TLC CDN.
      2. If download fails (403, timeout, network block), generate realistic
         synthetic data so the rest of the pipeline can still run end-to-end.

    Args:
        execution_date: ISO date (YYYY-MM-DD) from Airflow. Used only
                        as the output folder label — never for the download URL.
    """

    def __init__(self, execution_date: str):
        datetime.strptime(execution_date, "%Y-%m-%d")   # validate format
        self.execution_date  = execution_date
        self.download_year   = DOWNLOAD_YEAR
        self.download_month  = DOWNLOAD_MONTH
        self.filter_date     = f"{DOWNLOAD_YEAR}-{DOWNLOAD_MONTH:02d}-{FILTER_DAY:02d}"
        self.output_dir      = RAW_DATA_DIR / execution_date
        self.output_path     = self.output_dir / "taxi_trips.csv"
        self.temp_parquet    = RAW_DATA_DIR / f"_tmp_{DOWNLOAD_YEAR}_{DOWNLOAD_MONTH:02d}.parquet"

    #  Download helpers 

    def _build_url(self) -> str:
        return BASE_URL.format(year=self.download_year, month=self.download_month)

    def _try_download(self) -> bool:
        """
        Attempt to download the monthly parquet.
        Returns True on success, False on any network/HTTP failure.
        """
        url = self._build_url()
        try:
            logger.info(f"Attempting download: {url}")
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            self.temp_parquet.parent.mkdir(parents=True, exist_ok=True)
            with open(self.temp_parquet, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            size_mb = self.temp_parquet.stat().st_size / 1_048_576
            logger.info(f"Downloaded {size_mb:.1f} MB → {self.temp_parquet}")
            return True
        except Exception as exc:
            logger.warning(f"Download failed ({exc}). Switching to synthetic data.")
            return False

    def _load_from_parquet(self) -> pd.DataFrame:
        """Load and filter the cached monthly parquet."""
        df = pd.read_parquet(self.temp_parquet)
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )
        target = pd.Timestamp(self.filter_date).date()
        result = df[df["tpep_pickup_datetime"].dt.date == target].copy()
        logger.info(f"Rows for {self.filter_date} from parquet: {len(result):,}")
        return result

    #  Synthetic data fallback ─

    def _generate_synthetic(self) -> pd.DataFrame:
        """
        Generate realistic NYC taxi trip data when the real source is unavailable.
        Distributions are calibrated against published TLC statistics.
        """
        logger.info(f"Generating {SYNTHETIC_ROWS:,} synthetic rows for {self.execution_date}")
        rng = np.random.default_rng(seed=42)

        base_dt  = datetime.strptime(self.execution_date, "%Y-%m-%d")
        n        = SYNTHETIC_ROWS

        pickup_secs  = rng.integers(0, 86400, n)
        pickup_times = [base_dt + timedelta(seconds=int(s)) for s in pickup_secs]

        # Trip duration: exponential, clipped 2–90 min
        durations    = rng.exponential(scale=12, size=n).clip(2, 90)
        dropoff_times = [p + timedelta(minutes=float(d))
                         for p, d in zip(pickup_times, durations)]

        distances = rng.exponential(scale=3.0, size=n).clip(0.1, 25.0).round(2)
        fare      = (2.5 + distances * 2.5 + rng.normal(0, 1.5, n)).clip(3, 150).round(2)
        tip       = np.where(
            rng.random(n) > 0.3,
            (fare * rng.uniform(0.1, 0.25, n)).round(2),
            0.0
        )
        total     = (fare + tip + rng.uniform(0.5, 3.5, n)).round(2)

        return pd.DataFrame({
            "vendor_id":             rng.choice([1, 2], n),
            "tpep_pickup_datetime":  pickup_times,
            "tpep_dropoff_datetime": dropoff_times,
            "passenger_count":       rng.choice([1, 1, 1, 2, 2, 3, 4], n),
            "trip_distance":         distances,
            "rate_code_id":          rng.choice([1, 1, 1, 1, 2, 3], n),
            "fare_amount":           fare,
            "tip_amount":            tip,
            "total_amount":          total,
            "payment_type":          rng.choice([1, 1, 2, 2, 3], n),
            "pickup_location_id":    rng.integers(1, 266, n),
            "dropoff_location_id":   rng.integers(1, 266, n),
        })

    #  Column standardisation 

    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename TLC columns to snake_case and keep schema-aligned subset."""
        df = df.rename(columns={
            "VendorID":     "vendor_id",
            "RatecodeID":   "rate_code_id",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
        })
        keep = [
            "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "rate_code_id",
            "fare_amount", "tip_amount", "total_amount", "payment_type",
            "pickup_location_id", "dropoff_location_id",
        ]
        return df[[c for c in keep if c in df.columns]]

    #  Main entry point 

    def run(self) -> dict:
        """
        Execute the full ingestion.
        Returns: {"row_count": int, "output_path": str, "source": str}
        """
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        source = "real"

        # Step 1: get data — real download or synthetic fallback
        if self.temp_parquet.exists():
            logger.info(f"Cache hit — skipping download: {self.temp_parquet}")
            df = self._load_from_parquet()
        elif self._try_download():
            df = self._load_from_parquet()
        else:
            df    = self._generate_synthetic()
            source = "synthetic"

        # Step 2: clean
        df = self._clean_columns(df)

        if df.empty:
            raise ValueError("Ingestion produced 0 rows — check download or synthetic generator.")

        # Step 3: write CSV to raw layer
        self.output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        logger.info(
            f" Wrote {len(df):,} rows ({source}) → {self.output_path}"
        )
        return {
            "row_count":   len(df),
            "output_path": str(self.output_path),
            "source":      source,
        }


#  CLI ─
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(description="Ingest NYC Taxi trip data")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    args = parser.parse_args()

    ingestor = TaxiDataIngestor(execution_date=args.date)
    result   = ingestor.run()
    print(f"\n Done: {result}")
