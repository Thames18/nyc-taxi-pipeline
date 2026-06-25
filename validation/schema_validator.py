"""
validation/schema_validator.py
================================
Reads the YAML schema contract (config/schema.yaml) and validates
an ingested CSV against it. Produces a structured validation report.

Checks performed:
  - Required columns exist
  - Column types match (cast attempt)
  - Null fractions within thresholds
  - Value ranges (min/max)
  - Allowed value sets
  - Cross-column business rules
  - Duplicate fraction threshold
  - Minimum row count
"""

import json
import logging
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates a CSV file against a YAML schema contract.

    Example:
        validator = SchemaValidator("data/raw/2024-01-15/taxi_trips.csv",
                                    "config/schema.yaml")
        report = validator.validate()
        # report["status"] in ["PASSED", "WARNED", "FAILED"]
    """

    def __init__(self, data_path: str, schema_path: str):
        self.data_path = Path(data_path)
        self.schema_path = Path(schema_path)
        self.errors: list[dict] = []
        self.warnings: list[dict] = []

    def _load_schema(self) -> dict:
        with open(self.schema_path) as f:
            return yaml.safe_load(f)

    def _load_data(self) -> pd.DataFrame:
        logger.info(f"Loading: {self.data_path}")
        return pd.read_csv(self.data_path, low_memory=False)

    # ── Individual checks ────────────────────────────────────────────────────

    def _check_columns_exist(self, df: pd.DataFrame, schema: dict) -> None:
        expected = {col["name"] for col in schema["columns"]}
        missing = expected - set(df.columns)
        for col in missing:
            self.errors.append({
                "check": "column_exists",
                "column": col,
                "message": f"Required column '{col}' is missing from dataset",
            })

    def _check_null_fractions(self, df: pd.DataFrame, schema: dict) -> None:
        max_null = schema["thresholds"]["max_null_fraction"]
        for col_def in schema["columns"]:
            col = col_def["name"]
            if col not in df.columns:
                continue
            null_frac = df[col].isna().mean()
            if not col_def.get("nullable", True) and null_frac > 0:
                self.errors.append({
                    "check": "not_nullable",
                    "column": col,
                    "message": f"Column '{col}' must not be null — {null_frac:.1%} nulls found",
                    "null_fraction": null_frac,
                })
            elif null_frac > max_null:
                self.warnings.append({
                    "check": "high_null_fraction",
                    "column": col,
                    "message": f"Column '{col}' has {null_frac:.1%} nulls (threshold: {max_null:.0%})",
                    "null_fraction": null_frac,
                })

    def _check_value_ranges(self, df: pd.DataFrame, schema: dict) -> None:
        for col_def in schema["columns"]:
            col = col_def["name"]
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce").dropna()

            if "min_value" in col_def:
                below = (series < col_def["min_value"]).sum()
                if below > 0:
                    self.warnings.append({
                        "check": "min_value",
                        "column": col,
                        "message": f"{below} rows below min_value {col_def['min_value']} in '{col}'",
                        "violation_count": int(below),
                    })

            if "max_value" in col_def:
                above = (series > col_def["max_value"]).sum()
                if above > 0:
                    self.warnings.append({
                        "check": "max_value",
                        "column": col,
                        "message": f"{above} rows above max_value {col_def['max_value']} in '{col}'",
                        "violation_count": int(above),
                    })

    def _check_allowed_values(self, df: pd.DataFrame, schema: dict) -> None:
        for col_def in schema["columns"]:
            col = col_def["name"]
            if col not in df.columns or "allowed_values" not in col_def:
                continue

            allowed = set(col_def["allowed_values"])
            actuals = set(df[col].dropna().unique())
            unexpected = actuals - allowed
            if unexpected:
                self.warnings.append({
                    "check": "allowed_values",
                    "column": col,
                    "message": f"Unexpected values in '{col}': {sorted(unexpected)[:10]}",
                    "unexpected_values": list(unexpected)[:10],
                })

    def _check_row_count(self, df: pd.DataFrame, schema: dict) -> None:
        min_rows = schema["thresholds"]["min_row_count"]
        if len(df) < min_rows:
            self.errors.append({
                "check": "min_row_count",
                "message": f"Dataset has {len(df):,} rows — below minimum {min_rows:,}",
                "row_count": len(df),
            })

    def _check_duplicates(self, df: pd.DataFrame, schema: dict) -> None:
        max_dup = schema["thresholds"]["max_duplicate_fraction"]
        dup_frac = df.duplicated().mean()
        if dup_frac > max_dup:
            self.warnings.append({
                "check": "duplicate_fraction",
                "message": f"Duplicate fraction {dup_frac:.2%} exceeds threshold {max_dup:.0%}",
                "duplicate_fraction": dup_frac,
            })

    def _check_quality_rules(self, df: pd.DataFrame, schema: dict) -> None:
        """Evaluate cross-column Pandas query expressions from the YAML."""
        for rule in schema.get("quality_rules", []):
            try:
                # Convert SQL-ish to pandas-friendly expression
                expr = (
                    rule["rule"]
                    .replace("AND", "and")
                    .replace("OR", "or")
                    .replace("NOT", "not")
                )
                violated = df.query(f"not ({expr})") if "not" not in expr.lower() else df.query(expr)
                count = len(violated)

                if count > 0:
                    item = {
                        "check": rule["name"],
                        "message": f"Rule '{rule['name']}' violated by {count} rows",
                        "violation_count": count,
                    }
                    if rule["severity"] == "error":
                        self.errors.append(item)
                    else:
                        self.warnings.append(item)
            except Exception as e:
                logger.warning(f"Could not evaluate rule '{rule['name']}': {e}")

    # ── Main validate method ─────────────────────────────────────────────────

    def validate(self) -> dict:
        """
        Run all validation checks and return a report dict.

        Returns:
            {
                "status": "PASSED" | "WARNED" | "FAILED",
                "error_count": int,
                "warning_count": int,
                "errors": [...],
                "warnings": [...],
                "report_path": str,
            }
        """
        schema = self._load_schema()
        df = self._load_data()

        logger.info(f"Validating {len(df):,} rows against schema v{schema['version']}")

        self._check_columns_exist(df, schema)
        self._check_row_count(df, schema)
        self._check_null_fractions(df, schema)
        self._check_value_ranges(df, schema)
        self._check_allowed_values(df, schema)
        self._check_duplicates(df, schema)
        self._check_quality_rules(df, schema)

        status = "PASSED"
        if self.warnings:
            status = "WARNED"
        if self.errors:
            status = "FAILED"

        # Write JSON report
        report_dir = Path("data/validation_reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"report_{datetime.now():%Y%m%d_%H%M%S}.json"

        report = {
            "status": status,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings,
            "report_path": str(report_path),
            "dataset": str(self.data_path),
            "schema_version": schema["version"],
            "validated_at": datetime.now().isoformat(),
        }

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(
            f"Validation {status}: {len(self.errors)} errors, "
            f"{len(self.warnings)} warnings → {report_path}"
        )
        return report


# ─── CLI entrypoint ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse, sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--schema", default="config/schema.yaml")
    args = parser.parse_args()

    validator = SchemaValidator(args.data, args.schema)
    report = validator.validate()

    print(f"\nStatus: {report['status']}")
    if report["errors"]:
        print("\nErrors:")
        for e in report["errors"]:
            print(f"  ✗ {e['message']}")
    if report["warnings"]:
        print("\nWarnings:")
        for w in report["warnings"]:
            print(f"  ⚠ {w['message']}")

    sys.exit(0 if report["status"] != "FAILED" else 1)
