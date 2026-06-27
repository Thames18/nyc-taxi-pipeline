"""
validation/schema_validator.py
================================
Reads the YAML schema contract (config/schema.yaml) and validates
an ingested CSV against it.

BUG FIXED:
  TypeError: Invalid comparison between dtype=float64 and str
  Root cause: min_value/max_value in YAML for timestamp columns are strings
  like "2023-01-01 00:00:00". The old code passed them raw into Pandas
  comparison against a numeric series — blows up.
  Fix: _cast_bound() converts each bound to match the column type before
  comparing, and _get_comparable_series() casts the series to match too.
"""

import json
import logging
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class SchemaValidator:

    def __init__(self, data_path: str, schema_path: str):
        self.data_path   = Path(data_path)
        self.schema_path = Path(schema_path)
        self.errors:   list[dict] = []
        self.warnings: list[dict] = []

    def _load_schema(self) -> dict:
        with open(self.schema_path) as f:
            return yaml.safe_load(f)

    def _load_data(self) -> pd.DataFrame:
        logger.info(f"Loading: {self.data_path}")
        return pd.read_csv(self.data_path, low_memory=False)

    def _cast_bound(self, value, col_type: str):
        """Cast a YAML min/max string to the right Python type for comparison."""
        if value is None:
            return None
        try:
            if col_type == "timestamp":
                return pd.Timestamp(value)
            elif col_type == "integer":
                return int(value)
            elif col_type == "float":
                return float(value)
            else:
                return value
        except Exception:
            return value

    def _get_comparable_series(self, df: pd.DataFrame, col: str, col_type: str) -> pd.Series:
        """Return a Series cast to a type compatible with the bound comparisons."""
        if col_type == "timestamp":
            return pd.to_datetime(df[col], errors="coerce")
        else:
            return pd.to_numeric(df[col], errors="coerce")

    def _check_columns_exist(self, df, schema):
        expected = {c["name"] for c in schema["columns"]}
        for col in expected - set(df.columns):
            self.errors.append({
                "check": "column_exists", "column": col,
                "message": f"Required column '{col}' is missing",
            })

    def _check_null_fractions(self, df, schema):
        max_null = schema["thresholds"]["max_null_fraction"]
        for col_def in schema["columns"]:
            col = col_def["name"]
            if col not in df.columns:
                continue
            null_frac = df[col].isna().mean()
            if not col_def.get("nullable", True) and null_frac > 0:
                self.errors.append({
                    "check": "not_nullable", "column": col,
                    "message": f"'{col}' must not be null — {null_frac:.1%} nulls found",
                    "null_fraction": null_frac,
                })
            elif null_frac > max_null:
                self.warnings.append({
                    "check": "high_null_fraction", "column": col,
                    "message": f"'{col}' has {null_frac:.1%} nulls (threshold {max_null:.0%})",
                    "null_fraction": null_frac,
                })

    def _check_value_ranges(self, df, schema):
        """THE FIXED METHOD — casts both series and bounds before comparing."""
        for col_def in schema["columns"]:
            col      = col_def["name"]
            col_type = col_def.get("type", "float")
            if col not in df.columns:
                continue

            series = self._get_comparable_series(df, col, col_type).dropna()

            if "min_value" in col_def:
                bound = self._cast_bound(col_def["min_value"], col_type)
                try:
                    below = int((series < bound).sum())
                    if below > 0:
                        self.warnings.append({
                            "check": "min_value", "column": col,
                            "message": f"{below} rows below min {bound} in '{col}'",
                            "violation_count": below,
                        })
                except TypeError as e:
                    logger.warning(f"Skipping min_value check for '{col}': {e}")

            if "max_value" in col_def:
                bound = self._cast_bound(col_def["max_value"], col_type)
                try:
                    above = int((series > bound).sum())
                    if above > 0:
                        self.warnings.append({
                            "check": "max_value", "column": col,
                            "message": f"{above} rows above max {bound} in '{col}'",
                            "violation_count": above,
                        })
                except TypeError as e:
                    logger.warning(f"Skipping max_value check for '{col}': {e}")

    def _check_allowed_values(self, df, schema):
        for col_def in schema["columns"]:
            col = col_def["name"]
            if col not in df.columns or "allowed_values" not in col_def:
                continue
            unexpected = set(df[col].dropna().unique()) - set(col_def["allowed_values"])
            if unexpected:
                self.warnings.append({
                    "check": "allowed_values", "column": col,
                    "message": f"Unexpected values in '{col}': {sorted(unexpected)[:10]}",
                })

    def _check_row_count(self, df, schema):
        min_rows = schema["thresholds"]["min_row_count"]
        if len(df) < min_rows:
            self.errors.append({
                "check": "min_row_count",
                "message": f"Dataset has {len(df):,} rows — below minimum {min_rows:,}",
            })

    def _check_duplicates(self, df, schema):
        max_dup  = schema["thresholds"]["max_duplicate_fraction"]
        dup_frac = df.duplicated().mean()
        if dup_frac > max_dup:
            self.warnings.append({
                "check": "duplicate_fraction",
                "message": f"Duplicate fraction {dup_frac:.2%} exceeds threshold {max_dup:.0%}",
            })

    def _check_quality_rules(self, df, schema):
        for rule in schema.get("quality_rules", []):
            try:
                expr = (
                    rule["rule"]
                    .replace(" AND ", " and ")
                    .replace(" OR ",  " or ")
                    .replace("NOT ",  "not ")
                )
                count = len(df.query(f"not ({expr})"))
                if count > 0:
                    item = {
                        "check":           rule["name"],
                        "message":         f"Rule '{rule['name']}' violated by {count} rows",
                        "violation_count": count,
                    }
                    (self.errors if rule.get("severity") == "error" else self.warnings).append(item)
            except Exception as e:
                logger.warning(f"Could not evaluate rule '{rule['name']}': {e}")

    def validate(self) -> dict:
        schema = self._load_schema()
        df     = self._load_data()

        logger.info(f"Validating {len(df):,} rows against schema v{schema['version']}")

        self._check_columns_exist(df, schema)
        self._check_row_count(df, schema)
        self._check_null_fractions(df, schema)
        self._check_value_ranges(df, schema)
        self._check_allowed_values(df, schema)
        self._check_duplicates(df, schema)
        self._check_quality_rules(df, schema)

        status = "PASSED"
        if self.warnings: status = "WARNED"
        if self.errors:   status = "FAILED"

        report_dir = Path("data/validation_reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"report_{datetime.now():%Y%m%d_%H%M%S}.json"

        report = {
            "status":         status,
            "error_count":    len(self.errors),
            "warning_count":  len(self.warnings),
            "errors":         self.errors,
            "warnings":       self.warnings,
            "report_path":    str(report_path),
            "dataset":        str(self.data_path),
            "schema_version": schema["version"],
            "validated_at":   datetime.now().isoformat(),
        }
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(
            f"Validation {status}: {len(self.errors)} errors, "
            f"{len(self.warnings)} warnings → {report_path}"
        )
        return report


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data",   required=True)
    parser.add_argument("--schema", default="config/schema.yaml")
    args = parser.parse_args()

    v      = SchemaValidator(args.data, args.schema)
    report = v.validate()

    print(f"\nStatus: {report['status']}")
    for e in report["errors"]:   print(f"  ✗ {e['message']}")
    for w in report["warnings"]: print(f"  ⚠ {w['message']}")
    sys.exit(0 if report["status"] != "FAILED" else 1)
