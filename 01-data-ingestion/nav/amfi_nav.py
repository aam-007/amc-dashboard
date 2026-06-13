"""
amfi_nav.py
-----------
NAV ingestion pipeline for AMC analytics platform.

Downloads the latest AMFI NAV data (NAVAll.txt), normalises it, and
writes raw + processed (Parquet + CSV) layers.

This script is intentionally scoped to NAV tracking only.
Fund master metadata (AMC name, category, plan type) is NOT written here;
it lives in the separate fund master dataset keyed on scheme_code.

Directory layout expected / created at runtime
-----------------------------------------------
amc-dashboard/
  data/
    raw/nav/
    processed/nav/
    exports/
  01-data-ingestion/nav/
      amfi_nav.py          ← this file
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("amfi_nav")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NAVALL_URL: str = "https://portal.amfiindia.com/spages/NAVAll.txt"
REQUEST_TIMEOUT: int = 60          # seconds
MIN_EXPECTED_ROWS: int = 1_000     # abort threshold

# Relative to the project root (two levels above this file's directory)
_HERE = Path(__file__).resolve().parent
PROJECT_ROOT = _HERE.parent.parent  # amc-dashboard/

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "nav"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "nav"
EXPORTS_DIR = PROJECT_ROOT / "data" / "exports"

# ---------------------------------------------------------------------------
# Step 1 – Download
# ---------------------------------------------------------------------------


def download_navall(url: str = NAVALL_URL, timeout: int = REQUEST_TIMEOUT) -> str:
    """
    Fetch the NAVAll.txt file from AMFI and return its text content.

    Parameters
    ----------
    url : str
        Source URL for the AMFI NAV data file.
    timeout : int
        HTTP request timeout in seconds.

    Returns
    -------
    str
        Full raw text content of the downloaded file.

    Raises
    ------
    requests.HTTPError
        If the server returns a non-2xx status code.
    requests.RequestException
        On network-level failures.
    """
    log.info("Downloading NAV data from %s", url)
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.HTTPError as exc:
        log.error("HTTP error downloading NAVAll.txt: %s", exc)
        raise
    except requests.RequestException as exc:
        log.error("Network error downloading NAVAll.txt: %s", exc)
        raise

    size_kb = len(response.content) / 1024
    log.info("Download complete — %.1f KB received", size_kb)
    return response.text


# ---------------------------------------------------------------------------
# Step 2 – Save raw layer
# ---------------------------------------------------------------------------


def save_raw_file(content: str, raw_dir: Path = RAW_DIR) -> Path:
    """
    Persist the downloaded file verbatim to the raw layer.

    The file is stamped with UTC date and time (down to seconds) to support
    multiple runs per day: ``NAVAll_YYYYMMDD_HHMMSS.txt``

    Parameters
    ----------
    content : str
        Raw text content received from AMFI.
    raw_dir : Path
        Destination directory for raw files.

    Returns
    -------
    Path
        Absolute path of the saved file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")
    dest = raw_dir / f"NAVAll_{timestamp}.txt"

    dest.write_text(content, encoding="utf-8")
    log.info("Raw file saved → %s", dest)
    return dest


# ---------------------------------------------------------------------------
# Step 3 – Parse
# ---------------------------------------------------------------------------

# A valid scheme row starts with one or more digits followed immediately by
# a semicolon (the scheme_code field).
_SCHEME_ROW_RE = re.compile(r"^\d+;")


def parse_nav_rows(content: str) -> pd.DataFrame:
    """
    Extract valid scheme rows from NAVAll.txt text and return a raw DataFrame.

    Valid row criteria
    ------------------
    * Starts with digits (scheme_code).
    * Semicolon-separated.
    * Contains at least 6 fields.

    Ignored rows
    ------------
    * Blank lines.
    * AMC / category header lines.
    * Malformed lines with fewer than 6 fields.

    Parameters
    ----------
    content : str
        Full text of NAVAll.txt.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame with columns:
        scheme_code, isin_growth, isin_reinvestment, scheme_name, nav, nav_date
    """
    COLUMNS = [
        "scheme_code",
        "isin_growth",
        "isin_reinvestment",
        "scheme_name",
        "nav",
        "nav_date",
    ]

    records: list[dict] = []
    skipped = 0

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if not _SCHEME_ROW_RE.match(line):
            continue

        parts = line.split(";")
        if len(parts) < 6:
            skipped += 1
            continue

        records.append(dict(zip(COLUMNS, parts[:6])))

    log.info("Parsed %d valid scheme rows (%d malformed lines skipped)", len(records), skipped)

    if not records:
        raise ValueError("No valid scheme rows found in NAVAll.txt — aborting.")

    return pd.DataFrame(records, columns=COLUMNS)


# ---------------------------------------------------------------------------
# Step 4 – Clean
# ---------------------------------------------------------------------------


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise and clean the raw parsed DataFrame.

    Transformations applied
    -----------------------
    1. Strip leading/trailing whitespace from all string columns.
    2. Cast scheme_code to str.
    3. Coerce nav to numeric; rows where nav is non-numeric are dropped.
    4. Coerce nav_date to datetime; rows where nav_date cannot be parsed are dropped.
    5. Drop rows with missing scheme_code.
    6. De-duplicate on scheme_code, keeping the row with the latest nav_date.
    7. Sort by scheme_code ascending.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from ``parse_nav_rows``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with columns:
        scheme_code, scheme_name, nav, nav_date, isin_growth, isin_reinvestment
    """
    # 1. Whitespace
    str_cols = df.select_dtypes("object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # 2. scheme_code as str
    df["scheme_code"] = df["scheme_code"].astype(str)

    # 3. NAV to numeric
    before = len(df)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna(subset=["nav"])
    invalid_nav = before - len(df)
    if invalid_nav:
        log.warning("Dropped %d rows with non-numeric NAV", invalid_nav)

    # 4. nav_date to datetime
    before = len(df)
    df["nav_date"] = pd.to_datetime(df["nav_date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["nav_date"])
    invalid_date = before - len(df)
    if invalid_date:
        log.warning("Dropped %d rows with unparseable nav_date", invalid_date)

    # 5. Drop missing scheme_code (catches empty strings after strip)
    before = len(df)
    df = df[df["scheme_code"].str.len() > 0]
    missing_code = before - len(df)
    if missing_code:
        log.warning("Dropped %d rows with missing scheme_code", missing_code)

    # 6. De-duplicate: keep latest nav_date per scheme_code
    before = len(df)
    df = (
        df.sort_values("nav_date", ascending=False)
        .drop_duplicates(subset=["scheme_code"], keep="first")
    )
    dupes = before - len(df)
    if dupes:
        log.info("Removed %d duplicate scheme_code rows (kept latest nav_date)", dupes)

    # 7. Sort
    df = df.sort_values("scheme_code").reset_index(drop=True)

    # Enforce final column order: required first, optional trailing
    df = df[
        [
            "scheme_code",
            "scheme_name",
            "nav",
            "nav_date",
            "isin_growth",
            "isin_reinvestment",
        ]
    ]

    log.info("Cleaning complete — %d rows retained", len(df))
    return df


# ---------------------------------------------------------------------------
# Step 5 – Validate
# ---------------------------------------------------------------------------


def validate_dataframe(df: pd.DataFrame, min_rows: int = MIN_EXPECTED_ROWS) -> None:
    """
    Run quality checks on the cleaned DataFrame and log a summary.

    Logs
    ----
    * Total row count.
    * Unique scheme_code count.
    * Minimum nav_date.
    * Maximum nav_date.

    Aborts
    ------
    Raises ``ValueError`` if row count is below ``min_rows``.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned NAV DataFrame.
    min_rows : int
        Minimum acceptable row count.

    Raises
    ------
    ValueError
        If the DataFrame has fewer rows than ``min_rows``.
    """
    total = len(df)
    unique_codes = df["scheme_code"].nunique()
    min_date: Optional[pd.Timestamp] = df["nav_date"].min() if total > 0 else None
    max_date: Optional[pd.Timestamp] = df["nav_date"].max() if total > 0 else None

    log.info("── Validation summary ──────────────────────────")
    log.info("  Total rows       : %d", total)
    log.info("  Unique schemes   : %d", unique_codes)
    log.info("  Min nav_date     : %s", min_date.date() if min_date else "N/A")
    log.info("  Max nav_date     : %s", max_date.date() if max_date else "N/A")
    log.info("────────────────────────────────────────────────")

    if total < min_rows:
        raise ValueError(
            f"Validation failed: only {total} rows present (minimum required: {min_rows}). "
            "This likely indicates a bad download or parsing failure."
        )

    log.info("Validation passed.")


# ---------------------------------------------------------------------------
# Step 6 – Save processed layer
# ---------------------------------------------------------------------------


def save_processed(
    df: pd.DataFrame,
    processed_dir: Path = PROCESSED_DIR,
) -> tuple[Path, Path]:
    """
    Write the cleaned DataFrame to the processed layer in Parquet and CSV.

    Files are stamped with UTC date and time (down to seconds):
    ``nav_snapshot_YYYYMMDD_HHMMSS.parquet``
    ``nav_snapshot_YYYYMMDD_HHMMSS.csv``

    Parquet is the primary analytical format.

    Parameters
    ----------
    df : pd.DataFrame
        Validated, cleaned NAV DataFrame.
    processed_dir : Path
        Destination directory for processed files.

    Returns
    -------
    tuple[Path, Path]
        (parquet_path, csv_path) of the saved files.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")

    parquet_path = processed_dir / f"nav_snapshot_{timestamp}.parquet"
    csv_path = processed_dir / f"nav_snapshot_{timestamp}.csv"

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log.info("Parquet saved → %s", parquet_path)

    df.to_csv(csv_path, index=False)
    log.info("CSV saved     → %s", csv_path)

    return parquet_path, csv_path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def main() -> None:
    """
    End-to-end NAV ingestion pipeline.

    Steps
    -----
    1. Download NAVAll.txt from AMFI.
    2. Save raw file unchanged.
    3. Parse valid scheme rows.
    4. Clean and normalise the DataFrame.
    5. Validate quality thresholds.
    6. Save processed Parquet + CSV.
    """
    log.info("========== AMFI NAV ingestion pipeline start ==========")

    try:
        # 1. Download
        content = download_navall()

        # 2. Raw save
        save_raw_file(content)

        # 3. Parse
        raw_df = parse_nav_rows(content)

        # 4. Clean
        clean_df = clean_dataframe(raw_df)

        # 5. Validate
        validate_dataframe(clean_df)

        # 6. Save processed
        parquet_path, csv_path = save_processed(clean_df)

        log.info("Pipeline complete.")
        log.info("  Parquet → %s", parquet_path)
        log.info("  CSV     → %s", csv_path)

    except ValueError as exc:
        log.error("Pipeline aborted: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Unexpected error in NAV pipeline: %s", exc)
        sys.exit(2)

    log.info("========== AMFI NAV ingestion pipeline end ==========")


if __name__ == "__main__":
    main()