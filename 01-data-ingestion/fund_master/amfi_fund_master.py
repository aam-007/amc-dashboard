"""
amfi_fund_master.py
-------------------
Ingestion pipeline for the AMFI NAV master file (NAVAll.txt).
Downloads, parses, normalises, and persists a fund master dataset.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NAVALL_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
RAW_DIR = Path("data/raw/fund_master")
PROCESSED_DIR = Path("data/processed/fund_master")
PROCESSED_FILENAME = "fund_master_latest.csv"

REQUEST_TIMEOUT = 60          # seconds
REQUEST_RETRIES = 3
RETRY_BACKOFF = 2.0           # seconds between retries

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex helpers (compiled once at import time)
# ---------------------------------------------------------------------------

# A scheme row always starts with one-or-more digits followed by a semicolon.
_RE_SCHEME_ROW = re.compile(r"^\d+;")

# Category header: e.g. "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)"
_RE_CATEGORY_HEADER = re.compile(
    r"^Open Ended Schemes\s*\((.+?)\)\s*$"
    r"|^Close Ended Schemes\s*\((.+?)\)\s*$"
    r"|^Interval Fund\s*\((.+?)\)\s*$",
    re.IGNORECASE,
)

# AMC header: a non-empty line that is NOT a scheme row and NOT a category header.
# Detected by exclusion after category / scheme checks.

_RE_PLAN_DIRECT = re.compile(r"\bDIRECT\b", re.IGNORECASE)
_RE_PLAN_REGULAR = re.compile(r"\bREGULAR\b", re.IGNORECASE)
_RE_PLAN_RETAIL = re.compile(r"\bRETAIL\b", re.IGNORECASE)

_RE_OPT_GROWTH = re.compile(r"\bGROWTH\b", re.IGNORECASE)
_RE_OPT_IDCW = re.compile(
    r"\b(IDCW|DIVIDEND|DIV)\b"
    r"|\b(DAILY|WEEKLY|FORTNIGHTLY|MONTHLY|QUARTERLY|HALF[- ]?YEARLY|ANNUAL)\s+(IDCW|DIVIDEND)\b",
    re.IGNORECASE,
)
_RE_OPT_BONUS = re.compile(r"\bBONUS\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Step 1 – Download
# ---------------------------------------------------------------------------

def download_navall(
    url: str = NAVALL_URL,
    retries: int = REQUEST_RETRIES,
    timeout: int = REQUEST_TIMEOUT,
) -> str:
    """
    Download the AMFI NAVAll.txt file.

    Parameters
    ----------
    url : str
        Source URL.
    retries : int
        Number of retry attempts on transient network failure.
    timeout : int
        Per-request timeout in seconds.

    Returns
    -------
    str
        Raw text content of the downloaded file.

    Raises
    ------
    RuntimeError
        If all retry attempts fail.
    """
    import time

    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            log.info("Downloading NAVAll.txt (attempt %d/%d) …", attempt, retries)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            # AMFI files are typically Windows-1252 encoded
            response.encoding = response.apparent_encoding or "utf-8"
            log.info(
                "Download complete — %.2f KB received.",
                len(response.content) / 1024,
            )
            return response.text
        except requests.RequestException as exc:
            last_exc = exc
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < retries:
                time.sleep(RETRY_BACKOFF * attempt)

    raise RuntimeError(
        f"Failed to download NAVAll.txt after {retries} attempts."
    ) from last_exc


# ---------------------------------------------------------------------------
# Step 2 – Save raw file
# ---------------------------------------------------------------------------

def save_raw_file(content: str, raw_dir: Path = RAW_DIR) -> Path:
    """
    Persist the raw downloaded content to disk with a datestamp suffix.

    Parameters
    ----------
    content : str
        Raw text to write.
    raw_dir : Path
        Target directory.

    Returns
    -------
    Path
        Full path of the saved file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    datestamp = datetime.today().strftime("%Y%m%d")
    path = raw_dir / f"NAVAll_{datestamp}.txt"
    path.write_text(content, encoding="utf-8")
    log.info("Raw file saved → %s", path)
    return path


# ---------------------------------------------------------------------------
# Step 3 – Parse
# ---------------------------------------------------------------------------

def _extract_category(line: str) -> Optional[str]:
    """
    Return the category name from a category-header line, or None.

    Example
    -------
    "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)"
    → "Debt Scheme - Banking and PSU Fund"
    """
    m = _RE_CATEGORY_HEADER.match(line.strip())
    if m:
        return next(g for g in m.groups() if g is not None).strip()
    return None


def _is_amc_header(line: str) -> bool:
    """
    Return True if the line looks like an AMC name header.

    Heuristic: non-empty, no semicolons, not a category header,
    ends with "Mutual Fund" (common pattern) or is otherwise a
    plain-text non-scheme line.
    """
    stripped = line.strip()
    if not stripped:
        return False
    if _RE_SCHEME_ROW.match(stripped):
        return False
    if _RE_CATEGORY_HEADER.match(stripped):
        return False
    # Column header row emitted by AMFI
    if stripped.lower().startswith("scheme code"):
        return False
    return True


def parse_navall(content: str) -> pd.DataFrame:
    """
    Parse raw NAVAll.txt content into a DataFrame.

    State machine:
      - Tracks ``current_category`` and ``current_amc`` as context.
      - Emits one record per valid scheme row.

    Parameters
    ----------
    content : str
        Full text of NAVAll.txt.

    Returns
    -------
    pd.DataFrame
        Raw parsed records with columns:
        scheme_code, scheme_name, amc_name, category,
        isin_growth, isin_reinvestment, nav, nav_date.
    """
    records: list[dict] = []
    skipped = 0
    current_category: str = ""
    current_amc: str = ""

    for lineno, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()

        # Skip empty lines
        if not line:
            continue

        # ----- Category header -----
        category = _extract_category(line)
        if category is not None:
            current_category = category
            continue

        # ----- Scheme row -----
        if _RE_SCHEME_ROW.match(line):
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 6:
                log.debug("Line %d: malformed scheme row (fields=%d) — skipped.", lineno, len(parts))
                skipped += 1
                continue
            try:
                records.append(
                    {
                        "scheme_code": parts[0],
                        "isin_growth": parts[1] if parts[1] not in ("", "-") else None,
                        "isin_reinvestment": parts[2] if parts[2] not in ("", "-") else None,
                        "scheme_name": parts[3],
                        "nav": parts[4],
                        "nav_date": parts[5],
                        "amc_name": current_amc,
                        "category": current_category,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("Line %d: unexpected parse error (%s) — skipped.", lineno, exc)
                skipped += 1
            continue

        # ----- AMC header (by exclusion) -----
        if _is_amc_header(line):
            current_amc = line
            continue

        # Anything else is silently ignored (e.g. blank separators, notes)

    log.info(
        "Parsing complete — %d records extracted, %d rows skipped.",
        len(records),
        skipped,
    )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Step 4 – Derive plan_type
# ---------------------------------------------------------------------------

def derive_plan_type(scheme_name: str) -> str:
    """
    Infer plan type from the scheme name.

    Returns 'Direct', 'Regular', 'Retail', or 'Unknown'.
    """
    if _RE_PLAN_DIRECT.search(scheme_name):
        return "Direct"
    if _RE_PLAN_REGULAR.search(scheme_name):
        return "Regular"
    if _RE_PLAN_RETAIL.search(scheme_name):
        return "Retail"
    return "Unknown"


# ---------------------------------------------------------------------------
# Step 5 – Derive option_type
# ---------------------------------------------------------------------------

def derive_option_type(scheme_name: str) -> str:
    """
    Infer option type from the scheme name.

    Precedence: Growth > IDCW variants > Bonus > Other.
    IDCW frequency qualifiers (Daily / Weekly / Monthly …) are normalised
    to plain 'IDCW'.
    """
    if _RE_OPT_GROWTH.search(scheme_name):
        return "Growth"
    if _RE_OPT_IDCW.search(scheme_name):
        return "IDCW"
    if _RE_OPT_BONUS.search(scheme_name):
        return "Bonus"
    return "Other"


# ---------------------------------------------------------------------------
# Step 6 – Clean DataFrame
# ---------------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise, type-cast, and deduplicate the parsed DataFrame.

    Steps
    -----
    1. Strip whitespace from all string columns.
    2. Drop rows with a missing scheme_code or scheme_name.
    3. Cast scheme_code to str, nav to float, nav_date to datetime.
    4. Derive plan_type and option_type.
    5. Deduplicate on scheme_code, keeping the latest nav_date.
    6. Sort by amc_name, then scheme_name.
    7. Reorder columns to the canonical output schema.
    """
    if df.empty:
        log.warning("Empty DataFrame received — nothing to clean.")
        return df

    # -- 1. Strip whitespace --------------------------------------------------
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # -- 2. Drop rows missing key identifiers ---------------------------------
    before = len(df)
    df = df.dropna(subset=["scheme_code", "scheme_name"])
    df = df[df["scheme_code"].str.len() > 0]
    df = df[df["scheme_name"].str.len() > 0]
    dropped = before - len(df)
    if dropped:
        log.info("Dropped %d rows with missing scheme_code/scheme_name.", dropped)

    # -- 3. Type casting -------------------------------------------------------
    df["scheme_code"] = df["scheme_code"].astype(str)

    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    nav_nulls = df["nav"].isna().sum()
    if nav_nulls:
        log.info("%d rows have non-numeric NAV — retained with NaN.", nav_nulls)

    df["nav_date"] = pd.to_datetime(df["nav_date"], dayfirst=True, errors="coerce")
    date_nulls = df["nav_date"].isna().sum()
    if date_nulls:
        log.info("%d rows have unparseable nav_date — retained with NaT.", date_nulls)

    # -- 4. Derived fields -----------------------------------------------------
    df["plan_type"] = df["scheme_name"].apply(derive_plan_type)
    df["option_type"] = df["scheme_name"].apply(derive_option_type)

    # -- 5. Deduplicate --------------------------------------------------------
    before_dedup = len(df)
    # Sort so that for each scheme_code the latest nav_date comes first
    df = df.sort_values("nav_date", ascending=False, na_position="last")
    df = df.drop_duplicates(subset="scheme_code", keep="first")
    dupes = before_dedup - len(df)
    if dupes:
        log.info("Removed %d duplicate scheme_code rows (kept latest).", dupes)

    # -- 6. Sort ---------------------------------------------------------------
    df = df.sort_values(["amc_name", "scheme_name"], ignore_index=True)

    # -- 7. Column order -------------------------------------------------------
    output_cols = [
        "scheme_code",
        "scheme_name",
        "amc_name",
        "category",
        "isin_growth",
        "isin_reinvestment",
        "plan_type",
        "option_type",
        "nav",
        "nav_date",
    ]
    df = df[output_cols]

    log.info("Clean DataFrame — %d records, %d columns.", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Step 7 – Save processed file
# ---------------------------------------------------------------------------

def save_processed(df: pd.DataFrame, processed_dir: Path = PROCESSED_DIR) -> Path:
    """
    Write the cleaned DataFrame to CSV.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned fund master data.
    processed_dir : Path
        Target directory.

    Returns
    -------
    Path
        Full path of the written CSV.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / PROCESSED_FILENAME
    df.to_csv(path, index=False)
    log.info(
        "Processed file saved → %s  (%d rows × %d cols)",
        path,
        len(df),
        len(df.columns),
    )
    return path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full ingestion pipeline:
      download → save raw → parse → clean → save processed.
    """
    log.info("=" * 60)
    log.info("AMFI Fund Master Ingestion Pipeline — started")
    log.info("=" * 60)

    # 1. Download
    raw_content = download_navall()

    # 2. Save raw
    save_raw_file(raw_content)

    # 3. Parse
    df_raw = parse_navall(raw_content)
    if df_raw.empty:
        log.error("No records parsed — aborting pipeline.")
        sys.exit(1)

    # 4. Clean
    df_clean = clean_dataframe(df_raw)
    if df_clean.empty:
        log.error("No records survived cleaning — aborting pipeline.")
        sys.exit(1)

    # 5. Save processed
    save_processed(df_clean)

    log.info("=" * 60)
    log.info("Pipeline complete.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()