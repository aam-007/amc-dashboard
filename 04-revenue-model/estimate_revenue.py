"""
04-revenue-model/estimate_revenue.py

AMC-level management fee revenue estimator.
Revenue = AUM × Effective Yield (industry-wide: 0.75%)

Author: aditya.mishra10@nmims.in
"""

import sqlite3
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

YIELD: float = 0.0075  # 75 bps industry-wide yield assumption
YIELD_PCT: float = 0.75  # display value

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "02-data-warehouse" / "warehouse.db"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "revenue"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def connect_db(db_path: Path) -> sqlite3.Connection:
    """Open a read-only-safe SQLite connection to the warehouse."""
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    logger.info("Connected to warehouse: %s", db_path)
    return conn


def validate_columns(conn: sqlite3.Connection, table: str, required: list[str]) -> None:
    """Raise ValueError if any required column is absent from table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    present = {row["name"] for row in cursor.fetchall()}
    missing = [c for c in required if c not in present]
    if missing:
        raise ValueError(
            f"Table '{table}' is missing expected column(s): {missing}. "
            f"Present columns: {sorted(present)}"
        )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_latest_aum(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Load the most recent AUM snapshot from aum_history.

    The table uses period_end as the snapshot boundary; we take the
    maximum period_end as the 'latest' reporting date and return all
    rows for that date.

    Returns
    -------
    pd.DataFrame with columns: amc_name, average_aum_cr, period_start,
    period_end
    """
    required = ["amc_name", "average_aum_cr", "period_end"]
    validate_columns(conn, "aum_history", required)

    # Identify the latest reporting period
    latest_date: str = conn.execute(
        "SELECT MAX(period_end) AS latest FROM aum_history"
    ).fetchone()["latest"]

    if latest_date is None:
        raise ValueError("aum_history table is empty.")

    logger.info("Latest period_end in aum_history: %s", latest_date)

    query = """
        SELECT
            amc_name,
            average_aum_cr,
            period_start,
            period_end
        FROM aum_history
        WHERE period_end = ?
        ORDER BY amc_name
    """
    df = pd.read_sql_query(query, conn, params=(latest_date,))

    if df.empty:
        raise ValueError(f"No AUM rows found for period_end = {latest_date}.")

    logger.info("Loaded %d AMC rows for period_end = %s", len(df), latest_date)
    return df


# ---------------------------------------------------------------------------
# Revenue calculation
# ---------------------------------------------------------------------------

def estimate_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the fixed yield assumption to compute estimated management fee revenue.

    Parameters
    ----------
    df : DataFrame with at least amc_name, average_aum_cr, period_end

    Returns
    -------
    DataFrame with output columns, sorted by estimated_revenue_cr desc,
    with revenue rank added.
    """
    result = df.copy()

    result["yield_pct"] = YIELD_PCT
    result["estimated_revenue_cr"] = (result["average_aum_cr"] * YIELD).round(2)
    result["average_aum_cr"] = result["average_aum_cr"].round(2)

    result = result.sort_values("estimated_revenue_cr", ascending=False).reset_index(drop=True)
    result.insert(0, "rank", result.index + 1)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result["generated_at"] = generated_at

    # Rename for output schema
    result = result.rename(columns={
        "average_aum_cr": "aum_cr",
        "period_end": "report_date",
    })

    # Final column order
    result = result[[
        "rank",
        "amc_name",
        "aum_cr",
        "yield_pct",
        "estimated_revenue_cr",
        "report_date",
        "generated_at",
    ]]

    return result


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_results(df: pd.DataFrame, export_dir: Path) -> tuple[Path, Path]:
    """
    Write two CSV exports:
      - amc_revenue_<YYYYMMDD_HHMMSS>.csv  (timestamped snapshot)
      - amc_revenue_latest.csv             (overwritten each run)

    Returns
    -------
    (timestamped_path, latest_path)
    """
    export_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped = export_dir / f"amc_revenue_{ts}.csv"
    latest = export_dir / "amc_revenue_latest.csv"

    df.to_csv(timestamped, index=False)
    df.to_csv(latest, index=False)

    logger.info("Exported: %s", timestamped)
    logger.info("Exported: %s", latest)

    return timestamped, latest


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame, ts_path: Path, latest_path: Path) -> None:
    """Print a formatted run summary to stdout."""
    n_amcs = len(df)
    total_aum = df["aum_cr"].sum()
    total_rev = df["estimated_revenue_cr"].sum()

    print()
    print("=" * 50)
    print("AMC Revenue Estimation")
    print("=" * 22)
    print()
    print("Summary:")
    print(f"  AMCs Processed   : {n_amcs}")
    print(f"  Industry AUM     : ₹{total_aum:,.2f} Cr")
    print(f"  Estimated Revenue: ₹{total_rev:,.2f} Cr")
    print()
    print("Files:")
    print(f"  * {latest_path.name}")
    print(f"  * {ts_path.name}")
    print()
    print("=" * 50)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    steps = 5
    print()
    print("=" * 50)
    print("AMC Revenue Estimation")
    print("=" * 22)
    print()

    try:
        print(f"[1/{steps}] Connecting to warehouse...")
        conn = connect_db(DB_PATH)

        print(f"[2/{steps}] Loading latest AUM snapshot...")
        aum_df = load_latest_aum(conn)
        conn.close()

        print(f"[3/{steps}] Estimating revenue...")
        revenue_df = estimate_revenue(aum_df)

        print(f"[4/{steps}] Exporting results...")
        ts_path, latest_path = export_results(revenue_df, EXPORT_DIR)

        print(f"[5/{steps}] Complete")
        print_summary(revenue_df, ts_path, latest_path)

    except FileNotFoundError as exc:
        logger.error("Database not found: %s", exc)
        sys.exit(1)
    except ValueError as exc:
        logger.error("Data validation error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()