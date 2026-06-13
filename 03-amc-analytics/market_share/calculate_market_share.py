"""
calculate_market_share.py
=========================
AMC Market Share Calculator — Phase 3 Analytics Module

Calculates AMC-level market share for the latest available AUM reporting period
from the AMC Analytics data warehouse.

Author : aditya.mishra10@nmims.in
Project: AMC Analytics Pipeline
Module : 03-amc-analytics/market_share/
Python : 3.11+
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

# Script lives at: <project_root>/03-amc-analytics/market_share/calculate_market_share.py
# Warehouse lives at: <project_root>/02-data-warehouse/warehouse.db
# Exports go to:   <project_root>/data/exports/market_share/

_SCRIPT_DIR: Path = Path(__file__).resolve().parent
_PROJECT_ROOT: Path = _SCRIPT_DIR.parents[1]  # two levels up from this file


def get_db_path() -> Path:
    """
    Resolve the absolute path to the SQLite warehouse.

    Returns
    -------
    Path
        Absolute path to warehouse.db.

    Raises
    ------
    FileNotFoundError
        If the warehouse file does not exist at the expected location.
    """
    db_path: Path = _PROJECT_ROOT / "02-data-warehouse" / "warehouse.db"
    if not db_path.exists():
        raise FileNotFoundError(
            f"Warehouse not found at expected path: {db_path}\n"
            "Ensure Phase 2 (load_data.py) has been executed successfully."
        )
    return db_path


def get_export_dir() -> Path:
    """
    Resolve (and create if missing) the export directory.

    Returns
    -------
    Path
        Absolute path to data/exports/market_share/.
    """
    export_dir: Path = _PROJECT_ROOT / "data" / "exports" / "market_share"
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Open a read-only SQLite connection to the warehouse.

    Parameters
    ----------
    db_path : Path
        Absolute path to the SQLite database file.

    Returns
    -------
    sqlite3.Connection
        Active database connection with Row factory enabled.

    Raises
    ------
    sqlite3.OperationalError
        If the database cannot be opened.
    """
    logger.info("Connecting to warehouse: %s", db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _assert_table_exists(conn: sqlite3.Connection, table: str) -> None:
    """
    Assert that a table exists in the connected database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    table : str
        Table name to check.

    Raises
    ------
    RuntimeError
        If the table does not exist.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    cursor = conn.execute(query, (table,))
    if cursor.fetchone() is None:
        raise RuntimeError(
            f"Required table '{table}' not found in warehouse. "
            "Ensure Phase 2 schema creation and data loading are complete."
        )


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------

def get_latest_period(conn: sqlite3.Connection) -> str:
    """
    Determine the latest AUM reporting period end date in the warehouse.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.

    Returns
    -------
    str
        ISO-formatted date string (YYYY-MM-DD) of the latest period_end.

    Raises
    ------
    RuntimeError
        If aum_history is empty.
    """
    _assert_table_exists(conn, "aum_history")

    cursor = conn.execute("SELECT MAX(period_end) AS latest FROM aum_history")
    row = cursor.fetchone()

    if row is None or row["latest"] is None:
        raise RuntimeError(
            "aum_history table is present but contains no records. "
            "Run the AUM ingestion pipeline before executing this script."
        )

    latest: str = row["latest"]
    logger.info("Latest AUM period_end detected: %s", latest)
    return latest


def load_aum_data(conn: sqlite3.Connection, latest_period_end: str) -> pd.DataFrame:
    """
    Load all AMC AUM records for the latest reporting period.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    latest_period_end : str
        ISO date string identifying the latest period_end.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: amc_name, average_aum_cr, period_start, period_end.

    Raises
    ------
    ValueError
        If the result set is empty or contains non-positive AUM values that
        make market-share calculation impossible.
    """
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
    df: pd.DataFrame = pd.read_sql_query(
        query,
        conn,
        params=(latest_period_end,),
    )

    if df.empty:
        raise ValueError(
            f"No AUM records found for period_end = '{latest_period_end}'. "
            "The warehouse may be corrupted or the AUM load was incomplete."
        )

    # Coerce to numeric; surface any non-numeric corruption
    df["average_aum_cr"] = pd.to_numeric(df["average_aum_cr"], errors="coerce")
    n_invalid: int = df["average_aum_cr"].isna().sum()
    if n_invalid > 0:
        raise ValueError(
            f"{n_invalid} record(s) have non-numeric average_aum_cr values. "
            "Inspect aum_history for data quality issues before proceeding."
        )

    n_negative: int = (df["average_aum_cr"] < 0).sum()
    if n_negative > 0:
        logger.warning(
            "%d AMC(s) have negative AUM values — they will be included in "
            "the industry total as-is. Verify source data.",
            n_negative,
        )

    logger.info(
        "Loaded %d AMC records for period %s → %s",
        len(df),
        df["period_start"].iloc[0],
        latest_period_end,
    )
    return df


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def calculate_market_share(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute industry-total AUM and each AMC's percentage market share.

    Parameters
    ----------
    df : pd.DataFrame
        Raw AUM data with column ``average_aum_cr``.

    Returns
    -------
    pd.DataFrame
        Input DataFrame augmented with ``market_share_pct`` column.

    Raises
    ------
    ValueError
        If the industry total AUM is zero, making percentage undefined.
    """
    industry_aum: float = df["average_aum_cr"].sum()

    if industry_aum == 0.0:
        raise ValueError(
            "Industry AUM sums to zero — cannot compute market share percentages. "
            "Verify that average_aum_cr values are correctly populated."
        )

    logger.info(
        "Industry total AUM: ₹%,.2f Cr across %d AMCs",
        industry_aum,
        len(df),
    )

    df = df.copy()
    df["market_share_pct"] = (df["average_aum_cr"] / industry_aum) * 100
    return df


def assign_rankings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign dense rankings to AMCs ordered by AUM descending.

    Rank 1 is assigned to the AMC with the highest AUM.
    Dense ranking means no gaps in rank sequence for ties.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with ``average_aum_cr`` column.

    Returns
    -------
    pd.DataFrame
        DataFrame with ``rank`` column added, sorted by rank ascending.
    """
    df = df.copy()
    df["rank"] = (
        df["average_aum_cr"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    df.sort_values("rank", ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(
        "Rankings assigned. #1: %s (₹%,.2f Cr, %.4f%%)",
        df.loc[0, "amc_name"],
        df.loc[0, "average_aum_cr"],
        df.loc[0, "market_share_pct"],
    )
    return df


def _build_output_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and order final output columns.

    Parameters
    ----------
    df : pd.DataFrame
        Fully processed DataFrame.

    Returns
    -------
    pd.DataFrame
        Output-ready DataFrame with canonical column ordering.
    """
    return df[[
        "rank",
        "amc_name",
        "average_aum_cr",
        "market_share_pct",
        "period_start",
        "period_end",
    ]].copy()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_results(df: pd.DataFrame, export_dir: Path) -> tuple[Path, Path]:
    """
    Export market share results to CSV files.

    Writes two files:
    - ``market_share_latest.csv``  — always-overwritten canonical output.
    - ``market_share_<YYYYMMDD_HHMMSS>.csv`` — timestamped archival copy.

    Parameters
    ----------
    df : pd.DataFrame
        Final output DataFrame.
    export_dir : Path
        Directory to write exports into (created if missing).

    Returns
    -------
    tuple[Path, Path]
        (latest_path, timestamped_path) as absolute Path objects.
    """
    timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

    latest_path: Path = export_dir / "market_share_latest.csv"
    timestamped_path: Path = export_dir / f"market_share_{timestamp}.csv"

    csv_kwargs: dict = {
        "index": False,
        "float_format": "%.6f",
        "encoding": "utf-8-sig",  # BOM for Excel compatibility
    }

    df.to_csv(latest_path, **csv_kwargs)
    df.to_csv(timestamped_path, **csv_kwargs)

    logger.info("Exported latest CSV    : %s", latest_path)
    logger.info("Exported timestamped CSV: %s", timestamped_path)

    return latest_path, timestamped_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full AMC market share calculation pipeline.

    Pipeline stages:
    1. Resolve and validate paths.
    2. Connect to the warehouse.
    3. Detect the latest AUM reporting period.
    4. Load AUM data for that period.
    5. Calculate market share percentages.
    6. Assign dense rankings.
    7. Export results to CSV.
    8. Print a formatted summary table.

    Exits with code 1 on any unrecoverable error.
    """
    logger.info("=" * 60)
    logger.info("AMC Market Share Calculator — starting")
    logger.info("=" * 60)

    try:
        # ── 1. Paths ────────────────────────────────────────────────────────
        db_path: Path = get_db_path()
        export_dir: Path = get_export_dir()

        # ── 2. Connect ──────────────────────────────────────────────────────
        conn: sqlite3.Connection = get_connection(db_path)

        with conn:
            # ── 3. Latest period ────────────────────────────────────────────
            latest_period_end: str = get_latest_period(conn)

            # ── 4. Load raw AUM ─────────────────────────────────────────────
            raw_df: pd.DataFrame = load_aum_data(conn, latest_period_end)

        # ── 5. Market share ─────────────────────────────────────────────────
        df: pd.DataFrame = calculate_market_share(raw_df)

        # ── 6. Rankings ──────────────────────────────────────────────────────
        df = assign_rankings(df)

        # ── 7. Final column selection ────────────────────────────────────────
        output_df: pd.DataFrame = _build_output_df(df)

        # ── 8. Export ────────────────────────────────────────────────────────
        latest_path, ts_path = export_results(output_df, export_dir)

        # ── 9. Console summary ───────────────────────────────────────────────
        _print_summary(output_df, latest_path, ts_path)

        logger.info("=" * 60)
        logger.info("Market share calculation completed successfully.")
        logger.info("=" * 60)

    except FileNotFoundError as exc:
        logger.error("Database not found: %s", exc)
        sys.exit(1)
    except RuntimeError as exc:
        logger.error("Runtime error: %s", exc)
        sys.exit(1)
    except ValueError as exc:
        logger.error("Data validation error: %s", exc)
        sys.exit(1)
    except sqlite3.Error as exc:
        logger.error("SQLite error: %s", exc)
        sys.exit(1)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)


def _print_summary(
    df: pd.DataFrame,
    latest_path: Path,
    ts_path: Path,
) -> None:
    """
    Print a formatted market share summary table to stdout.

    Parameters
    ----------
    df : pd.DataFrame
        Final ranked market share DataFrame.
    latest_path : Path
        Path to the canonical latest CSV export.
    ts_path : Path
        Path to the timestamped CSV export.
    """
    period_start: str = df["period_start"].iloc[0]
    period_end: str = df["period_end"].iloc[0]
    industry_aum: float = df["average_aum_cr"].sum()
    amc_count: int = len(df)

    divider: str = "─" * 80

    print()
    print(divider)
    print("  AMC MARKET SHARE REPORT")
    print(f"  Period : {period_start}  →  {period_end}")
    print(f"  Industry AUM : ₹{industry_aum:>16,.2f} Cr")
    print(f"  AMC Count    : {amc_count}")
    print(divider)
    print(
        f"  {'Rank':>4}  {'AMC Name':<45}  {'AUM (₹ Cr)':>14}  {'Share %':>8}"
    )
    print(divider)

    for _, row in df.iterrows():
        print(
            f"  {int(row['rank']):>4}  {row['amc_name']:<45}  "
            f"{row['average_aum_cr']:>14,.2f}  {row['market_share_pct']:>7.4f}%"
        )

    print(divider)
    print(f"\n  Exports:")
    print(f"    Latest      → {latest_path}")
    print(f"    Timestamped → {ts_path}")
    print()


if __name__ == "__main__":
    main()