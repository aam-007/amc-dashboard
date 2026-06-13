"""
rank_amcs.py
============
AMC Rankings Module — Phase 3 Analytics

Produces leaderboard-style AMC rankings, Top 10 cut, market share leadership
table, and concentration metrics from the latest available AUM period in the
data warehouse.

Author : aditya.mishra10@nmims.in
Project: AMC Analytics Pipeline
Module : 03-amc-analytics/rankings/
Python : 3.11+
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from dataclasses import dataclass
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
# Constants
# ---------------------------------------------------------------------------

# Script: <project_root>/03-amc-analytics/rankings/rank_amcs.py
_SCRIPT_DIR: Path = Path(__file__).resolve().parent
_PROJECT_ROOT: Path = _SCRIPT_DIR.parents[1]

_TOP_N: int = 10
_CONCENTRATION_TIERS: tuple[int, ...] = (3, 5, 10)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConcentrationMetrics:
    """Immutable snapshot of market concentration figures."""

    top_3_share_pct: float
    top_5_share_pct: float
    top_10_share_pct: float
    industry_aum_cr: float
    amc_count: int
    period_start: str
    period_end: str


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


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
        If the warehouse file does not exist.
    """
    db_path: Path = _PROJECT_ROOT / "02-data-warehouse" / "warehouse.db"
    if not db_path.exists():
        raise FileNotFoundError(
            f"Warehouse not found: {db_path}\n"
            "Ensure Phase 2 (load_data.py) has been executed successfully."
        )
    return db_path


def get_export_dir() -> Path:
    """
    Resolve and create (if missing) the rankings export directory.

    Returns
    -------
    Path
        Absolute path to data/exports/rankings/.
    """
    export_dir: Path = _PROJECT_ROOT / "data" / "exports" / "rankings"
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
        Active connection with Row factory enabled.

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
    Assert that a required table exists in the connected database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    table : str
        Table name to verify.

    Raises
    ------
    RuntimeError
        If the table is absent from the schema.
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
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
    Identify the latest AUM reporting period end date in the warehouse.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.

    Returns
    -------
    str
        ISO date string (YYYY-MM-DD) of the most recent period_end.

    Raises
    ------
    RuntimeError
        If aum_history is empty or the table is missing.
    """
    _assert_table_exists(conn, "aum_history")

    cursor = conn.execute("SELECT MAX(period_end) AS latest FROM aum_history")
    row = cursor.fetchone()

    if row is None or row["latest"] is None:
        raise RuntimeError(
            "aum_history is present but empty. "
            "Run the AUM ingestion pipeline before executing this script."
        )

    latest: str = row["latest"]
    logger.info("Latest AUM period_end: %s", latest)
    return latest


def load_aum_data(conn: sqlite3.Connection, latest_period_end: str) -> pd.DataFrame:
    """
    Load all AMC AUM records for the specified period end date.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    latest_period_end : str
        ISO date string for the target period_end.

    Returns
    -------
    pd.DataFrame
        Columns: amc_name, average_aum_cr, period_start, period_end.

    Raises
    ------
    ValueError
        If no records exist for the period, or AUM values are corrupt.
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
        query, conn, params=(latest_period_end,)
    )

    if df.empty:
        raise ValueError(
            f"No AUM records found for period_end = '{latest_period_end}'. "
            "The warehouse may be corrupted or the AUM load was incomplete."
        )

    df["average_aum_cr"] = pd.to_numeric(df["average_aum_cr"], errors="coerce")
    n_invalid: int = int(df["average_aum_cr"].isna().sum())
    if n_invalid > 0:
        raise ValueError(
            f"{n_invalid} record(s) contain non-numeric average_aum_cr values. "
            "Inspect aum_history for data quality issues."
        )

    n_negative: int = int((df["average_aum_cr"] < 0).sum())
    if n_negative > 0:
        logger.warning(
            "%d AMC(s) have negative AUM — included in totals as-is. "
            "Verify source data.",
            n_negative,
        )

    logger.info(
        "Loaded %d AMC records  |  period: %s → %s",
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
    Append market_share_pct to the AUM DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw AUM data with column ``average_aum_cr``.

    Returns
    -------
    pd.DataFrame
        Copy of input with ``market_share_pct`` column added.

    Raises
    ------
    ValueError
        If industry AUM sums to zero.
    """
    industry_aum: float = float(df["average_aum_cr"].sum())

    if industry_aum == 0.0:
        raise ValueError(
            "Industry AUM sums to zero — market share percentages undefined. "
            "Verify that average_aum_cr values are correctly populated."
        )

    logger.info(
        "Industry AUM: ₹%,.2f Cr  |  AMC count: %d",
        industry_aum,
        len(df),
    )

    result = df.copy()
    result["market_share_pct"] = (result["average_aum_cr"] / industry_aum) * 100
    return result


def rank_amcs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign dense AUM-descending ranks and return the full ranked DataFrame.

    Rank 1 is the largest AMC by AUM. Dense ranking is used — ties share
    the same rank with no gap in the sequence.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing ``average_aum_cr`` and ``market_share_pct``.

    Returns
    -------
    pd.DataFrame
        Sorted ascending by rank with columns:
        rank, amc_name, average_aum_cr, market_share_pct, period_start, period_end.
    """
    result = df.copy()
    result["rank"] = (
        result["average_aum_cr"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    result.sort_values("rank", ascending=True, inplace=True)
    result.reset_index(drop=True, inplace=True)

    logger.info(
        "Ranked %d AMCs  |  #1: %s (₹%,.2f Cr)",
        len(result),
        result.loc[0, "amc_name"],
        result.loc[0, "average_aum_cr"],
    )
    return result


def calculate_concentration(ranked_df: pd.DataFrame) -> ConcentrationMetrics:
    """
    Compute market concentration metrics across standard AMC tiers.

    Calculates what percentage of total industry AUM is held by the
    top-3, top-5, and top-10 AMCs respectively.

    Parameters
    ----------
    ranked_df : pd.DataFrame
        Full ranked DataFrame, sorted by rank ascending.

    Returns
    -------
    ConcentrationMetrics
        Frozen dataclass containing all concentration figures.
    """
    def _top_n_share(n: int) -> float:
        """Sum market_share_pct for the top-n AMCs."""
        return float(ranked_df.head(n)["market_share_pct"].sum())

    metrics = ConcentrationMetrics(
        top_3_share_pct=_top_n_share(3),
        top_5_share_pct=_top_n_share(5),
        top_10_share_pct=_top_n_share(10),
        industry_aum_cr=float(ranked_df["average_aum_cr"].sum()),
        amc_count=len(ranked_df),
        period_start=ranked_df["period_start"].iloc[0],
        period_end=ranked_df["period_end"].iloc[0],
    )

    logger.info(
        "Concentration  |  Top 3: %.2f%%  Top 5: %.2f%%  Top 10: %.2f%%",
        metrics.top_3_share_pct,
        metrics.top_5_share_pct,
        metrics.top_10_share_pct,
    )
    return metrics


def _build_full_ranking(ranked_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select canonical output columns for the full rankings export.

    Parameters
    ----------
    ranked_df : pd.DataFrame
        Fully processed and ranked DataFrame.

    Returns
    -------
    pd.DataFrame
        Columns: rank, amc_name, average_aum_cr, market_share_pct,
                 period_start, period_end.
    """
    return ranked_df[[
        "rank",
        "amc_name",
        "average_aum_cr",
        "market_share_pct",
        "period_start",
        "period_end",
    ]].copy()


def _build_top_10(ranked_df: pd.DataFrame) -> pd.DataFrame:
    """
    Slice the top-10 AMCs from the full ranked DataFrame.

    Parameters
    ----------
    ranked_df : pd.DataFrame
        Full ranked DataFrame.

    Returns
    -------
    pd.DataFrame
        Top-10 AMCs with the same column structure as the full ranking.
    """
    return ranked_df.head(_TOP_N).copy()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def export_results(
    full_df: pd.DataFrame,
    top10_df: pd.DataFrame,
    export_dir: Path,
) -> dict[str, Path]:
    """
    Export ranking DataFrames to CSV (latest + timestamped pairs).

    Files written:
    - amc_rankings_latest.csv / amc_rankings_<ts>.csv
    - top_10_amcs_latest.csv  / top_10_amcs_<ts>.csv

    Parameters
    ----------
    full_df : pd.DataFrame
        Full AMC rankings DataFrame.
    top10_df : pd.DataFrame
        Top-10 AMC DataFrame.
    export_dir : Path
        Directory to write exports into.

    Returns
    -------
    dict[str, Path]
        Mapping of label → absolute file path for all four exports.

    Raises
    ------
    OSError
        If any file cannot be written.
    """
    timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_kwargs: dict = {
        "index": False,
        "float_format": "%.6f",
        "encoding": "utf-8-sig",
    }

    paths: dict[str, Path] = {
        "full_latest": export_dir / "amc_rankings_latest.csv",
        "full_ts": export_dir / f"amc_rankings_{timestamp}.csv",
        "top10_latest": export_dir / "top_10_amcs_latest.csv",
        "top10_ts": export_dir / f"top_10_amcs_{timestamp}.csv",
    }

    try:
        for label, path in paths.items():
            df = full_df if label.startswith("full") else top10_df
            df.to_csv(path, **csv_kwargs)
            logger.info("Exported %-14s → %s", label, path)
    except OSError as exc:
        raise OSError(f"Failed to write export file: {exc}") from exc

    return paths


# ---------------------------------------------------------------------------
# Console report
# ---------------------------------------------------------------------------


def print_report(
    ranked_df: pd.DataFrame,
    metrics: ConcentrationMetrics,
    export_paths: dict[str, Path],
) -> None:
    """
    Print a formatted leaderboard report to stdout.

    Parameters
    ----------
    ranked_df : pd.DataFrame
        Full ranked AMC DataFrame.
    metrics : ConcentrationMetrics
        Precomputed concentration metrics.
    export_paths : dict[str, Path]
        Export path mapping returned by export_results().
    """
    div: str = "─" * 82
    top_amc_name: str = ranked_df.loc[0, "amc_name"]
    top_amc_aum: float = float(ranked_df.loc[0, "average_aum_cr"])
    top_amc_share: float = float(ranked_df.loc[0, "market_share_pct"])

    print()
    print(div)
    print("  AMC RANKINGS REPORT")
    print(f"  Period        : {metrics.period_start}  →  {metrics.period_end}")
    print(f"  Industry AUM  : ₹{metrics.industry_aum_cr:>18,.2f} Cr")
    print(f"  AMC Count     : {metrics.amc_count}")
    print(div)

    # Concentration block
    print("  MARKET CONCENTRATION")
    print(f"    Top  3 AMCs : {metrics.top_3_share_pct:>7.4f}%  of industry AUM")
    print(f"    Top  5 AMCs : {metrics.top_5_share_pct:>7.4f}%  of industry AUM")
    print(f"    Top 10 AMCs : {metrics.top_10_share_pct:>7.4f}%  of industry AUM")
    print(div)

    # Top AMC callout
    print("  TOP AMC")
    print(
        f"    1. {top_amc_name}  "
        f"—  ₹{top_amc_aum:,.2f} Cr  ({top_amc_share:.4f}%)"
    )
    print(div)

    # Full leaderboard
    print(
        f"  {'Rank':>4}  {'AMC Name':<46}  {'AUM (₹ Cr)':>14}  {'Share %':>8}"
    )
    print(div)
    for _, row in ranked_df.iterrows():
        marker: str = " ◀" if int(row["rank"]) <= 3 else ""
        print(
            f"  {int(row['rank']):>4}  {row['amc_name']:<46}  "
            f"{row['average_aum_cr']:>14,.2f}  "
            f"{row['market_share_pct']:>7.4f}%{marker}"
        )
    print(div)

    # Top 10 summary
    print("\n  TOP 10 AMCs BY AUM")
    print(div)
    print(
        f"  {'Rank':>4}  {'AMC Name':<46}  {'AUM (₹ Cr)':>14}  {'Share %':>8}"
    )
    print(div)
    for _, row in ranked_df.head(_TOP_N).iterrows():
        print(
            f"  {int(row['rank']):>4}  {row['amc_name']:<46}  "
            f"{row['average_aum_cr']:>14,.2f}  {row['market_share_pct']:>7.4f}%"
        )
    print(div)

    # Exports
    print("\n  EXPORTS")
    for label, path in export_paths.items():
        print(f"    {label:<14} → {path}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Orchestrate the full AMC rankings pipeline.

    Stages
    ------
    1. Resolve and validate paths.
    2. Connect to warehouse (read-only).
    3. Detect latest AUM period.
    4. Load AUM data.
    5. Calculate market share percentages.
    6. Apply dense AUM-descending rankings.
    7. Compute concentration metrics.
    8. Build output DataFrames (full + top-10).
    9. Export to CSV.
    10. Print formatted report.

    Exit codes
    ----------
    0 : Success.
    1 : Any recoverable or data-quality failure.
    """
    logger.info("=" * 60)
    logger.info("AMC Rankings Module — starting")
    logger.info("=" * 60)

    try:
        # ── 1. Paths ─────────────────────────────────────────────────────
        db_path: Path = get_db_path()
        export_dir: Path = get_export_dir()

        # ── 2. Connect ───────────────────────────────────────────────────
        conn: sqlite3.Connection = get_connection(db_path)

        with conn:
            # ── 3. Latest period ─────────────────────────────────────────
            latest_period_end: str = get_latest_period(conn)

            # ── 4. Load AUM ──────────────────────────────────────────────
            raw_df: pd.DataFrame = load_aum_data(conn, latest_period_end)

        # ── 5. Market share ──────────────────────────────────────────────
        df: pd.DataFrame = calculate_market_share(raw_df)

        # ── 6. Rankings ──────────────────────────────────────────────────
        ranked_df: pd.DataFrame = rank_amcs(df)

        # ── 7. Concentration ─────────────────────────────────────────────
        metrics: ConcentrationMetrics = calculate_concentration(ranked_df)

        # ── 8. Output DataFrames ─────────────────────────────────────────
        full_output: pd.DataFrame = _build_full_ranking(ranked_df)
        top10_output: pd.DataFrame = _build_top_10(ranked_df)

        # ── 9. Export ────────────────────────────────────────────────────
        export_paths: dict[str, Path] = export_results(
            full_output, top10_output, export_dir
        )

        # ── 10. Report ───────────────────────────────────────────────────
        print_report(full_output, metrics, export_paths)

        logger.info("=" * 60)
        logger.info("AMC Rankings Module — completed successfully.")
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
    except OSError as exc:
        logger.error("Export failure: %s", exc)
        sys.exit(1)
    except sqlite3.Error as exc:
        logger.error("SQLite error: %s", exc)
        sys.exit(1)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Unexpected error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()