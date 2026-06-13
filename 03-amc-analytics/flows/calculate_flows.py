"""
calculate_flows.py
==================
AMC Dashboard · Phase 3 · Flow Analytics Module
Author : aditya.mishra10@nmims.in

PURPOSE
-------
Calculates net AMC flow data for Indian mutual fund AMCs tracked by the
AMC Dashboard project.  True net-flow calculations require at least two
consecutive AMC AUM periods (ΔAuM ≈ net-flows when adjusting for market
returns).  Because AMFI publishes AUM data quarterly, there will always be
a window after a fresh Phase-1 data-ingest where only a single period is
available.  This module handles that window gracefully by switching into
PROXY MODE rather than failing or producing empty exports.

PROXY MODE CONTRACT
-------------------
* Activated automatically when fewer than two AUM periods are detected.
* Proxy Flow Share % = Current Market Share % (proportional allocation
  of hypothetical industry flows based on AMC size).
* ALL proxy outputs are stamped with ``proxy_mode = TRUE`` and a
  methodology disclaimer column.
* Console output, logs, and CSV headers carry prominent warnings.
* THIS IS NOT A TRUE FLOW CALCULATION.

TRUE FLOW PLACEHOLDER
---------------------
When two or more AUM periods are detected the module still exports proxy
outputs today, but the function ``_calculate_true_flows()`` is reserved
and documented for future implementation once sufficient history exists.

PIPELINE POSITION
-----------------
Phase 1  Data Ingestion    ✓ Complete
Phase 2  Data Warehouse    ✓ Complete
Phase 3  Market Share      ✓ Complete
Phase 3  Rankings          ✓ Complete
Phase 3  Flows             ← This module

DEPENDENCIES
------------
Standard library only (pandas, sqlite3, pathlib, logging, datetime).
No third-party packages required beyond pandas.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Resolve project root relative to this file so the script can be run from
# any working directory.
_PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

WAREHOUSE_PATH: Path = _PROJECT_ROOT / "02-data-warehouse" / "warehouse.db"
EXPORT_DIR: Path = _PROJECT_ROOT / "data" / "exports" / "flows"

PROXY_METHODOLOGY: str = (
    "Flow data unavailable. Current AMC market share concentration is being "
    "used as a proxy for expected flow share until multiple AMC AUM periods "
    "become available."
)

PROXY_WARNING_BANNER: str = (
    "⚠  WARNING: PROXY MODE ACTIVE — THIS IS NOT A TRUE FLOW CALCULATION  ⚠\n"
    "   Proxy Flow Share % equals current Market Share %.\n"
    "   Values will be replaced by true net-flow calculations once\n"
    "   AMFI releases a second AMC AUM period into the warehouse."
)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger: logging.Logger = logging.getLogger("amc.flows")


# ---------------------------------------------------------------------------
# DIRECTORY HELPERS
# ---------------------------------------------------------------------------


def create_export_directory(export_dir: Path = EXPORT_DIR) -> Path:
    """
    Ensure the flow-export directory exists, creating it (and any missing
    parents) if necessary.

    Parameters
    ----------
    export_dir:
        Destination path for CSV exports.  Defaults to the project-level
        ``data/exports/flows/`` directory.

    Returns
    -------
    Path
        The resolved, guaranteed-to-exist export directory.

    Raises
    ------
    PermissionError
        If the process lacks write access to create the directory.
    """
    export_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Export directory confirmed: %s", export_dir)
    return export_dir


# ---------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------


def connect_database(warehouse_path: Path = WAREHOUSE_PATH) -> sqlite3.Connection:
    """
    Open a read-only SQLite connection to the data warehouse.

    Using ``check_same_thread=False`` is safe here because this module is
    single-threaded.  The connection is opened in read-only URI mode when
    the file exists to avoid accidental writes.

    Parameters
    ----------
    warehouse_path:
        Filesystem path to ``warehouse.db``.

    Returns
    -------
    sqlite3.Connection
        An open SQLite connection with ``row_factory`` set to
        ``sqlite3.Row`` for dict-like column access.

    Raises
    ------
    FileNotFoundError
        If ``warehouse_path`` does not exist.
    sqlite3.OperationalError
        If the file cannot be opened as a valid SQLite database.
    """
    if not warehouse_path.exists():
        raise FileNotFoundError(
            f"Warehouse not found at {warehouse_path}. "
            "Run Phase-1 and Phase-2 pipelines first."
        )

    conn = sqlite3.connect(
        f"file:{warehouse_path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    logger.info("Connected to warehouse: %s", warehouse_path)
    return conn


def _detect_aum_periods(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """
    Return all distinct (period_start, period_end) pairs present in
    ``aum_history``, ordered chronologically.

    Parameters
    ----------
    conn:
        Open SQLite connection to the warehouse.

    Returns
    -------
    list[tuple[str, str]]
        Ordered list of ``(period_start, period_end)`` ISO-date strings.
    """
    cursor = conn.execute(
        """
        SELECT DISTINCT period_start, period_end
        FROM   aum_history
        ORDER  BY period_start ASC
        """
    )
    periods: list[tuple[str, str]] = [
        (row["period_start"], row["period_end"]) for row in cursor.fetchall()
    ]
    logger.info("Detected %d AUM period(s) in warehouse.", len(periods))
    for i, (ps, pe) in enumerate(periods, 1):
        logger.info("  Period %d: %s → %s", i, ps, pe)
    return periods


# ---------------------------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------------------------


def load_latest_aum_period(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Load AMC names and AUM figures for the most recent period in the
    warehouse, returning a tidy DataFrame.

    The query joins ``amcs`` with ``aum_history`` on the latest available
    ``period_start`` so that the result is always deterministic regardless
    of how many periods exist.

    Parameters
    ----------
    conn:
        Open SQLite connection to the warehouse.

    Returns
    -------
    pd.DataFrame
        Columns: ``amc_id``, ``amc_name``, ``avg_aum_cr``,
        ``period_start``, ``period_end``.

    Raises
    ------
    ValueError
        If the warehouse contains no AUM data at all.
    """
    df = pd.read_sql_query(
        """
        SELECT
            amc_name,
            average_aum_cr  AS avg_aum_cr,
            period_start,
            period_end
        FROM   aum_history
        WHERE  period_start = (
                   SELECT MAX(period_start) FROM aum_history
               )
        ORDER  BY average_aum_cr DESC
        """,
        conn,
    )

    if df.empty:
        raise ValueError(
            "No AUM data found in warehouse. "
            "Ensure Phase-1 and Phase-2 pipelines have been executed."
        )

    logger.info(
        "Loaded AUM data for %d AMCs (period: %s → %s).",
        len(df),
        df["period_start"].iloc[0],
        df["period_end"].iloc[0],
    )
    return df


# ---------------------------------------------------------------------------
# FLOW CALCULATIONS
# ---------------------------------------------------------------------------


def _load_period(conn: sqlite3.Connection, period_start: str) -> pd.DataFrame:
    """
    Load AMC AUM data for a specific period from the warehouse.

    Parameters
    ----------
    conn:
        Open SQLite connection to the warehouse.
    period_start:
        ISO date string identifying the period to load (e.g. ``"2026-01-01"``).

    Returns
    -------
    pd.DataFrame
        Columns: ``amc_id``, ``amc_name``, ``avg_aum_cr``,
        ``period_start``, ``period_end``.
    """
    return pd.read_sql_query(
        """
        SELECT
            amc_name,
            average_aum_cr  AS avg_aum_cr,
            period_start,
            period_end
        FROM   aum_history
        WHERE  period_start = :period_start
        ORDER  BY amc_name ASC
        """,
        conn,
        params={"period_start": period_start},
    )


def _calculate_true_flows(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute simplified net flows as the AUM delta between two consecutive periods.

    Methodology:
        net_flow_cr          = AuM(t) − AuM(t-1)
        total_net_flow_cr    = sum of net_flow_cr across all AMCs
        net_flow_share_pct   = net_flow_cr / |total_net_flow_cr| × 100

    Note: A market-return adjustment (net_flow_cr ≈ AuM(t) − AuM(t-1) × (1 + r))
    is not applied here because a reliable benchmark series is not yet wired
    into the warehouse.  Add that adjustment once index return data is available.

    Parameters
    ----------
    current_df:
        Latest period AUM data, keyed on ``amc_id``.
    previous_df:
        Previous period AUM data, same schema as ``current_df``.

    Returns
    -------
    pd.DataFrame
        Columns: ``amc_id``, ``amc_name``, ``avg_aum_cr``, ``net_flow_cr``,
        ``net_flow_share_pct``, ``proxy_flow_share_pct``, ``proxy_mode``,
        ``methodology``, ``period_start``, ``period_end``.

    Raises
    ------
    ValueError
        If the two DataFrames share no common ``amc_id`` values.
    """
    merged = current_df.merge(
        previous_df[["amc_name", "avg_aum_cr"]].rename(columns={"avg_aum_cr": "prev_aum_cr"}),
        on="amc_name",
        how="inner",
    )

    if merged.empty:
        raise ValueError(
            "No matching AMC names between current and previous period. "
            "Cannot compute true flows."
        )

    merged["net_flow_cr"] = merged["avg_aum_cr"] - merged["prev_aum_cr"]

    total_abs_flow: float = merged["net_flow_cr"].abs().sum()
    if total_abs_flow == 0.0:
        merged["net_flow_share_pct"] = 0.0
    else:
        merged["net_flow_share_pct"] = (merged["net_flow_cr"] / total_abs_flow * 100).round(4)

    merged["proxy_flow_share_pct"] = merged["net_flow_share_pct"]
    merged["proxy_mode"] = "FALSE"
    merged["methodology"] = (
        "True net flow calculated as AuM(t) − AuM(t-1). "
        "No market-return adjustment applied."
    )

    logger.info(
        "True flows computed for %d AMCs (total abs flow: ₹%.2f Cr).",
        len(merged),
        total_abs_flow,
    )
    return merged.drop(columns=["prev_aum_cr"])


def calculate_proxy_flows(aum_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive a proxy flow-share estimate from current AMC market share.

    THIS IS NOT A TRUE FLOW CALCULATION.

    Proxy methodology:
        market_share_pct   = amc_aum / total_industry_aum × 100
        proxy_flow_share_pct = market_share_pct

    Interpretation: "If industry-level net flows were allocated
    proportionally to each AMC's current size, this is the expected
    share of flows each AMC would receive."  The value is a
    structural proxy only — it contains no information about actual
    investor behaviour or fund-level flows.

    Parameters
    ----------
    aum_df:
        DataFrame produced by ``load_latest_aum_period``.  Must contain
        columns ``amc_id``, ``amc_name``, ``avg_aum_cr``,
        ``period_start``, ``period_end``.

    Returns
    -------
    pd.DataFrame
        Original columns plus ``market_share_pct``,
        ``proxy_flow_share_pct``, ``proxy_mode``, ``methodology``.

    Raises
    ------
    ZeroDivisionError
        If total industry AUM sums to zero (data integrity failure).
    """
    logger.warning("PROXY MODE: Computing proxy flow shares from market share data.")
    logger.warning("THIS IS NOT A TRUE FLOW CALCULATION.")

    total_aum: float = aum_df["avg_aum_cr"].sum()
    if total_aum == 0.0:
        raise ZeroDivisionError(
            "Total industry AUM is zero — cannot compute market share. "
            "Check warehouse data integrity."
        )

    df = aum_df.copy()
    df["market_share_pct"] = (df["avg_aum_cr"] / total_aum * 100).round(4)

    # Proxy flow share equals market share — see module docstring.
    df["proxy_flow_share_pct"] = df["market_share_pct"]

    # Metadata columns — clearly label every row as proxy data.
    df["proxy_mode"] = "TRUE"
    df["methodology"] = PROXY_METHODOLOGY

    logger.info(
        "Proxy flow shares computed for %d AMCs (total industry AUM: ₹%.2f Cr).",
        len(df),
        total_aum,
    )
    return df


# ---------------------------------------------------------------------------
# RANKING
# ---------------------------------------------------------------------------


def assign_rankings(flow_df: pd.DataFrame, sort_column: str = "proxy_flow_share_pct") -> pd.DataFrame:
    """
    Sort the flow DataFrame and assign integer ranks (1 = largest share).

    Parameters
    ----------
    flow_df:
        DataFrame containing at minimum ``amc_name`` and ``sort_column``.
    sort_column:
        Column to rank on.  Defaults to ``proxy_flow_share_pct``.

    Returns
    -------
    pd.DataFrame
        Sorted DataFrame with a leading ``rank`` column (1-based integer).
    """
    df = flow_df.sort_values(sort_column, ascending=False).reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    logger.info("Rankings assigned (%d AMCs, sorted by %s).", len(df), sort_column)
    return df


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------


def export_results(
    flow_df: pd.DataFrame,
    export_dir: Path = EXPORT_DIR,
) -> tuple[Path, Path]:
    """
    Write flow results to two CSV files:
    1. ``flow_proxy_latest.csv``     — always-overwritten convenience file.
    2. ``flow_proxy_YYYYMMDD_HHMMSS.csv`` — timestamped archival copy.

    Parameters
    ----------
    flow_df:
        Ranked DataFrame produced by ``assign_rankings``.
    export_dir:
        Directory where CSV files are written.

    Returns
    -------
    tuple[Path, Path]
        ``(latest_path, timestamped_path)``

    Raises
    ------
    OSError
        If the directory is not writable.
    """
    required_columns: list[str] = [
        "rank",
        "amc_name",
        "avg_aum_cr",
        "proxy_flow_share_pct",
        "proxy_mode",
        "methodology",
        "period_start",
        "period_end",
    ]

    missing = [c for c in required_columns if c not in flow_df.columns]
    if missing:
        raise KeyError(f"Missing required columns in flow DataFrame: {missing}")

    optional_columns: list[str] = ["market_share_pct", "net_flow_cr", "net_flow_share_pct"]
    present_optionals: list[str] = [c for c in optional_columns if c in flow_df.columns]
    idx = required_columns.index("proxy_flow_share_pct")
    output_columns: list[str] = required_columns[:idx] + present_optionals + required_columns[idx:]

    export_df = flow_df[output_columns].copy()

    timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

    latest_path: Path = export_dir / "flow_proxy_latest.csv"
    timestamped_path: Path = export_dir / f"flow_proxy_{timestamp}.csv"

    export_df.to_csv(latest_path, index=False)
    export_df.to_csv(timestamped_path, index=False)

    logger.info("Exported latest CSV        → %s", latest_path)
    logger.info("Exported timestamped CSV   → %s", timestamped_path)
    return latest_path, timestamped_path


# ---------------------------------------------------------------------------
# CONSOLE REPORT
# ---------------------------------------------------------------------------


def print_report(
    flow_df: pd.DataFrame,
    periods: list[tuple[str, str]],
    latest_path: Path,
    timestamped_path: Path,
    proxy_mode: bool,
) -> None:
    """
    Print a formatted AMC Flow Report to stdout.

    Parameters
    ----------
    flow_df:
        Ranked flow DataFrame.
    periods:
        All detected (period_start, period_end) pairs from the warehouse.
    latest_path:
        Path to the ``flow_proxy_latest.csv`` export.
    timestamped_path:
        Path to the timestamped CSV export.
    proxy_mode:
        ``True`` when the module is running in proxy mode.
    """
    divider: str = "=" * 68
    thin_divider: str = "-" * 68

    latest_period_start: str = flow_df["period_start"].iloc[0]
    latest_period_end: str = flow_df["period_end"].iloc[0]
    amc_count: int = len(flow_df)
    total_aum: float = flow_df["avg_aum_cr"].sum()

    print()
    print(divider)
    print("  AMC FLOW PROXY REPORT")
    print(divider)

    if proxy_mode:
        print()
        print(PROXY_WARNING_BANNER)
        print()

    print(f"  Status         : {'PROXY MODE ⚠' if proxy_mode else 'TRUE FLOW MODE'}")
    if proxy_mode:
        print(
            f"  Reason         : Only {len(periods)} AUM period(s) in warehouse. "
            "True flows require ≥ 2."
        )
    print(f"  Latest Period  : {latest_period_start} → {latest_period_end}")
    print(f"  AMC Count      : {amc_count}")
    print(f"  Industry AUM   : ₹{total_aum:,.2f} Cr")
    print()
    print(
        "  Methodology    : Current market share concentration used as"
    )
    print(
        "                   expected flow share proxy."
    )
    print(
        "                   THIS IS NOT A TRUE FLOW CALCULATION."
    )
    print()
    has_market_share = "market_share_pct" in flow_df.columns
    has_net_flow = "net_flow_cr" in flow_df.columns

    if has_net_flow:
        header = (
            f"  {'Rank':>4}  {'AMC Name':<42}  {'AUM (₹ Cr)':>14}  "
            f"{'Net Flow (₹ Cr)':>15}  {'Flow Share %':>12}"
        )
    else:
        header = (
            f"  {'Rank':>4}  {'AMC Name':<42}  {'AUM (₹ Cr)':>14}  "
            f"{'Mkt Share %':>11}  {'Proxy Flow %':>12}"
        )

    print(thin_divider)
    print(header)
    print(thin_divider)

    for _, row in flow_df.iterrows():
        if has_net_flow:
            print(
                f"  {int(row['rank']):>4}  {row['amc_name']:<42}  "
                f"{row['avg_aum_cr']:>14,.2f}  "
                f"{row['net_flow_cr']:>15,.2f}  "
                f"{row['proxy_flow_share_pct']:>12.4f}"
            )
        else:
            mkt = f"{row['market_share_pct']:>11.4f}" if has_market_share else f"{'N/A':>11}"
            print(
                f"  {int(row['rank']):>4}  {row['amc_name']:<42}  "
                f"{row['avg_aum_cr']:>14,.2f}  "
                f"{mkt}  "
                f"{row['proxy_flow_share_pct']:>12.4f}"
            )

    print(thin_divider)
    print()
    print("  Exports:")
    print(f"    Latest      → {latest_path}")
    print(f"    Timestamped → {timestamped_path}")
    print()
    if proxy_mode:
        print("  ⚠  Reminder: All values above are PROXY data, not true flows.")
    print(divider)
    print()


# ---------------------------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Orchestrate the AMC flow analytics pipeline.

    Attempts true flow calculation when two or more AUM periods are available.
    Falls back automatically to proxy mode on any failure or insufficient data.

    Exit codes
    ----------
    0   Pipeline completed successfully (true or proxy mode).
    1   Unrecoverable error (warehouse missing, data integrity failure, etc.).
    """
    logger.info("=" * 60)
    logger.info("AMC Flow Analytics Pipeline — starting")
    logger.info("=" * 60)

    try:
        export_dir: Path = create_export_directory()
        conn: sqlite3.Connection = connect_database()
        periods: list[tuple[str, str]] = _detect_aum_periods(conn)
        current_df: pd.DataFrame = load_latest_aum_period(conn)

        proxy_mode: bool
        flow_df: pd.DataFrame

        try:
            if len(periods) < 2:
                raise ValueError(
                    f"Only {len(periods)} AUM period(s) found; true flows require ≥ 2."
                )

            previous_df: pd.DataFrame = _load_period(conn, periods[-2][0])
            flow_df = _calculate_true_flows(current_df, previous_df)
            proxy_mode = False
            logger.info("True flow calculation succeeded.")

        except (ValueError, KeyError, Exception) as exc:
            logger.warning("─" * 55)
            logger.warning("PROXY MODE ACTIVATED: %s", exc)
            logger.warning("THIS IS NOT A TRUE FLOW CALCULATION.")
            logger.warning("─" * 55)
            flow_df = calculate_proxy_flows(current_df)
            proxy_mode = True

        conn.close()
        logger.info("Database connection closed.")

        ranked_df: pd.DataFrame = assign_rankings(flow_df)
        latest_path: Path
        timestamped_path: Path
        latest_path, timestamped_path = export_results(ranked_df, export_dir)

        print_report(
            flow_df=ranked_df,
            periods=periods,
            latest_path=latest_path,
            timestamped_path=timestamped_path,
            proxy_mode=proxy_mode,
        )

        logger.info("AMC Flow Analytics Pipeline — completed successfully.")
        if proxy_mode:
            logger.info(
                "Reminder: All outputs are PROXY data. "
                "THIS IS NOT A TRUE FLOW CALCULATION."
            )

    except FileNotFoundError as exc:
        logger.error("Warehouse not found: %s", exc)
        sys.exit(1)
    except ValueError as exc:
        logger.error("Data error: %s", exc)
        sys.exit(1)
    except ZeroDivisionError as exc:
        logger.error("AUM integrity failure: %s", exc)
        sys.exit(1)
    except sqlite3.OperationalError as exc:
        logger.error("SQLite error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(0)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Unexpected error in flow pipeline: %s", exc)
        sys.exit(1)


# ---------------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()