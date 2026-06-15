"""
forecast_aum.py
───────────────────────────────────────────────────────────────────────────────
AMC AUM Forecasting Pipeline
Phase 05 — amc-dashboard

Reads the latest AUM snapshot from the SQLite warehouse, applies
growth-rate assumptions from three scenario modules (base_case, bull_case,
bear_case), and exports scenario-level forecasts to CSV.

Author : aditya.mishra10@nmims.in
Python : 3.12+
───────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

import sqlite3
import sys
import importlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# ── Resolve project root (two levels up from 05-forecasting/) ─────────────
THIS_FILE  = Path(__file__).resolve()
SCRIPT_DIR = THIS_FILE.parent                    # 05-forecasting/
PROJECT_ROOT = SCRIPT_DIR.parent                 # amc-dashboard/

# ── Paths ──────────────────────────────────────────────────────────────────
DB_PATH      : Path = PROJECT_ROOT / "02-data-warehouse" / "warehouse.db"
EXPORT_DIR   : Path = PROJECT_ROOT / "data" / "exports" / "forecasting"

LATEST_FILENAME   = "forecast_aum_latest.csv"
ARCHIVE_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
ARCHIVE_FILENAME  = f"forecast_aum_{ARCHIVE_TIMESTAMP}.csv"

# ── Scenario module names (must live in the same directory as this script) ─
SCENARIO_MODULES: list[str] = ["base_case", "bull_case", "bear_case"]

# ── Output columns ─────────────────────────────────────────────────────────
COL_AMC      = "amc_name"
COL_SCENARIO = "scenario"
COL_CURRENT  = "current_aum_cr"
COL_Y1       = "year_1_aum_cr"
COL_Y2       = "year_2_aum_cr"
COL_Y3       = "year_3_aum_cr"

OUTPUT_COLUMNS = [COL_AMC, COL_SCENARIO, COL_CURRENT, COL_Y1, COL_Y2, COL_Y3]

# ── Keywords used to discover the AUM table automatically ─────────────────
AUM_TABLE_KEYWORDS: list[str] = ["aum"]
AUM_COLUMN_CANDIDATES: list[str] = ["average_aum_cr", "aum_cr", "aum", "total_aum"]
AMC_COLUMN_CANDIDATES: list[str] = ["amc_name", "amc", "fund_house", "name"]
DATE_COLUMN_CANDIDATES: list[str] = ["period_end", "period_start", "date", "as_of"]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _banner(title: str) -> None:
    width = 49
    print("=" * width)
    print(title)
    print("=" * (len(title)))
    print()


def _step(n: int, total: int, description: str) -> None:
    print(f"[{n}/{total}] {description}...")


def _pass() -> None:
    print("PASS\n")


def _resolve_column(available: list[str], candidates: list[str], label: str) -> str:
    """Return the first candidate column present in *available*, case-insensitively."""
    lower_map = {c.lower(): c for c in available}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    raise ValueError(
        f"Cannot resolve {label} column. "
        f"Looked for {candidates} in table columns {available}."
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DATABASE ACCESS
# ══════════════════════════════════════════════════════════════════════════════

def connect_to_warehouse(db_path: Path) -> sqlite3.Connection:
    """
    Open a read-only connection to the SQLite warehouse.

    Raises
    ------
    FileNotFoundError
        If the database file does not exist.
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"Warehouse database not found at: {db_path}\n"
            "Ensure the data pipeline has been run before forecasting."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _list_tables(conn: sqlite3.Connection) -> list[str]:
    """Return a list of all user tables in the SQLite database."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    )
    return [row["name"] for row in cur.fetchall()]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """Return column names for *table*."""
    cur = conn.execute(f"PRAGMA table_info('{table}');")
    return [row["name"] for row in cur.fetchall()]


def discover_aum_table(conn: sqlite3.Connection) -> str:
    """
    Locate the table most likely to hold AMC AUM data by inspecting
    table names for AUM-related keywords.

    Returns
    -------
    str
        Name of the discovered AUM table.

    Raises
    ------
    ValueError
        If no suitable table can be found.
    """
    tables = _list_tables(conn)
    if not tables:
        raise ValueError("The warehouse database contains no tables.")

    for table in tables:
        name_lower = table.lower()
        if any(kw in name_lower for kw in AUM_TABLE_KEYWORDS):
            return table

    raise ValueError(
        f"No AUM table found. Available tables: {tables}\n"
        "Expected a table whose name contains one of: "
        f"{AUM_TABLE_KEYWORDS}"
    )


def load_latest_aum(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    """
    Load the most recent AUM value per AMC from *table*.

    Strategy
    --------
    1. Detect the AMC-name column, AUM-value column, and (if present) a
       date column via candidate lists.
    2. If a date column exists, filter to the single latest period_end date
       so that each AMC contributes exactly one row.
    3. Return a DataFrame with standardised column names: COL_AMC, COL_CURRENT.

    Raises
    ------
    ValueError
        If required columns cannot be resolved or the table is empty.
    """
    columns = _table_columns(conn, table)

    amc_col = _resolve_column(columns, AMC_COLUMN_CANDIDATES, "AMC name")
    aum_col = _resolve_column(columns, AUM_COLUMN_CANDIDATES, "AUM value")

    # Try to find a date column; if absent, take the full table as-is.
    date_col: str | None = None
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate.lower() in [c.lower() for c in columns]:
            date_col = candidate
            break

    if date_col:
        # Fetch the latest period available
        cur = conn.execute(f"SELECT MAX({date_col}) AS latest_date FROM \"{table}\";")
        row = cur.fetchone()
        latest_date = row["latest_date"] if row else None

        if latest_date is None:
            raise ValueError(f"Table '{table}' has no data in column '{date_col}'.")

        query = (
            f"SELECT \"{amc_col}\", \"{aum_col}\" "
            f"FROM \"{table}\" "
            f"WHERE \"{date_col}\" = ? "
            f"ORDER BY \"{amc_col}\";"
        )
        df = pd.read_sql_query(query, conn, params=(latest_date,))
        logging.info("Loaded AUM snapshot for period-end: %s", latest_date)
    else:
        query = (
            f"SELECT \"{amc_col}\", \"{aum_col}\" "
            f"FROM \"{table}\" "
            f"ORDER BY \"{amc_col}\";"
        )
        df = pd.read_sql_query(query, conn)

    # Standardise column names
    df = df.rename(columns={amc_col: COL_AMC, aum_col: COL_CURRENT})
    return df[[COL_AMC, COL_CURRENT]]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ValidationResult:
    passed: bool = True
    errors: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.errors.append(message)

    def raise_if_failed(self) -> None:
        if not self.passed:
            joined = "\n  • ".join(self.errors)
            raise ValueError(f"Validation failed:\n  • {joined}")


def validate_aum_dataframe(df: pd.DataFrame) -> None:
    """
    Pre-forecast validation of the raw AUM data.

    Checks
    ------
    * DataFrame is non-empty.
    * COL_AMC contains no nulls.
    * COL_CURRENT contains no nulls.
    * All AUM values are strictly positive.
    """
    vr = ValidationResult()

    if df.empty:
        vr.fail("AUM DataFrame is empty — no data loaded from warehouse.")
        vr.raise_if_failed()

    null_amc = df[COL_AMC].isna().sum()
    if null_amc > 0:
        vr.fail(f"{null_amc} row(s) have null AMC names.")

    null_aum = df[COL_CURRENT].isna().sum()
    if null_aum > 0:
        vr.fail(f"{null_aum} row(s) have null AUM values.")

    non_positive = (df[COL_CURRENT].dropna() <= 0).sum()
    if non_positive > 0:
        vr.fail(
            f"{non_positive} row(s) have non-positive AUM values. "
            "All AUM values must be > 0."
        )

    vr.raise_if_failed()


def validate_forecast_dataframe(df: pd.DataFrame, scenarios: list["ScenarioConfig"]) -> None:
    """
    Post-forecast validation of the combined output DataFrame.

    Checks
    ------
    * No null values in any column.
    * No negative forecast values.
    * No duplicate (AMC, scenario) combinations.
    * Year-on-year AUM increases for scenarios with positive growth rates.
    """
    vr = ValidationResult()

    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    if total_nulls > 0:
        vr.fail(f"Forecast DataFrame contains {total_nulls} null value(s):\n{null_counts[null_counts > 0]}")

    for col in [COL_Y1, COL_Y2, COL_Y3]:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            vr.fail(f"{negative_count} negative value(s) found in '{col}'.")

    duplicates = df.duplicated(subset=[COL_AMC, COL_SCENARIO]).sum()
    if duplicates > 0:
        vr.fail(f"{duplicates} duplicate (AMC, Scenario) row(s) detected.")

    for sc in scenarios:
        if sc.base_growth_rate > 0:
            subset = df[df[COL_SCENARIO] == sc.scenario_name]
            if not subset.empty:
                y1_lt_current = (subset[COL_Y1] <= subset[COL_CURRENT]).any()
                y2_lt_y1      = (subset[COL_Y2] <= subset[COL_Y1]).any()
                y3_lt_y2      = (subset[COL_Y3] <= subset[COL_Y2]).any()
                if y1_lt_current or y2_lt_y1 or y3_lt_y2:
                    vr.fail(
                        f"Scenario '{sc.scenario_name}' has a positive growth rate "
                        f"({sc.base_growth_rate:.0%}) but some forecast values do not increase."
                    )

    vr.raise_if_failed()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — FORECAST ENGINE
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ScenarioConfig:
    """Immutable container for a single forecasting scenario's assumptions."""
    scenario_name: str
    base_growth_rate: float
    forecast_horizon_years: int


def load_scenarios(module_names: list[str], script_dir: Path) -> list[ScenarioConfig]:
    """
    Dynamically import scenario modules and extract their constants.

    Each module must define:
        SCENARIO_NAME            : str
        BASE_GROWTH_RATE         : float
        FORECAST_HORIZON_YEARS   : int

    Parameters
    ----------
    module_names : list[str]
        List of module names to import (e.g. ["base_case", "bull_case"]).
    script_dir : Path
        Directory containing the scenario modules.

    Returns
    -------
    list[ScenarioConfig]
        One ScenarioConfig per successfully imported module.
    """
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))

    scenarios: list[ScenarioConfig] = []
    for name in module_names:
        try:
            mod = importlib.import_module(name)
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                f"Scenario module '{name}' not found in {script_dir}. "
                "Ensure base_case.py, bull_case.py, and bear_case.py exist."
            ) from exc

        for attr in ("SCENARIO_NAME", "BASE_GROWTH_RATE", "FORECAST_HORIZON_YEARS"):
            if not hasattr(mod, attr):
                raise AttributeError(
                    f"Scenario module '{name}' is missing required constant '{attr}'."
                )

        sc = ScenarioConfig(
            scenario_name=mod.SCENARIO_NAME,
            base_growth_rate=float(mod.BASE_GROWTH_RATE),
            forecast_horizon_years=int(mod.FORECAST_HORIZON_YEARS),
        )
        scenarios.append(sc)
        logging.info(
            "Loaded scenario: %-12s | growth=%.0f%% | horizon=%d yr",
            sc.scenario_name,
            sc.base_growth_rate * 100,
            sc.forecast_horizon_years,
        )

    return scenarios


def _forecast_single_scenario(
    aum_df: pd.DataFrame,
    scenario: ScenarioConfig,
) -> pd.DataFrame:
    """
    Apply compound-growth forecasting for one scenario across all AMCs.

    Formula
    -------
    Year_N_AUM = Current_AUM × (1 + growth_rate) ^ N

    Parameters
    ----------
    aum_df   : DataFrame with columns [COL_AMC, COL_CURRENT]
    scenario : ScenarioConfig with growth rate and horizon

    Returns
    -------
    DataFrame with columns defined in OUTPUT_COLUMNS.
    """
    g = scenario.base_growth_rate
    rows = aum_df.copy()
    rows[COL_SCENARIO] = scenario.scenario_name
    rows[COL_Y1] = rows[COL_CURRENT] * (1 + g) ** 1
    rows[COL_Y2] = rows[COL_CURRENT] * (1 + g) ** 2
    rows[COL_Y3] = rows[COL_CURRENT] * (1 + g) ** 3
    return rows[OUTPUT_COLUMNS]


def generate_forecasts(
    aum_df: pd.DataFrame,
    scenarios: list[ScenarioConfig],
) -> pd.DataFrame:
    """
    Run forecasting across all scenarios and return a combined DataFrame.

    Output has one row per (AMC × scenario) combination.
    """
    frames: list[pd.DataFrame] = []
    for sc in scenarios:
        frame = _forecast_single_scenario(aum_df, sc)
        frames.append(frame)
        logging.info(
            "Generated forecast — scenario: %-12s | rows: %d",
            sc.scenario_name,
            len(frame),
        )
    return pd.concat(frames, ignore_index=True).sort_values(
        [COL_AMC, COL_SCENARIO]
    ).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — EXPORT FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def export_forecasts(
    df: pd.DataFrame,
    export_dir: Path,
    latest_filename: str,
    archive_filename: str,
) -> tuple[Path, Path]:
    """
    Write the forecast DataFrame to two CSV files.

    Parameters
    ----------
    df               : Fully validated forecast DataFrame.
    export_dir       : Target directory (created if absent).
    latest_filename  : Always-overwritten file name.
    archive_filename : Timestamped archive file name.

    Returns
    -------
    tuple[Path, Path]
        (latest_path, archive_path)
    """
    export_dir.mkdir(parents=True, exist_ok=True)

    latest_path  = export_dir / latest_filename
    archive_path = export_dir / archive_filename

    csv_kwargs: dict[str, Any] = {
        "index": False,
        "encoding": "utf-8",
        "float_format": "%.4f",
    }

    df.to_csv(latest_path, **csv_kwargs)
    df.to_csv(archive_path, **csv_kwargs)

    logging.info("Exported latest  → %s", latest_path)
    logging.info("Exported archive → %s", archive_path)

    return latest_path, archive_path


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — MAIN EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Orchestrate the full AUM forecasting pipeline.

    Steps
    -----
    1. Connect to the warehouse.
    2. Discover and load the AUM table.
    3. Import scenario modules.
    4. Generate compound-growth forecasts.
    5. Validate the output.
    6. Export to CSV.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    TOTAL_STEPS = 6
    _banner("AMC AUM FORECAST")

    # ── Step 1: Connect ───────────────────────────────────────────────────
    _step(1, TOTAL_STEPS, "Connecting to warehouse")
    try:
        conn = connect_to_warehouse(DB_PATH)
    except FileNotFoundError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 2: Load AUM data ─────────────────────────────────────────────
    _step(2, TOTAL_STEPS, "Loading AUM data")
    try:
        aum_table = discover_aum_table(conn)
        logging.info("Discovered AUM table: '%s'", aum_table)

        aum_df = load_latest_aum(conn, aum_table)
        conn.close()

        validate_aum_dataframe(aum_df)
        logging.info("Loaded %d AMCs from table '%s'.", len(aum_df), aum_table)
    except (ValueError, KeyError) as exc:
        conn.close()
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 3: Load scenarios ────────────────────────────────────────────
    _step(3, TOTAL_STEPS, "Loading forecasting scenarios")
    try:
        scenarios = load_scenarios(SCENARIO_MODULES, SCRIPT_DIR)
    except (ModuleNotFoundError, AttributeError) as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 4: Generate forecasts ────────────────────────────────────────
    _step(4, TOTAL_STEPS, "Generating forecasts")
    try:
        forecast_df = generate_forecasts(aum_df, scenarios)
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 5: Validate output ───────────────────────────────────────────
    _step(5, TOTAL_STEPS, "Validating output")
    try:
        validate_forecast_dataframe(forecast_df, scenarios)
    except ValueError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 6: Export ────────────────────────────────────────────────────
    _step(6, TOTAL_STEPS, "Exporting results")
    try:
        latest_path, archive_path = export_forecasts(
            forecast_df,
            EXPORT_DIR,
            LATEST_FILENAME,
            ARCHIVE_FILENAME,
        )
    except OSError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Summary ───────────────────────────────────────────────────────────
    _banner("FORECAST SUMMARY")
    amc_count      = forecast_df[COL_AMC].nunique()
    scenario_count = forecast_df[COL_SCENARIO].nunique()
    row_count      = len(forecast_df)

    print(f"{'AMCs Processed':<16}: {amc_count}")
    print(f"{'Scenarios':<16}: {scenario_count}")
    print(f"{'Forecast Rows':<16}: {row_count}")
    print()
    print(f"Latest File:\n{latest_path}")
    print()
    print(f"Archive File:\n{archive_path}")
    print()
    print("Completed Successfully")


if __name__ == "__main__":
    main()