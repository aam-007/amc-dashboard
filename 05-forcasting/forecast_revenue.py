"""
forecast_revenue.py
───────────────────────────────────────────────────────────────────────────────
AMC Revenue Forecasting Pipeline
Phase 05 — amc-dashboard

Converts AUM forecasts produced by forecast_aum.py into revenue forecasts
by applying each AMC's current implied revenue yield (Revenue ÷ AUM).

Inputs
------
* data/exports/revenue/amc_revenue_latest.csv     — Phase 4 revenue estimates
* data/exports/forecasting/forecast_aum_latest.csv — Phase 5 AUM forecasts

Outputs
-------
* data/exports/forecasting/forecast_revenue_latest.csv
* data/exports/forecasting/forecast_revenue_YYYYMMDD_HHMMSS.csv

Author : aditya.mishra10@nmims.in
Python : 3.12+
───────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# ── Resolve project root (two levels up from 05-forecasting/) ─────────────
THIS_FILE    = Path(__file__).resolve()
SCRIPT_DIR   = THIS_FILE.parent               # 05-forecasting/
PROJECT_ROOT = SCRIPT_DIR.parent              # amc-dashboard/

# ── Input paths ────────────────────────────────────────────────────────────
REVENUE_CSV  : Path = PROJECT_ROOT / "data" / "exports" / "revenue" / "amc_revenue_latest.csv"
AUM_FCST_CSV : Path = PROJECT_ROOT / "data" / "exports" / "forecasting" / "forecast_aum_latest.csv"

# ── Output paths ───────────────────────────────────────────────────────────
EXPORT_DIR        : Path = PROJECT_ROOT / "data" / "exports" / "forecasting"
ARCHIVE_TIMESTAMP : str  = datetime.now().strftime("%Y%m%d_%H%M%S")
LATEST_FILENAME   : str  = "forecast_revenue_latest.csv"
ARCHIVE_FILENAME  : str  = f"forecast_revenue_{ARCHIVE_TIMESTAMP}.csv"

# ── Output column names ────────────────────────────────────────────────────
COL_AMC          = "amc_name"
COL_SCENARIO     = "scenario"
COL_YIELD        = "revenue_yield"
COL_CUR_REVENUE  = "current_revenue_cr"
COL_REV_Y1       = "revenue_y1_cr"
COL_REV_Y2       = "revenue_y2_cr"
COL_REV_Y3       = "revenue_y3_cr"

OUTPUT_COLUMNS = [
    COL_AMC,
    COL_SCENARIO,
    COL_YIELD,
    COL_CUR_REVENUE,
    COL_REV_Y1,
    COL_REV_Y2,
    COL_REV_Y3,
]

# ── Candidate column name lists for dynamic resolution ─────────────────────
AMC_COL_CANDIDATES: list[str] = [
    "amc_name", "amc", "fund_house", "name", "amcname",
]
CURRENT_AUM_CANDIDATES: list[str] = [
    "average_aum_cr", "current_aum_cr", "aum_cr", "aum", "total_aum",
    "average_aum", "aum_inr_cr",
]
REVENUE_CANDIDATES: list[str] = [
    "estimated_revenue_cr", "revenue_cr", "revenue", "total_revenue",
    "net_revenue_cr", "revenue_inr_cr",
]

# Column candidates for the AUM forecast file
FCST_AMC_CANDIDATES: list[str] = AMC_COL_CANDIDATES
FCST_SCENARIO_CANDIDATES: list[str] = [
    "scenario", "case", "scenario_name",
]
FCST_AUM_CURRENT_CANDIDATES: list[str] = [
    "current_aum_cr", "current_aum", "average_aum_cr", "aum_cr",
]
FCST_Y1_CANDIDATES: list[str] = ["year_1_aum_cr", "year_1_aum", "y1_aum", "y1_aum_cr"]
FCST_Y2_CANDIDATES: list[str] = ["year_2_aum_cr", "year_2_aum", "y2_aum", "y2_aum_cr"]
FCST_Y3_CANDIDATES: list[str] = ["year_3_aum_cr", "year_3_aum", "y3_aum", "y3_aum_cr"]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _banner(title: str) -> None:
    print("=" * 49)
    print(title)
    print("=" * len(title))
    print()


def _step(n: int, total: int, description: str) -> None:
    print(f"[{n}/{total}] {description}...")


def _pass() -> None:
    print("PASS\n")


def _resolve_column(
    available: list[str],
    candidates: list[str],
    label: str,
    source: str = "",
) -> str:
    """
    Return the first candidate column present in *available*, case-insensitively.

    Parameters
    ----------
    available  : Column names that actually exist in the DataFrame.
    candidates : Ordered list of preferred column names to try.
    label      : Human-readable description used in error messages.
    source     : Optional file/table name for richer error context.

    Raises
    ------
    ValueError
        If none of the candidates match any column in *available*.
    """
    lower_map = {c.lower(): c for c in available}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]

    context = f" in '{source}'" if source else ""
    raise ValueError(
        f"Cannot resolve {label} column{context}.\n"
        f"  Tried    : {candidates}\n"
        f"  Available: {available}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — INPUT LOADING
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class RevenueColumns:
    """Resolved column names for the revenue estimates file."""
    amc: str
    current_aum: str
    revenue: str


@dataclass(frozen=True)
class AumForecastColumns:
    """Resolved column names for the AUM forecast file."""
    amc: str
    scenario: str
    current_aum: str
    y1: str
    y2: str
    y3: str


def load_revenue_estimates(path: Path) -> tuple[pd.DataFrame, RevenueColumns]:
    """
    Load Phase 4 revenue estimates and resolve column names dynamically.

    Parameters
    ----------
    path : Path to amc_revenue_latest.csv.

    Returns
    -------
    tuple[DataFrame, RevenueColumns]
        Raw DataFrame and a dataclass of resolved column names.

    Raises
    ------
    FileNotFoundError : If the file does not exist.
    ValueError        : If required columns cannot be located.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Revenue estimates file not found: {path}\n"
            "Run estimate_revenue.py (Phase 4) before forecasting."
        )

    df = pd.read_csv(path, encoding="utf-8")
    logging.info("Loaded revenue file: %s  (%d rows, %d cols)", path.name, *df.shape)

    cols = list(df.columns)
    resolved = RevenueColumns(
        amc=_resolve_column(cols, AMC_COL_CANDIDATES, "AMC name", path.name),
        current_aum=_resolve_column(cols, CURRENT_AUM_CANDIDATES, "current AUM", path.name),
        revenue=_resolve_column(cols, REVENUE_CANDIDATES, "current revenue", path.name),
    )

    logging.info(
        "Revenue file columns resolved → amc='%s', aum='%s', revenue='%s'",
        resolved.amc, resolved.current_aum, resolved.revenue,
    )
    return df, resolved


def load_aum_forecasts(path: Path) -> tuple[pd.DataFrame, AumForecastColumns]:
    """
    Load AUM forecasts produced by forecast_aum.py and resolve column names.

    Parameters
    ----------
    path : Path to forecast_aum_latest.csv.

    Returns
    -------
    tuple[DataFrame, AumForecastColumns]
        Raw DataFrame and a dataclass of resolved column names.

    Raises
    ------
    FileNotFoundError : If the file does not exist.
    ValueError        : If required columns cannot be located.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"AUM forecast file not found: {path}\n"
            "Run forecast_aum.py before running forecast_revenue.py."
        )

    df = pd.read_csv(path, encoding="utf-8")
    logging.info("Loaded AUM forecast file: %s  (%d rows, %d cols)", path.name, *df.shape)

    cols = list(df.columns)
    resolved = AumForecastColumns(
        amc=_resolve_column(cols, FCST_AMC_CANDIDATES, "AMC name", path.name),
        scenario=_resolve_column(cols, FCST_SCENARIO_CANDIDATES, "scenario", path.name),
        current_aum=_resolve_column(cols, FCST_AUM_CURRENT_CANDIDATES, "current AUM", path.name),
        y1=_resolve_column(cols, FCST_Y1_CANDIDATES, "Year 1 AUM", path.name),
        y2=_resolve_column(cols, FCST_Y2_CANDIDATES, "Year 2 AUM", path.name),
        y3=_resolve_column(cols, FCST_Y3_CANDIDATES, "Year 3 AUM", path.name),
    )

    logging.info(
        "AUM forecast columns resolved → amc='%s', scenario='%s', y1='%s', y2='%s', y3='%s'",
        resolved.amc, resolved.scenario, resolved.y1, resolved.y2, resolved.y3,
    )
    return df, resolved


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ValidationResult:
    """Accumulator for validation errors; raises on demand."""
    passed: bool = True
    errors: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.errors.append(message)

    def raise_if_failed(self) -> None:
        if not self.passed:
            joined = "\n  • ".join(self.errors)
            raise ValueError(f"Validation failed:\n  • {joined}")


def validate_revenue_dataframe(
    df: pd.DataFrame,
    cols: RevenueColumns,
) -> None:
    """
    Validate the Phase 4 revenue DataFrame before any computation.

    Checks
    ------
    * Non-empty.
    * No null AMC names.
    * No null AUM values; all AUM values > 0.
    * No null revenue values; all revenue values > 0.
    """
    vr = ValidationResult()

    if df.empty:
        vr.fail("Revenue estimates DataFrame is empty.")
        vr.raise_if_failed()

    _check_nulls(df, cols.amc, "AMC name", vr)
    _check_nulls_and_positive(df, cols.current_aum, "current AUM", vr)
    _check_nulls_and_positive(df, cols.revenue, "current revenue", vr)

    vr.raise_if_failed()


def validate_aum_forecast_dataframe(
    df: pd.DataFrame,
    cols: AumForecastColumns,
) -> None:
    """
    Validate the AUM forecast DataFrame before merging.

    Checks
    ------
    * Non-empty.
    * No null AMC names or scenario names.
    * No null or non-positive forecast AUM values.
    """
    vr = ValidationResult()

    if df.empty:
        vr.fail("AUM forecast DataFrame is empty.")
        vr.raise_if_failed()

    _check_nulls(df, cols.amc, "AMC name", vr)
    _check_nulls(df, cols.scenario, "scenario", vr)

    for col_attr, label in [(cols.y1, "Year 1 AUM"), (cols.y2, "Year 2 AUM"), (cols.y3, "Year 3 AUM")]:
        _check_nulls_and_positive(df, col_attr, label, vr)

    vr.raise_if_failed()


def validate_output_dataframe(df: pd.DataFrame) -> None:
    """
    Post-generation validation of the combined revenue forecast DataFrame.

    Checks
    ------
    * No null values anywhere.
    * No negative revenue forecasts.
    * No duplicate (AMC, scenario) combinations.
    * Revenue yield values are positive.
    """
    vr = ValidationResult()

    total_nulls = df.isnull().sum().sum()
    if total_nulls > 0:
        null_detail = df.isnull().sum()
        vr.fail(
            f"Output DataFrame contains {total_nulls} null value(s):\n"
            f"{null_detail[null_detail > 0]}"
        )

    for col in [COL_REV_Y1, COL_REV_Y2, COL_REV_Y3]:
        neg = (df[col] < 0).sum()
        if neg > 0:
            vr.fail(f"{neg} negative value(s) found in '{col}'.")

    duplicates = df.duplicated(subset=[COL_AMC, COL_SCENARIO]).sum()
    if duplicates > 0:
        vr.fail(f"{duplicates} duplicate (AMC, Scenario) combinations detected.")

    non_positive_yield = (df[COL_YIELD] <= 0).sum()
    if non_positive_yield > 0:
        vr.fail(f"{non_positive_yield} row(s) have non-positive revenue yield.")

    vr.raise_if_failed()


# ── Internal validation helpers ────────────────────────────────────────────

def _check_nulls(
    df: pd.DataFrame,
    col: str,
    label: str,
    vr: ValidationResult,
) -> None:
    count = df[col].isna().sum()
    if count > 0:
        vr.fail(f"{count} null value(s) in {label} column ('{col}').")


def _check_nulls_and_positive(
    df: pd.DataFrame,
    col: str,
    label: str,
    vr: ValidationResult,
) -> None:
    _check_nulls(df, col, label, vr)
    non_positive = (pd.to_numeric(df[col], errors="coerce").fillna(0) <= 0).sum()
    if non_positive > 0:
        vr.fail(f"{non_positive} non-positive value(s) in {label} column ('{col}').")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — REVENUE YIELD CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def calculate_revenue_yields(
    rev_df: pd.DataFrame,
    rev_cols: RevenueColumns,
) -> pd.DataFrame:
    """
    Compute the implied revenue yield for each AMC.

    Formula
    -------
        Revenue Yield = Current Revenue / Current AUM

    The yield is assumed constant across forecast years (i.e. constant expense
    ratio and profitability). No adjustments are applied.

    Parameters
    ----------
    rev_df   : Phase 4 revenue DataFrame.
    rev_cols : Resolved column names for rev_df.

    Returns
    -------
    DataFrame with columns [COL_AMC, COL_YIELD, COL_CUR_REVENUE].
    """
    df = rev_df[[rev_cols.amc, rev_cols.revenue, rev_cols.current_aum]].copy()
    df = df.rename(columns={
        rev_cols.amc: COL_AMC,
        rev_cols.revenue: COL_CUR_REVENUE,
    })

    df[COL_YIELD] = (
        pd.to_numeric(df[COL_CUR_REVENUE], errors="raise")
        / pd.to_numeric(rev_df[rev_cols.current_aum], errors="raise")
    )

    logging.info(
        "Revenue yield stats — mean: %.4f%%  min: %.4f%%  max: %.4f%%",
        df[COL_YIELD].mean() * 100,
        df[COL_YIELD].min() * 100,
        df[COL_YIELD].max() * 100,
    )

    return df[[COL_AMC, COL_YIELD, COL_CUR_REVENUE]]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — REVENUE FORECAST ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def generate_revenue_forecasts(
    aum_fcst_df: pd.DataFrame,
    aum_cols: AumForecastColumns,
    yield_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge AUM forecasts with revenue yields and compute scenario revenue forecasts.

    Methodology
    -----------
    For each (AMC, Scenario) row in the AUM forecast file:

        Revenue_Yn = Year_N_AUM × Revenue_Yield

    The yield is sourced from the current-period revenue data and held constant.

    Parameters
    ----------
    aum_fcst_df : AUM forecast DataFrame (all scenarios, all AMCs).
    aum_cols    : Resolved column names for aum_fcst_df.
    yield_df    : DataFrame with columns [COL_AMC, COL_YIELD, COL_CUR_REVENUE].

    Returns
    -------
    DataFrame with columns defined in OUTPUT_COLUMNS.

    Raises
    ------
    ValueError
        If any AMC in the AUM forecast file has no matching yield.
    """
    # Normalise the AUM forecast to standard internal names
    aum_std = aum_fcst_df.rename(columns={
        aum_cols.amc: COL_AMC,
        aum_cols.scenario: COL_SCENARIO,
        aum_cols.y1: "_y1_aum",
        aum_cols.y2: "_y2_aum",
        aum_cols.y3: "_y3_aum",
    })[[COL_AMC, COL_SCENARIO, "_y1_aum", "_y2_aum", "_y3_aum"]]

    # Merge yields — left join so we detect missing AMCs
    merged = aum_std.merge(yield_df, on=COL_AMC, how="left")

    # Check for AMCs that did not get a yield
    missing_yield = merged[merged[COL_YIELD].isna()][COL_AMC].unique()
    if len(missing_yield) > 0:
        raise ValueError(
            f"{len(missing_yield)} AMC(s) in the AUM forecast file have no "
            f"matching revenue yield:\n  {list(missing_yield)}\n"
            "Ensure estimate_revenue.py (Phase 4) has been run and covers all AMCs."
        )

    # Apply yield to each forecast year
    merged[COL_REV_Y1] = pd.to_numeric(merged["_y1_aum"]) * merged[COL_YIELD]
    merged[COL_REV_Y2] = pd.to_numeric(merged["_y2_aum"]) * merged[COL_YIELD]
    merged[COL_REV_Y3] = pd.to_numeric(merged["_y3_aum"]) * merged[COL_YIELD]

    result = (
        merged[OUTPUT_COLUMNS]
        .sort_values([COL_AMC, COL_SCENARIO])
        .reset_index(drop=True)
    )

    logging.info(
        "Revenue forecasts generated — %d rows  (%d AMCs × %d scenarios)",
        len(result),
        result[COL_AMC].nunique(),
        result[COL_SCENARIO].nunique(),
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — EXPORT FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def export_revenue_forecasts(
    df: pd.DataFrame,
    export_dir: Path,
    latest_filename: str,
    archive_filename: str,
) -> tuple[Path, Path]:
    """
    Write the forecast DataFrame to a timestamped archive and a latest file.

    Parameters
    ----------
    df               : Validated revenue forecast DataFrame.
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
# SECTION 8 — MAIN EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Orchestrate the full revenue forecasting pipeline.

    Steps
    -----
    1. Load Phase 4 revenue estimates.
    2. Load Phase 5 AUM forecasts.
    3. Validate both inputs.
    4. Compute per-AMC revenue yields.
    5. Generate scenario revenue forecasts.
    6. Validate output.
    7. Export to CSV.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    TOTAL_STEPS = 7
    _banner("AMC REVENUE FORECAST")

    # ── Step 1: Load revenue estimates ────────────────────────────────────
    _step(1, TOTAL_STEPS, "Loading revenue estimates")
    try:
        rev_df, rev_cols = load_revenue_estimates(REVENUE_CSV)
    except (FileNotFoundError, ValueError) as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 2: Load AUM forecasts ────────────────────────────────────────
    _step(2, TOTAL_STEPS, "Loading AUM forecasts")
    try:
        aum_fcst_df, aum_cols = load_aum_forecasts(AUM_FCST_CSV)
    except (FileNotFoundError, ValueError) as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 3: Validate inputs ───────────────────────────────────────────
    _step(3, TOTAL_STEPS, "Validating inputs")
    try:
        validate_revenue_dataframe(rev_df, rev_cols)
        validate_aum_forecast_dataframe(aum_fcst_df, aum_cols)
    except ValueError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 4: Calculate revenue yields ─────────────────────────────────
    _step(4, TOTAL_STEPS, "Calculating revenue yields")
    try:
        yield_df = calculate_revenue_yields(rev_df, rev_cols)
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 5: Generate revenue forecasts ────────────────────────────────
    _step(5, TOTAL_STEPS, "Generating revenue forecasts")
    try:
        forecast_df = generate_revenue_forecasts(aum_fcst_df, aum_cols, yield_df)
    except ValueError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 6: Validate output ───────────────────────────────────────────
    _step(6, TOTAL_STEPS, "Validating outputs")
    try:
        validate_output_dataframe(forecast_df)
    except ValueError as exc:
        print(f"FAIL\n\n{exc}")
        sys.exit(1)
    _pass()

    # ── Step 7: Export results ────────────────────────────────────────────
    _step(7, TOTAL_STEPS, "Exporting results")
    try:
        latest_path, archive_path = export_revenue_forecasts(
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
    avg_yield_pct  = yield_df[COL_YIELD].mean() * 100

    print(f"{'AMCs Processed':<16}: {amc_count}")
    print(f"{'Scenarios':<16}: {scenario_count}")
    print(f"{'Forecast Rows':<16}: {row_count}")
    print()
    print(f"{'Average Yield':<16}: {avg_yield_pct:.2f}%")
    print()
    print(f"Latest File:\n{latest_path}")
    print()
    print(f"Archive File:\n{archive_path}")
    print()
    print("Completed Successfully")


if __name__ == "__main__":
    main()