"""
04-revenue-model/validate_revenue.py

Validates the output of estimate_revenue.py for internal consistency
and data quality. Produces a structured validation report.

Author: aditya.mishra10@nmims.in
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
INPUT_FILE = PROJECT_ROOT / "data" / "exports" / "revenue" / "amc_revenue_latest.csv"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "revenue"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS: list[str] = [
    "rank",
    "amc_name",
    "aum_cr",
    "yield_pct",
    "estimated_revenue_cr",
    "report_date",
    "generated_at",
]

YIELD_MIN: float = 0.05
YIELD_MAX: float = 2.00
REVENUE_TOLERANCE: float = 0.01

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_WARNING = "WARNING"

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
# Result model
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    check_name: str
    status: str
    details: str


@dataclass
class ValidationState:
    results: list[CheckResult] = field(default_factory=list)
    df: Optional[pd.DataFrame] = None
    total_aum_cr: float = 0.0
    total_revenue_cr: float = 0.0

    def add(self, name: str, status: str, details: str) -> CheckResult:
        r = CheckResult(check_name=name, status=status, details=details)
        self.results.append(r)
        return r

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == STATUS_PASS)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == STATUS_FAIL)

    @property
    def warnings(self) -> int:
        return sum(1 for r in self.results if r.status == STATUS_WARNING)

    @property
    def overall_status(self) -> str:
        return STATUS_FAIL if self.failed > 0 else STATUS_PASS


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_file_exists(state: ValidationState) -> None:
    name = "FILE EXISTS"
    if INPUT_FILE.exists():
        state.add(name, STATUS_PASS, f"Found: {INPUT_FILE}")
    else:
        state.add(name, STATUS_FAIL, f"File not found: {INPUT_FILE}")


def check_file_readable(state: ValidationState) -> None:
    name = "FILE READABLE"
    if not INPUT_FILE.exists():
        state.add(name, STATUS_FAIL, "Skipped — file does not exist")
        return
    try:
        state.df = pd.read_csv(INPUT_FILE)
        state.add(name, STATUS_PASS, f"Loaded {len(state.df)} rows")
    except Exception as exc:
        state.add(name, STATUS_FAIL, f"Read error: {exc}")


def check_required_columns(state: ValidationState) -> None:
    name = "REQUIRED COLUMNS"
    if state.df is None:
        state.add(name, STATUS_FAIL, "Skipped — file could not be read")
        return
    present = set(state.df.columns)
    missing = [c for c in REQUIRED_COLUMNS if c not in present]
    if not missing:
        state.add(name, STATUS_PASS, "All required columns present")
    else:
        state.add(name, STATUS_FAIL, f"Missing columns: {missing}")


def check_not_empty(state: ValidationState) -> None:
    name = "DATASET NOT EMPTY"
    if state.df is None:
        state.add(name, STATUS_FAIL, "Skipped — file could not be read")
        return
    if len(state.df) > 0:
        state.add(name, STATUS_PASS, f"{len(state.df)} rows present")
    else:
        state.add(name, STATUS_FAIL, "Dataset contains 0 rows")


def check_no_duplicate_amc_names(state: ValidationState) -> None:
    name = "NO DUPLICATE AMC NAMES"
    if state.df is None or "amc_name" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    dupes = state.df["amc_name"][state.df["amc_name"].duplicated()].tolist()
    if not dupes:
        state.add(name, STATUS_PASS, "No duplicate AMC names")
    else:
        state.add(name, STATUS_FAIL, f"Duplicate AMC names: {dupes}")


def check_rank_sequence(state: ValidationState) -> None:
    name = "RANK SEQUENCE VALID"
    if state.df is None or "rank" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    ranks = state.df["rank"].tolist()
    n = len(ranks)
    expected = list(range(1, n + 1))
    if ranks == expected:
        state.add(name, STATUS_PASS, f"Valid sequence 1–{n}")
    else:
        if ranks[0] != 1:
            detail = f"Does not start at 1 (starts at {ranks[0]})"
        elif len(set(ranks)) != len(ranks):
            detail = "Duplicate rank values detected"
        elif sorted(ranks) != expected:
            detail = f"Gaps or non-sequential ranks detected"
        else:
            detail = f"Rank order mismatch"
        state.add(name, STATUS_FAIL, detail)


def check_positive_aum(state: ValidationState) -> None:
    name = "POSITIVE AUM"
    if state.df is None or "aum_cr" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    bad = state.df[~(state.df["aum_cr"] > 0)]
    if bad.empty:
        state.add(name, STATUS_PASS, "All AUM values are positive")
    else:
        names = bad["amc_name"].tolist() if "amc_name" in bad.columns else bad.index.tolist()
        state.add(name, STATUS_FAIL, f"Non-positive AUM for: {names}")


def check_positive_revenue(state: ValidationState) -> None:
    name = "POSITIVE REVENUE"
    if state.df is None or "estimated_revenue_cr" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    bad = state.df[~(state.df["estimated_revenue_cr"] > 0)]
    if bad.empty:
        state.add(name, STATUS_PASS, "All revenue values are positive")
    else:
        names = bad["amc_name"].tolist() if "amc_name" in bad.columns else bad.index.tolist()
        state.add(name, STATUS_FAIL, f"Non-positive revenue for: {names}")


def check_yield_range(state: ValidationState) -> None:
    name = "VALID YIELD RANGE"
    if state.df is None or "yield_pct" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    bad = state.df[~state.df["yield_pct"].between(YIELD_MIN, YIELD_MAX)]
    if bad.empty:
        state.add(
            name,
            STATUS_PASS,
            f"All yields within [{YIELD_MIN}%, {YIELD_MAX}%]",
        )
    else:
        rows = bad[["amc_name", "yield_pct"]].to_dict(orient="records")
        state.add(name, STATUS_FAIL, f"Out-of-range yields: {rows}")


def check_revenue_formula(state: ValidationState) -> None:
    name = "REVENUE FORMULA RECONCILIATION"
    required = {"aum_cr", "yield_pct", "estimated_revenue_cr"}
    if state.df is None or not required.issubset(state.df.columns):
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    df = state.df.copy()
    df["_expected"] = (df["aum_cr"] * (df["yield_pct"] / 100)).round(2)
    df["_variance"] = (df["estimated_revenue_cr"] - df["_expected"]).abs().round(2)
    bad = df[df["_variance"] > REVENUE_TOLERANCE]
    if bad.empty:
        state.add(name, STATUS_PASS, f"All rows reconcile within ±{REVENUE_TOLERANCE}")
    else:
        rows = bad[["amc_name", "estimated_revenue_cr", "_expected", "_variance"]].to_dict(
            orient="records"
        )
        state.add(name, STATUS_FAIL, f"Formula mismatch (tolerance {REVENUE_TOLERANCE}): {rows}")


def check_revenue_sort_order(state: ValidationState) -> None:
    name = "REVENUE SORT ORDER"
    if state.df is None or "estimated_revenue_cr" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    revs = state.df["estimated_revenue_cr"].tolist()
    if revs == sorted(revs, reverse=True):
        state.add(name, STATUS_PASS, "Sorted descending by estimated_revenue_cr")
    else:
        state.add(name, STATUS_FAIL, "File is not sorted descending by estimated_revenue_cr")


def check_rank_revenue_consistency(state: ValidationState) -> None:
    name = "RANK VS REVENUE CONSISTENCY"
    required = {"rank", "estimated_revenue_cr"}
    if state.df is None or not required.issubset(state.df.columns):
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    df_sorted = state.df.sort_values("estimated_revenue_cr", ascending=False).reset_index(drop=True)
    df_sorted["_expected_rank"] = df_sorted.index + 1
    mismatches = df_sorted[df_sorted["rank"] != df_sorted["_expected_rank"]]
    if mismatches.empty:
        state.add(name, STATUS_PASS, "Rank is consistent with revenue order")
    else:
        rows = mismatches[["amc_name", "rank", "_expected_rank"]].to_dict(orient="records")
        state.add(name, STATUS_FAIL, f"Rank/revenue mismatch: {rows}")


def check_industry_totals(state: ValidationState) -> None:
    name = "INDUSTRY TOTALS"
    if state.df is None or "aum_cr" not in state.df.columns or "estimated_revenue_cr" not in state.df.columns:
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    state.total_aum_cr = round(state.df["aum_cr"].sum(), 2)
    state.total_revenue_cr = round(state.df["estimated_revenue_cr"].sum(), 2)
    state.add(
        name,
        STATUS_PASS,
        f"Total AUM: ₹{state.total_aum_cr:,.2f} Cr | Total Revenue: ₹{state.total_revenue_cr:,.2f} Cr",
    )


def check_top_amc_sanity(state: ValidationState) -> None:
    name = "TOP AMC SANITY CHECK"
    required = {"rank", "amc_name", "estimated_revenue_cr"}
    if state.df is None or not required.issubset(state.df.columns):
        state.add(name, STATUS_FAIL, "Skipped — prerequisite check failed")
        return
    top = state.df[state.df["rank"] == 1]
    if top.empty:
        state.add(name, STATUS_FAIL, "No row with rank = 1 found")
        return
    top_row = top.iloc[0]
    amc = top_row["amc_name"]
    rev = top_row["estimated_revenue_cr"]
    if pd.notna(amc) and str(amc).strip() and rev > 0:
        state.add(name, STATUS_PASS, f"Top AMC: '{amc}' | Revenue: ₹{rev:,.2f} Cr")
    else:
        state.add(name, STATUS_FAIL, f"Invalid top row — amc_name='{amc}', revenue={rev}")


def check_null_values(state: ValidationState) -> None:
    name = "NULL VALUE CHECK"
    if state.df is None:
        state.add(name, STATUS_FAIL, "Skipped — file could not be read")
        return
    cols_present = [c for c in REQUIRED_COLUMNS if c in state.df.columns]
    null_cols = [c for c in cols_present if state.df[c].isnull().any()]
    if not null_cols:
        state.add(name, STATUS_PASS, "No nulls in required columns")
    else:
        counts = {c: int(state.df[c].isnull().sum()) for c in null_cols}
        state.add(name, STATUS_FAIL, f"Null values found: {counts}")


# ---------------------------------------------------------------------------
# Check runner
# ---------------------------------------------------------------------------

CHECKS = [
    check_file_exists,
    check_file_readable,
    check_required_columns,
    check_not_empty,
    check_no_duplicate_amc_names,
    check_rank_sequence,
    check_positive_aum,
    check_positive_revenue,
    check_yield_range,
    check_revenue_formula,
    check_revenue_sort_order,
    check_rank_revenue_consistency,
    check_industry_totals,
    check_top_amc_sanity,
    check_null_values,
]

TOTAL_CHECKS = len(CHECKS)


def run_all_checks() -> ValidationState:
    """Execute all validation checks and return the populated state."""
    state = ValidationState()
    print()
    print("=" * 50)
    print("Revenue Validation")
    print("=" * 18)
    print()

    for i, check_fn in enumerate(CHECKS, start=1):
        label = check_fn.__name__.replace("check_", "").replace("_", " ").upper()
        print(f"[{i}/{TOTAL_CHECKS}] Checking {label.lower()}...")
        try:
            check_fn(state)
        except Exception as exc:
            state.add(label, STATUS_FAIL, f"Unexpected error: {exc}")
            logger.exception("Unexpected error in check '%s': %s", label, exc)

        last = state.results[-1]
        status_line = last.status
        if last.status == STATUS_FAIL:
            print(f"  {status_line}  →  {last.details}")
        else:
            print(f"  {status_line}")

    return state


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def export_results(state: ValidationState) -> tuple[Path, Path]:
    """Write validation results to CSV (timestamped + latest)."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    rows = [
        {"check_name": r.check_name, "status": r.status, "details": r.details}
        for r in state.results
    ]
    df = pd.DataFrame(rows)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts_path = EXPORT_DIR / f"revenue_validation_{ts}.csv"
    latest_path = EXPORT_DIR / "revenue_validation_latest.csv"

    df.to_csv(ts_path, index=False)
    df.to_csv(latest_path, index=False)

    logger.info("Exported: %s", ts_path)
    logger.info("Exported: %s", latest_path)

    return ts_path, latest_path


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(state: ValidationState, ts_path: Path, latest_path: Path) -> None:
    """Print the final validation summary block."""
    print()
    print("=" * 50)
    print("Validation Summary")
    print("=" * 18)
    print()
    print(f"  Checks Run : {len(state.results)}")
    print(f"  Passed     : {state.passed}")
    print(f"  Failed     : {state.failed}")
    print(f"  Warnings   : {state.warnings}")
    print()
    if state.total_aum_cr:
        print(f"  Industry AUM:")
        print(f"    ₹{state.total_aum_cr:,.2f} Cr")
        print()
    if state.total_revenue_cr:
        print(f"  Industry Revenue:")
        print(f"    ₹{state.total_revenue_cr:,.2f} Cr")
        print()
    print(f"  Validation Status: {state.overall_status}")
    print()
    print("  Files:")
    print(f"    * {latest_path.name}")
    print(f"    * {ts_path.name}")
    print()
    print("=" * 50)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    state = run_all_checks()
    ts_path, latest_path = export_results(state)
    print_summary(state, ts_path, latest_path)
    sys.exit(0 if state.overall_status == STATUS_PASS else 1)


if __name__ == "__main__":
    main()