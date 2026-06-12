"""
amfi_amc_expense_ratio.py
=========================
AMFI TER (Total Expense Ratio) ingestion pipeline.

Stages
------
  Stage 2 — DOWNLOADER   : Playwright-based Excel downloader from amfiindia.com
  Stage 3 — LOADER       : Read raw Excel into a pandas DataFrame
  Stage 4 — NORMALIZER   : Rename columns, keep only required fields
  Stage 5 — CLEANER      : Type coercions, null handling, timestamp injection
  Stage 6 — VALIDATOR    : Schema and business-rule checks
  Stage 7 — RUNNER       : Orchestrate all stages and write processed CSV

Assumptions
-----------
- The AMFI TER page uses MUI Autocomplete dropdowns that may render a
  conditional "Sub Category" dropdown after "Fund Type" is selected.
- The raw Excel is tabular with one row per (scheme × calendar date).
- TER values are expressed as percentages (float); negative values are
  treated as data errors.
- The processed CSV is written to data/processed/expense_ratio/.
- All timestamps are UTC ISO-8601.

Usage
-----
    python amfi_amc_expense_ratio.py

Dependencies
------------
    pip install playwright pandas openpyxl
    playwright install chromium
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from playwright.sync_api import Page, sync_playwright

# ── Directory layout ──────────────────────────────────────────────────────────

RAW_DIR       = Path("data/raw/expense_ratio")
PROCESSED_DIR = Path("data/processed/expense_ratio")

# ── Downloader config ─────────────────────────────────────────────────────────

URL = "https://www.amfiindia.com/ter-of-mf-schemes"

# ── Column mapping ─────────────────────────────────────────────────────────────

# Maps raw Excel headers → normalised column names.
COLUMN_MAP: dict[str, str] = {
    "NSDL Scheme Code"          : "scheme_code",
    "Scheme Name"               : "scheme_name",
    "Scheme Type"               : "scheme_type",
    "Scheme Category"           : "scheme_category",
    "TER Date"                  : "ter_date",
    "Regular Plan - Total TER (%)": "regular_ter",
    "Direct Plan - Total TER (%)" : "direct_ter",
}

# Only these columns survive normalisation.
REQUIRED_COLUMNS: list[str] = list(COLUMN_MAP.values())

# Values treated as missing during cleaning.
NULL_SENTINELS: set[str] = {"", "-", "na", "n/a", "null", "none"}

# Row-count thresholds for warnings (heuristic; adjust as AMFI data grows).
MIN_EXPECTED_ROWS = 1_000
MAX_EXPECTED_ROWS = 500_000


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 — DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════

class DownloadResult(NamedTuple):
    """Carries the artefacts produced by the downloader."""

    raw_path:    Path
    period_text: str   # e.g. "June-2026"
    fy_text:     str   # e.g. "2026-2027"


def _pick_first_in_dropdown(
    page: Page,
    dropdown_idx: int,
    wait_ms: int = 2_000,
    label_hint: str = "",
) -> str:
    """
    Open the nth MuiAutocomplete dropdown and click the topmost non-empty
    option.

    Parameters
    ----------
    page          : Playwright page object.
    dropdown_idx  : Zero-based index of the .MuiAutocomplete-root element.
    wait_ms       : Milliseconds to wait after clicking the option.
    label_hint    : Human-readable label for log output.

    Returns
    -------
    str
        The text label of the option that was selected.
    """
    if label_hint:
        print(f"    [dropdown {dropdown_idx}] selecting first option for '{label_hint}' …")
    else:
        print(f"    [dropdown {dropdown_idx}] selecting first option …")

    root = page.locator(".MuiAutocomplete-root").nth(dropdown_idx)
    inp  = root.locator("input")

    page.keyboard.press("Escape")
    page.wait_for_timeout(300)

    inp.click()
    inp.fill("")

    input_id = inp.get_attribute("id")
    print(f"    [dropdown {dropdown_idx}] waiting for listbox options to appear …")

    page.wait_for_function(
        """(inputId) => {
            const inp = document.getElementById(inputId);
            if (!inp) return false;
            const listboxId = inp.getAttribute('aria-controls');
            if (!listboxId) return false;
            const listbox = document.getElementById(listboxId);
            if (!listbox) return false;
            const opts = listbox.querySelectorAll('[role="option"]');
            return opts.length > 0 && opts[0].textContent.trim() !== '';
        }""",
        arg=input_id,
        timeout=15_000,
    )

    first_text = page.evaluate(
        """(inputId) => {
            const inp = document.getElementById(inputId);
            const listboxId = inp.getAttribute('aria-controls');
            const listbox = document.getElementById(listboxId);
            const opt = listbox.querySelector('[role="option"]');
            return opt ? opt.textContent.trim() : null;
        }""",
        input_id,
    )

    if not first_text:
        raise RuntimeError(f"Dropdown {dropdown_idx}: no options found in listbox.")

    print(f"    [dropdown {dropdown_idx}] clicking option: '{first_text}'")
    page.locator('[role="option"]').first.click()
    print(f"    [dropdown {dropdown_idx}] selected: '{first_text}'")
    page.wait_for_timeout(wait_ms)
    return first_text


def _dropdown_count(page: Page) -> int:
    """Return the number of MuiAutocomplete roots currently in the DOM."""
    return page.locator(".MuiAutocomplete-root").count()


def _wait_for_dropdown_count(page: Page, expected: int, timeout_ms: int = 10_000) -> None:
    """Block until at least *expected* MuiAutocomplete dropdowns are present."""
    print(f"    waiting for at least {expected} dropdowns to be present …")
    page.wait_for_function(
        f"() => document.querySelectorAll('.MuiAutocomplete-root').length >= {expected}",
        timeout=timeout_ms,
    )
    print(f"    found {_dropdown_count(page)} dropdowns.")


def _wait_for_go_enabled(page: Page, timeout_ms: int = 15_000) -> None:
    """Block until the GO button is no longer disabled."""
    print("    waiting for GO button to become enabled …")
    page.wait_for_function(
        """() => {
            const btn = Array.from(document.querySelectorAll('button'))
                .find(b => b.textContent.trim().toLowerCase() === 'go');
            return btn && !btn.disabled;
        }""",
        timeout=timeout_ms,
    )
    print("    GO button is now enabled.")


def download_ter_excel() -> DownloadResult:
    """
    Navigate to the AMFI TER page, select the most-recent period via the MUI
    Autocomplete filter cascade, click GO, and download the resulting Excel
    file.

    The file is saved to data/raw/expense_ratio/ with a timestamped filename.

    Returns
    -------
    DownloadResult
        Named tuple carrying the saved file path, period text, and FY text.

    Raises
    ------
    RuntimeError
        If fewer than 5 MUI dropdowns are found, if a listbox is empty, or if
        the GO button does not become enabled within the timeout.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[downloader] Download directory: {RAW_DIR.resolve()}")

    with sync_playwright() as pw:
        print("[downloader] Launching Chromium in headless mode …")
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        print(f"[downloader] Navigating to {URL} …")
        page.goto(URL)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3_000)
        print("[downloader] Page loaded, network idle.")

        initial_count = _dropdown_count(page)
        print(f"[downloader] Initial MuiAutocomplete dropdowns found: {initial_count}")
        if initial_count < 5:
            raise RuntimeError(
                f"Expected at least 5 MUI dropdowns, found {initial_count}."
            )

        print("[downloader] Starting filter selection …")

        # [0] Financial Year
        fy_text = _pick_first_in_dropdown(page, 0, wait_ms=3_000, label_hint="Financial Year")

        # [1] Month
        month_text = _pick_first_in_dropdown(page, 1, wait_ms=3_000, label_hint="Month")

        # [2] Fund Type — Sub Category may materialise after this selection
        _pick_first_in_dropdown(page, 2, wait_ms=1_000, label_hint="Fund Type")

        # Wait to detect whether Sub Category rendered (count 5 → 6)
        print("[downloader] Checking for Sub Category dropdown …")
        page.wait_for_timeout(2_000)
        count_after_fund_type = _dropdown_count(page)
        print(f"[downloader] Dropdowns after Fund Type: {count_after_fund_type}")

        has_sub_category = count_after_fund_type >= 6
        if has_sub_category:
            print("[downloader] Sub Category dropdown appeared (dynamic).")
        else:
            print("[downloader] No Sub Category dropdown detected.")

        # [3] Category
        _pick_first_in_dropdown(page, 3, wait_ms=3_000, label_hint="Category")

        if has_sub_category:
            # [4] Sub Category  [5] Mutual Fund
            _pick_first_in_dropdown(page, 4, wait_ms=3_000, label_hint="Sub Category")
            _pick_first_in_dropdown(page, 5, wait_ms=2_000, label_hint="Mutual Fund")
        else:
            # Sub Category absent — Mutual Fund is at index 4
            _pick_first_in_dropdown(page, 4, wait_ms=2_000, label_hint="Mutual Fund")

        # Enable check
        try:
            _wait_for_go_enabled(page, timeout_ms=15_000)
        except Exception:
            disabled = page.evaluate(
                """() => {
                    const btn = Array.from(document.querySelectorAll('button'))
                        .find(b => b.textContent.trim().toLowerCase() === 'go');
                    return btn ? btn.disabled : 'not found';
                }"""
            )
            raise RuntimeError(f"GO button did not enable. disabled={disabled}")

        print("[downloader] Clicking GO button …")
        go_btn = page.locator("button").filter(
            has_text=re.compile(r"^go$", re.IGNORECASE)
        )
        go_btn.click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(5_000)
        print("[downloader] GO clicked, waiting for results …")

        # Locate and trigger the Excel download
        print("[downloader] Looking for 'Download Excel' button …")
        dl_btn = page.locator("h4", has_text="Download Excel").locator("..")
        dl_btn.wait_for(timeout=60_000)
        print("[downloader] 'Download Excel' button found.")

        print("[downloader] Triggering download …")
        with page.expect_download(timeout=120_000) as dl_handle:
            dl_btn.click()

        download = dl_handle.value

        safe_fy    = re.sub(r"[^A-Za-z0-9]+", "_", fy_text)
        safe_month = re.sub(r"[^A-Za-z0-9]+", "_", month_text)
        timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_path   = RAW_DIR / f"amfi_expense_ratio_{safe_fy}_{safe_month}_{timestamp}.xlsx"

        print(f"[downloader] Saving file to: {raw_path.resolve()}")
        download.save_as(str(raw_path.resolve()))
        browser.close()

    print(f"[downloader] Download complete: {raw_path.resolve()}")
    return DownloadResult(raw_path=raw_path, period_text=month_text, fy_text=fy_text)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 — LOADER
# ══════════════════════════════════════════════════════════════════════════════

def load_excel(path: Path) -> pd.DataFrame:
    """
    Read the raw AMFI TER Excel file into a pandas DataFrame.

    All values are preserved as-is; no type coercions are applied here.
    That responsibility belongs to the cleaner (Stage 5).

    Parameters
    ----------
    path : Path
        Absolute or relative path to the .xlsx file produced by the downloader.

    Returns
    -------
    pd.DataFrame
        Raw data frame with original column names and dtypes inferred by
        pandas (typically object/str for most columns at this stage).

    Raises
    ------
    FileNotFoundError
        If *path* does not point to an existing file.
    ValueError
        If pandas cannot parse the file as Excel.
    """
    print(f"\n[loader] Reading Excel: {path}")

    if not path.exists():
        raise FileNotFoundError(f"[loader] File not found: {path}")

    try:
        df = pd.read_excel(path, dtype=str)   # dtype=str preserves all raw values
    except Exception as exc:
        raise ValueError(f"[loader] Failed to parse Excel file '{path}': {exc}") from exc

    print(f"[loader] Loaded shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"[loader] Columns: {df.columns.tolist()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Stage 4 — NORMALIZER
# ══════════════════════════════════════════════════════════════════════════════

def normalize_expense_ratio_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename raw Excel headers to the canonical schema and drop all columns that
    are not part of the output specification.

    Responsibilities
    ----------------
    - Apply COLUMN_MAP renames.
    - Retain only REQUIRED_COLUMNS.
    - Strip leading/trailing whitespace from all string cells.
    - Preserve original scheme naming (no case normalisation at this stage).

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame produced by load_excel().

    Returns
    -------
    pd.DataFrame
        DataFrame with exactly the columns listed in REQUIRED_COLUMNS.

    Raises
    ------
    KeyError
        If any column in COLUMN_MAP is absent from the raw DataFrame.
    """
    print("\n[normalizer] Starting column normalisation …")

    # Verify all expected source columns are present before renaming.
    missing = [col for col in COLUMN_MAP if col not in df.columns]
    if missing:
        raise KeyError(
            f"[normalizer] Expected column(s) not found in raw data: {missing}"
        )

    # Rename to canonical names.
    df = df.rename(columns=COLUMN_MAP)
    print(f"[normalizer] Renamed {len(COLUMN_MAP)} columns.")

    # Drop all intermediate / unrequired columns.
    df = df[REQUIRED_COLUMNS].copy()
    print(f"[normalizer] Retained {len(REQUIRED_COLUMNS)} required columns: {REQUIRED_COLUMNS}")

    # Strip whitespace from every string-valued cell.
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    print(f"[normalizer] Stripped whitespace from {len(str_cols)} string columns.")

    print(f"[normalizer] Output shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Stage 5 — CLEANER
# ══════════════════════════════════════════════════════════════════════════════

def _coerce_to_float(series: pd.Series, col_name: str) -> pd.Series:
    """
    Convert a string Series to float, treating NULL_SENTINELS as NaN.

    Parameters
    ----------
    series   : pd.Series  — raw string values.
    col_name : str        — column label used in log output.

    Returns
    -------
    pd.Series
        Float series with NaN where conversion failed or sentinel was found.
    """
    # Normalise sentinel values to NaN before numeric coercion.
    sentinel_mask = series.str.lower().isin(NULL_SENTINELS)
    n_sentinels   = sentinel_mask.sum()
    if n_sentinels:
        print(f"[cleaner]   '{col_name}': replaced {n_sentinels:,} sentinel values with NaN")
    series = series.where(~sentinel_mask, other=pd.NA)

    coerced = pd.to_numeric(series, errors="coerce")
    n_failed = coerced.isna().sum() - sentinel_mask.sum()
    if n_failed > 0:
        print(f"[cleaner]   '{col_name}': {n_failed:,} non-numeric values coerced to NaN")
    return coerced


def clean_expense_ratio_data(df: pd.DataFrame, period_text: str) -> pd.DataFrame:
    """
    Apply type coercions, remove invalid rows, and inject the ingestion
    timestamp.

    Cleaning steps
    --------------
    1. Convert ``ter_date`` to datetime; drop rows where conversion fails.
    2. Convert ``regular_ter`` to float (NULL_SENTINELS → NaN).
    3. Convert ``direct_ter``  to float (NULL_SENTINELS → NaN).
    4. Drop rows where ``regular_ter`` or ``direct_ter`` are NaN.
    5. Add ``ingested_at`` column (UTC ISO-8601 string, same for all rows).

    Parameters
    ----------
    df          : pd.DataFrame  — normalised DataFrame from normalize_expense_ratio_data().
    period_text : str           — human-readable period label (e.g. "June-2026"),
                                  included in log output for traceability.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame ready for validation.
    """
    print(f"\n[cleaner] Cleaning data for period: {period_text}")
    n_initial = len(df)

    # ── ter_date → datetime ───────────────────────────────────────────────────
    df["ter_date"] = pd.to_datetime(df["ter_date"], errors="coerce")
    n_bad_dates = df["ter_date"].isna().sum()
    if n_bad_dates:
        print(f"[cleaner] Dropping {n_bad_dates:,} rows with unparseable ter_date.")
    df = df.dropna(subset=["ter_date"])

    # ── TER columns → float ───────────────────────────────────────────────────
    df["regular_ter"] = _coerce_to_float(df["regular_ter"], "regular_ter")
    df["direct_ter"]  = _coerce_to_float(df["direct_ter"],  "direct_ter")

    # ── Drop rows missing mandatory TER values ────────────────────────────────
    n_before = len(df)
    df = df.dropna(subset=["regular_ter", "direct_ter"])
    n_dropped = n_before - len(df)
    if n_dropped:
        print(f"[cleaner] Dropped {n_dropped:,} rows with NaN regular_ter or direct_ter.")

    # ── Inject ingested_at timestamp ──────────────────────────────────────────
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["ingested_at"] = ingested_at
    print(f"[cleaner] ingested_at set to: {ingested_at}")

    n_final = len(df)
    print(
        f"[cleaner] Rows: {n_initial:,} → {n_final:,} "
        f"({n_initial - n_final:,} dropped total)"
    )
    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 6 — VALIDATOR
# ══════════════════════════════════════════════════════════════════════════════

class ValidationError(RuntimeError):
    """
    Raised when the cleaned DataFrame fails a fatal schema or business-rule
    check.  The message identifies the violated rule.
    """


def validate(df: pd.DataFrame) -> None:
    """
    Enforce schema and business-rule constraints on the cleaned DataFrame.

    Fatal checks (raise ValidationError)
    -------------------------------------
    - ``scheme_code``  must have no fully-null column.
    - ``scheme_name``  must have no fully-null column.
    - ``regular_ter``  must have no fully-null column.
    - ``direct_ter``   must have no fully-null column.
    - No row may carry a negative value for ``regular_ter`` or ``direct_ter``.

    Warnings (print, do not raise)
    --------------------------------
    - Duplicate (scheme_code, ter_date) pairs.
    - Row count below MIN_EXPECTED_ROWS.
    - Row count above MAX_EXPECTED_ROWS.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned DataFrame produced by clean_expense_ratio_data().

    Raises
    ------
    ValidationError
        On any fatal check failure.
    """
    print("\n[validator] Running validation checks …")

    # ── Fatal: required columns must not be entirely null ─────────────────────
    for col in ("scheme_code", "scheme_name", "regular_ter", "direct_ter"):
        if df[col].isna().all():
            raise ValidationError(
                f"[validator] FATAL — column '{col}' is entirely null. "
                "Pipeline cannot continue."
            )
        n_null = df[col].isna().sum()
        if n_null:
            raise ValidationError(
                f"[validator] FATAL — column '{col}' has {n_null:,} null values "
                "after cleaning. Expected zero nulls for mandatory fields."
            )
    print("[validator] ✓ No null values in mandatory columns.")

    # ── Fatal: negative TER values ────────────────────────────────────────────
    for col in ("regular_ter", "direct_ter"):
        n_neg = (df[col] < 0).sum()
        if n_neg:
            raise ValidationError(
                f"[validator] FATAL — '{col}' has {n_neg:,} negative value(s). "
                "TER values must be ≥ 0."
            )
    print("[validator] ✓ No negative TER values.")

    # ── Warning: duplicate (scheme_code, ter_date) pairs ─────────────────────
    n_dupes = df.duplicated(subset=["scheme_code", "ter_date"]).sum()
    if n_dupes:
        print(
            f"[validator] WARNING — {n_dupes:,} duplicate (scheme_code, ter_date) "
            "pairs detected. Downstream consumers should be aware."
        )
    else:
        print("[validator] ✓ No duplicate (scheme_code, ter_date) pairs.")

    # ── Warning: suspiciously low row count ───────────────────────────────────
    n_rows = len(df)
    if n_rows < MIN_EXPECTED_ROWS:
        print(
            f"[validator] WARNING — only {n_rows:,} rows found. "
            f"Expected at least {MIN_EXPECTED_ROWS:,}. "
            "Data may be incomplete."
        )
    elif n_rows > MAX_EXPECTED_ROWS:
        print(
            f"[validator] WARNING — {n_rows:,} rows found. "
            f"Exceeds upper bound of {MAX_EXPECTED_ROWS:,}. "
            "Possible duplicate ingestion."
        )
    else:
        print(f"[validator] ✓ Row count {n_rows:,} within expected range.")

    print("[validator] Validation complete.")


# ══════════════════════════════════════════════════════════════════════════════
# Stage 7 — RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def _build_output_path(period_text: str) -> Path:
    """
    Construct the processed CSV filename from the period label and current
    timestamp.

    Format: expense_ratio_clean_<safe_period>_<YYYYmmdd_HHMMSS>.csv

    Parameters
    ----------
    period_text : str
        Human-readable period label returned by the downloader, e.g. "June-2026".

    Returns
    -------
    Path
        Full output path under PROCESSED_DIR.
    """
    safe_period = re.sub(r"[^A-Za-z0-9]+", "_", period_text).strip("_")
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename    = f"expense_ratio_clean_{safe_period}_{timestamp}.csv"
    return PROCESSED_DIR / filename


def _print_summary(
    df:           pd.DataFrame,
    period_text:  str,
    fy_text:      str,
    output_path:  Path,
) -> None:
    """
    Print a structured run summary to stdout.

    Includes period metadata, row/scheme counts, the output CSV path, and
    the top-10 schemes by average ``regular_ter``.

    Parameters
    ----------
    df          : Validated, cleaned DataFrame.
    period_text : Period label (e.g. "June-2026").
    fy_text     : Financial year label (e.g. "2026-2027").
    output_path : Path where the processed CSV was saved.
    """
    unique_schemes = df["scheme_code"].nunique()

    print("\n" + "═" * 60)
    print("  AMFI TER INGESTION — RUN SUMMARY")
    print("═" * 60)
    print(f"  Period            : {period_text}")
    print(f"  Financial Year    : {fy_text}")
    print(f"  Rows              : {len(df):,}")
    print(f"  Unique Schemes    : {unique_schemes:,}")
    print(f"  Processed CSV     : {output_path.resolve()}")
    print("─" * 60)
    print("  Top 10 Schemes by Average Regular TER")
    print("─" * 60)

    top10 = (
        df.groupby("scheme_name")["regular_ter"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )
    for rank, (scheme, ter) in enumerate(top10.items(), start=1):
        print(f"  {rank:>2}. {scheme:<50}  {ter:.4f}%")

    print("═" * 60)


def run() -> None:
    """
    Orchestrate the complete AMFI TER ingestion pipeline (Stages 2–7).

    Workflow
    --------
    1. Download the raw TER Excel from amfiindia.com (Stage 2).
    2. Load the Excel into a DataFrame              (Stage 3).
    3. Normalise column names and drop extras       (Stage 4).
    4. Clean types, nulls, and add ingested_at      (Stage 5).
    5. Validate schema and business rules           (Stage 6).
    6. Write the cleaned DataFrame to a CSV         (Stage 7).
    7. Print the run summary.

    Raises
    ------
    ValidationError
        Propagated from validate() on fatal data-quality failures.
    Any exception from the downloader or file I/O is allowed to propagate
    so that callers / scheduler frameworks receive the full traceback.
    """
    print("╔══════════════════════════════════════════════════════╗")
    print("║   AMFI TER Ingestion Pipeline — starting             ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    # ── Stage 2: Download ─────────────────────────────────────────────────────
    print("[runner] Stage 2 — Downloading TER Excel …")
    result = download_ter_excel()
    raw_path    = result.raw_path
    period_text = result.period_text
    fy_text     = result.fy_text
    print(f"[runner] Downloaded: {raw_path}  |  period={period_text}  |  FY={fy_text}")

    # ── Stage 3: Load ─────────────────────────────────────────────────────────
    print("\n[runner] Stage 3 — Loading Excel …")
    raw_df = load_excel(raw_path)

    # ── Stage 4: Normalise ────────────────────────────────────────────────────
    print("\n[runner] Stage 4 — Normalising columns …")
    norm_df = normalize_expense_ratio_data(raw_df)

    # ── Stage 5: Clean ────────────────────────────────────────────────────────
    print("\n[runner] Stage 5 — Cleaning data …")
    clean_df = clean_expense_ratio_data(norm_df, period_text)

    # ── Stage 6: Validate ─────────────────────────────────────────────────────
    print("\n[runner] Stage 6 — Validating data …")
    validate(clean_df)

    # ── Stage 7: Save ─────────────────────────────────────────────────────────
    print("\n[runner] Stage 7 — Writing processed CSV …")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _build_output_path(period_text)
    clean_df.to_csv(output_path, index=False)
    print(f"[runner] Saved: {output_path.resolve()}")

    # ── Summary ───────────────────────────────────────────────────────────────
    _print_summary(clean_df, period_text, fy_text, output_path)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        run()
    except ValidationError as exc:
        print(f"\n[PIPELINE FAILED] Validation error: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\n[PIPELINE FAILED] Unexpected error: {exc}", file=sys.stderr)
        sys.exit(1)