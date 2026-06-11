"""
amfi_aum_pipeline.py
====================
Production-grade AMFI AUM ingestion pipeline for the AMC Dashboard platform.

Pipeline stages
---------------
  1. CONFIG            — paths, constants, sentinel values
  2. DOWNLOADER        — Playwright scraper → raw Excel
  3. LOADER            — raw Excel → untyped DataFrame
  4. EXTRACTOR         — AMC-level "Mutual Fund Total" rows → raw records
  5. CLEANER           — normalise names, coerce numerics, enrich metadata
  6. VALIDATOR         — sanity checks; raises on fatal violations
  7. RUNNER            — orchestrates stages, persists outputs, prints summary

Data contract
-------------
Only rows whose *first non-empty text cell* contains the literal phrase
"Mutual Fund Total" (case-insensitive) are treated as AMC AUM records.
All scheme-level rows, section headers, and blank rows are discarded.

Output schema:  amc_name | aum_cr | month | ingested_at

Engineering assumptions (documented where applied)
---------------------------------------------------
A. AUM value = rightmost parseable numeric in the row.
   AMFI occasionally shifts column positions between releases; scanning
   rightward is more robust than hard-coding a column index.
B. Any AMC whose AUM rounds to zero is dropped (data artefact, not a real fund).
C. "Mutual Fund Total" suffix regex is greedy — handles variants like
   "- Mutual Fund Total", "–Mutual Fund Total", etc.
D. Deduplication key is (amc_name, month); last occurrence wins inside a
   single export (should not happen, but guards against copy-paste rows).
E. VALID_AMC_RANGE = (10, 60) — SEBI registered AMC count as of 2024 is ~44.
   Pipeline warns (not fails) if the extracted count falls outside this range.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from playwright.sync_api import Page, sync_playwright


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

AMFI_URL       = "https://www.amfiindia.com/aum-data/average-aum"

RAW_DIR        = Path("data/raw/aum")
PROCESSED_DIR  = Path("data/processed/aum")

# Dropdown indices on the AMFI page (0-based MUI Autocomplete components)
class DropdownIdx:
    DATA   = 0   # "Schemewise" / "Fundwise"
    TYPE   = 1   # "Typewise" / "Overallwise"
    AMC    = 2   # "ALL" or specific AMC
    FY     = 3   # Financial year e.g. "April 2025 - March 2026"
    PERIOD = 4   # Month/quarter — we always pick the *first* (latest) option

FINANCIAL_YEAR = "April 2025 - March 2026"

# Marker text that uniquely identifies an AMC-level total row
AMC_TOTAL_MARKER = "Mutual Fund Total"

# Regex to strip the suffix from raw AMC names (assumption C above)
_SUFFIX_RE = re.compile(
    r"\s*[-–—]?\s*Mutual Fund Total\s*$",
    re.IGNORECASE,
)

# Sanity-check bounds for the number of AMCs we expect (assumption E)
VALID_AMC_RANGE: tuple[int, int] = (10, 60)

# Sentinel values that indicate an empty / non-numeric cell
_EMPTY_SENTINELS = frozenset({"", "-", "–", "—", "nan", "none", "n/a", "null"})


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — PLAYWRIGHT DOWNLOADER
# ═══════════════════════════════════════════════════════════════════════════════

class DownloadResult(NamedTuple):
    raw_path: Path
    period_text: str


def _wait_and_click_option(page: Page, dropdown_idx: int, option_text: str) -> None:
    """
    Open the nth MUI Autocomplete widget and click the named option.

    Uses exact=False so partial label matches still work when AMFI adds
    leading/trailing whitespace to option text (observed occasionally).
    """
    page.locator(".MuiAutocomplete-root").nth(dropdown_idx).click()
    option = page.get_by_role("option", name=option_text, exact=False)
    option.wait_for(timeout=15_0000)
    option.click()
    page.wait_for_timeout(8000)


def _pick_latest_period(page: Page) -> str:
    """
    Open the period dropdown and select the topmost (most recent) entry.
    Returns the human-readable period label, e.g. "March 2026".
    """
    page.locator(".MuiAutocomplete-root").nth(DropdownIdx.PERIOD).click()
    first_option = page.locator('[role="option"]').first
    first_option.wait_for(timeout=15_000)
    period_text = first_option.inner_text().strip()
    print(f"  [downloader] Latest period : {period_text}")
    first_option.click()
    page.wait_for_timeout(800)
    return period_text


def download_aum_excel() -> DownloadResult:
    """
    Stage 2 entry point.

    Drives the AMFI average-AUM page, applies the standard filter sequence,
    and downloads the resulting Excel file to RAW_DIR.

    Returns
    -------
    DownloadResult(raw_path, period_text)
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        print(f"  [downloader] Opening {AMFI_URL} …")
        page.goto(AMFI_URL)
        page.wait_for_load_state("networkidle")

        print("  [downloader] Applying filters …")
        _wait_and_click_option(page, DropdownIdx.DATA,  "Schemewise")
        _wait_and_click_option(page, DropdownIdx.TYPE,  "Typewise")
        _wait_and_click_option(page, DropdownIdx.AMC,   "ALL")
        _wait_and_click_option(page, DropdownIdx.FY,    FINANCIAL_YEAR)

        # Wait for the period dropdown to repopulate after FY selection
        page.wait_for_timeout(5_000)

        period_text = _pick_latest_period(page)

        print("  [downloader] Clicking GO …")
        page.get_by_role("button", name="GO").click()
        page.get_by_label("Download Excel").wait_for(timeout=60_000)

        print("  [downloader] Downloading Excel …")
        with page.expect_download(timeout=60_000) as dl_handle:
            page.get_by_label("Download Excel").click()

        download = dl_handle.value
        safe_period = re.sub(r"[^A-Za-z0-9]+", "_", period_text)
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_path    = RAW_DIR / f"amfi_aum_{safe_period}_{timestamp}.xlsx"

        download.save_as(str(raw_path))
        browser.close()

    print(f"  [downloader] Saved raw file : {raw_path.resolve()}")
    return DownloadResult(raw_path=raw_path, period_text=period_text)


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — EXCEL LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def load_excel(path: Path) -> pd.DataFrame:
    """
    Stage 3 entry point.

    Load the raw AMFI Excel into an untyped DataFrame with no header
    assumption.  Every value is kept as a string so that downstream stages
    control type coercion explicitly.

    Notes
    -----
    * header=None preserves all rows including AMC total rows that may
      appear before any clean column-header region.
    * dtype=str prevents silent numeric coercion of AUM figures by pandas.
    * map strip is cheaper than a per-column str.strip() loop.
    """
    df = pd.read_excel(path, header=None, dtype=str)
    df = df.map(lambda v: v.strip() if isinstance(v, str) else v)
    print(f"  [loader]     Raw shape     : {df.shape}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4 — AMC TOTAL EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_numeric(raw: str) -> float | None:
    """
    Parse a potentially dirty numeric string into a float.

    Handles:
      • Thousands commas        : "1,23,456.78" → 123456.78
      • Parenthetical negatives : "(1234.56)"   → -1234.56
      • Trailing/leading spaces : " 456 "       → 456.0
      • Empty sentinels         : "-", "nan"    → None
    """
    v = raw.strip().replace(",", "")
    if v.lower() in _EMPTY_SENTINELS:
        return None
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    try:
        return float(v)
    except ValueError:
        return None


def _rightmost_numeric(row_values: list) -> float | None:
    """
    Scan a row from right to left and return the first parseable float.

    Rationale: AMFI exports place the "Average AUM" figure in the last
    numeric column.  Column positions shift between releases (assumption A),
    so we scan rather than hard-code.  We skip columns 0 (the AMC name cell).
    """
    for cell in reversed(row_values[1:]):
        if pd.isna(cell):
            continue
        parsed = _parse_numeric(str(cell))
        if parsed is not None:
            return parsed
    return None


def extract_amc_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 4 entry point.

    Scan every row of the raw DataFrame and extract AMC-level total rows.

    Identification rule
    -------------------
    A row qualifies if its *first non-null, non-empty string cell* contains
    the substring "Mutual Fund Total" (case-insensitive).

    This matches:
      "SBI Mutual Fund Total"
      "HDFC Mutual Fund Total"
      "Axis Mutual Fund Total"

    And intentionally rejects:
      "Growth" / "Dividend" (scheme-level rows)
      "Equity" / "Debt"     (category headers)
      Completely blank rows

    AUM extraction
    --------------
    Uses _rightmost_numeric() — see assumption A in module docstring.

    Returns
    -------
    pd.DataFrame with columns: [amc_name_raw, aum_cr]
    Each row is a single AMC total record, undeduped, uncleaned.
    """
    records: list[dict] = []
    marker_lower = AMC_TOTAL_MARKER.lower()

    for _, row in df.iterrows():
        row_list = row.tolist()

        # Find the first cell that is a non-empty string
        first_text: str | None = None
        for cell in row_list:
            if pd.notna(cell) and str(cell).strip():
                first_text = str(cell).strip()
                break

        if first_text is None:
            continue  # blank row — skip

        if marker_lower not in first_text.lower():
            continue  # not an AMC total row — skip

        aum_value = _rightmost_numeric(row_list)

        # Drop rows where the AUM is missing or effectively zero (assumption B)
        if aum_value is None or abs(aum_value) < 1e-9:
            continue

        records.append({"amc_name_raw": first_text, "aum_cr": aum_value})

    if not records:
        return pd.DataFrame(columns=["amc_name_raw", "aum_cr"])

    return pd.DataFrame(records)


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 5 — CLEANER / NORMALIZER
# ═══════════════════════════════════════════════════════════════════════════════

def clean_amc_totals(raw: pd.DataFrame, period_text: str) -> pd.DataFrame:
    """
    Stage 5 entry point.

    Transform raw extracted records into the canonical output schema.

    Transformations
    ---------------
    amc_name    Strip "Mutual Fund Total" suffix (assumption C), collapse
                internal whitespace, title-case for consistency.
    aum_cr      Already float from extractor; cast explicitly to float64.
    month       Period label from the AMFI dropdown.
    ingested_at UTC ISO-8601 timestamp for this pipeline run.

    Post-clean deduplication
    ------------------------
    Deduplicate on (amc_name, month) keeping the last occurrence
    (assumption D).  Inside a single AMFI export duplicates should not
    exist, but this guards against copy-paste artefacts.
    """
    if raw.empty:
        return pd.DataFrame(columns=["amc_name", "aum_cr", "month", "ingested_at"])

    df = raw.copy()

    df["amc_name"] = (
        df["amc_name_raw"]
        .str.replace(_SUFFIX_RE, "", regex=True)   # drop "Mutual Fund Total"
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)       # collapse internal spaces
        .str.title()                                  # consistent casing
    )

    df["aum_cr"]     = df["aum_cr"].astype("float64")
    df["month"]      = period_text.strip()
    df["ingested_at"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Drop rows whose name resolved to empty after stripping
    df = df[df["amc_name"].str.len() > 0].copy()

    # Deduplicate — keep last occurrence per (amc_name, month)
    df = df.drop_duplicates(subset=["amc_name", "month"], keep="last")

    return df[["amc_name", "aum_cr", "month", "ingested_at"]].reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 6 — VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class ValidationError(RuntimeError):
    """Raised when the cleaned dataset fails a fatal sanity check."""


def validate(df: pd.DataFrame) -> None:
    """
    Stage 6 entry point.

    Run sanity checks on the cleaned DataFrame.  Fatal violations raise
    ValidationError and halt the pipeline.  Warnings are printed but do
    not stop execution.

    Checks
    ------
    FATAL  — null AMC names
    FATAL  — null AUM values
    FATAL  — negative AUM values  (negative AUM is a data error; net AUM
              by definition cannot be negative for a fund house total)
    WARN   — row count outside VALID_AMC_RANGE (assumption E)
    WARN   — duplicate AMC names (deduplication may have been imperfect)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # ── Fatal checks ────────────────────────────────────────────────────────

    null_names = df["amc_name"].isna() | (df["amc_name"].str.strip() == "")
    if null_names.any():
        errors.append(f"Null/empty AMC names in {null_names.sum()} row(s).")

    null_aum = df["aum_cr"].isna()
    if null_aum.any():
        errors.append(f"Null AUM values in {null_aum.sum()} row(s).")

    negative_aum = df["aum_cr"] < 0
    if negative_aum.any():
        bad = df.loc[negative_aum, "amc_name"].tolist()
        errors.append(f"Negative AUM values for: {bad}")

    # ── Warning checks ───────────────────────────────────────────────────────

    n = len(df)
    lo, hi = VALID_AMC_RANGE
    if not (lo <= n <= hi):
        warnings.append(
            f"Extracted {n} AMC rows; expected between {lo} and {hi}. "
            "Verify the AMFI export format has not changed."
        )

    dupes = df["amc_name"].duplicated(keep=False)
    if dupes.any():
        warnings.append(
            f"Duplicate AMC names after deduplication: "
            f"{df.loc[dupes, 'amc_name'].unique().tolist()}"
        )

    # ── Emit results ─────────────────────────────────────────────────────────

    for w in warnings:
        print(f"  [validator]  WARNING : {w}", file=sys.stderr)

    if errors:
        msg = "\n".join(f"  • {e}" for e in errors)
        raise ValidationError(f"Validation failed:\n{msg}")

    print(f"  [validator]  All checks passed ({n} AMCs).")


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 7 — MAIN RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def _build_output_path(period_text: str) -> Path:
    safe_period = re.sub(r"[^A-Za-z0-9]+", "_", period_text)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROCESSED_DIR / f"aum_clean_{safe_period}_{timestamp}.csv"


def run() -> None:
    """
    Orchestrate all pipeline stages end-to-end.

    Exit codes
    ----------
    0  — success
    1  — extraction yielded no records (format change or network error)
    2  — validation failed (data integrity violation)
    """
    sep = "═" * 58
    print(f"\n{sep}")
    print("  AMFI AUM Ingestion Pipeline")
    print(f"{sep}\n")

    # ── Stage 2: Download ────────────────────────────────────────────────────
    result = download_aum_excel()

    # ── Stage 3: Load ────────────────────────────────────────────────────────
    print("\n[Stage 3] Loading raw Excel …")
    raw_df = load_excel(result.raw_path)

    # ── Stage 4: Extract ─────────────────────────────────────────────────────
    print("\n[Stage 4] Extracting AMC totals …")
    extracted = extract_amc_totals(raw_df)

    if extracted.empty:
        print(
            "ERROR: No AMC total rows found. "
            "The Excel format may have changed — inspect the raw file.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"  [extractor]  Candidate rows : {len(extracted)}")

    # ── Stage 5: Clean ───────────────────────────────────────────────────────
    print("\n[Stage 5] Cleaning & normalising …")
    clean_df = clean_amc_totals(extracted, result.period_text)

    # ── Stage 6: Validate ────────────────────────────────────────────────────
    print("\n[Stage 6] Validating …")
    try:
        validate(clean_df)
    except ValidationError as exc:
        print(f"\n{exc}", file=sys.stderr)
        sys.exit(2)

    # ── Stage 7: Persist ─────────────────────────────────────────────────────
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _build_output_path(result.period_text)
    clean_df.to_csv(output_path, index=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_aum = clean_df["aum_cr"].sum()
    print(f"\n{sep}")
    print(f"  Period        : {result.period_text}")
    print(f"  AMCs          : {len(clean_df)}")
    print(f"  Industry AUM  : ₹{total_aum:,.0f} Cr")
    print(f"  Raw Excel     : {result.raw_path.resolve()}")
    print(f"  Clean CSV     : {output_path.resolve()}")
    print(f"{sep}\n")

    # Top-10 preview
    print("Top-10 AMCs by AUM (₹ Cr):\n")
    preview = (
        clean_df[["amc_name", "aum_cr"]]
        .sort_values("aum_cr", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    preview.index += 1
    preview.columns = ["AMC Name", "AUM (₹ Cr)"]
    preview["AUM (₹ Cr)"] = preview["AUM (₹ Cr)"].map("{:,.2f}".format)
    print(preview.to_string())
    print()


if __name__ == "__main__":
    run()

