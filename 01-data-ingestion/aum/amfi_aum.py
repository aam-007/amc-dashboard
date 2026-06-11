"""
amfi_aum.py
====================
Production-grade AMFI AUM ingestion pipeline for the AMC Dashboard platform.

Pipeline stages
---------------
  1. CONFIG            — paths, constants, sentinel values
  2. DOWNLOADER        — Playwright scraper → raw Excel
  3. LOADER            — raw Excel → untyped DataFrame
  4. EXTRACTOR         — AMC total rows → raw records
  5. CLEANER           — enrich metadata, coerce numerics
  6. VALIDATOR         — sanity checks; raises on fatal violations
  7. RUNNER            — orchestrates stages, persists outputs, prints summary

Data contract
-------------
Rows are identified as AMC totals by two patterns (both case-insensitive):

  Pattern A (50 AMCs):  "<Name> Mutual Fund Total"
  Pattern B (IL&FS):    "<Name> Mutual Fund (<qualifier>) Total"

Both patterns are matched by _AMC_TOTAL_RE.  "Grand Total" is explicitly
excluded.  All scheme-level rows, section headers, AMC header rows, and
blank rows are discarded.

AUM is reported in Rs Lakhs in the AMFI export and is converted to Crores
by dividing by 100.

Output schema:  amc_name | aum_cr | month | ingested_at

Engineering assumptions (documented where applied)
---------------------------------------------------
A. AUM value = largest absolute-value numeric in columns 1+ of the row.
   The AMFI export has four columns: [name, NaN, aum_total_lakhs, aum_sub_lakhs].
   col3 is a sub-component (e.g. debt-only AUM) and is 0 for AMCs without
   debt/hybrid schemes. Scanning right-to-left (original approach) would pick
   col3 = 0, causing those AMCs to be silently dropped. Taking the largest
   numeric value robustly selects col2 (aum_total_lakhs) regardless of column
   count or position shifts between AMFI releases.
B. Any AMC whose AUM rounds to zero is dropped (data artefact, not a real fund).
C. "Mutual Fund [(<qualifier>)] Total" suffix is stripped from the AMC name.
D. Deduplication key is (amc_name, month); last occurrence wins inside a
   single export (should not happen, but guards against copy-paste rows).
E. VALID_AMC_RANGE = (10, 65) — SEBI registered AMC count as of June 2025
   is ~51. Pipeline warns (not fails) if count falls outside this range.
F. No whitelist matching — _AMC_TOTAL_RE is the sole identification criterion,
   making the extractor resilient to new AMC registrations and AMFI format
   changes.
G. AMC name casing is preserved from the raw cell value rather than applying
   .title(), which mangles tokens like "IL&FS", "BNP", "quant", "UTI", "HSBC".
   Only internal whitespace is collapsed; leading/trailing whitespace is stripped.
H. EXPECTED_AMCS is a frozenset of canonical AMC names (post-strip, original
   casing) used exclusively for reconciliation warnings in the validator.
   It is NOT a filter — AMCs absent from this set still appear in output.
   New AMC registrations appear automatically; only a warning fires so the
   set can be updated.
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

AMFI_URL      = "https://www.amfiindia.com/aum-data/average-aum"

RAW_DIR       = Path("data/raw/aum")
PROCESSED_DIR = Path("data/processed/aum")


class DropdownIdx:
    DATA   = 0   # "Schemewise" / "Fundwise"
    TYPE   = 1   # "Typewise" / "Overallwise"
    AMC    = 2   # "ALL" or specific AMC
    FY     = 3   # Financial year e.g. "April 2025 - March 2026"
    PERIOD = 4   # Month/quarter — always pick the *first* (latest) option


FINANCIAL_YEAR = "April 2025 - March 2026"

# Sanity-check bounds for AMC count (assumption E).
# ~51 registered AMCs as of June 2025; upper bound left loose for new entries.
VALID_AMC_RANGE: tuple[int, int] = (10, 65)

# Sentinel values that indicate an empty / non-numeric cell
_EMPTY_SENTINELS = frozenset({"", "-", "–", "—", "nan", "none", "n/a", "null"})

# ── Compiled regexes ─────────────────────────────────────────────────────────

# Matches BOTH AMC total row patterns (assumption F):
#   Pattern A: "Aditya Birla Sun Life Mutual Fund Total"
#   Pattern B: "IL&FS Mutual Fund (IDF) Total"
# The optional non-capturing group (?:\([^)]*\)\s*)? handles the parenthetical
# qualifier that AMFI inserts for IL&FS instead of a second "Mutual Fund".
_AMC_TOTAL_RE = re.compile(
    r"mutual\s+fund\s+(?:\([^)]*\)\s*)?total\s*$",
    re.IGNORECASE,
)

# Strips the same suffix from the cell value to recover the clean AMC name.
_TOTAL_STRIP_RE = re.compile(
    r"\s*mutual\s+fund\s+(?:\([^)]*\)\s*)?total\s*$",
    re.IGNORECASE,
)

# Grand Total row — must be excluded explicitly since it also ends with "Total"
_GRAND_TOTAL_RE = re.compile(r"^grand\s+total\s*$", re.IGNORECASE)

# Expected AMC names — post-strip, original casing (assumption H).
# Used ONLY for reconciliation warnings. Not a filter.
EXPECTED_AMCS: frozenset[str] = frozenset({
    "360 ONE",
    "Abakkus",
    "Aditya Birla Sun Life",
    "Angel One",
    "Axis",
    "Bajaj Finserv",
    "Bandhan",
    "Bank of India",
    "Baroda BNP Paribas",
    "Canara Robeco",
    "Capitalmind",
    "Choice",
    "DSP",
    "Edelweiss",
    "Franklin Templeton",
    "Groww",
    "HDFC",
    "HSBC",
    "Helios",
    "ICICI Prudential",
    "IL&FS",           # "(IDF)" qualifier stripped by _TOTAL_STRIP_RE
    "ITI",
    "Invesco",
    "JM Financial",
    "Jio BlackRock",
    "Kotak Mahindra",
    "LIC",
    "Mahindra Manulife",
    "Mirae Asset",
    "Motilal Oswal",
    "NJ",
    "Navi",
    "Nippon India",
    "Old Bridge",
    "PGIM India",
    "PPFAS",
    "Quantum",
    "SBI",
    "Samco",
    "Shriram",
    "Sundaram",
    "Tata",
    "Taurus",
    "The Wealth Company",
    "Trust",
    "UTI",
    "Unifi",
    "Union",
    "WhiteOak Capital",
    "Zerodha",
    "quant",           # intentional lowercase brand name
})


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
    option.wait_for(timeout=15_000)
    option.click()
    page.wait_for_timeout(800)


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
    * header=None preserves all rows including the ones that contain AMC data.
    * dtype=str prevents silent numeric coercion of AUM figures by pandas.
    * map strip is cheaper than a per-column str.strip() loop.
    """
    df = pd.read_excel(path, header=None, dtype=str)
    df = df.map(lambda v: v.strip() if isinstance(v, str) else v)
    print(f"  [loader]     Raw shape     : {df.shape}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4 — AMC EXTRACTOR
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


def _largest_numeric(row_values: list) -> float | None:
    """
    Return the largest absolute-value parseable float from columns 1+ of a row.

    Rationale (assumption A):
    The AMFI Average AUM export has four columns:
      col0 = AMC name
      col1 = NaN (blank on total rows)
      col2 = Total AUM in Rs Lakhs          ← what we want
      col3 = Sub-component AUM (e.g. debt)  ← 0 for equity-only AMCs

    The previous right-to-left scan picked col3 first. For the majority of
    AMCs (those without significant debt/hybrid AUM) col3 = 0, causing them
    to be silently dropped by the near-zero guard. Taking the largest absolute
    value correctly selects col2 regardless of column count or future position
    shifts in the AMFI format.
    """
    numerics: list[float] = []
    for cell in row_values[1:]:
        if pd.isna(cell):
            continue
        parsed = _parse_numeric(str(cell))
        if parsed is not None:
            numerics.append(parsed)
    if not numerics:
        return None
    return max(numerics, key=abs)


def _clean_amc_name(raw_cell: str) -> str:
    """
    Derive a clean AMC name from a raw total row cell value.

    Strategy (assumption G):
      1. Strip the trailing "Mutual Fund [(<qualifier>)] Total" suffix.
      2. Collapse internal whitespace to single spaces.
      3. Strip leading/trailing whitespace.
      4. Preserve original casing — do NOT apply .title().

    Rationale for not using .title():
      .title() corrupts intentional branding:
        "IL&FS"  → "Il&Fs"   (wrong)
        "quant"  → "Quant"   (wrong — registered as lowercase)
        "HSBC"   → "Hsbc"    (wrong)
        "BNP"    → "Bnp"     (wrong)
        "UTI"    → "Uti"     (wrong)
      Original casing from the AMFI export is the authoritative source.
    """
    name = _TOTAL_STRIP_RE.sub("", raw_cell).strip()
    return re.sub(r"\s+", " ", name)


def extract_amc_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 4 entry point.

    Extracts AMC-level AUM from the AMFI Average AUM Excel dump.

    Identification rule (assumption F — no whitelist):
        _AMC_TOTAL_RE matches rows ending with:
          • "Mutual Fund Total"             (standard — 50 AMCs)
          • "Mutual Fund (<qualifier>) Total" (IL&FS-style — 1 AMC)
        "Grand Total" is excluded by _GRAND_TOTAL_RE before pattern match.

    AUM selection (assumption A):
        _largest_numeric() returns the largest absolute-value figure in
        columns 1+, which is always the total AUM column regardless of
        whether col3 (the sub-component) is zero or non-zero.

    AUM unit conversion:
        The AMFI export is denominated in Rs Lakhs.
        Divide by 100 to produce Crores.

    Name cleaning:
        Delegated to _clean_amc_name(), which preserves original casing
        (assumption G).

    Safety checks:
        • Drop rows with missing AUM
        • Drop rows with zero / near-zero AUM (assumption B)
        • Deduplicate on amc_name_raw (keep last; assumption D)

    Returns
    -------
    pd.DataFrame with columns: [amc_name_raw, aum_cr]
    """
    records: list[dict] = []

    for _, row in df.iterrows():
        row_list = row.tolist()

        first_col_raw = row_list[0] if row_list else None
        if pd.isna(first_col_raw):
            continue
        first_col = str(first_col_raw).strip()

        # Exclude Grand Total before pattern match
        if _GRAND_TOTAL_RE.match(first_col):
            continue

        # Core identification (assumption F)
        if not _AMC_TOTAL_RE.search(first_col):
            continue

        # Largest numeric = total AUM in Lakhs (assumption A)
        aum_lakhs = _largest_numeric(row_list)

        # Drop missing or near-zero AUM (assumption B)
        if aum_lakhs is None or abs(aum_lakhs) < 1e-9:
            continue

        # Preserve casing (assumption G)
        amc_name = _clean_amc_name(first_col)

        records.append({
            "amc_name_raw": amc_name,
            "aum_cr": aum_lakhs / 100,   # Rs Lakhs → Crores
        })

    if not records:
        return pd.DataFrame(columns=["amc_name_raw", "aum_cr"])

    result = pd.DataFrame(records)

    # Safety: drop empty names, null AUM, near-zero AUM
    result = result[result["amc_name_raw"].str.len() > 0]
    result = result[result["aum_cr"].notna()]
    result = result[result["aum_cr"].abs() >= 1e-9]

    # Deduplicate on AMC name — keep last occurrence (assumption D)
    result = result.drop_duplicates(subset=["amc_name_raw"], keep="last")

    return result.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 5 — CLEANER / NORMALIZER
# ═══════════════════════════════════════════════════════════════════════════════

def clean_amc_totals(raw: pd.DataFrame, period_text: str) -> pd.DataFrame:
    """
    Stage 5 entry point.

    Transform raw extracted records into the canonical output schema.

    Transformations
    ---------------
    amc_name    Taken directly from amc_name_raw — already cleaned and
                casing-preserved by the extractor.
    aum_cr      Already float from extractor; cast explicitly to float64.
    month       Period label from the AMFI dropdown.
    ingested_at UTC ISO-8601 timestamp for this pipeline run.

    Post-clean deduplication
    ------------------------
    Deduplicate on (amc_name, month) keeping the last occurrence (assumption D).
    """
    if raw.empty:
        return pd.DataFrame(columns=["amc_name", "aum_cr", "month", "ingested_at"])

    df = raw.copy()

    df["amc_name"]    = df["amc_name_raw"]
    df["aum_cr"]      = df["aum_cr"].astype("float64")
    df["month"]       = period_text.strip()
    df["ingested_at"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    df = df[df["amc_name"].str.len() > 0].copy()
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
    FATAL  — negative AUM values
    WARN   — row count outside VALID_AMC_RANGE (assumption E)
    WARN   — duplicate AMC names
    WARN   — expected AMCs missing from output (assumption H)
    WARN   — new AMCs in output not in EXPECTED_AMCS (assumption H)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # ── Fatal checks ─────────────────────────────────────────────────────────

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

    # ── Warning checks ────────────────────────────────────────────────────────

    n = len(df)
    lo, hi = VALID_AMC_RANGE
    if not (lo <= n <= hi):
        warnings.append(
            f"Final AMC count {n} is outside expected range [{lo}, {hi}]. "
            "Verify the AMFI export format has not changed."
        )

    dupes = df["amc_name"].duplicated(keep=False)
    if dupes.any():
        warnings.append(
            f"Duplicate AMC names after deduplication: "
            f"{df.loc[dupes, 'amc_name'].unique().tolist()}"
        )

    # ── Reconciliation (assumption H) ────────────────────────────────────────

    extracted_names: set[str] = set(df["amc_name"].tolist())

    missing = EXPECTED_AMCS - extracted_names
    if missing:
        warnings.append(
            "Expected AMC(s) NOT found in output — possible name mismatch "
            "or missing total row in export:\n"
            + "\n".join(f"    • {m}" for m in sorted(missing))
        )

    new_amcs = extracted_names - EXPECTED_AMCS
    if new_amcs:
        warnings.append(
            "New AMC(s) in output not present in EXPECTED_AMCS "
            "— new SEBI registration or name change. Update EXPECTED_AMCS "
            "if correct:\n"
            + "\n".join(f"    • {a}" for a in sorted(new_amcs))
        )

    # ── Emit results ──────────────────────────────────────────────────────────

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
    print("\n[Stage 4] Extracting AMC total rows …")
    extracted = extract_amc_totals(raw_df)

    if extracted.empty:
        print(
            "ERROR: No AMC rows found. "
            "The Excel format may have changed — inspect the raw file.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"  [extractor]  Raw extracted rows (pre-cleaning): {len(extracted)}")

    # ── Stage 5: Clean ───────────────────────────────────────────────────────
    print("\n[Stage 5] Cleaning & normalising …")
    clean_df = clean_amc_totals(extracted, result.period_text)

    print(f"  [cleaner]    Final AMC rows (post-cleaning): {len(clean_df)}")

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