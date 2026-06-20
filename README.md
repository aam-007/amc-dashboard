# AMC Analytics Platform (`amc-dashboard`)

A production-grade data engineering and quantitative analytics pipeline that turns the Association of Mutual Funds in India's (AMFI) fragmented public disclosures — AUM, NAV, TER, and fund master data — into a single queryable SQLite warehouse, with market share, ranking, revenue, and forecasting modules built on top.

> Built and maintained by **Aditya Mishra** — MBA Tech, Data Science & Finance, NMIMS-MPSTME Mumbai.

---

## Table of Contents

- [Why this exists](#why-this-exists)
- [Architecture at a glance](#architecture-at-a-glance)
- [Repository layout](#repository-layout)
- [The twelve-stage pipeline](#the-twelve-stage-pipeline)
- [Phase-by-phase breakdown](#phase-by-phase-breakdown)
  - [Phase 1 — Data Ingestion](#phase-1--data-ingestion)
  - [Phase 2 — Data Warehouse](#phase-2--data-warehouse)
  - [Phase 3 — AMC Analytics](#phase-3--amc-analytics)
  - [Phase 4 — Revenue Model](#phase-4--revenue-model)
  - [Phase 5 — Forecasting](#phase-5--forecasting)
  - [Phase 7 — Automation](#phase-7--automation)
- [Data warehouse schema](#data-warehouse-schema)
- [Setup](#setup)
- [Running the pipeline](#running-the-pipeline)
- [Output files](#output-files)
- [The data manifest](#the-data-manifest)
- [Design principles](#design-principles)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)

---

## Why this exists

AMFI publishes everything an analyst needs on India's mutual fund industry — but spread across four independently-built web pages, each with its own filter UI, file format, and identifier system, and none of them offering a public API:

| Data domain | Disclosed | Granularity | Format on AMFI |
|---|---|---|---|
| AUM | Quarterly | Per AMC | MUI dropdown export (.xlsx) |
| NAV | Daily | Per scheme | Flat text feed (`NAVAll.txt`) |
| TER (expense ratio) | Per disclosure cycle | Per fund | MUI dropdown export (.xlsx) |
| Fund master (AMC/category/plan) | Embedded in NAV feed | Per scheme | Same flat text feed |

None of these feeds share a primary key. **AMC Analytics Platform** scrapes/downloads all four, normalizes them into one relational warehouse, and layers market-share, ranking, revenue, and three-scenario forecasting analytics on top — turning four incompatible disclosures into one set of answerable questions ("what's AMC X's market share trend vs. its expense ratio?", "what's the 3-year bear-case revenue for the top 10 AMCs?").

---

## Architecture at a glance

```
AMFI Website                  Phase 1: Ingestion              Phase 2: Warehouse
┌─────────────┐         ┌──────────────────────────┐      ┌──────────────────┐
│ Average AUM │ ──────▶ │ amfi_aum.py               │ ──┐  │                  │
│ (MUI/.xlsx) │         │ (Playwright)              │   │  │                  │
└─────────────┘         └──────────────────────────┘   │  │                  │
┌─────────────┐         ┌──────────────────────────┐   │  │   create_schema  │
│ TER Page    │ ──────▶ │ amfi_amc_expense_ratio.py │ ──┼─▶│        +         │
│ (MUI/.xlsx) │         │ (Playwright)              │   │  │   load_data.py   │
└─────────────┘         └──────────────────────────┘   │  │                  │
┌─────────────┐         ┌──────────────────────────┐   │  │   warehouse.db   │
│ NAVAll.txt  │ ──┬────▶ │ amfi_fund_master.py      │ ──┤  │   (SQLite)       │
│ (flat text) │   │     └──────────────────────────┘   │  │                  │
│             │   └────▶ │ amfi_nav.py               │ ──┘  └──────────────────┘
└─────────────┘         └──────────────────────────┘                │
                                                                      ▼
                         Phase 5: Forecasting        Phase 4: Revenue        Phase 3: Analytics
                    ┌──────────────────────┐    ┌──────────────────┐   ┌─────────────────────┐
                    │ forecast_aum.py       │◀──│ estimate_revenue │◀──│ calculate_market_   │
                    │ forecast_revenue.py   │   │ validate_revenue │   │   share.py           │
                    │ (base/bull/bear)      │   └──────────────────┘   │ ranking_amcs.py      │
                    └──────────────────────┘                          └─────────────────────┘
                                  │
                                  ▼
                    Phase 7: Automation
                    run_pipeline.py (PySide6 GUI orchestrator)
                    push_to_github.py (versioned publishing)
```

Every script resolves its own paths via `Path(__file__).resolve().parent` rather than the working directory, so any stage can be invoked from anywhere on disk and still find the right `data/` and `warehouse.db` locations.

---

## Repository layout

```
amc-dashboard/
├── 01-data-ingestion/
│   ├── aum/amfi_aum.py
│   ├── expense_ratio/amfi_amc_expense_ratio.py
│   ├── fund_master/amfi_fund_master.py
│   └── nav/amfi_nav.py
├── 02-data-warehouse/
│   ├── create_schema.py
│   ├── load_data.py
│   └── warehouse.db
├── 03-amc-analytics/
│   ├── market_share/calculate_market_share.py
│   └── rankings/ranking_amcs.py
├── 04-revenue-model/
│   ├── estimate_revenue.py
│   └── validate_revenue.py
├── 05-forcasting/
│   ├── base_case.py
│   ├── bull_case.py
│   ├── bear_case.py
│   ├── forecast_aum.py
│   └── forecast_revenue.py
├── 07-automation/
│   ├── run_pipeline.py        # PySide6 GUI orchestrator
│   └── push_to_github.py      # Git publish stage
├── data/
│   ├── raw/{aum,nav,expense_ratio,fund_master}/
│   ├── processed/{aum,nav,expense_ratio,fund_master}/
│   └── exports/{market_share,rankings,revenue,forecasting,pipeline}/
└── data_manifest.json         # generated state file — current "latest" pointers
```

> **Note on numbering:** Phase 6 initially housed "index.html" (to better engage with "data_manifest.json" , which was later moved to the root folder. The names of the sub-folders were not changed because it would be a tedious task. This is being mentioned here to avoid any confusion.
---

## The twelve-stage pipeline

`run_pipeline.py` defines the canonical execution order. Stages are strictly dependency-ordered: all ingestion must finish before the warehouse loads, which must finish before any analytics/revenue stage runs, which must finish before forecasting can project forward.

| # | Stage | Script | Depends on |
|---|---|---|---|
| 1 | AUM Ingestion | `01-data-ingestion/aum/amfi_aum.py` | — |
| 2 | Expense Ratio Ingestion | `01-data-ingestion/expense_ratio/amfi_amc_expense_ratio.py` | — |
| 3 | Fund Master Ingestion | `01-data-ingestion/fund_master/amfi_fund_master.py` | — |
| 4 | NAV Ingestion | `01-data-ingestion/nav/amfi_nav.py` | — |
| 5 | Warehouse Load | `02-data-warehouse/load_data.py` | 1–4 |
| 6 | Market Share Calculation | `03-amc-analytics/market_share/calculate_market_share.py` | 5 |
| 7 | AMC Rankings | `03-amc-analytics/rankings/ranking_amcs.py` | 5 |
| 8 | Revenue Estimation | `04-revenue-model/estimate_revenue.py` | 5 |
| 9 | Revenue Validation | `04-revenue-model/validate_revenue.py` | 8 |
| 10 | AUM Forecast | `05-forcasting/forecast_aum.py` | 5 |
| 11 | Revenue Forecast | `05-forcasting/forecast_revenue.py` | 8, 10 |
| 12 | Push to GitHub | `07-automation/push_to_github.py` | all (optional) |

Stage 12 is the only stage with no data dependency — it's purely an artifact-publishing step, and can be toggled off for local test runs.

---

## Phase-by-phase breakdown

### Phase 1 — Data Ingestion

All four ingestion scripts follow the same internal structure: **configure → download/fetch → load → extract/normalize → clean → validate → persist**. Each produces a timestamped historical file *and* an overwritten `*_latest.csv` canonical file for downstream stages to consume.

#### `amfi_aum.py` — AUM Ingestion
- Drives AMFI's Average AUM page (a React/MUI single-page app) via **Playwright**, cascading through five Autocomplete dropdowns: data type → type → AMC (`ALL`) → latest financial year → latest period.
- Captures the resulting `.xlsx` download and extracts AMC-total rows using one regex: `mutual\s+fund\s+(?:\([^)]*\)\s*)?total\s*$` — this matches both the standard `"<Name> Mutual Fund Total"` format and the one structural exception, IL&FS's `"... Mutual Fund (IDF) Total"`.
- **Key fix baked in:** AUM is selected as the **largest absolute-value** numeric column per row, not a fixed column position — the original right-to-left scan was silently picking a debt/hybrid sub-component column and zeroing out AMCs without that exposure.
- Preserves AMFI's original AMC-name casing (no `.title()` — that would corrupt names like `quant` or `HSBC`).
- Uses a reconciliation set of ~50 known AMC names purely to **warn** on missing/unexpected AMCs — never to filter, so new SEBI registrations flow through automatically.
- Converts AMFI's Rs Lakhs to Rs Crores (÷100), the unit used throughout the platform.
- **Validation:** fatal on null/negative AUM; advisory on AMC count outside [10, 65] and reconciliation mismatches.

#### `amfi_amc_expense_ratio.py` — TER Ingestion
- Drives AMFI's TER disclosure page via Playwright — the most complex dropdown cascade in the system, because a "Sub Category" filter is **conditionally rendered** depending on the selected Fund Type.
- Detects whether Sub Category appeared by counting `.MuiAutocomplete-root` elements before/after selecting Fund Type, and shifts subsequent dropdown indices accordingly — no hard-coded indices.
- Polls each Autocomplete's `aria-controls` listbox for populated options (rather than fixed sleeps) and polls the `GO` button's `disabled` attribute before clicking.
- Normalizes verbose AMFI headers (e.g. `"Regular Plan - Total TER (%)"`) to snake_case (`regular_ter`), treating sentinel strings (`"-"`, `"na"`, `"n/a"`, `"null"`, `"none"`) as missing rather than zero.
- **Validation:** fatal on entirely-null required columns or negative TER; advisory on duplicate (scheme_code, ter_date) pairs and row counts outside [1,000, 500,000].

#### `amfi_fund_master.py` — Fund Master Ingestion
- Parses `NAVAll.txt` with a small **state machine** since AMC name and category appear only as embedded header lines with no type marker. The parser tracks `current_amc` and `current_category` and stamps every subsequent scheme row with whichever is currently active.
- Three line types, distinguished by pattern: category headers (regex on parenthetical content), scheme rows (`^\d+;` with ≥6 fields), and AMC headers (identified *by exclusion* — anything that's neither of the above).
- Derives **plan type** (Direct/Regular/Retail/Unknown) and **option type** (Growth/IDCW/Bonus/Other) from the scheme name string via ordered regex precedence, since AMFI doesn't publish these as structured fields.
- Deduplicates by keeping the most recent `nav_date` per `scheme_code`. Deliberately does **not** persist NAV/nav_date to the warehouse `schemes` table — that's the dedicated NAV pipeline's job.

#### `amfi_nav.py` — NAV Ingestion
- Downloads the same `NAVAll.txt` feed with a single-purpose parser (`^\d+;`) — no state tracking needed, since this pipeline only needs `(scheme_code, nav, nav_date)` + ISINs.
- Cleaning: strip whitespace → cast scheme_code to string → coerce NAV numeric (drop failures) → coerce nav_date datetime, day-first (drop failures) → drop empty scheme_codes → dedupe on scheme_code keeping latest nav_date.
- **Validation:** minimum row-count threshold of 1,000 — a coarse but effective guard against a truncated/empty download.
- Outputs **both Parquet** (primary analytical format) **and CSV**, timestamped to the second in IST (`Asia/Kolkata`) to support multiple intraday runs.

| Pattern | AUM | TER | Fund Master | NAV |
|---|---|---|---|---|
| Source | Playwright (MUI) | Playwright (MUI) | HTTP GET | HTTP GET |
| Raw layer | `.xlsx` verbatim | `.xlsx` verbatim | `.txt` verbatim | `.txt` verbatim |
| Output formats | CSV | CSV | CSV | Parquet + CSV |
| Dedup key | (amc_name, month) | (scheme_code, ter_date) — warn only | scheme_code, latest nav_date | scheme_code, latest nav_date |
| Fatal validation | null/neg AUM | null/neg TER | best-effort | row count < 1,000 |

### Phase 2 — Data Warehouse

#### `create_schema.py`
Creates a single SQLite database (`warehouse.db`) with two dimension tables (`amcs`, `schemes`) and three fact tables (`nav_history`, `expense_ratio_history`, `aum_history`), all via idempotent `CREATE TABLE IF NOT EXISTS` inside one transaction. Every connection sets `PRAGMA foreign_keys = ON` and `PRAGMA journal_mode = WAL`.

**Foreign keys are applied selectively, not universally:**
- `nav_history.scheme_code → schemes.scheme_code` (CASCADE) — a hard FK, because NAV is meaningless without a parent scheme.
- `expense_ratio_history.scheme_code` has **no FK**, by design — AMFI's TER feed uses its own fund-level alphanumeric codes (e.g. `360O/O/H/BHF/23/07/0007`), while `schemes` uses MFI plan-level numeric codes (e.g. `152073`). Different identifier systems, different granularities, zero overlap. A FK here would always fail; joins are done by scheme-name matching at query time instead.
- `schemes.amc_name` and `aum_history.amc_name` have **no FK** to `amcs.amc_name` — both fact/dimension sources can arrive before the AMC dimension is fully populated; consistency is enforced procedurally in the loader, not structurally in the schema.

#### `load_data.py`
Scoped to exactly one job: **read processed CSVs → validate structure → load warehouse → validate load.** No cleaning, no business logic — that all happens upstream/downstream.
- Locates each dataset's latest file by filesystem **modification time**, not filename, so it's insensitive to naming drift.
- The one real transform here: `parse_aum_period()` converts AMFI's human label (`"January - March 2026"`) into `period_start`/`period_end` ISO dates, correctly handling December year-end rollover.
- Opens the connection with `isolation_level=None` (manual autocommit control) and wraps the whole load in one explicit `BEGIN ... COMMIT`. Clears child tables first, then parents (respecting FK order); loads parents first, then children. Inserts in 500-row chunks to stay under SQLite's 999-bound-parameter ceiling.
- Post-load: asserts every table is non-empty; any empty table triggers a `ValueError` → explicit `ROLLBACK`, guarded against double-committing into an already-closed transaction.

### Phase 3 — AMC Analytics

Both modules connect **read-only** (`file:{db_path}?mode=ro`) — Phase 3 only ever consumes the warehouse.

#### `calculate_market_share.py`
1. Finds the latest `period_end` in `aum_history`.
2. Computes `market_share_pct = (average_aum_cr / industry_aum) × 100` for every AMC in that period.
3. Ranks with `.rank(method="dense", ascending=False)` — ties share a rank with no gap afterward.
4. Exports `market_share_latest.csv` + a timestamped archive, both `utf-8-sig` encoded so the ₹ symbol renders correctly when opened directly in Excel.

#### `ranking_amcs.py`
Extends market share with a Top-10 leaderboard and a `ConcentrationMetrics` dataclass:
```
top_n_share(n) = Σ market_share_pct  for AMCs ranked 1..n
```
Reports the standard CR3 / CR5 / CR10 concentration tiers. Exports four files per run: full rankings (latest + timestamped) and top-10 (latest + timestamped).

### Phase 4 — Revenue Model

#### `estimate_revenue.py`
```
Estimated Revenue (₹ Cr) = Average AUM (₹ Cr) × 0.75%
```
A flat, configurable industry-wide yield (`YIELD = 0.0075`) applied at AMC granularity — an explicit, documented simplification rather than a scheme-level TER-weighted calculation. Loads the latest `period_end` slice of `aum_history`, ranks descending by revenue, exports timestamped + `amc_revenue_latest.csv`.

#### `validate_revenue.py`
An **independent QA pass** over the published output — not embedded mid-pipeline like the Phase 1 validators. Runs a 15-check registry, each returning PASS/FAIL/WARNING, and *all fifteen always run* (a failure in one doesn't short-circuit the rest):

| # | Check |
|---|---|
| 1 | File exists |
| 2 | File readable |
| 3 | Required columns present |
| 4 | Dataset not empty |
| 5 | No duplicate AMC names |
| 6 | Rank sequence is exactly 1..N, no gaps |
| 7 | All AUM values positive |
| 8 | All revenue values positive |
| 9 | Yield within [0.05%, 2.00%] |
| 10 | Revenue ≈ AUM × yield (±0.01 Cr tolerance) |
| 11 | Sorted descending by revenue |
| 12 | Rank matches revenue-implied order |
| 13 | Industry totals reported |
| 14 | Rank-1 row sanity check |
| 15 | No nulls in required columns |

Exit code is 0 only if **all 15** pass.

### Phase 5 — Forecasting

Three scenario modules, each declaring exactly three constants, loaded dynamically via `importlib.import_module()`:

| Scenario | `SCENARIO_NAME` | `BASE_GROWTH_RATE` | Horizon |
|---|---|---|---|
| `base_case.py` | Base Case | 12% p.a. | 3 years |
| `bull_case.py` | Bull Case | 18% p.a. | 3 years |
| `bear_case.py` | Bear Case | 6% p.a. | 3 years |

Adding a 4th scenario (e.g. a stress case) means dropping in one new file — no changes to the forecasting logic.

#### `forecast_aum.py`
Auto-discovers the relevant warehouse table (scans `sqlite_master` for any table name containing `"aum"`) and resolves AMC/value/date columns from ordered candidate-name lists — resilient to schema renames.
```
Year_N_AUM = Current_AUM × (1 + growth_rate)^N    for N = 1, 2, 3
```
Post-forecast validation includes a **monotonicity check**: for any scenario with growth > 0, Year-1 must exceed current AUM, Year-2 must exceed Year-1, Year-3 must exceed Year-2 — catches sign errors that simple null/range checks would miss.

#### `forecast_revenue.py`
Does **not** re-apply the flat 75bps assumption. Instead derives each AMC's implied current yield:
```
Revenue Yield = Current Revenue ÷ Current AUM
```
...then holds that yield constant across the 3-year horizon:
```
Revenue_Yn = Year_N_AUM (from Phase 5 AUM forecast) × Revenue Yield
```
This is currently a mathematical no-op (every AMC uses the same 0.75%), but it's the architecturally correct approach — it decouples this module from Phase 4's specific yield assumption, so it would automatically pick up AMC-specific yields if Phase 4 were later upgraded. The merge between AUM forecasts and revenue yields explicitly **raises** on any AMC present in one file but not the other, rather than silently producing NaN revenue.

### Phase 7 — Automation

#### `run_pipeline.py` — PySide6 GUI Orchestrator
A desktop app (PySide6, with PyQt6 fallback) that runs each of the 12 stages as an isolated subprocess via `QThread` workers, streaming live stdout/stderr into a dark-mode console. Each stage is a `StageRow` with a toggle switch — disable the GitHub push for local test runs, or skip ingestion when only re-running analytics against an already-loaded warehouse.
- `find_missing_stages()` verifies every script path exists *before* any stage runs.
- `LogExporter` writes the full console transcript of every run to `data/exports/pipeline/{SUCCESS_RUN_LOGS,FAILED_RUN_LOGS}/`, keyed by outcome — a persistent audit trail independent of the live view.

#### `push_to_github.py`
The final, optional stage. An 8-step sequence: verify Git installed → verify inside a Git repo → check for uncommitted changes (no-op exit if clean) → stage all → generate timestamped commit message + commit → `git pull --rebase origin/main` → push → print summary (branch, commit hash, remote URL).

**Safety contract:** this script will **never** run `git push --force`, `git reset --hard`, `git clean -fd`, or anything that deletes files or modifies branches — important given it's the unattended final step of a fully automated run.

---

## Data warehouse schema

| Table | Type | Primary Key | Foreign Keys |
|---|---|---|---|
| `amcs` | Dimension | `amc_id` (autoincrement) | none |
| `schemes` | Dimension | `scheme_code` | none (soft reference on `amc_name`) |
| `nav_history` | Fact | `(scheme_code, nav_date)` | `scheme_code → schemes` (CASCADE) |
| `expense_ratio_history` | Fact | `(scheme_code, ter_date)` | none — deliberately omitted |
| `aum_history` | Fact | `(amc_name, period_start, period_end)` | none — deliberately omitted |

See [Phase 2](#phase-2--data-warehouse) above for the full rationale behind the omitted foreign keys — this isn't an oversight, it reflects a genuine mismatch between AMFI's TER fund-level codes and the warehouse's MFI plan-level scheme codes.

---

## Setup

```bash
# Clone and enter the repo
git clone https://github.com/aam-007/amc-dashboard
cd amc-dashboard

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install pandas requests playwright pyarrow PySide6

# Install Playwright's browser binaries (required for AUM + TER ingestion)
playwright install chromium
```

> If `PySide6` isn't available on your platform, `run_pipeline.py` falls back to `PyQt6` — install whichever is available for your Python version.

---

## Running the pipeline

**Full automated run** (recommended — gives you the GUI with per-stage toggles and a live console):
```bash
python 07-automation/run_pipeline.py
```

**Manual / scripted run**, stage by stage, in dependency order:
```bash
# Phase 1 — Ingestion (can run in any order/in parallel)
python 01-data-ingestion/aum/amfi_aum.py
python 01-data-ingestion/expense_ratio/amfi_amc_expense_ratio.py
python 01-data-ingestion/fund_master/amfi_fund_master.py
python 01-data-ingestion/nav/amfi_nav.py

# Phase 2 — Warehouse (schema must exist before loading)
python 02-data-warehouse/create_schema.py
python 02-data-warehouse/load_data.py

# Phase 3 — Analytics
python 03-amc-analytics/market_share/calculate_market_share.py
python 03-amc-analytics/rankings/ranking_amcs.py

# Phase 4 — Revenue
python 04-revenue-model/estimate_revenue.py
python 04-revenue-model/validate_revenue.py

# Phase 5 — Forecasting
python 05-forcasting/forecast_aum.py
python 05-forcasting/forecast_revenue.py

# Phase 7 — Publish (optional)
python 07-automation/push_to_github.py
```

---

## Output files

Every export follows the same convention: a timestamped archival CSV **and** an overwritten canonical `*_latest.csv` (or `.parquet` for NAV).

| Output | Path |
|---|---|
| AMC rankings | `data/exports/rankings/amc_rankings_latest.csv` |
| Market share | `data/exports/market_share/market_share_latest.csv` |
| Revenue estimates | `data/exports/revenue/amc_revenue_latest.csv` |
| AUM forecast | `data/exports/forecasting/forecast_aum_latest.csv` |
| Revenue forecast | `data/exports/forecasting/forecast_revenue_latest.csv` |
| NAV snapshot | `data/processed/nav/nav_snapshot_<timestamp>.{parquet,csv}` |
| Expense ratio | `data/processed/expense_ratio/expense_ratio_clean_<period>_<timestamp>.csv` |
| Processed AUM | `data/processed/aum/aum_clean_<period>_<timestamp>.csv` |
| Pipeline run logs | `data/exports/pipeline/{SUCCESS_RUN_LOGS,FAILED_RUN_LOGS}/` |

As of the last recorded run, the warehouse held **51 AMCs** and **14,214 schemes**.

---

## The data manifest

`data_manifest.json` is a **generated** state file (not hand-maintained) that gives any downstream consumer — a dashboard, a notebook, another script — one fixed entry point to resolve the current canonical file per dataset, without needing to know the platform's directory or timestamp conventions:

```json
{
  "latest": {
    "rankings": "data/exports/rankings/amc_rankings_latest.csv",
    "market_share": "data/exports/market_share/market_share_latest.csv",
    "revenue": "data/exports/revenue/amc_revenue_latest.csv",
    "forecast_aum": "data/exports/forecasting/forecast_aum_latest.csv",
    "forecast_revenue": "data/exports/forecasting/forecast_revenue_latest.csv",
    "nav_snapshot": "...",
    "expense_ratio": "...",
    "aum_processed": "..."
  },
  "historical_files": { "...": "full chronological list per dataset" },
  "metadata": { "last_run": "...", "amc_count": 51, "scheme_count": 14214 }
}
```

---

## Design principles

1. **Separation of cleaning, loading, and analysis.** `load_data.py` does no cleaning and no business logic by design — all domain cleaning happens upstream in Phase 1, all domain analytics happens downstream in Phase 3/4/5.
2. **Idempotency by construction.** Every `CREATE TABLE`/`CREATE INDEX` uses `IF NOT EXISTS`; every warehouse load is a full delete-then-insert inside one transaction; every export writes both a timestamped and a `_latest` file. Re-running any stage converges to the same end state.
3. **Fatal vs. advisory validation.** Every validator distinguishes checks that must halt the pipeline (null PKs, negative values, empty tables) from checks that should only warn (row counts outside a heuristic range, reconciliation mismatches against expected AMC lists).
4. **Foreign keys reflect genuine identifier-system analysis, not blanket normalization.** Where two datasets' identifier spaces don't actually overlap (TER fund codes vs. MFI scheme codes), the schema says so explicitly instead of forcing a constraint that would always fail.

---

## Known limitations

- **Flat industry-wide revenue yield (75bps).** A deliberate simplification; replacing it with scheme-level TER-weighted yields (using the already-ingested `expense_ratio_history` table) would improve accuracy, at the cost of the fuzzy name-based join described above.
- **TER-to-scheme linkage is name-based, not key-based**, since no FK exists between the two identifier systems — exposed to fuzzy-matching risk on scheme name variants.
- **Browser-automation fragility.** Both Playwright downloaders depend on AMFI's current MUI structure; a front-end redesign would require updating dropdown-index logic (though the TER module's dynamic dropdown-count detection is built specifically to absorb minor changes).
- **Forecasting is deterministic, not probabilistic.** Three fixed-growth-rate scenarios produce point estimates, not a distribution of outcomes.
- **Revenue model omits expense leakage.** Modeled at gross management-fee level — no distribution commissions, TER caps, or AMC operating costs netted out.

## Roadmap

- [ ] Replace flat 75bps yield with TER-weighted, scheme-level revenue estimation
- [ ] Monte Carlo AUM forecasting over a distribution of growth rates per AMC
- [ ] Resilience layer for AMFI front-end structural changes (dropdown auto-discovery beyond TER's sub-category case)
- [ ] Net profitability model (commissions, TER caps, operating cost overlay)

---

## Author

**Aditya Mishra**
MBA Tech, Data Science & Finance — NMIMS-MPSTME, Mumbai

aditya.mishra10@nmims.in 

Designed and developed during an 8-week internship at Helios Capital AMC.
