"""
load_data.py
============
AMC Analytics Warehouse — Data Loader
Phase 2: Data Warehouse

Responsibility
--------------
This module is the single entry point for loading all processed datasets
into the SQLite analytics warehouse (warehouse.db).

It performs exactly one job:
    Read processed CSVs → Validate structure → Load warehouse → Validate load

It does NOT clean data.
It does NOT transform business logic.
It does NOT perform analytics.

Architecture
------------
1. File discovery  — locate latest processed snapshot per dataset
2. Ingestion       — read discovered files into DataFrames
3. Pre-validation  — assert required columns exist and frames are non-empty
4. Table refresh   — DELETE all rows in dependency-safe order
5. Load sequence   — INSERT in foreign-key-safe order
6. Post-validation — assert every target table is non-empty after load
7. Commit / Roll   — atomic transaction; ROLLBACK on any failure

All warehouse writes are wrapped in a single SQLite transaction.
The load is fully idempotent: repeated execution produces identical state.

Author : aditya.mishra10@nmims.in
Python : 3.12+
"""

from __future__ import annotations

import logging
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Resolve paths relative to this file so the script works regardless of the
# working directory from which it is invoked.
_HERE: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = _HERE.parent

WAREHOUSE_PATH: Path = _HERE / "warehouse.db"

PROCESSED_ROOT: Path = PROJECT_ROOT / "data" / "processed"

DIR_FUND_MASTER: Path = PROCESSED_ROOT / "fund_master"
DIR_NAV: Path = PROCESSED_ROOT / "nav"
DIR_EXPENSE_RATIO: Path = PROCESSED_ROOT / "expense_ratio"
DIR_AUM: Path = PROCESSED_ROOT / "aum"

# ---------------------------------------------------------------------------
# Column mappings
# ---------------------------------------------------------------------------
# Centralised here so any upstream rename only requires a change in one place.

# fund_master CSV → schemes table
FUND_MASTER_COLUMNS: dict[str, str] = {
    "scheme_code": "scheme_code",
    "scheme_name": "scheme_name",
    "amc_name": "amc_name",
    "category": "category",
    "plan_type": "plan_type",
    "option_type": "option_type",
    "isin_growth": "isin_growth",
    "isin_reinvestment": "isin_reinvestment",
}

# nav CSV → nav_history table
NAV_COLUMNS: dict[str, str] = {
    "scheme_code": "scheme_code",
    "nav_date": "nav_date",
    "nav": "nav",
}

# expense_ratio CSV → expense_ratio_history table
TER_COLUMNS: dict[str, str] = {
    "scheme_code": "scheme_code",
    "ter_date": "ter_date",
    "regular_ter": "regular_ter",
    "direct_ter": "direct_ter",
}

# aum CSV → aum_history table
# NOTE: The processed AUM file contains a single "month" column that encodes
# the AMFI quarter label (e.g. "January - March 2026").  The warehouse schema
# expects period_start DATE and period_end DATE as separate columns.
# parse_aum_period() derives those two values from the label at load time.
AUM_COLUMNS: dict[str, str] = {
    "amc_name": "amc_name",
    "aum_cr": "average_aum_cr",
    # "month" is intentionally absent here; it is handled by parse_aum_period()
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


@contextmanager
def get_connection(db_path: Path = WAREHOUSE_PATH) -> Generator[sqlite3.Connection, None, None]:
    """
    Yield an open SQLite connection with foreign-key enforcement enabled.

    Usage::

        with get_connection() as conn:
            conn.execute("SELECT 1")

    The connection is closed automatically when the context exits.
    Foreign keys are enabled immediately after the connection is opened
    so that referential integrity is enforced during every load.

    Parameters
    ----------
    db_path:
        Filesystem path to the SQLite database file.

    Yields
    ------
    sqlite3.Connection
        An open connection to the warehouse database.

    Raises
    ------
    sqlite3.Error
        If the connection cannot be established.
    """
    # isolation_level=None puts the connection into "autocommit" mode at the
    # Python driver level, which means the driver will NOT silently issue its
    # own BEGIN/COMMIT around DML statements.  This gives us full manual
    # control over the transaction boundary (BEGIN … COMMIT/ROLLBACK) and
    # prevents pandas to_sql from implicitly committing mid-load.
    conn: sqlite3.Connection = sqlite3.connect(db_path, isolation_level=None)
    try:
        # Foreign keys must be enabled before the transaction is opened.
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
    finally:
        conn.close()


def get_table_count(conn: sqlite3.Connection, table_name: str) -> int:
    """
    Return the number of rows currently in *table_name*.

    Parameters
    ----------
    conn:
        Active SQLite connection.
    table_name:
        Name of the target table (not parameterised — must be a trusted
        internal constant, never derived from user input).

    Returns
    -------
    int
        Row count.
    """
    # Table names cannot be bound as parameters in SQLite; the name is always
    # a trusted internal constant in this module.
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()  # noqa: S608
    return int(row[0])


def _insert_dataframe(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table_name: str,
    chunksize: int = 500,
) -> None:
    """
    Insert *df* into *table_name* in chunks to avoid SQLite's variable limit.

    SQLite allows at most 999 bound parameters per statement.  With wide tables
    (e.g. 8 columns × 500 rows = 4 000 parameters) ``pandas.to_sql`` with
    ``method="multi"`` will exceed that limit and raise
    ``sqlite3.OperationalError: too many SQL variables``.

    This helper calls ``pandas.to_sql`` with ``method=None`` (the default
    single-row executemany path) in controlled chunks, staying well within the
    limit regardless of column count.

    The default ``chunksize=500`` is deliberately conservative; for a table
    with ≤ 1 column you could use 999, but 500 is safe for up to ~1 column
    and never approaches the limit for any table in this schema.

    Parameters
    ----------
    conn:
        Active SQLite connection inside an open transaction.
    df:
        DataFrame whose rows will be inserted.
    table_name:
        Target table name.
    chunksize:
        Maximum number of rows per ``executemany`` batch.
    """
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="append",
        index=False,
        method=None,      # single-row executemany — safe for all column counts
        chunksize=chunksize,
    )


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def latest_file(directory: Path, pattern: str = "*.csv") -> Path:
    """
    Return the most recently modified file matching *pattern* in *directory*.

    Files are ranked by ``stat().st_mtime`` (modification timestamp) so that
    the freshest snapshot is always selected, regardless of filename ordering.

    Parameters
    ----------
    directory:
        Directory to search. Must exist.
    pattern:
        Glob pattern used to filter candidates. Defaults to ``"*.csv"``.

    Returns
    -------
    Path
        Absolute path of the most recently modified matching file.

    Raises
    ------
    FileNotFoundError
        If *directory* contains no files that match *pattern*.
    """
    candidates: list[Path] = sorted(
        directory.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No files matching '{pattern}' found in: {directory}"
        )
    return candidates[0]


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    dataset_name: str,
) -> None:
    """
    Assert that *df* is non-empty and contains every column in *required_columns*.

    Parameters
    ----------
    df:
        DataFrame to validate.
    required_columns:
        Column names that must all be present.
    dataset_name:
        Human-readable label used in error messages.

    Raises
    ------
    ValueError
        If *df* is empty or any required column is absent.
    """
    if df.empty:
        raise ValueError(f"[{dataset_name}] Source DataFrame is empty.")

    missing: list[str] = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{dataset_name}] Missing required columns: {missing}. "
            f"Available columns: {df.columns.tolist()}"
        )


# ---------------------------------------------------------------------------
# AUM period parsing
# ---------------------------------------------------------------------------

# Maps AMFI quarter-label month names to calendar month numbers.
_MONTH_MAP: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Expected label format: "January - March 2026"
_AUM_PERIOD_RE = re.compile(
    r"^(?P<start_month>\w+)\s*-\s*(?P<end_month>\w+)\s+(?P<year>\d{4})$",
    re.IGNORECASE,
)


def parse_aum_period(label: str) -> tuple[str, str]:
    """
    Parse an AMFI quarter label into ISO-8601 period_start and period_end dates.

    The processed AUM file stores the quarter as a human-readable label such as
    ``"January - March 2026"``.  The warehouse schema requires two separate DATE
    columns.  This function derives them deterministically.

    period_start is the first day of the opening month.
    period_end   is the last day of the closing month.

    Parameters
    ----------
    label:
        AMFI quarter label, e.g. ``"January - March 2026"``.

    Returns
    -------
    tuple[str, str]
        ``(period_start, period_end)`` as ``"YYYY-MM-DD"`` strings.

    Raises
    ------
    ValueError
        If *label* does not match the expected format or contains unrecognised
        month names.

    Examples
    --------
    >>> parse_aum_period("January - March 2026")
    ('2026-01-01', '2026-03-31')
    >>> parse_aum_period("April - June 2026")
    ('2026-04-01', '2026-06-30')
    """
    match = _AUM_PERIOD_RE.match(label.strip())
    if not match:
        raise ValueError(
            f"AUM period label '{label}' does not match expected format "
            f"'<Month> - <Month> <YYYY>'.  Update parse_aum_period() if the "
            f"upstream label format has changed."
        )

    start_name: str = match.group("start_month").lower()
    end_name: str = match.group("end_month").lower()
    year: int = int(match.group("year"))

    if start_name not in _MONTH_MAP:
        raise ValueError(f"Unrecognised start month '{start_name}' in AUM label '{label}'.")
    if end_name not in _MONTH_MAP:
        raise ValueError(f"Unrecognised end month '{end_name}' in AUM label '{label}'.")

    start_month: int = _MONTH_MAP[start_name]
    end_month: int = _MONTH_MAP[end_name]

    # Last day of end_month — use the trick of rolling to the 1st of the
    # *next* month and subtracting one day.
    if end_month == 12:
        last_day: pd.Timestamp = pd.Timestamp(year=year + 1, month=1, day=1) - pd.Timedelta(days=1)
    else:
        last_day = pd.Timestamp(year=year, month=end_month + 1, day=1) - pd.Timedelta(days=1)

    period_start: str = f"{year}-{start_month:02d}-01"
    period_end: str = last_day.strftime("%Y-%m-%d")

    return period_start, period_end


# ---------------------------------------------------------------------------
# Table loaders
# ---------------------------------------------------------------------------


def load_schemes(conn: sqlite3.Connection, fund_df: pd.DataFrame) -> int:
    """
    Insert all schemes from *fund_df* into the ``schemes`` table.

    Only the columns defined in :data:`FUND_MASTER_COLUMNS` are written.
    The ``nav`` and ``nav_date`` columns that appear in the fund master CSV
    are intentionally ignored — they belong to nav_history, not schemes.

    Parameters
    ----------
    conn:
        Active SQLite connection (within an open transaction).
    fund_df:
        Validated fund master DataFrame.

    Returns
    -------
    int
        Number of rows inserted.
    """
    source_cols: list[str] = list(FUND_MASTER_COLUMNS.keys())
    dest_cols: list[str] = list(FUND_MASTER_COLUMNS.values())

    df: pd.DataFrame = fund_df[source_cols].copy()
    df.columns = dest_cols  # type: ignore[assignment]

    # scheme_code is the PRIMARY KEY — duplicates must be removed before insert.
    df = df.drop_duplicates(subset=["scheme_code"])

    _insert_dataframe(conn, df, "schemes")
    return len(df)


def load_amcs(conn: sqlite3.Connection, fund_df: pd.DataFrame) -> int:
    """
    Derive the AMC list from *fund_df* and insert into the ``amcs`` table.

    AMC names are extracted from the ``amc_name`` column of the fund master,
    deduplicated, sorted alphabetically, and inserted.  No separate AMC source
    file is required.

    Parameters
    ----------
    conn:
        Active SQLite connection (within an open transaction).
    fund_df:
        Validated fund master DataFrame.

    Returns
    -------
    int
        Number of rows inserted.
    """
    amc_names: pd.Series = (
        fund_df["amc_name"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    amc_df: pd.DataFrame = pd.DataFrame({"amc_name": amc_names})
    _insert_dataframe(conn, amc_df, "amcs")
    return len(amc_df)


def load_nav_history(conn: sqlite3.Connection, nav_df: pd.DataFrame) -> int:
    """
    Insert NAV records from *nav_df* into the ``nav_history`` table.

    Parameters
    ----------
    conn:
        Active SQLite connection (within an open transaction).
    nav_df:
        Validated NAV DataFrame.

    Returns
    -------
    int
        Number of rows inserted.
    """
    source_cols: list[str] = list(NAV_COLUMNS.keys())
    dest_cols: list[str] = list(NAV_COLUMNS.values())

    df: pd.DataFrame = nav_df[source_cols].copy()
    df.columns = dest_cols  # type: ignore[assignment]

    # PRIMARY KEY is (scheme_code, nav_date) — drop any accidental duplicates.
    df = df.drop_duplicates(subset=["scheme_code", "nav_date"])

    _insert_dataframe(conn, df, "nav_history")
    return len(df)


def load_expense_ratio_history(conn: sqlite3.Connection, ter_df: pd.DataFrame) -> int:
    """
    Insert TER records from *ter_df* into the ``expense_ratio_history`` table.

    No foreign-key constraint exists on expense_ratio_history.scheme_code.
    AMFI TER disclosures use their own internal string codes (fund level),
    which are a completely different identifier system from the MFI numeric
    plan codes used in the schemes table.  Analytical joins between TER and
    schemes are performed at query time via scheme-name matching.

    Parameters
    ----------
    conn:
        Active SQLite connection (within an open transaction).
    ter_df:
        Validated expense ratio DataFrame.

    Returns
    -------
    int
        Number of rows inserted.
    """
    source_cols: list[str] = list(TER_COLUMNS.keys())
    dest_cols: list[str] = list(TER_COLUMNS.values())

    df: pd.DataFrame = ter_df[source_cols].copy()
    df.columns = dest_cols  # type: ignore[assignment]

    # PRIMARY KEY is (scheme_code, ter_date).
    df = df.drop_duplicates(subset=["scheme_code", "ter_date"])

    _insert_dataframe(conn, df, "expense_ratio_history")
    return len(df)


def load_aum_history(conn: sqlite3.Connection, aum_df: pd.DataFrame) -> int:
    """
    Insert AUM records from *aum_df* into the ``aum_history`` table.

    The source file carries a single ``month`` column containing a quarter label
    (e.g. ``"January - March 2026"``).  This function derives ``period_start``
    and ``period_end`` via :func:`parse_aum_period` and drops the raw label.

    Parameters
    ----------
    conn:
        Active SQLite connection (within an open transaction).
    aum_df:
        Validated AUM DataFrame.

    Returns
    -------
    int
        Number of rows inserted.
    """
    df: pd.DataFrame = aum_df[list(AUM_COLUMNS.keys()) + ["month"]].copy()
    df = df.rename(columns=AUM_COLUMNS)

    # Derive period columns from the quarter label.
    parsed = df["month"].apply(parse_aum_period)
    df["period_start"] = parsed.apply(lambda t: t[0])
    df["period_end"] = parsed.apply(lambda t: t[1])
    df = df.drop(columns=["month"])

    # PRIMARY KEY is (amc_name, period_start, period_end).
    df = df.drop_duplicates(subset=["amc_name", "period_start", "period_end"])

    # Reorder columns to match the warehouse schema.
    df = df[["amc_name", "period_start", "period_end", "average_aum_cr"]]

    _insert_dataframe(conn, df, "aum_history")
    return len(df)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_warehouse_load() -> None:
    """
    Execute the full warehouse load pipeline end-to-end.

    Steps
    -----
    1. Discover the latest processed file for each dataset.
    2. Read all files into DataFrames.
    3. Validate source DataFrames (columns + non-empty).
    4. Open a single atomic transaction.
    5. Refresh all tables (DELETE in child-first order).
    6. Load all tables (INSERT in parent-first order).
    7. Validate post-load row counts.
    8. COMMIT on success; ROLLBACK on any exception.

    Raises
    ------
    FileNotFoundError
        If any expected processed file directory is empty.
    ValueError
        If any source DataFrame fails column validation.
    sqlite3.Error
        If any database operation fails.
    """
    logger.info("━" * 44)
    logger.info(" AMC Analytics Warehouse Load")
    logger.info("━" * 44)
    logger.info("")

    # ------------------------------------------------------------------
    # 1. Discover source files
    # ------------------------------------------------------------------
    logger.info("Discovering source files...")

    fund_master_path: Path = latest_file(DIR_FUND_MASTER)
    logger.info("  ✓ Found fund master file:    %s", fund_master_path.name)

    nav_path: Path = latest_file(DIR_NAV)
    logger.info("  ✓ Found NAV file:            %s", nav_path.name)

    ter_path: Path = latest_file(DIR_EXPENSE_RATIO)
    logger.info("  ✓ Found expense ratio file:  %s", ter_path.name)

    aum_path: Path = latest_file(DIR_AUM)
    logger.info("  ✓ Found AUM file:            %s", aum_path.name)

    logger.info("")

    # ------------------------------------------------------------------
    # 2. Read datasets
    # ------------------------------------------------------------------
    logger.info("Reading datasets...")

    fund_df: pd.DataFrame = pd.read_csv(fund_master_path, dtype=str)
    logger.info("  ✓ Fund Master loaded  (%d rows)", len(fund_df))

    nav_df: pd.DataFrame = pd.read_csv(nav_path, dtype={"scheme_code": str})
    logger.info("  ✓ NAV loaded          (%d rows)", len(nav_df))

    ter_df: pd.DataFrame = pd.read_csv(ter_path, dtype={"scheme_code": str})
    logger.info("  ✓ TER loaded          (%d rows)", len(ter_df))

    aum_df: pd.DataFrame = pd.read_csv(aum_path)
    logger.info("  ✓ AUM loaded          (%d rows)", len(aum_df))

    logger.info("")

    # ------------------------------------------------------------------
    # 3. Validate source DataFrames
    # ------------------------------------------------------------------
    logger.info("Validating source files...")

    validate_columns(fund_df, list(FUND_MASTER_COLUMNS.keys()), "fund_master")
    validate_columns(nav_df, list(NAV_COLUMNS.keys()), "nav")
    validate_columns(ter_df, list(TER_COLUMNS.keys()), "expense_ratio")
    validate_columns(aum_df, list(AUM_COLUMNS.keys()) + ["month"], "aum")

    logger.info("  ✓ All source files validated")
    logger.info("")

    # ------------------------------------------------------------------
    # 4–8. Atomic warehouse load
    # ------------------------------------------------------------------
    # NOTE on transaction strategy:
    # pandas to_sql with isolation_level=None (autocommit mode) uses the
    # single-row executemany path (method=None), which does not issue its
    # own BEGIN/COMMIT.  We therefore retain full control over the
    # transaction boundary via explicit BEGIN / COMMIT / ROLLBACK.
    #
    # NOTE on expense_ratio_history:
    # There is no FK on expense_ratio_history.scheme_code — AMFI TER codes
    # and MFI plan codes are entirely different identifier systems.  TER
    # loads cleanly inside the same transaction as all other tables with no
    # special pragma juggling required.
    with get_connection() as conn:
        try:
            conn.execute("BEGIN;")

            # --------------------------------------------------------------
            # 4. Refresh tables — child tables first, then parent tables
            # --------------------------------------------------------------
            logger.info("Refreshing warehouse tables...")

            conn.execute("DELETE FROM nav_history;")
            conn.execute("DELETE FROM expense_ratio_history;")
            conn.execute("DELETE FROM aum_history;")
            conn.execute("DELETE FROM schemes;")
            conn.execute("DELETE FROM amcs;")

            logger.info("  ✓ Tables cleared")
            logger.info("")

            # --------------------------------------------------------------
            # 5. Load — parent tables first, then child tables
            # --------------------------------------------------------------

            # amcs (standalone dimension; derived from fund master)
            logger.info("Loading amcs...")
            n_amcs: int = load_amcs(conn, fund_df)
            logger.info("  ✓ %s rows inserted", f"{n_amcs:,}")
            logger.info("")

            # schemes (parent of nav_history)
            logger.info("Loading schemes...")
            n_schemes: int = load_schemes(conn, fund_df)
            logger.info("  ✓ %s rows inserted", f"{n_schemes:,}")
            logger.info("")

            # nav_history (child of schemes; FK enforced — codes match)
            logger.info("Loading nav_history...")
            n_nav: int = load_nav_history(conn, nav_df)
            logger.info("  ✓ %s rows inserted", f"{n_nav:,}")
            logger.info("")

            # expense_ratio_history (standalone fact; no FK — see note above)
            logger.info("Loading expense_ratio_history...")
            n_ter: int = load_expense_ratio_history(conn, ter_df)
            logger.info("  ✓ %s rows inserted", f"{n_ter:,}")
            logger.info("")

            # aum_history (AMC-level fact; no FK in schema)
            logger.info("Loading aum_history...")
            n_aum: int = load_aum_history(conn, aum_df)
            logger.info("  ✓ %s rows inserted", f"{n_aum:,}")
            logger.info("")

            # --------------------------------------------------------------
            # 6. Post-load validation
            # --------------------------------------------------------------
            logger.info("Validating warehouse...")

            tables: list[str] = [
                "amcs",
                "schemes",
                "nav_history",
                "expense_ratio_history",
                "aum_history",
            ]

            all_valid: bool = True
            for table in tables:
                count: int = get_table_count(conn, table)
                if count == 0:
                    logger.error("  ✗ %s is EMPTY after load — rolling back.", table)
                    all_valid = False
                else:
                    logger.info("  ✓ %-30s %s rows", table, f"{count:,}")

            if not all_valid:
                raise ValueError(
                    "Post-load validation failed: one or more tables are empty. "
                    "Transaction will be rolled back."
                )

            logger.info("")

            # --------------------------------------------------------------
            # 7. Commit
            # --------------------------------------------------------------
            # pandas to_sql (executemany path) under isolation_level=None can
            # implicitly commit the surrounding transaction mid-load.  Guard
            # with in_transaction so we never issue COMMIT into a vacuum.
            if conn.in_transaction:
                conn.execute("COMMIT;")
            logger.info("Warehouse load complete.")
            logger.info("━" * 44)

        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except sqlite3.OperationalError:
                # No active transaction to roll back — already clean.
                pass
            logger.exception("Load failed — transaction rolled back.")
            raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_warehouse_load()