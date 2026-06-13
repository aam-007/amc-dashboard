"""
create_schema.py
================
Initialises the AMC Analytics SQLite data warehouse.

Responsibilities
----------------
* Locate (or create) the warehouse database at a path resolved relative to
  this file, so the module is portable regardless of the working directory.
* Enable SQLite foreign-key enforcement for every connection.
* Create all dimension and fact tables idempotently (CREATE TABLE IF NOT
  EXISTS), so the script is safe to re-run at any time.
* Build supporting indexes idempotently (CREATE INDEX IF NOT EXISTS).
* Wrap all DDL in a single transaction so the schema is either fully
  created or left untouched on failure.
* Emit structured log messages at every meaningful step for operational
  visibility.

Usage
-----
    python 02-data-warehouse/create_schema.py

Exit codes
----------
    0 — schema initialised (or already up-to-date) successfully
    1 — unrecoverable error; details in stderr via logging
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

# Resolve relative to *this file*, not the caller's cwd, so the module
# works correctly regardless of where Python is invoked from.
_THIS_FILE: Path = Path(__file__).resolve()
_WAREHOUSE_DIR: Path = _THIS_FILE.parent          # 02-data-warehouse/
DB_PATH: Path = _WAREHOUSE_DIR / "warehouse.db"

# ---------------------------------------------------------------------------
# DDL — tables
# ---------------------------------------------------------------------------

# Design notes on foreign keys
# ─────────────────────────────
# • schemes.amc_name  is NOT a FK to amcs.amc_name because fund_master data
#   arrives before the amcs table is populated, and some edge-case scheme
#   records may reference AMCs that are not yet in the amcs dimension.
#   A soft reference (plain TEXT column) is therefore preferred; referential
#   integrity is enforced at load time in load_data.py instead.
#
# • nav_history.scheme_code  → schemes.scheme_code
#   NAV records are meaningless without a matching scheme; a hard FK
#   enforces this correctly.
#
# • expense_ratio_history.scheme_code  — intentionally NOT a FK.
#   AMFI's TER disclosure uses its own internal string codes
#   (e.g. "360O/O/H/BHF/23/07/0007") which operate at fund level (one row
#   per fund).  The schemes table uses MFI numeric codes (e.g. "152073")
#   which operate at plan level (one row per Regular/Direct × Growth/IDCW
#   variant).  These are entirely separate identifier systems with zero
#   overlap — a FK would always fail.  Analytical joins between TER and
#   schemes are performed via scheme-name matching in query layer views.
#
# • aum_history.amc_name  is NOT a FK to amcs.amc_name because AUM data
#   is aggregated at AMC level and may arrive before all AMC dimension rows
#   exist.  Again, load_data.py is responsible for ensuring consistency.

_DDL_TABLES: list[str] = [
    # ------------------------------------------------------------------
    # Dimension: AMCs
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS amcs (
        amc_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        amc_name TEXT    UNIQUE NOT NULL
    )
    """,

    # ------------------------------------------------------------------
    # Dimension: Schemes
    # ------------------------------------------------------------------
    # scheme_code is the MFI-assigned unique identifier for every mutual-
    # fund plan.  It is used as the natural PK throughout the warehouse
    # because it is stable, compact, and already present in every source
    # dataset that originates from the fund master.
    """
    CREATE TABLE IF NOT EXISTS schemes (
        scheme_code       TEXT PRIMARY KEY,
        scheme_name       TEXT NOT NULL,
        amc_name          TEXT NOT NULL,
        category          TEXT,
        plan_type         TEXT,
        option_type       TEXT,
        isin_growth       TEXT,
        isin_reinvestment TEXT
    )
    """,

    # ------------------------------------------------------------------
    # Fact: NAV history
    # ------------------------------------------------------------------
    # FK to schemes ensures every NAV row has a parent scheme.
    # The composite PK (scheme_code, nav_date) enforces one NAV per
    # scheme per day and enables efficient range scans via the covering
    # index that the PK creates implicitly.
    """
    CREATE TABLE IF NOT EXISTS nav_history (
        scheme_code TEXT NOT NULL,
        nav_date    DATE NOT NULL,
        nav         REAL NOT NULL,

        PRIMARY KEY (scheme_code, nav_date),

        CONSTRAINT fk_nav_scheme
            FOREIGN KEY (scheme_code)
            REFERENCES schemes (scheme_code)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    )
    """,

    # ------------------------------------------------------------------
    # Fact: Expense-ratio history (Total Expense Ratio)
    # ------------------------------------------------------------------
    # No FK on scheme_code.  AMFI TER disclosures use a different code
    # system than the MFI plan codes in the schemes table — see design
    # notes above.  Analytical joins are performed at query time via
    # name matching.  Both regular_ter and direct_ter are nullable because
    # a fund may legitimately offer only one plan type at a given point.
    """
    CREATE TABLE IF NOT EXISTS expense_ratio_history (
        scheme_code TEXT NOT NULL,
        ter_date    DATE NOT NULL,
        regular_ter REAL,
        direct_ter  REAL,

        PRIMARY KEY (scheme_code, ter_date)
    )
    """,

    # ------------------------------------------------------------------
    # Fact: AUM history (AMC-level)
    # ------------------------------------------------------------------
    # period_start / period_end reflect the AMFI quarterly disclosure
    # window (e.g. January 1 – March 31).  Both are nullable to
    # accommodate point-in-time snapshots where only a single date is
    # reported.  amc_name is intentionally NOT a FK — see design notes.
    """
    CREATE TABLE IF NOT EXISTS aum_history (
        amc_name        TEXT NOT NULL,
        period_start    DATE,
        period_end      DATE,
        average_aum_cr  REAL,

        PRIMARY KEY (amc_name, period_start, period_end)
    )
    """,
]

# ---------------------------------------------------------------------------
# DDL — indexes
# ---------------------------------------------------------------------------

# The composite PKs on nav_history and expense_ratio_history already cover
# lookups by (scheme_code, date).  The single-column indexes below
# additionally accelerate:
#   • All NAV rows for a scheme  (JOIN with schemes)
#   • All TER rows for a scheme  (analytical name-join grouping)
#   • All AUM rows for an AMC    (JOIN with amcs / GROUP BY amc_name)

_DDL_INDEXES: list[str] = [
    """
    CREATE INDEX IF NOT EXISTS idx_nav_scheme
        ON nav_history (scheme_code)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ter_scheme
        ON expense_ratio_history (scheme_code)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_aum_amc
        ON aum_history (amc_name)
    """,
]

# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open (and if necessary create) the SQLite database at *db_path*.

    Parameters
    ----------
    db_path:
        Absolute path to the ``.db`` file.

    Returns
    -------
    sqlite3.Connection
        An open connection with foreign-key enforcement already enabled.

    Raises
    ------
    sqlite3.OperationalError
        If the parent directory does not exist or is not writable.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    # Row-level foreign-key checks must be enabled per connection in SQLite.
    conn.execute("PRAGMA foreign_keys = ON")
    # Write-Ahead Logging gives better concurrency for future read workloads.
    conn.execute("PRAGMA journal_mode = WAL")
    logger.debug("PRAGMA foreign_keys=ON, journal_mode=WAL set on connection")
    return conn


def _execute_ddl_batch(
    cursor: sqlite3.Cursor,
    statements: list[str],
    label: str,
) -> None:
    """Execute a list of DDL statements, logging each one.

    Parameters
    ----------
    cursor:
        An open SQLite cursor (inside an active transaction).
    statements:
        Iterable of SQL strings to execute in order.
    label:
        Human-readable category name used in log messages.
    """
    for i, stmt in enumerate(statements, start=1):
        # Extract the first non-blank line for a concise log entry.
        summary = next(
            (line.strip() for line in stmt.splitlines() if line.strip()),
            stmt[:60],
        )
        logger.debug("[%s %d/%d] %s", label, i, len(statements), summary)
        cursor.execute(stmt)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_schema(db_path: Path = DB_PATH) -> None:
    """Create all warehouse tables and indexes if they do not already exist.

    The function is **idempotent**: calling it on a database that already
    contains the schema is a no-op (all statements use ``IF NOT EXISTS``).

    All DDL is executed inside a single transaction.  If any statement
    fails the transaction is rolled back automatically by the context
    manager and the exception is re-raised.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Defaults to the canonical
        warehouse location relative to this module.

    Raises
    ------
    sqlite3.Error
        On any database-level failure.
    OSError
        If the parent directory cannot be created.
    """
    logger.info("━━━ AMC Analytics — Schema Initialisation ━━━")
    logger.info("Database path : %s", db_path)

    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        # All DDL runs inside one implicit transaction provided by the
        # sqlite3 context manager (commits on __exit__, rolls back on
        # exception).
        logger.info("Creating tables …")
        _execute_ddl_batch(cursor, _DDL_TABLES, label="TABLE")
        logger.info("  ✓  %d table(s) processed", len(_DDL_TABLES))

        logger.info("Creating indexes …")
        _execute_ddl_batch(cursor, _DDL_INDEXES, label="INDEX")
        logger.info("  ✓  %d index(es) processed", len(_DDL_INDEXES))

    # Verify by introspection — open a fresh read-only connection so we do
    # not hold the write lock longer than necessary.
    _verify_schema(db_path)

    logger.info("Schema initialisation complete.")


def _verify_schema(db_path: Path) -> None:
    """Log a summary of tables and indexes found in the database.

    Parameters
    ----------
    db_path:
        Path to the SQLite database to inspect.
    """
    with get_connection(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables: list[str] = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        )
        indexes: list[str] = [row[0] for row in cursor.fetchall()]

    logger.info("Verified tables  : %s", ", ".join(tables) if tables else "(none)")
    logger.info(
        "Verified indexes : %s",
        ", ".join(idx for idx in indexes if not idx.startswith("sqlite_"))
        or "(none)",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """CLI entry point.

    Returns
    -------
    int
        0 on success, 1 on failure.
    """
    try:
        create_schema()
        return 0
    except sqlite3.Error as exc:
        logger.error("Database error: %s", exc, exc_info=True)
        return 1
    except OSError as exc:
        logger.error("Filesystem error: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())