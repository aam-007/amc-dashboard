"""
run_pipeline.py
───────────────────────────────────────────────────────────────────────────────
AMC Dashboard Pipeline
End-to-End Analytics Orchestrator

Author  : aditya.mishra10@nmims.in
Requires: PySide6, psutil, Python 3.12+
───────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

import os
import queue
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import psutil

try:
    from PySide6.QtCore import (
        QObject, QThread, QTimer, Signal, Qt,
    )
    from PySide6.QtGui import (
        QColor, QFont, QFontDatabase, QPalette, QTextCursor,
    )
    from PySide6.QtWidgets import (
        QApplication, QFrame, QHBoxLayout, QLabel,
        QMainWindow, QPlainTextEdit, QPushButton, QScrollArea,
        QSizePolicy, QSplitter, QVBoxLayout, QWidget, QCheckBox,
    )
except ImportError:
    from PyQt6.QtCore import QObject, QThread, QTimer, pyqtSignal as Signal, Qt  # type: ignore
    from PyQt6.QtGui import QColor, QFont, QFontDatabase, QPalette, QTextCursor  # type: ignore
    from PyQt6.QtWidgets import (  # type: ignore
        QApplication, QFrame, QHBoxLayout, QLabel,
        QMainWindow, QPlainTextEdit, QPushButton, QScrollArea,
        QSizePolicy, QSplitter, QVBoxLayout, QWidget, QCheckBox,
    )

# ── Project root ────────────────────────────────────────────────────────────
# This file lives in `07-automation/`, and every script referenced below
# lives one level up, in the repo root (sibling to 01-data-ingestion,
# 02-data-warehouse, etc.) — hence `.parent.parent`, not `.parent`.
THIS_FILE    = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent

# ── Log export paths ───────────────────────────────────────────────────────
PIPELINE_LOG_DIR   = PROJECT_ROOT / "data" / "exports" / "pipeline"
SUCCESS_LOG_SUBDIR = "SUCCESS_RUN_LOGS"
FAILED_LOG_SUBDIR  = "FAILED_RUN_LOGS"

# ── Window ─────────────────────────────────────────────────────────────────
WINDOW_TITLE  = "AMC Dashboard Pipeline"
WINDOW_W      = 1400
WINDOW_H      = 900

# ── Palette — graphite dark-mode system ───────────────────────────────────
C_BG           = "#0d0d0f"   # near-black base
C_SURFACE      = "#141416"   # card background
C_SURFACE_2    = "#1c1c1f"   # elevated card / sidebar
C_BORDER       = "#2a2a2e"   # subtle dividers
C_BORDER_SOFT  = "#222226"
C_TEXT_PRIMARY = "#f0f0f3"
C_TEXT_SECONDARY = "#8e8e96"
C_TEXT_DIM     = "#4a4a52"

C_BLUE         = "#3a86ff"   # primary action / running
C_BLUE_HOVER   = "#5b9bff"
C_GREEN        = "#30d158"   # success
C_RED          = "#ff453a"   # failure / kill
C_ORANGE       = "#ff9f0a"   # killed / warning
C_GRAY         = "#636366"   # pending

C_CONSOLE_BG   = "#0a0a0c"
C_CONSOLE_TEXT = "#c8c8d0"
C_TIMESTAMP    = "#3a86ff"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — PIPELINE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PipelineStage:
    """Defines a single executable stage in the pipeline."""
    index: int
    label: str
    script_path: str          # relative to PROJECT_ROOT

PIPELINE_STAGES: list[PipelineStage] = [
    PipelineStage(1,  "AUM Ingestion",           "01-data-ingestion/aum/amfi_aum.py"),
    PipelineStage(2,  "Expense Ratio Ingestion",  "01-data-ingestion/expense_ratio/amfi_amc_expense_ratio.py"),
    PipelineStage(3,  "Fund Master Ingestion",    "01-data-ingestion/fund_master/amfi_fund_master.py"),
    PipelineStage(4,  "NAV Ingestion",            "01-data-ingestion/nav/amfi_nav.py"),
    PipelineStage(5,  "Warehouse Load",           "02-data-warehouse/load_data.py"),
    PipelineStage(6,  "Market Share Calculation", "03-amc-analytics/market_share/calculate_market_share.py"),
    PipelineStage(7,  "AMC Rankings",             "03-amc-analytics/rankings/ranking_amcs.py"),
    PipelineStage(8,  "Revenue Estimation",       "04-revenue-model/estimate_revenue.py"),
    PipelineStage(9,  "Revenue Validation",       "04-revenue-model/validate_revenue.py"),
    PipelineStage(10, "AUM Forecast",             "05-forcasting/forecast_aum.py"),
    PipelineStage(11, "Revenue Forecast",         "05-forcasting/forecast_revenue.py"),
    # ── NEW: Push latest results to GitHub ────────────────────────────────
    PipelineStage(12, "Push to GitHub",           "07-automation/push_to_github.py"),
]


def find_missing_stages(stages: list[PipelineStage]) -> list[PipelineStage]:
    """Return the subset of stages whose script file does not exist on disk.

    Resolves each stage's script_path against PROJECT_ROOT so that path
    mistakes (wrong root, typo'd folder names, renamed scripts) are caught
    up front instead of surfacing as an opaque subprocess failure on stage 1.
    """
    missing: list[PipelineStage] = []
    for stage in stages:
        script = PROJECT_ROOT / stage.script_path
        if not script.is_file():
            missing.append(stage)
    return missing


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — STAGE & PIPELINE STATUS ENUMS
# ══════════════════════════════════════════════════════════════════════════════

class StageStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    PASSED  = auto()
    FAILED  = auto()
    KILLED  = auto()

class PipelineStatus(Enum):
    READY     = auto()
    RUNNING   = auto()
    COMPLETED = auto()
    FAILED    = auto()
    KILLED    = auto()

STATUS_COLORS: dict[StageStatus, str] = {
    StageStatus.PENDING: C_GRAY,
    StageStatus.RUNNING: C_BLUE,
    StageStatus.PASSED:  C_GREEN,
    StageStatus.FAILED:  C_RED,
    StageStatus.KILLED:  C_ORANGE,
}

PIPELINE_STATUS_COLORS: dict[PipelineStatus, str] = {
    PipelineStatus.READY:     C_GRAY,
    PipelineStatus.RUNNING:   C_BLUE,
    PipelineStatus.COMPLETED: C_GREEN,
    PipelineStatus.FAILED:    C_RED,
    PipelineStatus.KILLED:    C_ORANGE,
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — LOG DATA MODEL
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class StageResult:
    """Captured output and timing for a single completed stage."""
    stage: PipelineStage
    status: StageStatus
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — LOG EXPORT SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

class LogExporter:
    """Writes structured pipeline log files to disk."""

    def export(
        self,
        results: list[StageResult],
        pipeline_status: PipelineStatus,
        total_runtime_seconds: float,
        started_at: datetime,
        ended_at: datetime,
    ) -> Path:
        """
        Serialise pipeline results to a timestamped .log file.

        Returns the path of the written file.
        """
        subdir = (
            SUCCESS_LOG_SUBDIR
            if pipeline_status == PipelineStatus.COMPLETED
            else FAILED_LOG_SUBDIR
        )
        log_dir = PIPELINE_LOG_DIR / subdir
        log_dir.mkdir(parents=True, exist_ok=True)

        ts       = started_at.strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"pipeline_{ts}.log"

        lines: list[str] = []
        _sep = "=" * 72

        lines += [
            _sep,
            "AMC DASHBOARD PIPELINE — RUN LOG",
            _sep,
            f"Pipeline Status : {pipeline_status.name}",
            f"Started         : {started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Ended           : {ended_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Runtime   : {self._fmt_duration(total_runtime_seconds)}",
            f"Stages Executed : {len(results)}",
            f"Stages Passed   : {sum(1 for r in results if r.status == StageStatus.PASSED)}",
            f"Stages Failed   : {sum(1 for r in results if r.status == StageStatus.FAILED)}",
            "",
        ]

        for r in results:
            lines += [
                _sep,
                f"STAGE {r.stage.index:02d} — {r.stage.label.upper()}",
                f"Status   : {r.status.name}",
                f"Script   : {r.stage.script_path}",
                f"Started  : {r.started_at.strftime('%H:%M:%S') if r.started_at else '—'}",
                f"Ended    : {r.ended_at.strftime('%H:%M:%S') if r.ended_at else '—'}",
                f"Duration : {self._fmt_duration(r.duration_seconds)}",
                "",
                "── STDOUT ──",
                r.stdout or "(no output)",
                "",
                "── STDERR ──",
                r.stderr or "(no output)",
                "",
            ]

        lines += [_sep, "END OF LOG", _sep]

        log_path.write_text("\n".join(lines), encoding="utf-8")
        return log_path

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — WORKER THREAD (Pipeline execution in background)
# ══════════════════════════════════════════════════════════════════════════════

class PipelineMessage:
    """Typed message passed from worker thread to GUI via queue."""
    __slots__ = ("kind", "payload")
    def __init__(self, kind: str, payload: object = None) -> None:
        self.kind    = kind
        self.payload = payload


class PipelineWorker(QThread):
    """
    Runs each pipeline stage sequentially in a background QThread.

    Signals
    -------
    message_ready : emitted for every status update / log line.
    """
    message_ready = Signal(object)   # PipelineMessage

    # Maximum seconds to wait for a single stage (adjust as needed)
    STAGE_TIMEOUT_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        stages: list[PipelineStage],
        verbose: bool,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self.stages  = stages
        self.verbose = verbose
        self._kill_requested = threading.Event()
        self._current_proc: Optional[subprocess.Popen] = None
        self._results: list[StageResult] = []

    # ── Public API ─────────────────────────────────────────────────────────

    def request_kill(self) -> None:
        """Signal the worker to stop at the next opportunity and kill the subprocess."""
        self._kill_requested.set()
        self._terminate_current_process()

    @property
    def results(self) -> list[StageResult]:
        return self._results

    # ── Thread entry ───────────────────────────────────────────────────────

    def run(self) -> None:
        self._emit("pipeline_start")
        pipeline_failed = False

        for stage in self.stages:
            if self._kill_requested.is_set():
                break

            self._emit("stage_start", stage)
            result = self._run_stage(stage)
            self._results.append(result)
            self._emit("stage_end", result)

            if result.status == StageStatus.KILLED:
                pipeline_failed = True
                break
            if result.status == StageStatus.FAILED:
                pipeline_failed = True
                break

        if self._kill_requested.is_set():
            self._emit("pipeline_killed")
        elif pipeline_failed:
            self._emit("pipeline_failed")
        else:
            self._emit("pipeline_completed")

    # ── Stage execution ────────────────────────────────────────────────────
    #
    # ** FIXED VERSION **
    #  - Reads stdout and stderr concurrently via a daemon thread to prevent
    #    pipe-buffer deadlocks.
    #  - Redirects stdin to DEVNULL to avoid any input‑waiting hangs.
    #  - Adds a timeout on proc.wait() and a kill on TimeoutExpired.
    #  - Ensures proper cleanup even on errors.
    # ────────────────────────────────────────────────────────────────────────

    def _run_stage(self, stage: PipelineStage) -> StageResult:
        script  = PROJECT_ROOT / stage.script_path
        started = datetime.now()

        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        if not script.is_file():
            msg = f"Script not found: {script}"
            self._emit("log_line", (stage.label, msg, "stderr"))
            ended = datetime.now()
            return StageResult(
                stage=stage,
                status=StageStatus.FAILED,
                stdout="",
                stderr=msg,
                duration_seconds=(ended - started).total_seconds(),
                started_at=started,
                ended_at=ended,
            )

        proc = None
        try:
            proc = subprocess.Popen(
                [sys.executable, str(script)],
                cwd=str(PROJECT_ROOT),
                stdin=subprocess.DEVNULL,          # prevent any stdin hang
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env={
                    **os.environ,
                    "PYTHONUNBUFFERED": "1",
                    "PYTHONIOENCODING": "utf-8",
                },
                **self._popen_kwargs(),
            )
            self._current_proc = proc

            # ── Daemon thread to drain stderr concurrently ────────────────
            def read_stderr():
                try:
                    for line in proc.stderr:
                        if self._kill_requested.is_set():
                            break
                        line = line.rstrip()
                        stderr_lines.append(line)
                        if self.verbose and line.strip():
                            self._emit("log_line", (stage.label, line, "stderr"))
                except ValueError:
                    # pipe already closed
                    pass

            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stderr_thread.start()

            # ── Main thread: read stdout ──────────────────────────────────
            for line in proc.stdout:
                if self._kill_requested.is_set():
                    break
                line = line.rstrip()
                stdout_lines.append(line)
                if self.verbose or line.startswith(("PASS", "FAIL", "[", "=")):
                    self._emit("log_line", (stage.label, line, "stdout"))

            # Close stdout – child will receive SIGPIPE/EPIPE if it tries to
            # write further. This is safe because we have fully consumed it.
            proc.stdout.close()

            # Wait for stderr drain to finish (with a guard timeout)
            stderr_thread.join(timeout=5.0)
            if not proc.stderr.closed:
                proc.stderr.close()

            # Wait for the process itself, with a safety timeout
            try:
                proc.wait(timeout=self.STAGE_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                # Process did not finish in time; treat as killed
                self._emit("log_line", (stage.label,
                    f"Stage timed out after {self.STAGE_TIMEOUT_SECONDS}s", "stderr"))
                self._terminate_current_process()
                proc.wait()  # final reaping
                self._current_proc = None

        except Exception as exc:
            stderr_lines.append(str(exc))
            self._emit("log_line", (stage.label, f"EXCEPTION: {exc}", "stderr"))
            ended = datetime.now()
            return StageResult(
                stage=stage,
                status=StageStatus.FAILED,
                stdout="\n".join(stdout_lines),
                stderr="\n".join(stderr_lines),
                duration_seconds=(ended - started).total_seconds(),
                started_at=started,
                ended_at=ended,
            )

        ended = datetime.now()

        if self._kill_requested.is_set():
            status = StageStatus.KILLED
        elif proc.returncode == 0:
            status = StageStatus.PASSED
        else:
            status = StageStatus.FAILED

        return StageResult(
            stage=stage,
            status=status,
            stdout="\n".join(stdout_lines),
            stderr="\n".join(stderr_lines),
            duration_seconds=(ended - started).total_seconds(),
            started_at=started,
            ended_at=ended,
        )

    # ── Process management ─────────────────────────────────────────────────

    def _terminate_current_process(self) -> None:
        """Kill the running subprocess and all its children via psutil."""
        proc = self._current_proc
        if proc is None:
            return
        try:
            parent = psutil.Process(proc.pid)
            children = parent.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            parent.kill()
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass

    @staticmethod
    def _popen_kwargs() -> dict:
        """Return OS-appropriate kwargs for subprocess.Popen process groups."""
        if sys.platform == "win32":
            return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        return {"start_new_session": True}

    # ── Internal emit ──────────────────────────────────────────────────────

    def _emit(self, kind: str, payload: object = None) -> None:
        self.message_ready.emit(PipelineMessage(kind, payload))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — GUI COMPONENTS
# ══════════════════════════════════════════════════════════════════════════════

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _label(
    text: str,
    size: int = 13,
    color: str = C_TEXT_PRIMARY,
    bold: bool = False,
    mono: bool = False,
) -> QLabel:
    lbl = QLabel(text)
    font = QFont()
    font.setPointSize(size)
    if bold:
        font.setWeight(QFont.Weight.Bold)
    if mono:
        font.setFamily("SF Mono, Menlo, Consolas, monospace")
    lbl.setFont(font)
    lbl.setStyleSheet(f"color: {color}; background: transparent;")
    return lbl


def _card(radius: int = 12) -> QFrame:
    frame = QFrame()
    frame.setObjectName("card")
    frame.setStyleSheet(f"""
        QFrame#card {{
            background: {C_SURFACE};
            border: 1px solid {C_BORDER};
            border-radius: {radius}px;
        }}
    """)
    return frame


def _divider() -> QFrame:
    line = QFrame()
    line.setFrameShape(QFrame.Shape.HLine)
    line.setStyleSheet(f"background: {C_BORDER}; border: none; max-height: 1px;")
    return line


# ─── Stage Row Widget ─────────────────────────────────────────────────────────

class StageRow(QWidget):
    """A single pipeline stage row with dot indicator, name, and status badge."""

    def __init__(self, stage: PipelineStage, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.stage = stage
        self._status = StageStatus.PENDING

        self.setFixedHeight(42)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 0, 16, 0)
        layout.setSpacing(12)

        # Stage number
        num = _label(f"{stage.index:02d}", 11, C_TEXT_DIM, mono=True)
        num.setFixedWidth(22)
        layout.addWidget(num)

        # Dot indicator
        self._dot = QLabel("●")
        self._dot.setFixedWidth(16)
        font = QFont()
        font.setPointSize(10)
        self._dot.setFont(font)
        self._dot.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._dot)

        # Stage label
        name = _label(stage.label, 13, C_TEXT_PRIMARY)
        name.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(name)

        # Status badge
        self._badge = QLabel("PENDING")
        self._badge.setFixedWidth(70)
        self._badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        badge_font = QFont()
        badge_font.setPointSize(10)
        badge_font.setWeight(QFont.Weight.Bold)
        self._badge.setFont(badge_font)
        layout.addWidget(self._badge)

        self._apply_style()

    def set_status(self, status: StageStatus) -> None:
        self._status = status
        self._apply_style()

    def _apply_style(self) -> None:
        s = self._status
        color = STATUS_COLORS[s]
        label = s.name
        self._dot.setStyleSheet(f"color: {color}; background: transparent;")
        self._badge.setStyleSheet(f"""
            color: {color};
            background: transparent;
            border: 1px solid {color}44;
            border-radius: 5px;
            padding: 2px 6px;
        """)
        self._badge.setText(label)


# ─── Toggle Switch ────────────────────────────────────────────────────────────

class ToggleSwitch(QCheckBox):
    """Pill-shaped ON/OFF toggle rendered via stylesheet."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setText("")
        self.setFixedSize(52, 28)
        self._update_style()
        self.stateChanged.connect(lambda _: self._update_style())

    def _update_style(self) -> None:
        on = self.isChecked()
        track = C_BLUE if on else C_BORDER
        offset = "26px" if on else "2px"
        self.setStyleSheet(f"""
            QCheckBox {{
                background: {track};
                border-radius: 14px;
                border: none;
            }}
            QCheckBox::indicator {{
                width: 24px; height: 24px;
                border-radius: 12px;
                background: {C_TEXT_PRIMARY};
                margin-left: {offset};
                margin-top: 2px;
                border: none;
            }}
        """)


# ─── Console Widget ───────────────────────────────────────────────────────────

class ConsoleWidget(QPlainTextEdit):
    """Terminal-style log console with timestamp prefixing and auto-scroll."""

    MAX_BLOCKS = 5000   # performance ceiling

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setReadOnly(True)
        self.setMaximumBlockCount(self.MAX_BLOCKS)
        self.setStyleSheet(f"""
            QPlainTextEdit {{
                background: {C_CONSOLE_BG};
                color: {C_CONSOLE_TEXT};
                border: 1px solid {C_BORDER};
                border-radius: 10px;
                padding: 12px;
                font-family: 'SF Mono', 'Menlo', 'Consolas', monospace;
                font-size: 12px;
                selection-background-color: {C_BLUE}55;
            }}
            QScrollBar:vertical {{
                background: {C_SURFACE};
                width: 8px;
                border-radius: 4px;
            }}
            QScrollBar::handle:vertical {{
                background: {C_BORDER};
                border-radius: 4px;
                min-height: 20px;
            }}
        """)

    def append_line(self, stage: str, text: str, stream: str = "stdout") -> None:
        ts    = datetime.now().strftime("%H:%M:%S")
        color = C_TIMESTAMP if stream == "stdout" else C_ORANGE
        self.appendHtml(
            f'<span style="color:{color}; font-weight:600;">{ts}</span>'
            f'<span style="color:{C_TEXT_DIM};"> | </span>'
            f'<span style="color:{C_TEXT_SECONDARY};">{stage}</span>'
            f'<span style="color:{C_TEXT_DIM};"> → </span>'
            f'<span style="color:{C_CONSOLE_TEXT};">{text}</span>'
        )
        self.moveCursor(QTextCursor.MoveOperation.End)

    def append_system(self, text: str, color: str = C_BLUE) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.appendHtml(
            f'<span style="color:{color}; font-weight:700;">{ts} | ──── {text} ────</span>'
        )
        self.moveCursor(QTextCursor.MoveOperation.End)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — MAIN WINDOW
# ══════════════════════════════════════════════════════════════════════════════

class MainWindow(QMainWindow):
    """
    AMC Dashboard Pipeline main window.

    Owns the pipeline worker thread, runtime timer, all UI widgets, and
    the log export system.
    """

    def __init__(self) -> None:
        super().__init__()
        self._worker: Optional[PipelineWorker] = None
        self._timer  = QTimer(self)
        self._elapsed_secs = 0
        self._pipeline_started_at: Optional[datetime] = None
        self._stage_rows: dict[int, StageRow] = {}
        self._log_exporter = LogExporter()

        self._init_window()
        self._build_ui()
        self._connect_timer()
        self._run_startup_validation()

    # ── Window setup ───────────────────────────────────────────────────────

    def _init_window(self) -> None:
        self.setWindowTitle(WINDOW_TITLE)
        self.resize(WINDOW_W, WINDOW_H)
        self.setMinimumSize(1000, 640)

        # Centre on screen
        screen = QApplication.primaryScreen().geometry()
        x = (screen.width()  - WINDOW_W) // 2
        y = (screen.height() - WINDOW_H) // 2
        self.move(x, y)

        self.setStyleSheet(f"""
            QMainWindow, QWidget {{
                background: {C_BG};
                color: {C_TEXT_PRIMARY};
                font-family: -apple-system, 'SF Pro Text', 'Helvetica Neue', Arial, sans-serif;
            }}
            QSplitter::handle {{
                background: {C_BORDER};
                width: 1px;
            }}
            QScrollArea {{
                border: none;
                background: transparent;
            }}
            QScrollBar:vertical {{
                background: {C_SURFACE};
                width: 6px;
                border-radius: 3px;
            }}
            QScrollBar::handle:vertical {{
                background: {C_BORDER};
                border-radius: 3px;
                min-height: 20px;
            }}
        """)

    # ── UI construction ────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        root_layout.addWidget(self._build_header())
        root_layout.addWidget(self._build_body(), stretch=1)

    def _build_header(self) -> QWidget:
        header = QWidget()
        header.setFixedHeight(72)
        header.setStyleSheet(f"""
            background: {C_SURFACE_2};
            border-bottom: 1px solid {C_BORDER};
        """)
        layout = QHBoxLayout(header)
        layout.setContentsMargins(28, 0, 28, 0)

        # Title block
        title_col = QVBoxLayout()
        title_col.setSpacing(2)
        title_col.addWidget(_label("AMC Dashboard Pipeline", 18, C_TEXT_PRIMARY, bold=True))
        title_col.addWidget(_label("End-to-End Analytics Orchestrator", 12, C_TEXT_SECONDARY))
        layout.addLayout(title_col)

        layout.addStretch()

        # Live clock
        self._clock_label = _label("", 13, C_TEXT_DIM, mono=True)
        self._update_clock()
        clock_timer = QTimer(self)
        clock_timer.timeout.connect(self._update_clock)
        clock_timer.start(1000)
        layout.addWidget(self._clock_label)

        return header

    def _build_body(self) -> QWidget:
        body = QWidget()
        outer = QVBoxLayout(body)
        outer.setContentsMargins(20, 20, 20, 20)
        outer.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(16)
        splitter.setChildrenCollapsible(False)
        splitter.setStyleSheet(f"""
            QSplitter::handle {{
                background: transparent;
            }}
            QSplitter::handle:hover {{
                background: {C_BORDER};
            }}
        """)

        # ── Left column ───────────────────────────────────────────────────
        left = QVBoxLayout()
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(14)

        left.addWidget(self._build_status_card())
        left.addWidget(self._build_runtime_card())
        left.addWidget(self._build_controls_card())
        left.addStretch()

        left_widget = QWidget()
        left_widget.setLayout(left)
        left_widget.setMinimumWidth(240)

        # ── Centre column — stages ────────────────────────────────────────
        centre = QVBoxLayout()
        centre.setContentsMargins(0, 0, 0, 0)
        centre.setSpacing(0)

        stages_header = _label("Pipeline Stages", 13, C_TEXT_SECONDARY, bold=True)
        stages_header.setContentsMargins(4, 0, 0, 8)
        centre.addWidget(stages_header)
        centre.addWidget(self._build_stages_panel())

        centre_widget = QWidget()
        centre_widget.setLayout(centre)
        centre_widget.setMinimumWidth(300)

        # ── Right column — console ────────────────────────────────────────
        right = QVBoxLayout()
        right.setContentsMargins(0, 0, 0, 0)
        right.setSpacing(0)

        console_header = _label("Live Output", 13, C_TEXT_SECONDARY, bold=True)
        console_header.setContentsMargins(4, 0, 0, 8)
        right.addWidget(console_header)

        self._console = ConsoleWidget()
        right.addWidget(self._console, stretch=1)

        right_widget = QWidget()
        right_widget.setLayout(right)
        right_widget.setMinimumWidth(360)

        # Console gets the lion's share of the window and absorbs resize
        # changes; the side columns default to roughly their old widths but
        # are now user-resizable via the splitter handles.
        splitter.addWidget(left_widget)
        splitter.addWidget(centre_widget)
        splitter.addWidget(right_widget)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 0)
        splitter.setStretchFactor(2, 1)
        splitter.setSizes([260, 320, 760])

        outer.addWidget(splitter)
        return body

    # ── Cards ──────────────────────────────────────────────────────────────

    def _build_status_card(self) -> QFrame:
        card = _card()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(8)

        layout.addWidget(_label("Pipeline Status", 11, C_TEXT_DIM, bold=True))

        row = QHBoxLayout()
        row.setSpacing(10)

        self._status_dot = QLabel("●")
        dot_font = QFont()
        dot_font.setPointSize(20)
        self._status_dot.setFont(dot_font)
        self._status_dot.setStyleSheet(f"color: {C_GRAY}; background: transparent;")
        row.addWidget(self._status_dot)

        self._status_label = _label("Ready", 22, C_TEXT_PRIMARY, bold=True)
        row.addWidget(self._status_label)
        row.addStretch()
        layout.addLayout(row)

        return card

    def _build_runtime_card(self) -> QFrame:
        card = _card()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(8)

        layout.addWidget(_label("Live Runtime", 11, C_TEXT_DIM, bold=True))
        self._runtime_label = _label("00:00:00", 28, C_TEXT_PRIMARY, bold=True, mono=True)
        layout.addWidget(self._runtime_label)

        return card

    def _build_controls_card(self) -> QFrame:
        card = _card()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(12)

        layout.addWidget(_label("Controls", 11, C_TEXT_DIM, bold=True))

        # Start button
        self._start_btn = QPushButton("▶  Start Pipeline")
        self._start_btn.setFixedHeight(44)
        self._start_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._start_btn.setStyleSheet(f"""
            QPushButton {{
                background: {C_BLUE};
                color: #ffffff;
                border: none;
                border-radius: 10px;
                font-size: 14px;
                font-weight: 700;
                padding: 0 20px;
            }}
            QPushButton:hover {{ background: {C_BLUE_HOVER}; }}
            QPushButton:disabled {{
                background: {C_BORDER};
                color: {C_TEXT_DIM};
            }}
        """)
        self._start_btn.clicked.connect(self._on_start)
        layout.addWidget(self._start_btn)

        # Kill button
        self._kill_btn = QPushButton("✕  Kill Pipeline")
        self._kill_btn.setFixedHeight(40)
        self._kill_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._kill_btn.setEnabled(False)
        self._kill_btn.setStyleSheet(f"""
            QPushButton {{
                background: transparent;
                color: {C_RED};
                border: 1px solid {C_RED}66;
                border-radius: 10px;
                font-size: 13px;
                font-weight: 600;
                padding: 0 20px;
            }}
            QPushButton:hover {{
                background: {C_RED}18;
                border-color: {C_RED};
            }}
            QPushButton:disabled {{
                color: {C_TEXT_DIM};
                border-color: {C_BORDER};
            }}
        """)
        self._kill_btn.clicked.connect(self._on_kill)
        layout.addWidget(self._kill_btn)

        layout.addWidget(_divider())

        # Verbose toggle
        vrow = QHBoxLayout()
        vrow.setSpacing(10)
        vrow.addWidget(_label("Verbose Mode", 13, C_TEXT_PRIMARY))
        vrow.addStretch()
        self._verbose_toggle = ToggleSwitch()
        vrow.addWidget(self._verbose_toggle)
        layout.addLayout(vrow)

        return card

    def _build_stages_panel(self) -> QFrame:
        card = _card()
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet(f"background: {C_SURFACE}; border-radius: 12px; border: 1px solid {C_BORDER};")

        container = QWidget()
        container.setStyleSheet(f"background: {C_SURFACE};")
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 8, 0, 8)
        layout.setSpacing(0)

        for i, stage in enumerate(PIPELINE_STAGES):
            row = StageRow(stage)
            row.setStyleSheet(f"background: transparent;")
            self._stage_rows[stage.index] = row
            layout.addWidget(row)

            if i < len(PIPELINE_STAGES) - 1:
                div = QFrame()
                div.setFrameShape(QFrame.Shape.HLine)
                div.setStyleSheet(f"background: {C_BORDER_SOFT}; border: none; max-height: 1px; margin: 0 16px;")
                layout.addWidget(div)

        scroll_area.setWidget(container)
        return scroll_area

    # ── Timer ──────────────────────────────────────────────────────────────

    def _connect_timer(self) -> None:
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._tick_runtime)

    def _tick_runtime(self) -> None:
        self._elapsed_secs += 1
        h = self._elapsed_secs // 3600
        m = (self._elapsed_secs % 3600) // 60
        s = self._elapsed_secs % 60
        self._runtime_label.setText(f"{h:02d}:{m:02d}:{s:02d}")

    def _update_clock(self) -> None:
        self._clock_label.setText(datetime.now().strftime("%A, %d %b %Y  %H:%M:%S"))

    # ── Event handlers ─────────────────────────────────────────────────────

    def _on_start(self) -> None:
        missing = find_missing_stages(PIPELINE_STAGES)
        if missing:
            self._show_validation_error(missing)
            return

        self._reset_ui()
        self._pipeline_started_at = datetime.now()
        self._elapsed_secs = 0
        self._timer.start()

        verbose = self._verbose_toggle.isChecked()
        self._worker = PipelineWorker(PIPELINE_STAGES, verbose, parent=self)
        self._worker.message_ready.connect(self._handle_message)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.start()

        self._start_btn.setEnabled(False)
        self._kill_btn.setEnabled(True)
        self._console.append_system("Pipeline started", C_BLUE)

    def _on_kill(self) -> None:
        if self._worker and self._worker.isRunning():
            self._console.append_system("Kill requested — terminating…", C_ORANGE)
            self._worker.request_kill()

    def _on_worker_finished(self) -> None:
        self._timer.stop()
        self._start_btn.setEnabled(True)
        self._kill_btn.setEnabled(False)

    def _handle_message(self, msg: PipelineMessage) -> None:
        kind    = msg.kind
        payload = msg.payload

        if kind == "pipeline_start":
            self._set_pipeline_status(PipelineStatus.RUNNING)

        elif kind == "stage_start":
            stage: PipelineStage = payload
            self._stage_rows[stage.index].set_status(StageStatus.RUNNING)
            self._console.append_system(f"Stage {stage.index:02d} — {stage.label}", C_BLUE)

        elif kind == "stage_end":
            result: StageResult = payload
            self._stage_rows[result.stage.index].set_status(result.status)
            color = {
                StageStatus.PASSED: C_GREEN,
                StageStatus.FAILED: C_RED,
                StageStatus.KILLED: C_ORANGE,
            }.get(result.status, C_TEXT_DIM)
            self._console.append_system(
                f"{result.stage.label} → {result.status.name}  "
                f"({result.duration_seconds:.1f}s)",
                color,
            )

        elif kind == "log_line":
            stage_label, text, stream = payload
            self._console.append_line(stage_label, text, stream)

        elif kind == "pipeline_completed":
            self._set_pipeline_status(PipelineStatus.COMPLETED)
            self._export_logs(PipelineStatus.COMPLETED)

        elif kind == "pipeline_failed":
            self._set_pipeline_status(PipelineStatus.FAILED)
            self._export_logs(PipelineStatus.FAILED)

        elif kind == "pipeline_killed":
            self._set_pipeline_status(PipelineStatus.KILLED)
            self._export_logs(PipelineStatus.KILLED)

    # ── Status helpers ─────────────────────────────────────────────────────

    def _set_pipeline_status(self, status: PipelineStatus) -> None:
        color = PIPELINE_STATUS_COLORS[status]
        label = status.name.title()
        self._status_dot.setStyleSheet(f"color: {color}; background: transparent;")
        self._status_label.setText(label)
        self._status_label.setStyleSheet(f"color: {color}; background: transparent; font-size: 22px; font-weight: 700;")

    def _reset_ui(self) -> None:
        self._runtime_label.setText("00:00:00")
        self._console.clear()
        for row in self._stage_rows.values():
            row.set_status(StageStatus.PENDING)
        self._set_pipeline_status(PipelineStatus.RUNNING)

    # ── Validation ─────────────────────────────────────────────────────────

    def _run_startup_validation(self) -> None:
        """Check every stage script exists as soon as the window opens, so a
        broken PROJECT_ROOT or renamed folder is visible immediately rather
        than discovered after pressing Start."""
        missing = find_missing_stages(PIPELINE_STAGES)
        if missing:
            self._console.append_system(
                f"Warning: {len(missing)} stage script(s) not found under {PROJECT_ROOT}",
                C_ORANGE,
            )
            for stage in missing:
                self._console.append_system(
                    f"  Stage {stage.index:02d} — {stage.label}: missing {stage.script_path}",
                    C_TEXT_SECONDARY,
                )
            self._console.append_system(
                "Pipeline can be started, but will fail validation until these are fixed",
                C_TEXT_SECONDARY,
            )
        else:
            self._console.append_system(
                f"All {len(PIPELINE_STAGES)} stage scripts found under {PROJECT_ROOT}",
                C_GREEN,
            )

    def _show_validation_error(self, missing: list[PipelineStage]) -> None:
        """Block the run and print a clear list of unresolved script paths,
        instead of letting the worker fail noisily on the first stage."""
        self._console.clear()
        self._set_pipeline_status(PipelineStatus.FAILED)
        self._console.append_system("Pipeline validation failed", C_RED)
        self._console.append_system(f"Project root resolved to: {PROJECT_ROOT}", C_TEXT_SECONDARY)
        self._console.append_system(f"{len(missing)} stage script(s) could not be found:", C_RED)
        for stage in missing:
            self._console.append_system(
                f"  Stage {stage.index:02d} — {stage.label}: {PROJECT_ROOT / stage.script_path}",
                C_ORANGE,
            )
        self._console.append_system(
            "Fix the paths in PIPELINE_STAGES or PROJECT_ROOT, then try again",
            C_TEXT_SECONDARY,
        )

    # ── Log export ─────────────────────────────────────────────────────────

    def _export_logs(self, status: PipelineStatus) -> None:
        if self._worker is None:
            return

        ended   = datetime.now()
        results = self._worker.results

        try:
            log_path = self._log_exporter.export(
                results=results,
                pipeline_status=status,
                total_runtime_seconds=self._elapsed_secs,
                started_at=self._pipeline_started_at or ended,
                ended_at=ended,
            )
            self._console.append_system(f"Log exported → {log_path}", C_GREEN)
        except Exception as exc:
            self._console.append_system(f"Log export failed: {exc}", C_RED)

        self._print_summary(results, status)

    def _print_summary(self, results: list[StageResult], status: PipelineStatus) -> None:
        passed = sum(1 for r in results if r.status == StageStatus.PASSED)
        failed = sum(1 for r in results if r.status == StageStatus.FAILED)
        h = self._elapsed_secs // 3600
        m = (self._elapsed_secs % 3600) // 60
        s = self._elapsed_secs % 60
        runtime = f"{h:02d}:{m:02d}:{s:02d}"

        lines = [
            "━" * 48,
            f"  PIPELINE SUMMARY",
            f"  Stages Executed : {len(results)}",
            f"  Stages Passed   : {passed}",
            f"  Stages Failed   : {failed}",
            f"  Total Runtime   : {runtime}",
            f"  Final Status    : {status.name}",
            "━" * 48,
        ]
        color = PIPELINE_STATUS_COLORS[status]
        for line in lines:
            self._console.append_system(line, color)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — APPLICATION ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Initialise and launch the AMC Dashboard Pipeline application."""
    app = QApplication(sys.argv)
    app.setApplicationName(WINDOW_TITLE)
    app.setOrganizationName("AMC Analytics")

    # Force dark palette application-wide
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window,          QColor(C_BG))
    palette.setColor(QPalette.ColorRole.WindowText,      QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.ColorRole.Base,            QColor(C_SURFACE))
    palette.setColor(QPalette.ColorRole.AlternateBase,   QColor(C_SURFACE_2))
    palette.setColor(QPalette.ColorRole.Text,            QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.ColorRole.Button,          QColor(C_SURFACE_2))
    palette.setColor(QPalette.ColorRole.ButtonText,      QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.ColorRole.Highlight,       QColor(C_BLUE))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#ffffff"))
    app.setPalette(palette)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()