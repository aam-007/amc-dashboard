"""
1_downloader.py
=============
Stage 2 — AMFI TER Excel downloader.

Dropdown cascade (MUI Autocomplete):
    Financial Year  → always present at index 0
    Month           → always present at index 1
    Fund Type       → always present at index 2
    Category        → always present at index 3
    Sub Category    → appears ONLY after Fund Type is selected (index 4)
    Mutual Fund     → last; index 4 if Sub Category absent, 5 if present

Strategy: after each pick, locate remaining dropdowns by their visible
placeholder label rather than by fixed index, so conditional fields
appearing mid-flow don't break the sequence.

Now runs headless with verbose terminal output.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from playwright.sync_api import Page, sync_playwright

# ── Config ────────────────────────────────────────────────────────────────────

URL     = "https://www.amfiindia.com/ter-of-mf-schemes"
RAW_DIR = Path("data/raw/expense_ratio")


class DownloadResult(NamedTuple):
    raw_path:    Path
    period_text: str   # e.g. "June-2026"
    fy_text:     str   # e.g. "2026-2027"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pick_first_in_dropdown(page: Page, dropdown_idx: int, wait_ms: int = 2_000, label_hint: str = "") -> str:
    """
    Open the nth MuiAutocomplete dropdown and click the topmost non-empty option.
    Returns the option's text label.
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
    return page.locator(".MuiAutocomplete-root").count()


def _wait_for_dropdown_count(page: Page, expected: int, timeout_ms: int = 10_000) -> None:
    """Wait until the page has at least `expected` MuiAutocomplete dropdowns."""
    print(f"    waiting for at least {expected} dropdowns to be present …")
    page.wait_for_function(
        f"() => document.querySelectorAll('.MuiAutocomplete-root').length >= {expected}",
        timeout=timeout_ms,
    )
    print(f"    found {_dropdown_count(page)} dropdowns.")


def _wait_for_go_enabled(page: Page, timeout_ms: int = 15_000) -> None:
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


# ── Main downloader ───────────────────────────────────────────────────────────

def download_ter_excel() -> DownloadResult:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[downloader] Download directory: {RAW_DIR.resolve()}")

    with sync_playwright() as pw:
        print("[downloader] Launching Chromium in headless mode …")
        browser = pw.chromium.launch(
            headless=True,                # ← now headless
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
            raise RuntimeError(f"Expected at least 5 MUI dropdowns, found {initial_count}.")

        print("[downloader] Starting filter selection …")

        # [0] Financial Year
        fy_text = _pick_first_in_dropdown(page, 0, wait_ms=3_000, label_hint="Financial Year")

        # [1] Month
        month_text = _pick_first_in_dropdown(page, 1, wait_ms=3_000, label_hint="Month")

        # [2] Fund Type — Sub Category may appear after this
        _pick_first_in_dropdown(page, 2, wait_ms=1_000, label_hint="Fund Type")

        # Wait to see if Sub Category materialises (count goes 5 → 6)
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
            # [4] Sub Category
            _pick_first_in_dropdown(page, 4, wait_ms=3_000, label_hint="Sub Category")
            # [5] Mutual Fund
            _pick_first_in_dropdown(page, 5, wait_ms=2_000, label_hint="Mutual Fund")
        else:
            # Sub Category never appeared — Mutual Fund is at index 4
            _pick_first_in_dropdown(page, 4, wait_ms=2_000, label_hint="Mutual Fund")

        # Wait for GO to become enabled
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
        go_btn = page.locator("button").filter(has_text=re.compile(r"^go$", re.IGNORECASE))
        go_btn.click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(5_000)
        print("[downloader] GO clicked, waiting for results …")

        # Download Excel
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


if __name__ == "__main__":
    try:
        result = download_ter_excel()
        print(f"\n Done. FY={result.fy_text}  Month={result.period_text}")
        print(f" File: {result.raw_path}")
    except Exception as exc:
        print(f" ERROR: {exc}", file=sys.stderr)
        sys.exit(1)