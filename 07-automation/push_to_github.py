"""
push_to_github.py
───────────────────────────────────────────────────────────────────────────────
AMC Dashboard Pipeline
Stage 12 — GitHub Sync

Author  : aditya.mishra10@nmims.in
Requires: Git (with an authenticated SSH remote), Python 3.12+
───────────────────────────────────────────────────────────────────────────────

Synchronises the local AMC Dashboard repository with its GitHub remote:

    1. Verifies Git is installed and the script is running inside a Git
       repository.
    2. Checks for uncommitted changes. If none exist, exits successfully
       without doing anything further.
    3. Stages all changes (`git add .`).
    4. Generates a timestamped commit message and commits the staged
       changes (a clean working tree after staging is treated as success,
       not failure).
    5. Rebases onto the latest `origin/main` (`git pull --rebase`).
    6. Pushes the result to `origin/main` (`git push`).
    7. Prints a summary of the sync.

This script is designed to run as the final stage of
`07-automation/run_pipeline.py`, invoked as:

    subprocess.Popen([sys.executable, script], cwd=PROJECT_ROOT)

It returns proper exit codes for the orchestrator:

    sys.exit(0)  -> stage succeeded
    sys.exit(1)  -> stage failed

Safety
------
This script NEVER runs any of the following:
    * git push --force
    * git reset --hard
    * git clean -fd
    * any command that deletes files or modifies branches

It assumes the remote `origin` and branch `main` already exist and are
correctly configured with an authenticated SSH remote, e.g.:

    git@github.com:aam-007/amc-dashboard.git
───────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# ── Project root ────────────────────────────────────────────────────────────
# This file lives in `07-automation/`; the Git repository root (and the
# directory `git` commands must run from) is one level up — sibling to
# 01-data-ingestion, 02-data-warehouse, etc.
THIS_FILE: Path = Path(__file__).resolve()
PROJECT_ROOT: Path = THIS_FILE.parent.parent

# ── Git configuration ──────────────────────────────────────────────────────
# Per project requirements, `origin` and `main` are assumed to already exist
# and to be correctly configured (e.g. with an SSH remote such as
# git@github.com:aam-007/amc-dashboard.git). This script never creates,
# renames, or reconfigures remotes or branches.
REMOTE_NAME: str = "origin"
BRANCH_NAME: str = "main"

# Expected prefix for an SSH-based GitHub remote. Used only to print an
# informational warning — never to modify the remote configuration.
SSH_REMOTE_PREFIX: str = "git@github.com:"

# ── Output formatting ─────────────────────────────────────────────────────
SEPARATOR: str = "=" * 49
TOTAL_STEPS: int = 8


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CommandResult:
    """Result of a single executed shell command."""

    command: list[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        """True if the command exited with status code 0."""
        return self.returncode == 0


@dataclass
class GitSyncConfig:
    """Static configuration for a single sync run."""

    project_root: Path
    repo_name: str
    remote: str = REMOTE_NAME
    branch: str = BRANCH_NAME


# ══════════════════════════════════════════════════════════════════════════════
# LOW-LEVEL COMMAND EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def run_command(command: Sequence[str], cwd: Path, timeout: Optional[int] = 300) -> CommandResult:
    """
    Execute `command` inside `cwd` and capture its output.

    This is the single choke point through which every Git command in this
    script is executed, so that error handling, timeouts, and environment
    configuration stay consistent and non-duplicated.

    - `capture_output=True, text=True, check=False`: never raises on a
      non-zero exit code — callers inspect `CommandResult.returncode` /
      `CommandResult.ok` instead.
    - `GIT_TERMINAL_PROMPT=0`: ensures Git never blocks waiting for
      interactive username/password input. Any authentication failure
      (e.g. SSH key not loaded) surfaces immediately as a non-zero exit
      code with a message in stderr, instead of hanging the pipeline.
    - A wall-clock `timeout` guards against a hung network call (pull/push)
      stalling the orchestrator indefinitely.
    """
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

    try:
        completed = subprocess.run(
            list(command),
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=timeout,
        )
        return CommandResult(
            command=list(command),
            returncode=completed.returncode,
            stdout=completed.stdout.strip(),
            stderr=completed.stderr.strip(),
        )
    except FileNotFoundError as exc:
        # e.g. `git` is not on PATH at all.
        return CommandResult(command=list(command), returncode=127, stdout="", stderr=str(exc))
    except subprocess.TimeoutExpired:
        return CommandResult(
            command=list(command),
            returncode=124,
            stdout="",
            stderr=f"Command timed out after {timeout} seconds.",
        )


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def print_step(step: int, total: int, message: str) -> None:
    """Print a standard `[n/total] ...` progress line."""
    print(f"[{step}/{total}] {message}")


def print_error(message: str, result: CommandResult) -> None:
    """
    Print a detailed failure report for a failed command.

    Always includes the command that was executed, its exit code, and any
    stdout/stderr produced — so a failure is fully diagnosable from the
    pipeline's captured output alone.
    """
    print()
    print(f"ERROR: {message}")
    print(f"  Command   : {' '.join(result.command)}")
    print(f"  Exit code : {result.returncode}")
    if result.stdout:
        print("  Stdout    :")
        for line in result.stdout.splitlines():
            print(f"    {line}")
    if result.stderr:
        print("  Stderr    :")
        for line in result.stderr.splitlines():
            print(f"    {line}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — VERIFY GIT IS INSTALLED
# ══════════════════════════════════════════════════════════════════════════════

def check_git_installed() -> None:
    """
    Verify that `git` is installed and reachable on PATH.

    Runs `git --version`. Exits 1 with a clear error if Git cannot be
    invoked at all.
    """
    result = run_command(["git", "--version"], cwd=PROJECT_ROOT)
    if not result.ok:
        print_error("Git is not installed or is not available on PATH.", result)
        sys.exit(1)
    print(f"      {result.stdout}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — VERIFY WE ARE INSIDE A GIT REPOSITORY
# ══════════════════════════════════════════════════════════════════════════════

def check_git_repository(project_root: Path) -> None:
    """
    Verify `project_root` is inside a Git working tree.

    Runs `git rev-parse --is-inside-work-tree`. Exits 1 with a clear error
    if `project_root` is not part of a Git repository.
    """
    result = run_command(["git", "rev-parse", "--is-inside-work-tree"], cwd=project_root)
    if not result.ok or result.stdout != "true":
        print_error(f"{project_root} is not inside a Git repository.", result)
        sys.exit(1)
    print(f"      Repository root : {project_root}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — CHECK FOR UNCOMMITTED CHANGES
# ══════════════════════════════════════════════════════════════════════════════

def check_changes_present(project_root: Path) -> Tuple[bool, str]:
    """
    Return `(has_changes, porcelain_output)`.

    Runs `git status --porcelain`. Any output means the working tree has
    uncommitted changes (modified, staged, deleted, or untracked files).
    Exits 1 if the repository status cannot be determined at all.
    """
    result = run_command(["git", "status", "--porcelain"], cwd=project_root)
    if not result.ok:
        print_error("Unable to determine repository status.", result)
        sys.exit(1)
    return bool(result.stdout), result.stdout


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — STAGE ALL CHANGES
# ══════════════════════════════════════════════════════════════════════════════

def stage_changes(project_root: Path) -> None:
    """Run `git add .` to stage every change. Exits 1 on failure."""
    result = run_command(["git", "add", "."], cwd=project_root)
    if not result.ok:
        print_error("Failed to stage changes.", result)
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — GENERATE COMMIT MESSAGE
# ══════════════════════════════════════════════════════════════════════════════

def generate_commit_message() -> str:
    """
    Return an automatically generated commit message.

    Format: "AMC Dashboard Update | YYYY-MM-DD HH:MM"
    Example: "AMC Dashboard Update | 2026-06-15 13:45"
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"AMC Dashboard Update | {timestamp}"


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — CREATE COMMIT
# ══════════════════════════════════════════════════════════════════════════════

# Git's various phrasings for "there is nothing staged to commit". Matched
# case-insensitively against combined stdout+stderr.
_NOTHING_TO_COMMIT_MARKERS: Tuple[str, ...] = (
    "nothing to commit",
    "nothing added to commit",
    "no changes added to commit",
)


def create_commit(project_root: Path, message: str) -> bool:
    """
    Run `git commit -m <message>`.

    Returns
    -------
    True  if a new commit was created.
    False if there was nothing to commit (treated as success, not failure,
          per the pipeline's requirements).

    Exits 1 for any other commit failure.
    """
    result = run_command(["git", "commit", "-m", message], cwd=project_root)
    if result.ok:
        print(f"      Commit created: {message!r}")
        return True

    combined = f"{result.stdout}\n{result.stderr}".lower()
    if any(marker in combined for marker in _NOTHING_TO_COMMIT_MARKERS):
        print("      Nothing to commit — working tree already clean after staging.")
        return False

    print_error("Failed to create commit.", result)
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — SYNCHRONISE WITH REMOTE (REBASE PULL)
# ══════════════════════════════════════════════════════════════════════════════

# Phrasings Git uses when a rebase hits a conflict. Matched case-insensitively
# against combined stdout+stderr.
_CONFLICT_MARKERS: Tuple[str, ...] = (
    "conflict",
    "could not apply",
    "unmerged files",
    "needs merge",
)


def pull_latest(project_root: Path, remote: str, branch: str) -> None:
    """
    Run `git pull --rebase <remote> <branch>`.

    On success, the local branch is rebased onto the latest remote history.

    On a rebase conflict, prints a detailed error explaining how to resolve
    it manually and exits 1. The script does NOT run `git rebase --abort`
    or any other repository-mutating recovery command — the in-progress
    rebase is left exactly as Git left it, for the developer to resolve.

    Any other pull failure (network error, auth failure, etc.) is also
    reported in detail and exits 1.
    """
    result = run_command(["git", "pull", "--rebase", remote, branch], cwd=project_root)
    if result.ok:
        if result.stdout:
            print(f"      {result.stdout}")
        return

    combined = f"{result.stdout}\n{result.stderr}".lower()
    if any(marker in combined for marker in _CONFLICT_MARKERS):
        print_error(
            f"Rebase conflict while pulling '{remote}/{branch}'. "
            "Resolve the conflicts manually, then run "
            "'git rebase --continue' (or 'git rebase --abort' to cancel) "
            "before re-running this script.",
            result,
        )
    else:
        print_error(f"Failed to pull latest changes from '{remote}/{branch}'.", result)
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — PUSH TO GITHUB
# ══════════════════════════════════════════════════════════════════════════════

def push_changes(project_root: Path, remote: str, branch: str) -> None:
    """
    Run `git push <remote> <branch>`.

    Never uses `--force`. On failure, prints a detailed error (including
    stderr, which typically explains the cause — e.g. a non-fast-forward
    rejection or an SSH authentication failure) and exits 1.
    """
    result = run_command(["git", "push", remote, branch], cwd=project_root)
    if not result.ok:
        print_error(f"Failed to push changes to '{remote}/{branch}'.", result)
        sys.exit(1)
    if result.stdout:
        print(f"      {result.stdout}")
    if result.stderr:
        # `git push` writes its normal progress/summary output to stderr
        # even on success.
        for line in result.stderr.splitlines():
            print(f"      {line}")


# ══════════════════════════════════════════════════════════════════════════════
# REPOSITORY INFO HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_current_branch(project_root: Path) -> str:
    """Return the current branch name, or 'unknown' if it cannot be determined."""
    result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=project_root)
    return result.stdout if result.ok else "unknown"


def get_current_commit(project_root: Path) -> str:
    """Return the short hash of HEAD, or 'unknown' if it cannot be determined."""
    result = run_command(["git", "rev-parse", "--short", "HEAD"], cwd=project_root)
    return result.stdout if result.ok else "unknown"


def get_remote_url(project_root: Path, remote: str) -> str:
    """Return the URL configured for `remote`, or 'unknown' if it cannot be determined."""
    result = run_command(["git", "remote", "get-url", remote], cwd=project_root)
    return result.stdout if result.ok else "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — PRINT SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(config: GitSyncConfig, status: str, branch: str, commit: str, remote_url: str) -> None:
    """Print the final sync summary block shown after a successful push."""
    print()
    print(SEPARATOR)
    print("GitHub Sync Complete")
    print(SEPARATOR)
    print()
    print(f"Repository : {config.repo_name}")
    print(f"Remote     : {remote_url}")
    print(f"Branch     : {branch}")
    print(f"Commit     : {commit}")
    print(f"Status     : {status}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    """
    Run the full GitHub sync sequence.

    Returns 0 on success (including the "nothing to do" case) and 1 on any
    failure. The caller (`sys.exit(main())`) translates this directly into
    the process exit code the pipeline orchestrator expects.
    """
    config = GitSyncConfig(project_root=PROJECT_ROOT, repo_name=PROJECT_ROOT.name)

    print(SEPARATOR)
    print("AMC Dashboard — GitHub Sync")
    print(SEPARATOR)
    print(f"Project root : {config.project_root}")
    print()

    # ── [1/8] Git installed? ────────────────────────────────────────────
    print_step(1, TOTAL_STEPS, "Checking Git installation...")
    check_git_installed()

    # ── [2/8] Inside a Git repository? ──────────────────────────────────
    print_step(2, TOTAL_STEPS, "Checking repository...")
    check_git_repository(config.project_root)

    remote_url = get_remote_url(config.project_root, config.remote)
    print(f"      Remote '{config.remote}' : {remote_url}")
    if not remote_url.startswith(SSH_REMOTE_PREFIX):
        print(
            "      Warning: remote does not look like an SSH URL "
            f"(expected it to start with '{SSH_REMOTE_PREFIX}'). "
            "If the push step fails with an authentication error, "
            "configure the remote to use SSH, e.g.:"
        )
        print(
            f"        git remote set-url {config.remote} "
            "git@github.com:aam-007/amc-dashboard.git"
        )

    # ── [3/8] Anything to sync? ─────────────────────────────────────────
    print_step(3, TOTAL_STEPS, "Checking for changes...")
    has_changes, status_output = check_changes_present(config.project_root)
    if not has_changes:
        print()
        print("No changes detected.")
        print("Repository already up to date.")
        return 0

    changed_files = len(status_output.splitlines())
    print(f"      {changed_files} changed file(s) detected.")

    # ── [4/8] Stage everything ──────────────────────────────────────────
    print_step(4, TOTAL_STEPS, "Staging changes...")
    stage_changes(config.project_root)

    # ── [5/8] Build the commit message ──────────────────────────────────
    print_step(5, TOTAL_STEPS, "Generating commit message...")
    commit_message = generate_commit_message()
    print(f"      Message: {commit_message}")

    # ── [6/8] Commit ─────────────────────────────────────────────────────
    print_step(6, TOTAL_STEPS, "Creating commit...")
    create_commit(config.project_root, commit_message)

    # ── [7/8] Rebase onto the latest remote history ─────────────────────
    print_step(7, TOTAL_STEPS, f"Pulling latest changes from {config.remote}/{config.branch}...")
    pull_latest(config.project_root, config.remote, config.branch)

    # ── [8/8] Push ───────────────────────────────────────────────────────
    print_step(8, TOTAL_STEPS, f"Pushing changes to {config.remote}/{config.branch}...")
    push_changes(config.project_root, config.remote, config.branch)

    # ── Summary ──────────────────────────────────────────────────────────
    branch = get_current_branch(config.project_root)
    commit = get_current_commit(config.project_root)
    print_summary(config, status="SUCCESS", branch=branch, commit=commit, remote_url=remote_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())