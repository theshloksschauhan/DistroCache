"""Tests for the harness's Windows kill-on-close Job Object helper.

These are plain unit tests (no cluster), so they run in the default `pytest` selection. Each
test asserts the behaviour that applies to the platform it runs on — there is no skip marker:
off Windows the helper is a documented no-op, and that no-op is itself asserted. The genuine
kill-on-close proof runs on Windows (the unit CI job is Linux, so it is exercised locally and in
the Windows integration job, where every harness child is placed in the job).
"""

from __future__ import annotations

import subprocess
import sys
import time

from tests.integration import winjob

_SLEEP = [sys.executable, "-c", "import time; time.sleep(60)"]


def _reap(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.kill()
    proc.wait(10)


def test_place_in_kill_on_close_job_contract() -> None:
    """Assigning a running child to the job never kills it, and reports placement by platform."""
    proc = subprocess.Popen(_SLEEP)
    try:
        placed = winjob.place_in_kill_on_close_job(proc)
        assert proc.poll() is None, "assigning a live child to a job must not kill it"
        assert placed is winjob.IS_WINDOWS
    finally:
        _reap(proc)


def test_kill_on_close_reaps_child() -> None:
    """On Windows, closing a kill-on-close job terminates its children; off Windows, no-op."""
    if not winjob.IS_WINDOWS:
        # No Job Object off Windows: the public helper must be an inert no-op and leave the
        # child running. (POSIX cleanup is the harness's process handling, not this module.)
        proc = subprocess.Popen(_SLEEP)
        try:
            assert winjob.place_in_kill_on_close_job(proc) is False
            time.sleep(0.2)
            assert proc.poll() is None
        finally:
            _reap(proc)
        return

    # Windows: a child assigned to a fresh kill-on-close job must die when the only handle to
    # the job is closed. This is exactly what protects the harness when pytest-timeout exits
    # without running atexit handlers.
    handle = winjob._create_job()
    assert handle is not None, "could not create a Job Object"
    proc = subprocess.Popen(_SLEEP)
    try:
        assert winjob._assign(handle, proc) is True
        assert proc.poll() is None, "child should be alive before the job handle is closed"
        assert winjob._kernel32.CloseHandle(handle)  # drop the last handle -> kill-on-close fires
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.05)
        assert proc.poll() is not None, "child survived the job closing: it was orphaned"
    finally:
        _reap(proc)
