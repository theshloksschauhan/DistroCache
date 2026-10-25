"""Put harness child processes in a Windows Job Object with kill-on-close.

pytest-timeout fires on a hung cluster and ends the test process (on Windows it calls
``os._exit``), which skips ``atexit`` handlers — so the harness's atexit cleanup cannot be
relied on to reap children on a timeout. A Job Object with ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``
closes that gap at the OS level: every child is assigned to one job held open by the pytest
process, and when that process dies for any reason the kernel closes the last job handle and
terminates everything still in the job. Children a child spawns inherit the job too, so a whole
cluster is reaped together.

Off Windows this module is a no-op: ``place_in_kill_on_close_job`` returns ``False`` and POSIX
process groups plus the harness's own teardown handle cleanup there.

Implemented with ``ctypes`` against ``kernel32`` so the test suite needs no pywin32 dependency.
Everything is best-effort: a failure to create or assign the job is logged and swallowed, never
raised, because losing the orphan-proofing must not turn into a spurious test failure.
"""

from __future__ import annotations

import ctypes
import logging
import subprocess
import sys

logger = logging.getLogger("distrocache.tests.winjob")

IS_WINDOWS = sys.platform == "win32"

# Winapi constants (winnt.h / jobapi2.h).
_JobObjectExtendedLimitInformation = 9
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

# One process-wide job. Held open for the life of the pytest process; its handle is never
# closed explicitly, so the job stays alive (and keeps its children) until the process exits,
# which is exactly the trigger for kill-on-close.
_job_handle: int | None = None
_job_unavailable = False


if IS_WINDOWS:
    from ctypes import wintypes

    _ULONG_PTR = ctypes.c_size_t

    class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", _ULONG_PTR),
            ("MaximumWorkingSetSize", _ULONG_PTR),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", _ULONG_PTR),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", _IO_COUNTERS),
            ("ProcessMemoryLimit", _ULONG_PTR),
            ("JobMemoryLimit", _ULONG_PTR),
            ("PeakProcessMemoryUsed", _ULONG_PTR),
            ("PeakJobMemoryUsed", _ULONG_PTR),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    _kernel32.SetInformationJobObject.restype = wintypes.BOOL
    _kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    _kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    # The production job is never closed explicitly (that is the whole point: the OS closes it
    # when the pytest process dies). CloseHandle is bound — with argtypes, so a 64-bit HANDLE is
    # not truncated to a C int — only so a test can prove kill-on-close with a throwaway job.
    _kernel32.CloseHandle.restype = wintypes.BOOL
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

    def _create_job() -> int | None:
        handle = _kernel32.CreateJobObjectW(None, None)
        if not handle:
            logger.warning("CreateJobObject failed: %s", ctypes.WinError(ctypes.get_last_error()))
            return None
        info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        ok = _kernel32.SetInformationJobObject(
            handle,
            _JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not ok:
            logger.warning(
                "SetInformationJobObject failed: %s", ctypes.WinError(ctypes.get_last_error())
            )
            return None
        return int(handle)

    def _assign(handle: int, popen: subprocess.Popen) -> bool:
        # Popen._handle is the OS process handle on Windows; good enough to assign to the job
        # and avoids opening a second handle. Nested jobs (Windows 8+) let this succeed even
        # when a CI runner already placed the pytest process in its own job.
        if not _kernel32.AssignProcessToJobObject(handle, int(popen._handle)):  # type: ignore[attr-defined]
            logger.warning(
                "AssignProcessToJobObject failed for pid %s: %s",
                popen.pid,
                ctypes.WinError(ctypes.get_last_error()),
            )
            return False
        return True


def place_in_kill_on_close_job(popen: subprocess.Popen) -> bool:
    """Assign ``popen`` to the shared kill-on-close job. Returns True if it is now in the job.

    No-op on non-Windows (returns False). Best-effort on Windows: any winapi failure is logged
    and False is returned, so the harness keeps working without the orphan-proofing.
    """
    global _job_handle, _job_unavailable
    if not IS_WINDOWS or _job_unavailable:
        return False
    if _job_handle is None:
        _job_handle = _create_job()
        if _job_handle is None:
            _job_unavailable = True  # don't retry creation for every child
            return False
    return _assign(_job_handle, popen)
