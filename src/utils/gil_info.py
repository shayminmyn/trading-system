"""
GIL status detection and runtime diagnostics.

Python 3.13+ introduced free-threaded builds (no GIL).
To run without GIL:
  - Install Python 3.13t or 3.14t (free-threaded build via pyenv or python.org)
  - OR set env var: PYTHON_GIL=0 (Python 3.13+)
  - Check: python -c "import sys; print(sys._is_gil_enabled())"

With no-GIL, this system uses threading.Thread and ThreadPoolExecutor
for true CPU-parallel execution across symbols and strategies.
"""

import sys
import os
import platform
import threading


def is_gil_enabled() -> bool:
    """Return True if the GIL is currently active."""
    if hasattr(sys, "_is_gil_enabled"):
        return sys._is_gil_enabled()
    return True  # Pre-3.13 always has GIL


def get_thread_count() -> int:
    return threading.active_count()


def print_runtime_info() -> None:
    gil_status = is_gil_enabled()
    gil_label = "ENABLED (standard build)" if gil_status else "DISABLED (free-threaded) ✓"

    lines = [
        "─" * 60,
        f"  Python     : {sys.version}",
        f"  Platform   : {platform.system()} {platform.release()} ({platform.machine()})",
        f"  GIL        : {gil_label}",
        f"  CPU cores  : {os.cpu_count()}",
    ]
    if not gil_status:
        lines.append("  Mode       : True parallel threads enabled")
    else:
        lines.append(
            "  Tip        : Use python3.13t/3.14t or PYTHON_GIL=0 for no-GIL mode"
        )
    lines.append("─" * 60)

    for line in lines:
        print(line)


def get_optimal_workers(default: int = 4) -> int:
    """
    Return optimal number of worker threads.
    With no-GIL: use CPU count for CPU-bound work.
    With GIL: threading still helps for I/O-bound tasks.
    """
    cpu_count = os.cpu_count() or default
    if not is_gil_enabled():
        return max(cpu_count, default)
    return default
