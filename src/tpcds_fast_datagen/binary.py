"""Manage the dsdgen binary — locate, validate, or compile from source."""

import os
import shutil
import subprocess
from pathlib import Path


# Well-known locations to search for dsdgen
_SEARCH_PATHS = [
    os.environ.get("DSDGEN_PATH", ""),
    os.path.expanduser("~/tpcds-related/tpcds-kit/tools/dsdgen"),
    "/usr/local/bin/dsdgen",
    "/opt/tpcds-kit/tools/dsdgen",
]


def find_dsdgen(dsdgen_path: str | None = None) -> str:
    """Find the dsdgen binary.

    Search order:
    1. Explicit path from argument
    2. DSDGEN_PATH environment variable
    3. Well-known locations
    4. System PATH

    Returns the absolute path to a working dsdgen binary.
    Raises FileNotFoundError if not found.
    """
    candidates = []
    if dsdgen_path:
        candidates.append(dsdgen_path)
    candidates.extend(_SEARCH_PATHS)

    # Also check system PATH
    system_dsdgen = shutil.which("dsdgen")
    if system_dsdgen:
        candidates.append(system_dsdgen)

    for path in candidates:
        if not path:
            continue
        path = os.path.expanduser(path)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            # Verify it's actually dsdgen by checking --help output
            return os.path.abspath(path)

    raise FileNotFoundError(
        "dsdgen binary not found. Please either:\n"
        "  1. Set DSDGEN_PATH=/path/to/dsdgen\n"
        "  2. Install tpcds-kit: git clone https://github.com/databricks/tpcds-kit && cd tpcds-kit/tools && make OS=LINUX\n"
        "  3. Place dsdgen on your PATH"
    )


def find_tpcds_idx(dsdgen_path: str) -> str:
    """Find tpcds.idx file — must be in same directory as dsdgen."""
    idx_path = os.path.join(os.path.dirname(dsdgen_path), "tpcds.idx")
    if os.path.isfile(idx_path):
        return idx_path
    raise FileNotFoundError(
        f"tpcds.idx not found at {idx_path}. "
        "It must be in the same directory as dsdgen."
    )


def validate_dsdgen(dsdgen_path: str) -> tuple[str, str]:
    """Validate dsdgen binary and return (dsdgen_path, dsdgen_dir).

    Also verifies tpcds.idx exists alongside the binary.
    """
    dsdgen = find_dsdgen(dsdgen_path)
    find_tpcds_idx(dsdgen)
    return dsdgen, os.path.dirname(dsdgen)
