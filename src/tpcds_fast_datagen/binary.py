"""Manage the dsdgen binary — locate, download, validate, or compile from source.

Lookup order (``find_dsdgen``):

1. Explicit ``dsdgen_path`` argument (e.g. ``--dsdgen-path``).
2. ``DSDGEN_PATH`` environment variable.
3. Cached download at ``~/.cache/tpcds-fast-datagen/dsdgen``.
4. Well-known install locations (``~/tpcds-related/…``, ``/usr/local/bin``, ``/opt/tpcds-kit/…``).
5. System ``PATH``.
6. If ``DSDGEN_URL`` is set and nothing above matched, download the binary
   (and sibling ``tpcds.idx``) from that URL into the cache and use it.

This means notebook environments like Microsoft Fabric or Databricks can get a
working ``dsdgen`` by simply setting ``DSDGEN_URL`` to a blob/S3/HTTPS URL
pointing at a prebuilt binary — no ``git clone`` / ``make`` required.
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable


# Well-known locations to search for dsdgen
_SEARCH_PATHS = [
    os.environ.get("DSDGEN_PATH", ""),
    os.path.expanduser("~/tpcds-related/tpcds-kit/tools/dsdgen"),
    "/usr/local/bin/dsdgen",
    "/opt/tpcds-kit/tools/dsdgen",
]


def cache_dir() -> Path:
    """Return the per-user cache directory used for downloaded dsdgen binaries."""
    root = os.environ.get("TPCDS_CACHE_DIR")
    if root:
        p = Path(root)
    else:
        xdg = os.environ.get("XDG_CACHE_HOME")
        p = Path(xdg) / "tpcds-fast-datagen" if xdg else Path.home() / ".cache" / "tpcds-fast-datagen"
    p.mkdir(parents=True, exist_ok=True)
    return p


def platform_tag() -> str:
    """Return a short ``<os>-<arch>`` tag used to pick a prebuilt asset."""
    system = platform.system().lower()  # 'linux', 'darwin', 'windows'
    machine = platform.machine().lower()
    # Normalise arch names
    if machine in ("x86_64", "amd64"):
        arch = "x86_64"
    elif machine in ("aarch64", "arm64"):
        arch = "arm64"
    else:
        arch = machine
    if system == "darwin":
        system = "macos"
    return f"{system}-{arch}"


def _cached_dsdgen_path() -> Path:
    return cache_dir() / "dsdgen"


def _download(url: str, dest: Path) -> None:
    """Download ``url`` to ``dest`` atomically. Supports http(s) and file://.

    For ``abfs://`` / ``s3://`` / ``wasbs://`` we delegate to fsspec if
    available, because the notebook environments that need those schemes
    always ship fsspec.
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    tmp = dest.with_suffix(dest.suffix + ".part")
    if scheme in ("", "file"):
        src = Path(parsed.path if scheme == "file" else url)
        shutil.copyfile(src, tmp)
    elif scheme in ("http", "https"):
        with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:  # noqa: S310
            shutil.copyfileobj(resp, out)
    else:
        try:
            import fsspec  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                f"Cannot download {url!r}: scheme {scheme!r} needs `fsspec` "
                "(and the right backend: s3fs / adlfs / gcsfs). "
                "Install with `pip install fsspec adlfs s3fs gcsfs` or use an https:// URL."
            ) from e
        with fsspec.open(url, "rb") as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out)
    tmp.replace(dest)


def install_dsdgen_from_url(
    dsdgen_url: str,
    idx_url: str | None = None,
    dest_dir: Path | None = None,
) -> str:
    """Download a ``dsdgen`` binary (and matching ``tpcds.idx``) into the cache.

    Returns the absolute path to the installed ``dsdgen``.

    If ``idx_url`` is omitted we first try ``<dsdgen_url>.idx`` then
    replace the basename with ``tpcds.idx`` in the same directory.
    """
    dest_dir = dest_dir or cache_dir()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dsdgen_dest = dest_dir / "dsdgen"
    idx_dest = dest_dir / "tpcds.idx"

    print(f"[tpcds-fast-datagen] downloading dsdgen from {dsdgen_url} …", file=sys.stderr)
    _download(dsdgen_url, dsdgen_dest)
    dsdgen_dest.chmod(dsdgen_dest.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # Figure out where to get tpcds.idx from
    candidates: list[str] = []
    if idx_url:
        candidates.append(idx_url)
    else:
        # Same directory, file named tpcds.idx
        parsed = urllib.parse.urlparse(dsdgen_url)
        base = parsed.path.rsplit("/", 1)[0]
        idx_guess = parsed._replace(path=f"{base}/tpcds.idx").geturl()
        candidates.append(idx_guess)

    last_err: Exception | None = None
    for candidate in candidates:
        try:
            print(f"[tpcds-fast-datagen] downloading tpcds.idx from {candidate} …", file=sys.stderr)
            _download(candidate, idx_dest)
            last_err = None
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
    if last_err is not None:
        raise RuntimeError(
            f"Downloaded dsdgen but could not fetch tpcds.idx. "
            f"Set DSDGEN_IDX_URL explicitly. Last error: {last_err}"
        )

    return str(dsdgen_dest.resolve())


def _try_download_from_env() -> str | None:
    """If ``DSDGEN_URL`` is set, download it into the cache. Return path or None."""
    url = os.environ.get("DSDGEN_URL")
    if not url:
        return None
    idx_url = os.environ.get("DSDGEN_IDX_URL")
    return install_dsdgen_from_url(url, idx_url=idx_url)


def _iter_candidates(dsdgen_path: str | None) -> Iterable[str]:
    if dsdgen_path:
        yield dsdgen_path
    yield from _SEARCH_PATHS
    yield str(_cached_dsdgen_path())
    system_dsdgen = shutil.which("dsdgen")
    if system_dsdgen:
        yield system_dsdgen


def find_dsdgen(dsdgen_path: str | None = None) -> str:
    """Find the dsdgen binary. See module docstring for lookup order.

    Returns the absolute path to a working ``dsdgen`` binary.
    Raises :class:`FileNotFoundError` if not found anywhere and
    ``DSDGEN_URL`` is unset (or the download failed).
    """
    for path in _iter_candidates(dsdgen_path):
        if not path:
            continue
        path = os.path.expanduser(path)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return os.path.abspath(path)

    # Nothing local — try DSDGEN_URL as a last resort
    try:
        downloaded = _try_download_from_env()
    except Exception as e:  # noqa: BLE001
        raise FileNotFoundError(
            f"dsdgen binary not found locally and DSDGEN_URL download failed: {e}\n"
            "Options:\n"
            "  1. Run `tpcds-gen install-dsdgen` (downloads a prebuilt binary).\n"
            "  2. Set DSDGEN_PATH=/path/to/dsdgen.\n"
            "  3. Set DSDGEN_URL=<https/abfs/s3/wasbs/gs URL of a prebuilt dsdgen>.\n"
            "  4. Build from source: "
            "git clone https://github.com/databricks/tpcds-kit && cd tpcds-kit/tools && make OS=LINUX"
        ) from e
    if downloaded:
        return downloaded

    raise FileNotFoundError(
        "dsdgen binary not found. Options:\n"
        "  1. Run `tpcds-gen install-dsdgen` (downloads a prebuilt binary for your platform).\n"
        "  2. Set DSDGEN_PATH=/path/to/dsdgen.\n"
        "  3. Set DSDGEN_URL=<https/abfs/s3/wasbs/gs URL of a prebuilt dsdgen>.\n"
        "  4. Build from source: "
        "git clone https://github.com/databricks/tpcds-kit && cd tpcds-kit/tools && make OS=LINUX"
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
    """Validate dsdgen binary and return ``(dsdgen_path, dsdgen_dir)``.

    Also verifies ``tpcds.idx`` exists alongside the binary.
    """
    dsdgen = find_dsdgen(dsdgen_path)
    find_tpcds_idx(dsdgen)
    return dsdgen, os.path.dirname(dsdgen)


def sha256sum(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
