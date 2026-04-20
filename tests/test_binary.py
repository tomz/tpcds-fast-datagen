"""Regression tests for ``tpcds_fast_datagen.binary``.

Covers:

* ``platform_tag()`` normalisation (linux-x86_64, macos-arm64, …).
* ``cache_dir()`` respects ``TPCDS_CACHE_DIR`` / ``XDG_CACHE_HOME``.
* ``install_dsdgen_from_url()`` with ``file://`` URLs — the bootstrap path
  we exercise in the Fabric/Databricks notebook case (via ``DSDGEN_URL``).
* ``find_dsdgen()`` consults the cache after ``DSDGEN_PATH`` and before
  falling through to ``DSDGEN_URL`` download.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from tpcds_fast_datagen import binary


def test_platform_tag_is_normalised():
    tag = binary.platform_tag()
    # Always <system>-<arch>
    assert "-" in tag
    system, arch = tag.split("-", 1)
    assert system in {"linux", "macos", "windows"}
    assert arch in {"x86_64", "arm64"} or arch.isalnum()


def test_cache_dir_respects_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TPCDS_CACHE_DIR", str(tmp_path / "custom_cache"))
    cache = binary.cache_dir()
    assert cache == tmp_path / "custom_cache"
    assert cache.is_dir()


def test_cache_dir_falls_back_to_xdg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("TPCDS_CACHE_DIR", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    cache = binary.cache_dir()
    assert cache == tmp_path / "xdg" / "tpcds-fast-datagen"


def test_install_dsdgen_from_url_file_scheme(tmp_path: Path):
    # Pretend we have a prebuilt dsdgen + tpcds.idx on disk (the "Azure
    # blob / S3 asset" equivalent in a test). Install them into a scratch
    # cache_dir, verify the binary ends up executable.
    src_bin = tmp_path / "src" / "dsdgen"
    src_idx = tmp_path / "src" / "tpcds.idx"
    src_bin.parent.mkdir()
    src_bin.write_bytes(b"\x7fELF fake\n")
    src_idx.write_bytes(b"idx data\n")

    dest = tmp_path / "cache"
    result = binary.install_dsdgen_from_url(
        f"file://{src_bin}",
        idx_url=f"file://{src_idx}",
        dest_dir=dest,
    )
    assert result == str((dest / "dsdgen").resolve())
    assert (dest / "dsdgen").is_file()
    assert (dest / "tpcds.idx").is_file()
    # Binary must be executable by owner.
    mode = (dest / "dsdgen").stat().st_mode
    assert mode & stat.S_IXUSR


def test_install_dsdgen_idx_url_fallback(tmp_path: Path):
    # When idx_url is not given, we try <dirname(dsdgen_url)>/tpcds.idx.
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "dsdgen").write_bytes(b"bin")
    (src_dir / "tpcds.idx").write_bytes(b"idx")

    dest = tmp_path / "cache"
    result = binary.install_dsdgen_from_url(
        f"file://{src_dir}/dsdgen",
        dest_dir=dest,
    )
    assert Path(result).name == "dsdgen"
    assert (dest / "tpcds.idx").read_bytes() == b"idx"


def test_find_dsdgen_prefers_cache_over_system(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Put a fake dsdgen in the cache dir and make sure find_dsdgen picks it
    # up without consulting DSDGEN_URL. Keep HOME / PATH / well-known paths
    # out of it, because the dev box we run on might already have dsdgen
    # at one of the hardcoded search locations.
    monkeypatch.setenv("TPCDS_CACHE_DIR", str(tmp_path))
    monkeypatch.delenv("DSDGEN_PATH", raising=False)
    monkeypatch.delenv("DSDGEN_URL", raising=False)
    monkeypatch.setattr(binary, "_SEARCH_PATHS", [])
    monkeypatch.setenv("PATH", str(tmp_path / "nope"))

    cached_bin = tmp_path / "dsdgen"
    cached_bin.write_bytes(b"fake")
    cached_bin.chmod(cached_bin.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    found = binary.find_dsdgen(None)
    assert found == str(cached_bin.resolve())


def test_find_dsdgen_raises_with_helpful_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("TPCDS_CACHE_DIR", str(tmp_path))
    monkeypatch.delenv("DSDGEN_PATH", raising=False)
    monkeypatch.delenv("DSDGEN_URL", raising=False)
    monkeypatch.setattr(binary, "_SEARCH_PATHS", [])
    monkeypatch.setenv("PATH", str(tmp_path / "nope"))

    with pytest.raises(FileNotFoundError) as exc:
        binary.find_dsdgen(None)

    msg = str(exc.value)
    # The error should mention the recommended remedy first.
    assert "install-dsdgen" in msg
    assert "DSDGEN_URL" in msg


def test_dsdgen_url_triggers_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # With DSDGEN_URL set and nothing on disk, find_dsdgen should download
    # into the cache and return the path.
    monkeypatch.setenv("TPCDS_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.delenv("DSDGEN_PATH", raising=False)
    monkeypatch.setattr(binary, "_SEARCH_PATHS", [])
    monkeypatch.setenv("PATH", str(tmp_path / "nope"))

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "dsdgen").write_bytes(b"bin")
    (src_dir / "tpcds.idx").write_bytes(b"idx")
    monkeypatch.setenv("DSDGEN_URL", f"file://{src_dir}/dsdgen")

    found = binary.find_dsdgen(None)
    assert Path(found).name == "dsdgen"
    assert Path(found).parent == (tmp_path / "cache").resolve()
