"""Unit tests for GenConfig validation and auto-engine resolution."""
import pytest

from tpcds_fast_datagen.config import GenConfig


def test_default_engine_auto_resolves_to_duckdb_below_threshold():
    cfg = GenConfig(scale_factor=10)
    assert cfg.engine == "duckdb"


def test_default_engine_auto_resolves_to_dsdgen_above_threshold():
    cfg = GenConfig(scale_factor=100)
    assert cfg.engine == "dsdgen"


def test_threshold_boundary_inclusive_for_duckdb():
    """SF == threshold goes to duckdb (>= behaviour)."""
    cfg = GenConfig(scale_factor=50, auto_threshold=50)
    assert cfg.engine == "duckdb"


def test_threshold_strict_above_for_dsdgen():
    cfg = GenConfig(scale_factor=51, auto_threshold=50)
    assert cfg.engine == "dsdgen"


def test_custom_threshold_low_pushes_to_dsdgen_early():
    cfg = GenConfig(scale_factor=20, auto_threshold=10)
    assert cfg.engine == "dsdgen"


def test_explicit_duckdb_not_overridden_at_large_sf():
    """User opt-in to duckdb at large SF is honored (their footgun)."""
    cfg = GenConfig(scale_factor=10000, engine="duckdb")
    assert cfg.engine == "duckdb"


def test_explicit_dsdgen_at_small_sf():
    cfg = GenConfig(scale_factor=1, engine="dsdgen")
    assert cfg.engine == "dsdgen"


def test_invalid_engine_rejected():
    with pytest.raises(ValueError, match="Engine must be"):
        GenConfig(scale_factor=1, engine="spark")


def test_invalid_scale_factor_rejected():
    with pytest.raises(ValueError, match="Scale factor"):
        GenConfig(scale_factor=0)


def test_parallel_zero_resolves_to_cpu_count():
    cfg = GenConfig(scale_factor=1, parallel=0)
    assert cfg.parallel >= 1


def test_parallel_explicit_preserved():
    cfg = GenConfig(scale_factor=1, parallel=8)
    assert cfg.parallel == 8
