"""Configuration for TPC-DS data generation."""

from dataclasses import dataclass, field
import os


@dataclass
class GenConfig:
    """Configuration for a TPC-DS data generation run."""
    scale_factor: int = 1
    parallel: int = 0  # 0 = auto (use cpu_count)
    output_dir: str = "./tpcds_data"
    row_group_size_mb: int = 128
    tables: list[str] | None = None  # None = all tables
    dsdgen_path: str | None = None  # None = auto-detect
    overwrite: bool = False
    compression: str = "snappy"  # parquet compression
    engine: str = "auto"  # "auto", "duckdb", "dsdgen", or "spark"
    auto_threshold: int = 50  # SF strictly above this picks dsdgen when engine=='auto'

    def __post_init__(self):
        if self.parallel <= 0:
            self.parallel = os.cpu_count() or 4
        if self.scale_factor < 1:
            raise ValueError(f"Scale factor must be >= 1, got {self.scale_factor}")
        if self.engine not in ("auto", "duckdb", "dsdgen", "spark"):
            raise ValueError(
                f"Engine must be 'auto', 'duckdb', 'dsdgen', or 'spark', got {self.engine!r}"
            )
        # Resolve 'auto' to a concrete engine based on scale factor.
        # DuckDB is fastest for small SF (matches its native dsdgen speed) but
        # holds all 25 tables in memory before exporting, so it OOMs for large SF.
        # The dsdgen multiprocess engine streams .dat -> Parquet with constant
        # memory per worker (~200 MB) and is bounded only by local disk.
        if self.engine == "auto":
            self.engine = "duckdb" if self.scale_factor <= self.auto_threshold else "dsdgen"
