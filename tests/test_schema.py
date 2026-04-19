"""Schema invariant tests."""
import pyarrow as pa

from tpcds_fast_datagen.schema import (
    ALL_TABLES,
    COMPANION_PAIRS,
    COMPANION_PRIMARY,
    FACT_TABLES,
)


def test_all_tables_have_unique_column_names():
    for name, tdef in ALL_TABLES.items():
        names = [c[0] for c in tdef.columns]
        assert len(names) == len(set(names)), f"{name} has duplicate column names"


def test_all_tables_schema_round_trips():
    """Every TableDef must produce a valid pyarrow.Schema."""
    for name, tdef in ALL_TABLES.items():
        schema = tdef.schema
        assert isinstance(schema, pa.Schema)
        assert len(schema) == len(tdef.columns)


def test_companion_pairs_are_consistent():
    """COMPANION_PAIRS must be symmetric and COMPANION_PRIMARY must point at a member."""
    for a, b in COMPANION_PAIRS.items():
        assert COMPANION_PAIRS.get(b) == a, f"{a} <-> {b} pairing not symmetric"
    for child, primary in COMPANION_PRIMARY.items():
        # primary is itself in COMPANION_PAIRS (paired with the child or vice versa)
        assert primary in COMPANION_PAIRS or primary == child, (
            f"{child} maps to primary {primary} which is not in COMPANION_PAIRS"
        )
        assert primary in ALL_TABLES, f"primary {primary} is not a known table"


def test_fact_tables_exist():
    for t in FACT_TABLES:
        assert t in ALL_TABLES, f"FACT_TABLES references unknown table {t}"


def test_row_counts_monotonic_in_sf():
    """Row counts must be non-decreasing as SF grows."""
    for name, tdef in ALL_TABLES.items():
        r1 = tdef.row_count(1)
        r10 = tdef.row_count(10)
        r100 = tdef.row_count(100)
        assert r1 <= r10 <= r100, f"{name} row_count not monotonic: {r1}, {r10}, {r100}"


def test_expected_25_tables():
    """TPC-DS spec defines 25 tables (24 + dbgen_version metadata)."""
    assert len(ALL_TABLES) == 25
