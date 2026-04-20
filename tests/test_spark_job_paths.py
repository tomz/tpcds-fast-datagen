"""Regression tests for ``tpcds_fast_datagen.spark.job`` path routing.

These cover bugs caught by live Spark-in-docker testing:

* :issue:`#B` — ``file://`` URIs were treated as remote HDFS and routed through
  ``hdfs dfs -put``, which fails on environments without a Hadoop CLI on
  ``$PATH`` (laptop Spark, kubernetes Spark without hadoop-client, etc.).
"""
from __future__ import annotations

import pytest

from tpcds_fast_datagen.spark.job import _is_remote, normalize_output


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/out",
        "./relative/path",
        "file:///tmp/out",
        "file:///var/data/sf1",
        "dbfs:/FileStore/tpcds",  # normalise_output handles this
    ],
)
def test_is_remote_false_for_local_and_file_scheme(path: str):
    assert _is_remote(path) is False, f"{path!r} should be treated as local"


@pytest.mark.parametrize(
    "path",
    [
        "hdfs://nn:8020/tpcds",
        "abfss://container@acct.dfs.core.windows.net/tpcds",
        "wasbs://container@acct.blob.core.windows.net/tpcds",
        "s3a://bucket/tpcds",
        "gs://bucket/tpcds",
    ],
)
def test_is_remote_true_for_cluster_filesystems(path: str):
    assert _is_remote(path) is True, f"{path!r} should be treated as remote"


def test_normalize_output_strips_file_scheme():
    assert normalize_output("file:///tmp/sf1_spark") == "/tmp/sf1_spark"


def test_normalize_output_preserves_dbfs():
    # dbfs:/foo/bar -> /dbfs/foo/bar (Databricks FUSE mount)
    assert normalize_output("dbfs:/FileStore/tpcds/sf1") == "/dbfs/FileStore/tpcds/sf1"


def test_normalize_output_preserves_posix():
    assert normalize_output("/tmp/sf1") == "/tmp/sf1"


def test_normalize_output_preserves_remote_uris():
    # These must NOT be rewritten — job.py writes through hdfs dfs -put for them.
    for uri in (
        "abfss://x@y.dfs.core.windows.net/tpcds",
        "wasbs://x@y.blob.core.windows.net/tpcds",
        "hdfs://nn:8020/tpcds",
        "s3a://b/tpcds",
    ):
        assert normalize_output(uri) == uri
