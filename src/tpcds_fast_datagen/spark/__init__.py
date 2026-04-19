"""Distributed Spark generation API.

Use this from any Spark environment that has the ``tpcds_fast_datagen``
wheel installed on driver + executors:

* HDInsight / EMR / Dataproc Spark 3.x (`%pip install` or `--py-files`)
* Microsoft Fabric notebooks
* Databricks notebooks
* Livy session statements
* Bare ``spark-submit`` via the bundled bootstrap script

Quick example::

    from tpcds_fast_datagen.spark import generate

    generate(spark, scale=1000, output="abfs:///tpcds/sf1000")

For SSH / spark-submit users without a notebook, see
``tpcds_fast_datagen.spark.submit``.
"""

from .api import generate, GenerateResult

__all__ = ["generate", "GenerateResult"]
