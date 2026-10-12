import os
import sys

import pytest
from pyspark.sql import SparkSession

# Spark launches its Python workers with whatever `python3` is first on PATH unless
# told otherwise, which can be a different minor version than the one running pytest
# (PYTHON_VERSION_MISMATCH). Pin the workers to this interpreter; an explicit
# PYSPARK_PYTHON in the environment still wins.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pyspark-contracts-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
