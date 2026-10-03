import os

import pytest

from retail_lakehouse.config import Settings
from retail_lakehouse.spark import get_spark
from retail_lakehouse.storage import Lakehouse


@pytest.fixture(scope="session")
def spark_and_format():
    # CI sets LAKEHOUSE_FORMAT=delta (Maven reachable); locally we fall back to Parquet if needed.
    want_delta = os.getenv("LAKEHOUSE_FORMAT", "parquet") == "delta"
    spark, fmt = get_spark("retail-lakehouse-tests", want_delta=want_delta)
    spark.sparkContext.setLogLevel("ERROR")
    yield spark, fmt
    spark.stop()


@pytest.fixture(scope="session")
def spark(spark_and_format):
    return spark_and_format[0]


@pytest.fixture
def lakehouse(spark_and_format, tmp_path):
    spark, fmt = spark_and_format
    settings = Settings(landing_path=str(tmp_path / "landing"), base_path=str(tmp_path / "data"), fmt=fmt)
    return Lakehouse(spark, settings)
