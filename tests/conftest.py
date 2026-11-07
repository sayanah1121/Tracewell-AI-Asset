import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Plain local Spark: the unit tests need no Iceberg catalog or cluster.
    s = (SparkSession.builder.master("local[1]").appName("tracewell-tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()
