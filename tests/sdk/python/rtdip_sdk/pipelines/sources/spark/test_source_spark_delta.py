import sys
sys.path.insert(0, '.')
import pytest
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

@pytest.fixture(scope="class")
def test_spark_delta_read_batch(mocker: MockerFixture):
    delta_source = SparkDeltaSource("test_spark_delta_read_batch")
    delta_destination = SparkDeltaDestination("test_spark_delta_read_batch")
    spark = get_spark_session([delta_source, delta_destination], "test_spark_delta_read_batch", {})
    df = spark.createDataFrame([{"id": "1"}])
    result = delta_destination.write_batch(df, {}, "overwrite")
    df = delta_source.read_batch(spark, {})
    assert isinstance(df, DataFrame)
    assert df.schema == StructType([StructField('id', StringType(), True)])
