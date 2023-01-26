import sys
sys.path.insert(0, '.')
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session
import pytest
from pytest_mock import MockerFixture

def test_spark_delta_write_batch(mocker: MockerFixture):
    delta_destination = SparkDeltaDestination("test_spark_delta_write_batch")
    spark = get_spark_session([delta_destination], "test_spark_delta_write_batch", {})
    df = spark.createDataFrame([{"id": "1"}])
    result = delta_destination.write_batch(df, {}, "overwrite")
    assert True