import sys
sys.path.insert(0, '.')
import pytest
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session

def test_get_spark_session(mocker: MockerFixture):
    spark = get_spark_session("testapp", {"configuration_test1": "configuration_test_value1", "configuration_test2": "configuration_test_value2"})
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("configuration_test1") == "configuration_test_value1"
    assert spark.conf.get("configuration_test2") == "configuration_test_value2"