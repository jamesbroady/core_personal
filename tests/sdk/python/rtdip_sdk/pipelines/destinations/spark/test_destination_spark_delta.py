import sys
sys.path.insert(0, '.')
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from tests.sdk.python.rtdip_sdk.pipelines.utils.spark_configuration_constants import spark_session

def test_spark_delta_write_batch(spark_session):
    delta_destination = SparkDeltaDestination("test_spark_delta_write_batch")
    df = spark_session.createDataFrame([{"id": "1"}])
    result = delta_destination.write_batch(df, {}, "overwrite")
    assert True