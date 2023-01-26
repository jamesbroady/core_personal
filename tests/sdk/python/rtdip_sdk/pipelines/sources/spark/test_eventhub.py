
import sys
sys.path.insert(0, '.')
import pytest
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from tests.sdk.python.rtdip_sdk.pipelines.utils.spark_configuration_constants import spark_session
from pyspark.sql.types import StructType, StructField, BinaryType, StringType, LongType, TimestampType, MapType
import json
from pyspark.sql import DataFrame

def test_spark_eventhub_read_batch(spark_session):
    eventhub_source = SparkEventhubSource()
    
    connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
    eventhub_configuration = {
        "eventhubs.connectionString": connection_string, 
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    }
    df = eventhub_source.read_batch(spark_session, eventhub_configuration)
    assert isinstance(df, DataFrame)
    assert df.schema == StructType([StructField('body', BinaryType(), True), StructField('partition', StringType(), True), StructField('offset', StringType(), True), StructField('sequenceNumber', LongType(), True), StructField('enqueuedTime', TimestampType(), True), StructField('publisher', StringType(), True), StructField('partitionKey', StringType(), True), StructField('properties', MapType(StringType(), StringType(), True), True), StructField('systemProperties', MapType(StringType(), StringType(), True), True)])
