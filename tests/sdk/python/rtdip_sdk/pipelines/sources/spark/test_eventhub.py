import pytest
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import read
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session
import json
from pyspark.sql import DataFrame

def test_spark_eventhub_read(mocker: MockerFixture):
    spark = get_spark_session("testapp", {"spark.jars.packages": "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22"})
    sc = spark.sparkContext
    connection_string = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test")
    eventhub_configuration = {
        "eventhubs.connectionString": connection_string, 
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    }
    df = read(spark, eventhub_configuration)
    assert isinstance(df, DataFrame)
    # assert df.schema 
    # assert df.count() == 0
