# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
sys.path.insert(0, '.')
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
