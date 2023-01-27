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
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from tests.sdk.python.rtdip_sdk.pipelines.utils.spark_configuration_constants import spark_session
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def test_spark_delta_read_batch(spark_session):
    delta_source = SparkDeltaSource("test_spark_delta_read_batch")
    delta_destination = SparkDeltaDestination("test_spark_delta_read_batch")
    df = spark_session.createDataFrame([{"id": "1"}])
    result = delta_destination.write_batch(df, {}, "overwrite")
    df = delta_source.read_batch(spark_session, {})
    assert isinstance(df, DataFrame)
    assert df.schema == StructType([StructField('id', StringType(), True)])
