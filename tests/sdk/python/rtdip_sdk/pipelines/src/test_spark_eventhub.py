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

import pytest
from unittest import expectedFailure
from unittest.mock import Mock, PropertyMock, patch
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.pipelines.utils.spark_context_mock import SparkContextMock
from src.sdk.python.rtdip_sdk.pipelines.src import spark_eventhub

def setup_class(self):
    self.spark = (SparkSession
                 .builder
                 .master("local[*]")
                 .appName("Unit-tests")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .getOrCreate()
                )

def test_spark_read(self, mocker: MockerFixture):
    sc = SparkContextMock()
    self.test_config["sc"] = sc 
    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.src.spark_eventhubs.read", new_callable=PropertyMock, return_value = Mock(readStream=Mock(format=Mock(return_value=Mock(options=Mock(return_value=Mock(load=Mock(return_value=self.raw_event_stream_df))))))))
    actual = spark_eventhub.read(sc)
    
    assert 




def setup_raw_event_stream_df(self, mocker: MockerFixture):
sc = SparkContextMock()
self.test_config["sc"] = sc        
test_delta_conversion = DeltaConversion(**self.test_config)
test_delta_conversion.set_asset_config(**self.asset_config)
mocker.patch("src.spark.pyspark.ssip_ingestion.delta_conversion.classes.DeltaConversion.spark", new_callable=PropertyMock, return_value = Mock(readStream=Mock(format=Mock(return_value=Mock(options=Mock(return_value=Mock(load=Mock(return_value=self.raw_event_stream_df))))))))

test_delta_conversion.eventhub_configuration_settings()
return test_delta_conversion.raw_event_stream_df(test_delta_conversion.eventhub_configuration)    

def test_raw_event_stream_df(self, mocker: MockerFixture):
# test raw_event_stream_df
raw_event_stream_df = self.setup_raw_event_stream_df(mocker)
assert raw_event_stream_df.count() == 1
assert self.raw_event_stream_df.schema.fields == raw_event_stream_df.schema.fields