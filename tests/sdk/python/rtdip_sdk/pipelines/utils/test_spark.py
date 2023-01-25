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
import pytest
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session

def test_get_spark_session(mocker: MockerFixture):
    spark = get_spark_session([], "testapp", {"configuration_test1": "configuration_test_value1", "configuration_test2": "configuration_test_value2"})
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("configuration_test1") == "configuration_test_value1"
    assert spark.conf.get("configuration_test2") == "configuration_test_value2"

def test_get_spark_session_exception(mocker: MockerFixture):
    with pytest.raises(Exception) as excinfo:  
        spark = get_spark_session([], "testapp", "configuration_test1")
    assert str(excinfo.value) == 'not all arguments converted during string formatting' 