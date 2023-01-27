# Copyright 2022 RTDIP
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
import os
import shutil
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from src.sdk.python.rtdip_sdk.pipelines.utils.spark import get_spark_session

SPARK_TESTING_CONFIGURATION = {
    "spark.executor.cores": "1",
    "spark.executor.instances": "1",
    "spark.sql.shuffle.partitions": "1",
}

@pytest.fixture(scope="session")
def spark_session():
    component_list = [SparkDeltaSource("test_table"), SparkDeltaDestination("test_table"), SparkEventhubSource()]
    spark = get_spark_session(component_list, "test_app", SPARK_TESTING_CONFIGURATION, "local[1]")
    path = spark.conf.get("spark.sql.warehouse.dir")
    prefix = "file:"
    if path.startswith(prefix):
        path = path[len(prefix):]    
    if os.path.isdir(path):
        shutil.rmtree(path)    
    yield spark
    spark.stop()
    if os.path.isdir(path):
        shutil.rmtree(path)    