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

import logging
from pyspark.sql import DataFrame, SparkSession

from src.sdk.python.rtdip_sdk.pipelines.sources.interfaces import SourceInterface
from src.sdk.python.rtdip_sdk.pipelines.utils.models import Libraries, MavenLibrary, SystemType

class SparkEventhubSource(SourceInterface):
    '''

    ''' 
    @property
    def system_type(self):
        return SystemType.PYSPARK

    def libraries(self):
        libraries = Libraries()
        libraries.add_maven_library(
            MavenLibrary(
                group_id="com.microsoft.azure",
                artifact_id="azure-eventhubs-spark_2.12",
                version="2.3.22"
            )
        )
        return libraries
    
    def settings(self) -> dict:
        return {}
    
    def pre_read_validation(self):
        return True
    
    def post_read_validation(self):
        return True

    def read_batch(self, spark: SparkSession, options: dict) -> DataFrame:
        '''
        '''
        try:
            if "eventhubs.connectionString" in options:
                sc = spark.sparkContext
                options["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(options["eventhubs.connectionString"])

            return (spark
                .read
                .format("eventhubs")
                .options(**options)
                .load()
            )

        except Exception as e:
            print(e)
            logging.exception("error with spark read batch eventhub function")
            raise e
        
    def read_stream(self, spark: SparkSession, options: dict) -> DataFrame:
        try:
            if "eventhubs.connectionString" in options:
                sc = spark.sparkContext
                options["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(options["eventhubs.connectionString"])

            return (spark
                .readStream
                .format("eventhubs")
                .options(**options)
                .load()
            )

        except Exception as e:
            print(e)
            logging.exception("error with spark read stream eventhub function")
            raise e