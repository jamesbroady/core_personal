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

class SparkDeltaSource(SourceInterface):
    '''

    ''' 
    spark: SparkSession
    options: dict
    table_name: str

    def __init__(self, spark: SparkSession, options: dict, table_name: str) -> None:
        self.spark = spark
        self.options = options
        self.table_name = table_name

    @property
    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(
            MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-core_2.12",
                version="2.2.0"
            )
        )
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_read_validation(self):
        return True
    
    def post_read_validation(self):
        return True

    def read_batch(self):
        '''
        '''
        try:
            return (self.spark
                .read
                .format("delta")
                .table(self.table_name)
            )

        except Exception as e:
            logging.exception('error with spark read batch delta function', e.__traceback__)
            raise e
        
    def read_stream(self, spark: SparkSession, options: dict) -> DataFrame:
        '''
        '''
        try:
            return (spark
                .readStream
                .format("delta")
                .options(**options)
                .load(self.table_name)
            )

        except Exception as e:
            logging.exception('error with spark read stream delta function', e.__traceback__)
            raise e
