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
from py4j.protocol import Py4JJavaError

from src.sdk.python.rtdip_sdk.pipelines.sources.interfaces import SourceInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, MavenLibrary, SystemType

class SparkDeltaSharingSource(SourceInterface):
    '''

    ''' 
    spark: SparkSession
    options: dict
    table_path: str

    def __init__(self, spark: SparkSession, options: dict, table_path: str) -> None:
        self.spark = spark
        self.options = options
        self.table_path = table_path

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(
            MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-sharing-spark_2.12",
                version="0.6.2"
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
                .format("deltaSharing")
                .options(**self.options)
                .table(self.table_path)
            )

        except Py4JJavaError as e:
            logging.exception('error with spark read batch delta sharing function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark read batch delta sharing function', e.__traceback__)
            raise e
        
    def read_stream(self) -> DataFrame:
        '''
        '''
        try:
            return (self.spark
                .readStream
                .format("deltaSharing")
                .options(**self.options)
                .load(self.table_path)
            )
        
        except Py4JJavaError as e:
            logging.exception('error with spark read stream delta sharing function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark read stream delta sharing function', e.__traceback__)
            raise e
