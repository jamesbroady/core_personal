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
from delta.tables import DeltaTable

from src.sdk.python.rtdip_sdk.pipelines.destinations.interfaces import DestinationInterface
from src.sdk.python.rtdip_sdk.pipelines.utils.models import Libraries, MavenLibrary, SystemType

class SparkDeltaDestination(DestinationInterface):
    '''

    '''
    table_name: str
    options: dict
    mode: str

    def __init__(self, table_name:str, options: dict, mode: str = "append") -> None:
        self.table_name = table_name
        self.options = options
        self.mode = mode

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
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    
    def destination_definition(self, spark: SparkSession) -> dict:
        delta_table = (
            DeltaTable
            .createIfNotExists(spark)
            .tableName(self.table_name)
            .addColumn("id", "string")
            .execute()       
        )
    
    def pre_write_validation(self):
        return True
    
    def post_write_validation(self):
        return True

    def write_batch(self, df: DataFrame):
        '''
        '''
        try:
            return (
                df
                .write
                .format("delta")
                .mode(self.mode)
                .options(**self.options)
                .saveAsTable(self.table_name)
            )

        # except Py4JJavaError as e:
        #     logging.exception('error with spark write function', e.errmsg)
        #     raise e
        except Exception as e:
            logging.exception('error with spark write batch delta function', e.__traceback__)
            raise e
        
    def write_stream(self, df: DataFrame) -> DataFrame:
        '''
        '''
        try:
            return (df
                .writeStream
                .format("delta")
                .outputMode(self.mode)
                .options(**self.options)
                .toTable(self.table_name)
            )

        # except Py4JJavaError as e:
        #     logging.exception('error with spark write function', e.errmsg)
        #     raise e
        except Exception as e:
            logging.exception('error with spark write stream delta function', e.__traceback__)
            raise e
