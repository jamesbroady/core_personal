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
import time
from pyspark.sql import DataFrame
from py4j.protocol import Py4JJavaError

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, MavenLibrary, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

class SparkDeltaDestination(DestinationInterface):
    '''

    '''
    table_name: str
    options: dict
    mode: str
    trigger: str
    query_name: str

    def __init__(self, table_name:str, options: dict, mode: str = "append", trigger="10 seconds", query_name="DeltaDestination") -> None:
        self.table_name = table_name
        self.options = options
        self.mode = mode
        self.trigger = trigger
        self.query_name = query_name

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(DEFAULT_PACKAGES["spark_delta_core"])
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    
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

        except Py4JJavaError as e:
            logging.exception('error with spark write batch delta function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark write batch delta function', e.__traceback__)
            raise e
        
    def write_stream(self, df: DataFrame) -> DataFrame:
        '''
        '''
        try:
            query = (df
                .writeStream
                .trigger(processingTime=self.trigger)
                .format("delta")
                .queryName(self.query_name)
                .outputMode(self.mode)
                .options(**self.options)
                .toTable(self.table_name)
            )
            
            while query.isActive:
                if query.lastProgress:
                    logging.info(query.lastProgress)
                time.sleep(30)

        except Py4JJavaError as e:
            logging.exception('error with spark write stream delta function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark write stream delta function', e.__traceback__)
            raise e
