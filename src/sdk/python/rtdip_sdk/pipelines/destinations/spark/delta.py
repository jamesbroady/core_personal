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
    The Spark Delta Source is used to write data to a Delta table. 

    Args:
        table_name (str): Name of the Hive Metastore or Unity Catalog Delta Table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available for [batch](https://docs.delta.io/latest/delta-batch.html#write-to-a-table){ target="_blank" } and [streaming](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-sink){ target="_blank" }.
        mode (str): Method of writing to Delta Table - append/overwrite (batch), append/complete (stream)
        trigger (str): Frequency of the write operation
        query_name (str): Unique name for the query in associated SparkSession

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
        txnAppId (str): A unique string that you can pass on each DataFrame write. (Batch & Streaming)
        txnVersion (str): A monotonically increasing number that acts as transaction version. (Batch & Streaming)
        maxRecordsPerFile (int str): Specify the maximum number of records to write to a single file for a Delta Lake table. (Batch)
        replaceWhere (str): Condition(s) for overwriting. (Batch)
        partitionOverwriteMode (str): When set to dynamic, overwrites all existing data in each logical partition for which the write will commit new data. Default is static. (Batch)
        overwriteSchema (bool str): If True, overwrites the schema as well as the table data. (Batch)
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
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''             
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
        Writes batch data to Delta. Most of the options provided by the Apache Spark DataFrame write API are supported for performing batch writes on tables.
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
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
        
    def write_stream(self, df: DataFrame) -> DataFrame:
        '''
        Writes streaming data to Delta. Exactly-once processing is guaranteed
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
                time.sleep(10)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e