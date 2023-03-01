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
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from py4j.protocol import Py4JJavaError
from delta.tables import DeltaTable

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, MavenLibrary, SystemType

class TableCreateUtility(UtilitiesInterface):
    '''

    ''' 
    spark: SparkSession
    table_name: str
    columns: list[StructField]
    partitioned_by: list[str]
    location: str
    properties: dict
    comment: str

    def __init__(self, spark: SparkSession, table_name: str, columns: list[StructField], partitioned_by: list[str] = None, location: str = None, properties: dict = None, comment: str = None) -> None:
        self.spark = spark
        self.table_name = table_name
        self.columns = columns
        self.partitioned_by = partitioned_by
        self.location = location
        self.properties = properties
        self.comment = comment

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

    def execute(self):
        try:
            delta_table = (
                DeltaTable
                .createIfNotExists(self.spark)
                .tableName(self.table_name)
                .addColumns(self.columns)
            )

            if self.partitioned_by is not None:
                delta_table = delta_table.partitionedBy(self.partitioned_by)

            if self.location is not None:
                delta_table = delta_table.location(self.location)

            if self.properties is not None:
                for key, value in self.properties.items():
                    delta_table = delta_table.property(key, value)
            
            if self.comment is not None:
                delta_table = delta_table.comment(self.comment)

            delta_table.execute()

        except Py4JJavaError as e:
            logging.exception('error with spark delta table create function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark delta table create function', e.__traceback__)
            raise e