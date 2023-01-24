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
from Crypto.Cipher import AES
import base64
import hashlib

def read(spark: SparkSession, eventhub_configuration: dict) -> DataFrame:
    '''
    '''
    try:
        return (spark
            .read
            .format("eventhubs")
            .options(**eventhub_configuration)
            .load()
           )

    except Exception as e:
        logging.exception('error with spark read function', e.__traceback__)
        raise e