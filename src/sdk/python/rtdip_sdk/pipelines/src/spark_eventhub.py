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
from pyspark.sql import DataFrame

def read(spark, eventhub_configuration) -> DataFrame:
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
        logging.exception('error with spark read function')
        raise e