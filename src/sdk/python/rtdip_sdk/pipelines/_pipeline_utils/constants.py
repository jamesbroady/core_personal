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

from .models import MavenLibrary, PyPiLibrary

DEFAULT_PACKAGES = {
    "spark_delta_core": MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-core_2.12",
                version="2.3.0"
            ),
    "spark_delta_sharing": MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-sharing-spark_2.12",
                version="0.6.3"
            ),
    "spark_azure_eventhub": MavenLibrary(
                group_id="com.microsoft.azure", 
                artifact_id="azure-eventhubs-spark_2.12",
                version="2.3.22"
            ),
    "spark_sql_kafka": MavenLibrary(
                group_id="org.apache.spark", 
                artifact_id="spark-sql-kafka-0-10_2.12",
                version="3.4.0"
            ),
    "rtdip_sdk": PyPiLibrary(
                name="rtdip_sdk",
                version="0.2.0"
            )
}
