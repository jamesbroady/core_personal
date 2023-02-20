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

from dependency_injector import containers, providers
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import SparkClient

class Configs(containers.DeclarativeContainer):
    """Container for pipeline configs."""

    config: dict = providers.Configuration("config")
    pipeline_configuration: dict = providers.Configuration("pipeline_configuration")
    spark_configuration: dict = providers.Configuration("spark_configuration")
    spark_libraries: Libraries = providers.Configuration("spark_libraries")
    step_configuration: dict = providers.Configuration("step_configuration")

class Clients(containers.DeclarativeContainer):
    """Container for pipeline clients."""

    spark_client = providers.Singleton(
        SparkClient,
        spark_configuration=Configs.spark_configuration,
        spark_libraries=Configs.spark_libraries,
    )

# class Factories(containers.DeclarativeContainer):
#     """Container for pipeline factories."""

#     source_spark_eventhub_factory = providers.Factory(
#         SparkEventhubSource,
#         spark=Clients.spark_client().spark_session,
#         options=Configs.component_configuration,
#     )

# def ComponentContainer():
#     """Container for pipeline components."""

#     components = containers.DynamicContainer()
#     components.source_spark_eventhub = Factories.source_spark_eventhub_factory()

#     return component_container
# component_container = containers.DynamicContainer()