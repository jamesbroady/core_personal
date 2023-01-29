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
from dependency_injector.containers import DynamicContainer
from src.sdk.python.rtdip_sdk.pipelines.destinations.interfaces import DestinationInterface
from src.sdk.python.rtdip_sdk.pipelines.execute.container import Clients, Configs
from src.sdk.python.rtdip_sdk.pipelines.sources.interfaces import SourceInterface
from src.sdk.python.rtdip_sdk.pipelines.interfaces import PipelineComponentBaseInterface
from src.sdk.python.rtdip_sdk.pipelines.transformers.interfaces import TransformerInterface
from src.sdk.python.rtdip_sdk.pipelines.utils.models import Libraries, SystemType
from pyspark.sql import SparkSession

class PipelineStep():
    name: str
    description: str
    depends_on_step: list[str]
    component: PipelineComponentBaseInterface
    component_parameters: dict
    provide_output_to_step: list[str]

    def __init__(self, name: str, description: str, component: PipelineComponentBaseInterface, component_parameters: dict, depends_on_step: str = None, provide_output_to_step: list[str] = None):
        self.name = name
        self.description = description
        self.component = component
        self.component_parameters = component_parameters
        self.depends_on_step = depends_on_step
        self.provide_output_to_step = provide_output_to_step

class PipelineTask():
    name: str
    description: str
    depends_on_task: list[str]
    step_list: list[PipelineStep]
    provide_output_to_task: list[str]

    def __init__(self, name: str, description: str, step_list: list[PipelineStep], depends_on_task: str = None, provide_output_to_task: str = None):
        self.name = name
        self.description = description
        self.depends_on_task = depends_on_task
        self.step_list = step_list
        self.provide_output_to_task = provide_output_to_task

class PipelineJob():
    name: str
    description: str
    task_list: list[PipelineTask]
    batch_job: bool

    def __init__(self, name: str, description: str, task_list: list[PipelineTask], batch_job: bool = False):
        self.name = name
        self.description = description
        self.task_list = task_list
        self.batch_job = batch_job

class PipelineJobExecute():
    job: PipelineJob

    def __init__(self, job: PipelineJob, batch_job: bool = False):
        self.job = job

    def _tasks_order(self, task_list: list[PipelineTask]):
        ordered_task_list = []
        temp_task_list = task_list.copy()
        while len(temp_task_list) > 0:
            for task in temp_task_list:
                if task.depends_on_task is None:
                    ordered_task_list.append(task)
                    temp_task_list.remove(task)
                else:
                    for ordered_task in ordered_task_list:
                        if task.depends_on_task == ordered_task.name:
                            ordered_task_list.append(task)
                            temp_task_list.remove(task)
                            break
        return ordered_task_list
    
    def _steps_order(self, step_list: list[PipelineStep]):
        ordered_step_list = []
        temp_step_list = step_list.copy()
        while len(temp_step_list) > 0:
            for step in temp_step_list:
                if step.depends_on_step is None:
                    ordered_step_list.append(step)
                    temp_step_list.remove(step)
                else:
                    for ordered_step in ordered_step_list:
                        if step.depends_on_step == ordered_step.name:
                            ordered_step_list.append(step)
                            temp_step_list.remove(step)
                            break
        return ordered_step_list

    def _task_setup_dependency_injection(self, step_list: list[PipelineStep]):
        container = containers.DynamicContainer()
        task_libraries = Libraries()
        task_step_configuration = {}
        task_spark_configuration = {}
        # setup container configuration
        for step in step_list:
            if step.component.system_type() == SystemType.PYSPARK or step.component.system_type() == SystemType.PYSPARK_DATABRICKS:

                # set spark configuration
                task_spark_configuration = {**task_spark_configuration, **step.component.settings()}
                        
                # set spark libraries
                component_libraries = step.component.libraries()
                for library in component_libraries.pypi_libraries:
                    task_libraries.pypi_libraries.append(library)
                for library in component_libraries.maven_libraries:
                    task_libraries.maven_libraries.append(library)
                for library in component_libraries.pythonwheel_libraries:
                    task_libraries.pythonwheel_libraries.append(library)

        Configs.spark_configuration.override(task_spark_configuration)
        Configs.step_configuration.override(task_step_configuration)
        Configs.spark_libraries.override(task_libraries)

        # setup container provider factories
        for step in step_list:
            # setup factory provider for component
            provider = providers.Factory(step.component)
            attributes = step.component.__annotations__.items()
            # add spark session, if needed
            for key, value in attributes:
                # if isinstance(value, SparkSession): # TODO: fix this as value does not seem to be an instance of SparkSession
                if key == "spark":
                    provider.add_kwargs(spark=Clients.spark_client().spark_session)
            # add parameters
            provider.add_kwargs(**step.component_parameters)
            # set provider
            container.set_provider(
                step.name,
                provider
            )
        return container

    def run(self):
        
        ordered_task_list = self._tasks_order(self.job.task_list)

        for task in ordered_task_list:
            container = self._task_setup_dependency_injection(task.step_list)
            ordered_step_list = self._steps_order(task.step_list)
            task_results = {}
            for step in ordered_step_list:
                factory = container.providers.get(step.name)
                # source components
                if isinstance(factory(), SourceInterface):
                    if self.job.batch_job:
                        result = factory().read_batch()
                    else:
                        result = factory().read_stream()
                # transformer components
                elif isinstance(factory(), TransformerInterface):
                    result = factory().transform(task_results[step.name])
                # destination components
                elif isinstance(factory(), DestinationInterface):
                    if self.job.batch_job:
                        result = factory().write_batch(task_results[step.name])
                    else:
                        result = factory().read_stream(task_results[step.name])
                # store results for steps that need it as input
                if step.provide_output_to_step is not None:
                    for step in step.provide_output_to_step:
                        task_results[step] = result
                    