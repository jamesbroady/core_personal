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
import json
from importlib_metadata import PackageNotFoundError, version

from databricks_cli.configure.provider import update_and_persist_config
from dbx.commands.deploy import deploy as dbx_deploy
from dbx.api.configure import ProjectConfigurationManager
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

from src.sdk.python.rtdip_sdk.pipelines.deploy.interfaces import DeployInterface
from src.sdk.python.rtdip_sdk.pipelines.deploy.models.databricks import DatabricksJob, DatabricksJobForPipelineJob, DatabricksSparkPythonTask, DatabricksTask, DatabricksLibraries, DatabricksLibrariesMaven, DatbricksLibrariesPypi
from src.sdk.python.rtdip_sdk.pipelines.execute.job import PipelineJob

class DataBricksDeploy(DeployInterface):
    '''

    '''
    pipeline_job: PipelineJob
    databricks_job_for_pipeline_job: DatabricksJobForPipelineJob
    host: str
    token: str

    def __init__(self, pipeline_job: PipelineJob, databricks_job_for_pipeline_job: DatabricksJobForPipelineJob, host: str, token: str) -> None:
        self.pipeline_job = pipeline_job
        self.databricks_job_for_pipeline_job = databricks_job_for_pipeline_job
        self.host = host
        self.token = token
    
    def deploy(self):
        # get Api Client
        api_client = ApiClient(host=self.host, token=self.token)
        jobs_api = JobsApi(api_client)

        # create Databricks Job Tasks
        databricks_tasks = []
        for task in self.pipeline_job.task_list:
            databricks_job_task = DatabricksTask(task_key=task.name, libraries=[], depends_on=[])
            if self.databricks_job_for_pipeline_job.databricks_task_for_pipeline_task_list is not None:
                databricks_task_for_pipeline_task = next(x for x in self.databricks_job_for_pipeline_job.databricks_task_for_pipeline_task_list if x.name == task.name)
                if databricks_task_for_pipeline_task is not None:
                    databricks_job_task.__dict__.update(databricks_task_for_pipeline_task.__dict__)
            
            databricks_job_task.name = task.name
            databricks_job_task.depends_on = task.depends_on_task

            # get libraries
            for step in task.step_list:
                libraries = step.component.libraries()
                for pypi_library in libraries.pypi_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package=pypi_library.to_string(), repo=pypi_library.repo)))
                for maven_library in libraries.maven_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(maven=DatabricksLibrariesMaven(coordinates=maven_library.to_string(), repo=maven_library.repo)))
                for wheel_library in libraries.pythonwheel_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(whl=wheel_library))

            try:
                rtdip_version = version("rtdip-sdk")
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk=={}".format(rtdip_version))))
            except PackageNotFoundError as e:
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk")))

            databricks_job_task.spark_python_task = DatabricksSparkPythonTask(python_file="dbfs:/python_file.py")
            databricks_tasks.append(databricks_job_task)

        # create Databricks Job
        databricks_job = DatabricksJob(name=self.pipeline_job.name, tasks=databricks_tasks)
        databricks_job.__dict__.update(self.databricks_job_for_pipeline_job.__dict__)
        databricks_job.__dict__.pop("databricks_task_for_pipeline_task_list", None)

        # create Databricks Job
        result = jobs_api.create_job(databricks_job.dict(exclude_none=True), version="2.1")
        return result
                            
    def launch(self, job_id):
        api_client = ApiClient(host=self.host, token=self.token)
        jobs_api = JobsApi(api_client)
        jobs_api.run_now(job_id)
    
