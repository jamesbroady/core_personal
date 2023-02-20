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

from src.sdk.python.rtdip_sdk.pipelines.deploy.interfaces import DeployInterface
from databricks_cli.configure.provider import update_and_persist_config
from dbx.commands.deploy import deploy as dbx_deploy
from dbx.api.configure import ProjectConfigurationManager

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

from src.sdk.python.rtdip_sdk.pipelines.execute.job import PipelineJob

class DataBricksDeploy(DeployInterface):
    '''

    ''' 
    job: PipelineJob
    host: str
    token: str

    def __init__(self, job: PipelineJob, host: str, token: str) -> None:
        self.job = job
        self.host = host
        self.token = token
    
    def deploy(self):
        api_client = ApiClient(host=self.host, token=self.token)
        jobs_api = JobsApi(api_client)
        job_tasks = []
        for task in self.job.task_list:
            
        jobs_api.create_job(
            {
                "name": self.job.name,
                "tasks": []
            }
        )

    def launch(self, job_id):
        api_client = ApiClient(host=self.host, token=self.token)
        jobs_api = JobsApi(api_client)
        jobs_api.run_now(job_id)
    
