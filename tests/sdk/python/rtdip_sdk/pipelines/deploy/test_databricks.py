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

import sys

from src.sdk.python.rtdip_sdk.pipelines.deploy.databricks import DataBricksDeploy
from src.sdk.python.rtdip_sdk.pipelines.deploy.models.databricks import DatabricksCluster, DatabricksJobCluster, DatabricksJobForPipelineJob, DatabricksTaskForPipelineTask
sys.path.insert(0, '.')

from pyspark.sql.types import StructField, TimestampType, StringType, FloatType, DateType

from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta import TableCreateUtility
from src.sdk.python.rtdip_sdk.pipelines.execute.job import PipelineJob, PipelineJobExecute, PipelineStep, PipelineTask
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.eventhub import EventhubBodyBinaryToString
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta_sharing import SparkDeltaSharingSource
import json

def test_pipeline_job_deploy():
    step_list = []

    # read step
    # connection_string_secret = PipelineSecrets(type="AzureKeyVault", name="azasex", secret_name="eventhub-connection-string")
    connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
    eventhub_configuration = {
        "eventhubs.connectionString": connection_string, 
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    }    
    step_list.append(PipelineStep(
        name="test_step1",
        description="test_step1",
        component=SparkEventhubSource,
        component_parameters={"options": eventhub_configuration},
        provide_output_to_step=["test_step2"]
    ))

    # transform step
    step_list.append(PipelineStep(
        name="test_step2",
        description="test_step2",
        component=EventhubBodyBinaryToString,
        component_parameters={},
        depends_on_step="test_step1",
        provide_output_to_step=["test_step3"]
    ))

    # write step
    step_list.append(PipelineStep(
        name="test_step3",
        description="test_step3",
        component=SparkDeltaDestination,
        component_parameters={
            "table_name": "test_table",
            "options": {},
            "mode": "overwrite"    
        },
        depends_on_step="test_step2"
    ))

    task = PipelineTask(
        name="test_task",
        description="test_task",
        step_list=step_list,
        batch_task=True
    )

    pipeline_job = PipelineJob(
        name="test_job",
        description="test_job", 
        task_list=[task]
    )

    databricks_job_cluster = DatabricksJobCluster(
        job_cluster_key="test_job_cluster", 
        new_cluster=DatabricksCluster(
            node_type_id = "Standard_DS3_v2",
            spark_version = "12.1.x-scala2.12",
            num_workers = 2
        )
    )

    databricks_task = DatabricksTaskForPipelineTask(name="test_task", job_cluster_key="test_job_cluster")

    databricks_job = DatabricksJobForPipelineJob(
        job_clusters=[databricks_job_cluster],
        databricks_task_for_pipeline_task_list=[databricks_task]
    )

    databricks_job = DataBricksDeploy(pipeline_job=pipeline_job, databricks_job_for_pipeline_job=databricks_job, host="https://adb-3073476248944970.10.azuredatabricks.net", token="dapicb7febf1ac9240a943834f03759c537b")

    result = databricks_job.deploy()
    
    assert True

