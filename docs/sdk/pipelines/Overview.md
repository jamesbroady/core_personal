# Pipelines

Pipelines allow for ingesting time series data from both batch and streaming data sources that are commonly used for time series data. It also aims to map data models from common industry standard protocols and formats into a common data model for the relevant domain and store it in optimized formats.

## Pipeline Jobs

Pipeline Jobs consist of tasks and steps. The relationship between each is described below

``` mermaid
erDiagram
  JOB ||--|{ TASK : contains
  TASK ||--|{ STEP : contains
  JOB {
    string name
    string description
    list task_list
  }
  TASK {
    string name
    string description
    string depends_on_task
    list step_list
    bool batch_task
  }
  STEP {
    string name
    string description
    list depends_on_step
    list provides_output_to_step
    class component
    dict component_parameters
  }
```

## Getting started

Below is an example of a simple pipeline that can be deployed to an orchestration engine. The example sets up the code for building a pipeline and then adds sources, transformers, destinations and utilities as components. Finally, the pipeline is configured for deployment to the relevant orchestration engine

### Setup

The setup imports the relevant components for the pipeline and creates a list to capture each step. 

```python
from rtdip_sdk.pipelines.execute.job import PipelineJob, PipelineJobExecute, PipelineStep, PipelineTask
from rtdip_sdk.pipelines.utilities.spark.delta import TableCreateUtility
from rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from rtdip_sdk.pipelines.transformers.spark.eventhub import EventhubBodyBinaryToString
from rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination

step_list = []
```

### Utility 

The first step in the task is to create a Delta Table(Unity Catalog compliant) if it does not exist. For more information on the list of utilites for RTDIP Pipelines, please click [here](https://www.rtdip.io/blog/rtdip_ingestion_pipelines/#utilities).

```python
# create table
step_list.append(PipelineStep(
    name="test_step1",
    description="test_step1",
    component=TableCreateUtility,
    component_parameters={
        "table_name": "catalog.schema.test_table",
        "columns": [
            StructField("EventDate", DateType(), False, {"delta.generationExpression": "CAST(EventTime AS DATE)"}),
            StructField("TagName", StringType(), False),
            StructField("EventTime", TimestampType(), False),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ],
        "partitioned_by": ["EventDate"],
        "properties": {"delta.logRetentionDuration": "7 days", "delta.enableChangeDataFeed": "true"},
        "comment": "Test Table"
    }
))
```

### Source

The next step is to stream data from an Eventhub. For more information on the list of sources for RTDIP Pipelines, please click [here](https://www.rtdip.io/blog/rtdip_ingestion_pipelines/#sources).

```python
# read the data stream
connection_string_secret = PipelineSecrets(type="AzureKeyVault", name="azasex"  secret_name="eventhub-connection-string")
eventhub_configuration = {
    "eventhubs.connectionString": connection_string, 
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
}

step_list.append(PipelineStep(
    name="test_step2",
    description="test_step2",
    component=SparkEventhubSource,
    component_parameters={"options": eventhub_configuration},
    depends_on_step=["test_step1"],
    provide_output_to_step=["test_step3"]
))
```

### Transformer

Transformers convert the data, including converting column types and structures to meet relevant data model requirements, if required. The below component converts a binary column from the eventhub stream to a string. For more information on the list of transformers for RTDIP Pipelines, please click [here](https://www.rtdip.io/blog/rtdip_ingestion_pipelines/#transformers).

```python
# transform step
step_list.append(PipelineStep(
    name="test_step3",
    description="test_step3",
    component=EventhubBodyBinaryToString,
    component_parameters={},
    depends_on_step=["test_step2"],
    provide_output_to_step=["test_step4"]
))
```

### Destinations

Destination components write data to their relevant target systems. For more information on the list of transformers for RTDIP Pipelines, please click [here](https://www.rtdip.io/blog/rtdip_ingestion_pipelines/#destinations).

```python
# write step
step_list.append(PipelineStep(
    name="test_step4",
    description="test_step4",
    component=SparkDeltaDestination,
    component_parameters={
        "table_name": "catalog.schema.test_table",
        "options": {},
        "mode": "overwrite"    
    },
    depends_on_step=["test_step3"]
))
```

### Task

It is now possible to define a Pipeline Task. This can be done as per the below.

```python
task_list = [PipelineTask(
    name="test_task",
    description="test_task",
    step_list=step_list,
)]
```

### Job

The tasks can then be added to the Pipeline job as per the below.

```python
pipeline_job = PipelineJob(
    name="test_job",
    description="test_job", 
    version="0.0.1",
    task_list=task_list
)
```

### Execute

It is now possible to execute the pipeline with the below commands. This can be done using the PipelineJobExecute as per the code below.

```python
# run pipeline
pipeline = PipelineJobExecute(job)

pipeline.run()
```

### Deploy

Typically, a job will be deployed to an environment, where it can be orchestrated and monitored with other pipelines. Below is an example of how the above pipeline can be deployed to Databricks.  For more information on the list of orchetration engines for RTDIP Pipelines, please click [here](https://www.rtdip.io/blog/rtdip_ingestion_pipelines/#pipeline-orchestration).

```python
# deploy to databricks
databricks_job_cluster = DatabricksJobCluster(
    job_cluster_key="test_job_cluster", 
    new_cluster=DatabricksCluster(
        spark_version = "11.3.x-scala2.12",
        node_type_id = "Standard_E4ds_v5",
        num_workers = 2
    )
)

databricks_task = DatabricksTaskForPipelineTask(name="test_task", job_cluster_key="test_job_cluster")

databricks_job = DatabricksJobForPipelineJob(
    job_clusters=[databricks_job_cluster],
    databricks_task_for_pipeline_task_list=[databricks_task]
)

databricks_job = DatabricksDBXDeploy(pipeline_job=pipeline_job, databricks_job_for_pipeline_job=databricks_job, host="https://test.databricks.net", token="test_token")

databricks_job.deploy()
```

## Conclusion

RTDIP Pipelines allow for modular components to be setup as a series of steps to for a task of a job. This job is then able to be deployed to an orcehstration engine to be run as per the relevant use case.