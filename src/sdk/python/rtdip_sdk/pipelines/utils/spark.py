import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.sdk.python.rtdip_sdk.pipelines.interfaces import PipelineBaseInterface

def get_spark_session(components_list: list[PipelineBaseInterface], app_name: str = "rtdip", spark_configuration: dict = {}, master=None) -> SparkSession:

    try:
        spark = SparkSession \
            .builder \
            .appName(app_name)
        
        if master is not None:
            spark = spark.master(master)
            
        for component in components_list:
            libraries = component.libraries()
            if len(libraries.maven_libraries) > 0:
                spark_configuration["spark.jars.packages"] = ','.join(maven_package.to_string() for maven_package in libraries.maven_libraries)
                
            settings = component.settings()
            if len(settings) > 0:
                for setting in settings.items():
                    spark = spark.config(setting[0], setting[1])

        for configuration in spark_configuration.items():
            spark = spark.config(configuration[0], configuration[1])

        return spark.getOrCreate()
    except Exception as e:
        logging.exception('error with spark session function', e.__traceback__)
        raise e
    