import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

from src.sdk.python.rtdip_sdk.pipelines.sources.interfaces import SourceInterface

def get_spark_session(sources_list: list[SourceInterface], app_name: str = "rtdip", spark_configuration: dict = {}) -> SparkSession:

    try:
        spark = SparkSession \
            .builder \
            .appName(app_name)
        
        for source in sources_list:
            libraries = source.libraries()
            if len(libraries.maven_libraries) > 0:
                spark_configuration["spark.jars.packages"] = ','.join(maven_package.to_string() for maven_package in libraries.maven_libraries)

        for configuration in spark_configuration.items():
            spark = spark.config(configuration[0], configuration[1])

        return spark.getOrCreate()

    except Exception as e:
        logging.exception('error with spark session function', e.__traceback__)
        raise e
    