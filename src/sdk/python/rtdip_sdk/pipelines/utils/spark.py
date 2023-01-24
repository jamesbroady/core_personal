import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def get_spark_session(app_name: str = "rtdip", spark_configuration: dict = {}) -> SparkSession:

    try:
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()

        for configuration in spark_configuration.items():
            spark.conf.set(configuration[0], configuration[1])

        return spark

    except Exception as e:
        logging.exception('error with spark session function', e.__traceback__)
        raise e
    