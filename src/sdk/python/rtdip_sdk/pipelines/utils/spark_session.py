import logging
from pyspark.sql import SparkSession

def spark_session(spark, session_configurations) -> SparkSession:

    try:
        spark = SparkSession \
            .builder \
            .appName("pipeines") \
            .config(**session_configurations) \
            .getOrCreate()

        for configuration in session_configurations:

        # loop through configurations and set them indvidually
        # for config in session_configurations:
            # spark_session.conf.set(config, '8g')
            # spark_session.conf.set('spark.executor.cores', '3')
            # spark_session.conf.set('spark.cores.max', '3')
            # spark_session.conf.set("spark.driver.memory",'8g')
            # sc = spark_session.sparkContext

            #if in dict do .iter to loop 

        
        return spark


    except Exception as e:
        logging.exception('error with spark session function')
        raise e
    