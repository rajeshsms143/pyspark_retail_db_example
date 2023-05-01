from pyspark.sql import SparkSession

def spark_session(appName,mode):
    spark = SparkSession. \
        builder. \
        appName(appName). \
        master(mode). \
        getOrCreate()
    return spark