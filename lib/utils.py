import configparser
import sys
from pyspark.sql.types import *
from pyspark import SparkConf


def load_survey(spark, filepath):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)

def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def createDFfromRDD(spark):
    schema = StructType([StructField('id', StringType()),
                             StructField('order_date', StringType())])
    rows = [Row('1', '23-4-2025'),
            Row('2', '24-4-2025'),
            Row('3', '25-4-2025')]
    # df = spark.createDataFrame(rows, schema)
    # sparkContext.parallelize creates RDD from an iterable
    myRDD = spark.sparkContext.parallelize(rows, 2)
    df = spark.createDataFrame(myRDD, schema)
    return df.show()
