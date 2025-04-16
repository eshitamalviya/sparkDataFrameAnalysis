import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

print("starting spark...")

    # Configuring Spark Session
    # Method 1
#spark = SparkSession \
    #     .builder \
    #     .appName("Hello Spark") \
    #     .master("local[3]") \
    #     .getOrCreate()

    # Method 2 - RECOMMENDED
    # To avoid hard coding, this method must be used
conf = get_spark_app_config()
spark = SparkSession \
        .builder \
        .config(conf = conf) \
        .getOrCreate()

    # Method 3
    # we can also use SparkConf to set configurations
# conf = SparkConf()
# conf.set("spark.app.name","Hello Spark")
# conf.set("master","local[3]")
# spark = SparkSession \
    #     .builder \
    #     .config(conf = conf) \
    #     .getOrCreate()

logger = Log4j(spark)

logger.info("Starting HelloSpark")

logger.info("Finished HelloSpark")
input("enter something to exit...")
# spark.stop()

