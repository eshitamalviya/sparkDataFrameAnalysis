from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4j
from readDatafile_from_source import flights_parquet_df

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("sparkTables") \
        .enableHiveSupport() \
        .getOrCreate()
    # for managed tables : Hive support must be enabled to use hive metastore

    logger = Log4j(spark)

    # spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    # spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flights_parquet_df = flights_parquet_df(spark)
    flights_parquet_df.write.option("mode", "overwrite").saveAsTable("flights_managedtbl")

    logger.info(spark.catalog.listTables())