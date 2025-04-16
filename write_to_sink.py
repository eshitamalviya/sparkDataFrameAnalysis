from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4j
from readDatafile_from_source import flights_parquet_df

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("WriteToSink") \
        .getOrCreate()

    logger = Log4j(spark)

    flightsPARQdf = flights_parquet_df(spark)

    # Get the number of partitions our dataframe is split into
    logger.info("Num Partitions before: " + str(flightsPARQdf.rdd.getNumPartitions()))
    # Group by partition id to get number of records in each partition
    flightsPARQdf.groupBy(spark_partition_id()).count().show()

    # experiment with number of partitions
    partitionedDF = flightsPARQdf.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # In order to write AVRO files to sink we need to add SCALA package so add below line in spark-defaults.conf file
    # spark.jars.packages       org.apache.spark:spark-avro_2.11:2.4.5
    # partitionedDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flightsPARQdf.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 100) \
        .save()

