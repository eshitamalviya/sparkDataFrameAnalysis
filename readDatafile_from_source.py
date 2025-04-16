from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession.builder \
                .appName("ReadFromSource") \
                .master("local[3]") \
                .getOrCreate()

    logger = Log4j(spark)


    # Method I : define schema using StructType
    flightsSchema = StructType([
            StructField("FL_DATE", DateType()),
            StructField("OP_CARRIER", StringType()),
            StructField("OP_CARRIER_FL_NUM", IntegerType()),
            StructField("ORIGIN", StringType()),
            StructField("ORIGIN_CITY_NAME", StringType()),
            StructField("DEST", StringType()),
            StructField("DEST_CITY_NAME", StringType()),
            StructField("CRS_DEP_TIME", IntegerType()),
            StructField("DEP_TIME", IntegerType()),
            StructField("WHEELS_ON", IntegerType()),
            StructField("TAXI_IN", IntegerType()),
            StructField("CRS_ARR_TIME", IntegerType()),
            StructField("ARR_TIME", IntegerType()),
            StructField("CANCELLED", IntegerType()),
            StructField("DISTANCE", IntegerType())
        ])

    # Method II : define schema using DDL
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING,
                 ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT,
                 WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # Read CSV File (using method I schema)
    flightsCSVdf = spark.read.format("csv") \
                        .option("header", "true") \
                        .schema(flightsSchema) \
                        .option("mode", "FAILFAST") \
                        .option("dateFormat", "M/d/y") \
                        .load("./dataSourceFiles/flights.csv")
    flightsCSVdf.show(10)

    #Instead of inferSchema option use- schema, mode and dateFormat(only then dates will be parsed from string to date)
    # to correctly map dataSourceFiles types

    logger.info("CSV Schema : " + flightsCSVdf.schema.simpleString())

    # Read JSON File (using method II schema)
    flightsJSONdf = spark.read.format("json") \
            .schema(flightSchemaDDL) \
            .option("dateFormat", "M/d/y") \
            .load("./dataSourceFiles/flights.json")
    flightsJSONdf.show(10)

    logger.info("JSON Schema : " + flightsJSONdf.schema.simpleString())

# Read PARQUET File
# the schema information is already contained in a parquet file hence correct schema is automatically
# inferred while reading the file
# example : date columns are in date format instead of string
# Hence parquet format must be used
def flights_parquet_df(spark):
    flightsPARQdf = spark.read.format("parquet") \
            .load("./dataSourceFiles/flights.parquet")
    #logger.info("Schema : " + flightsPARQdf.schema.simpleString())
    return flightsPARQdf





