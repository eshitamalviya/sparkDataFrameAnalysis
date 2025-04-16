from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from readDatafile_from_source import flights_parquet_df
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession.builder \
                .appName("DataTransformations") \
                .master("local[3]") \
                .getOrCreate()

    logger = Log4j(spark)

    flights_df = flights_parquet_df(spark)
    # flights_df.show()
    # # print column name and data type
    logger.info(flights_df.schema.simpleString())
    # print all columns in a list
    logger.info(flights_df.columns)

    # 4 ways to select columns
    flights_ = flights_df.select("FL_DATE", col('DEST'), column('ORIGIN'), flights_df.CANCELLED)
    flights_.show(10)

    # Adding column
    flights_newcol = flights_df.withColumn('CARRIER_FL_NUM', concat_ws('-', 'OP_CARRIER','OP_CARRIER_FL_NUM'))
    flights_newcol.show()

    # working with SQL Expressions
    flights_expr = flights_df.select('ORIGIN', expr("concat_ws('-', OP_CARRIER,OP_CARRIER_FL_NUM) as CARRIER_FL_NUM"))
    flights_expr.show()

    # same transformation using column object
    flights_concat = flights_df.select('ORIGIN', concat_ws('-', 'OP_CARRIER','OP_CARRIER_FL_NUM').alias('CARRIER_FL_NUM'))
    flights_concat.show()

    # using case when, dropDuplicates, sort
    flight_more_col = flights_df.select(flights_df.DISTANCE, flights_df.CANCELLED).withColumn('id', monotonically_increasing_id()) \
                                .withColumn('Proximity', expr('''
                                case when Distance > 500 then "Far"
                                when Distance < 500 then "Near"
                                else "NA" end ''')) \
                                .withColumn('Flag', when(flights_df.CANCELLED== 0, 'Not Cancel')
                                                    .when(flights_df.CANCELLED != 0, 'Cancel')
                                                    .otherwise('NA')) \
                                .dropDuplicates(['DISTANCE']) \
                                .sort(col('DISTANCE').desc())
    flight_more_col.show()

    # use aggregate functions
    total_wheels = flights_df.groupby('OP_CARRIER', 'DEST') \
                            .agg(countDistinct('ORIGIN').alias('Distinct_Origin'), sum('TAXI_IN').alias('sum_Taxi'))
    total_wheels.show()

    # Create Temp View and run spark.sql
    flights_df.createOrReplaceTempView('flights')
    total_wheels = spark.sql('''
    SELECT OP_CARRIER, DEST, SUM(WHEELS_ON) AS TOTAL_WHEELS
    FROM FLIGHTS
    GROUP BY OP_CARRIER, DEST''')
    total_wheels.show()

    # running total
    running_total_window = Window.partitionBy("ORIGIN") \
        .orderBy("DEP_TIME") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    flights_df.withColumn("RunningTotal",
                          sum("TAXI_IN").over(running_total_window)).show()
