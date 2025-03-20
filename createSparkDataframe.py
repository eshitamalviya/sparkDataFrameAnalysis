from pyspark.sql import *

# CREATE ENTRY POINT TO PROGRAMMING SPARK DATASETS AND DATAFRAME API
spark = SparkSession.builder\
            .appName("SparkAPP")\
            .master("local[2]")\
            .getOrCreate()

# CREATE A SPARK DATAFRAME
countries_df = spark.read\
            .format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .load("C:/Users/HP/Downloads/Countries/countries.csv")
# countries_df.show(10)

# CREATE A GLOBAL TEMP VIEW TO USE SQL QUERIES
countries_df.createGlobalTempView("countries_df_gtv")

get_id_name = spark.sql("select country_id, name from global_temp.countries_df_gtv limit 10") #GLOBAL TEMP VIEWS ARE STORED IN global_temp folder
get_id_name.show()

