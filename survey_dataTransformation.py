import sys, re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey, count_by_country

# Using UDF
def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == "__main__":
        spark = SparkSession \
                .builder \
                .appName('Survey') \
                .master('local[3]') \
                .getOrCreate()

        logger = Log4j(spark)

          #Instead of hard coding the file path give it as a system parameter
            # Run > Edit Configurations... > Parameters (provide file path)
            #If path is not found, below code will log an error and exit the application
        print("sys.argv : \n", sys.argv)
        if len(sys.argv) != 2:
            logger.error("Error: Cannot fine file")
            sys.exit(-1)

        # Read dataSourceFiles
        survey_df = load_survey(spark, sys.argv[1])
        survey_df.show()

        # # Transformation 1
        # # After repartition operation also add shuffle.partitions in spark.conf
        # partitioned_df = survey_df.repartition(2)
        # count_df = count_by_country(partitioned_df)
        # logger.info(count_df.collect()) # returns dataframe as python list

        # # To read all the configuratons from spark.conf
        # conf_out = spark.sparkContext.getConf()
        # # To debug
        # logger.info(conf_out.toDebugString())

        survey_df.show(10)

        parse_gender_udf = udf(parse_gender, returnType=StringType())
        logger.info("Catalog Entry:")
        [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
        [logger.info(r.name) for r in spark.catalog.listFunctions()]


        survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
        survey_df2.show(10)

        spark.udf.register("parse_gender_udf", parse_gender, StringType())
        logger.info("Catalog Entry:")
        [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

        survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
        survey_df3.show(10)