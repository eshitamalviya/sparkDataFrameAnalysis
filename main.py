from pyspark.sql import *

if __name__ == "__main__":
    print("Hello")
    spark = SparkSession.builder\
            .appName("SparkAPP")\
            .master("local[2]")\
            .getOrCreate()

    data = [("Eshita",24),
            ("Hiral", 13),
            ("Mummy",44)]

    df = spark.createDataFrame(data).toDF("Name","Age")
    df.show()
