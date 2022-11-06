from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("with").getOrCreate()

    header = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("city", StringType()),
        StructField("salary", DoubleType())
    ])

    df = spark.read.load(r"C:\Users\Admin\Desktop\pyspark\employee1.txt",format="csv",schema=header)

    #df.printSchema()
    #df.show()

    df.withColumn("state", lit("MH"))
    df.printSchema()
    df.show()

