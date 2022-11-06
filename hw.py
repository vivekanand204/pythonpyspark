from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
      spark = SparkSession.builder.master("local[*]").appName("with").getOrCreate()
      # df =spark.read.load(r"C:\Users\Admin\Desktop\hive pract\input data\sparktext.txt" ,format="text")
      #
      # df.show()
      # df2= df.withColumn("words" ,size(split(col('value') ," ")))
      #
      # df2.agg({"words" :"sum"}).show()




      header = StructType([
         StructField("id", IntegerType()),
         StructField("name", StringType()),
         StructField("age", IntegerType()),
         StructField("city", StringType()),
         StructField("salary", DoubleType())
            ])

      df3 = spark.read.load(r"C:\Users\Admin\Desktop\pythonpyspark\inputData\employee.csv", format="csv",schema=header)
#Q1.
      df3.agg({"salary":'sum'}).show()
#Q2
      df3.select(max("salary")).show()
#Q3.
      df3.filter(df3.age > 30).show()

