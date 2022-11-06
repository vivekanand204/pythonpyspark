from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("data frame").getOrCreate()

    data1 = [("ram","male"),("sham","male"),("kiran","female")]
    header = ["name", "gender"]

#creating dataframe from existing rdd
    rdd1 = spark.sparkContext.parallelize(data1)
    #in_df = rdd1.toDF()
    #in_df.show()

#creating dataframe from existing rdd with column name
    in_df1 = rdd1.toDF(header)
    # in_df1.show()
# to get datetype
    #in_df1.printSchema()

 #create dataFrame
    #in_df2 = spark.createDataFrame(data1,header)
    #in_df2.show()

    # create DataFrame from input fille
    csv_df = spark.read.csv(r"C:\Users\Admin\Desktop\pythonpyspark\inputData\employee.csv",schema="id int, name string, city string, salary int")
    csv_df.show()
    csv_df.printSchema()