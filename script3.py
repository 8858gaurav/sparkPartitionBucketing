import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")


    spark = SparkSession \
           .builder \
           .appName("weeeknew application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    student_schema = 'student_id Integer,exam_center_id Integer,subject String,year Integer, \
    quarter Integer,score Integer,grade String'

    customer_df = spark.read.format("csv").schema(student_schema).load("/user/itv020752/writerOutputData/csv/*.csv")
    customer_df.show(5)

    # +----------+--------------+----------+----+-------+-----+-----+
    # |student_id|exam_center_id|   subject|year|quarter|score|grade|
    # +----------+--------------+----------+----+-------+-----+-----+
    # |    150855|             5| Chemistry|2012|      1|   43|    D|
    # |    150855|             5|Philosophy|2012|      1|   77|    A|
    # |    150855|             5|Modern Art|2012|      1|   56|    C|
    # |    150855|             5| Geography|2012|      1|   67|    B|
    # |    150856|             5|   English|2012|      1|   74|    B|
    # +----------+--------------+----------+----+-------+-----+-----+

    spark.sql("create database if not exists itv020752_db").show()

    customer_df.write.format("csv").mode("overwrite").bucketBy(4, "exam_center_id") \
    .option("path", "/user/itv020752/writerOutputData/csv/bucketing_data").saveAsTable("itv020752_db.customer_bucketing")
    # 4 type of mode: Overwrite, ignore, append, errorifexists
    # save will not work here, we need to use saveAsTable method
    # this will also work, our database is in !hadoop fs -ls warehouse/itv020752_db.db, and our table will get created under
    # !hadoop fs -ls /user/itv020752/writerOutputData/csv/bucketing_data 
    # managed table will be created under this location: !hadoop fs -ls /user/itv020752/writerOutputData/csv/bucketing_data 
    # but our database location is in: !hadoop fs -ls warehouse/

    customer_df.write.format("csv").mode("overwrite").bucketBy(4, "exam_center_id") \
    .saveAsTable("itv020752_db.customer_bucketing")
    # 4 type of mode: Overwrite, ignore, append, errorifexists
    # save will not work here, we need to use saveAsTable method
    # our database is in !hadoop fs -ls warehouse/itv020752_db.db, and our table will get created under
    # !hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db
    # managed table will be created under this location: !hadoop fs -ls /user/itv020752/warehouse/itv020752_db.db
    # but our database location is in: !hadoop fs -ls warehouse/