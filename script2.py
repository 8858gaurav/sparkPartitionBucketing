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

   # Read the code for this output from script.py : /user/itv020752/writerOutputData/csv

   #!hadoop fs -ls /user/itv020752/writerOutputData/csv
   # Found 13 items, along with 1 success file
   # 12 files in this case, with RF = 3, 11 files are having a size of 5.1 Mb, and 1 files having size of 1.1 MB

    student_schema = 'student_id Integer,exam_center_id Integer,subject String,year Integer, \
    quarter Integer,score Integer,grade String'

    customer_df = spark.read.format("csv").schema(student_schema).load("/user/itv020752/writerOutputData/csv")
    customer_df.show(5)
    customer_df.rdd.getNumPartitions() # 12
    # +----------+--------------+----------+----+-------+-----+-----+
    # |student_id|exam_center_id|   subject|year|quarter|score|grade|
    # +----------+--------------+----------+----+-------+-----+-----+
    # |    150855|             5| Chemistry|2012|      1|   43|    D|
    # |    150855|             5|Philosophy|2012|      1|   77|    A|
    # |    150855|             5|Modern Art|2012|      1|   56|    C|
    # |    150855|             5| Geography|2012|      1|   67|    B|
    # |    150856|             5|   English|2012|      1|   74|    B|
    # +----------+--------------+----------+----+-------+-----+-----+

    customer_df.createOrReplaceTempView("customer")
    spark.sql("select count(*) from customer where subject = 'Math'").show()
    # it will hit all the big 12 files from this folder to get the counts: /user/itv020752/writerOutputData/csv. 

    spark.sql("show tables").filter("tableName = 'customer'").show()
    # +--------+---------+-----------+
    # |database|tableName|isTemporary|
    # +--------+---------+-----------+
    # |        | customer|       true|
    # +--------+---------+-----------+


    ##########################################################################################################

    ##########################################################################################################

    # doing the same thing with the help of partitioning
    #!hadoop fs -mkdir /user/itv020752/writerOutputData/csv/partition_data

    customer_df.write.format("csv").mode("overwrite") \
    .partitionBy("Subject") \
    .option("path", "/user/itv020752/writerOutputData/csv/partition_data").save()
    # 4 type of mode: Overwrite, ignore, append, errorifexists
    # use partitonBy only, when we have a less no of distinct values.

    #!hadoop fs -ls /user/itv020752/writerOutputData/csv/partition_data
    # output of this is: 14, spark.sql("select count(distinct Subject) from customer").show()
    # it has a 15 folder, 14 (No of distict values under Subject) + 1 succes files.

    #!hadoop fs -ls /user/itv020752/writerOutputData/csv/partition_data/Subject=Spanish
    # it has 12 files in every differnt subject folder, since we have a 12 partitions, customer_df.rdd.getNumPartitions(): 12

    # now read the partition data
    customer_df_new = spark.read.format("csv").schema(student_schema).load("/user/itv020752/writerOutputData/csv/partition_data")

    customer_df_new.createOrReplaceTempView("customer_new")
    spark.sql("select count(*) from customer_new where subject = 'Math'")
    # now it will read all the small files from the Math folder only, math folder has 12 files, so it will hit 12 files.

    spark.sql("show tables").filter("tableName = 'customer_new'").show()
    # +--------+------------+-----------+
    # |database|   tableName|isTemporary|
    # +--------+------------+-----------+
    # |        |customer_new|       true|
    # +--------+------------+-----------+