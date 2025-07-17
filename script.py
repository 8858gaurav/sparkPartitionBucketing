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
           .appName("weeek application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')

    #!hadoop fs -ls /user/itv020752/warehouse
    # No file has been now

    #!hadoop fs -mkdir data

    #!hadoop fs -ls data
    # No file has been now

    #!hadoop fs -put /data/trendytech/students.csv /user/itv020752/data

    #!hadoop fs -ls -h data
    # file has been aaded now

   student_schema = 'student_id Integer,exam_center_id Integer,subject String,year Integer, quarter Integer,score Integer,grade String'

   customer_df = spark.read.format("csv").schema(student_schema).option('header', True).load("/user/itv020752/data/students.csv")
   customer_df.show(5)

    # +----------+--------------+-------+----+-------+-----+-----+
    # |student_id|exam_center_id|subject|year|quarter|score|grade|
    # +----------+--------------+-------+----+-------+-----+-----+
    # |         1|             1|   Math|2005|      1|   41|    D|
    # |         1|             1|Spanish|2005|      1|   51|    C|
    # |         1|             1| German|2005|      1|   39|    D|
    # |         1|             1|Physics|2005|      1|   35|    D|
    # |         1|             1|Biology|2005|      1|   53|    C|
    # +----------+--------------+-------+----+-------+-----+-----+

   customer_df.rdd.getNumPartitions()
    # 12 
    # in local machine (gateway node), the size of this file students.csv is 58M
    # in hdfs the size of this file will become 57.4 MB, with RF = 3

    #!hadoop fs -mkdir -p writerOutputData/csv

   customer_df.write.format("csv").mode("overwrite").option("path", "/user/itv020752/writerOutputData/csv").save()
    # 4 type of mode: Overwrite, ignore, append, errorifexists

    #!hadoop fs -ls -h writerOutputData/csv
    # will create a 12 files in this case, with RF = 3, 11 files are having a size of 5.1 Mb, and 1 files having size of 1.1 MB

   customer_df.write.format("parquet").mode("overwrite").option("path", "/user/itv020752/writerOutputData/parquet").save()
    # 4 type of mode: Overwrite, ignore, append, errorifexists

    #!hadoop fs -ls -h writerOutputData/parquet
    # will create a 12 files in this case, with RF = 3, 11 files are having a size of 400 KB, and 1 files having size of 100 KB

   customer_df.write.format("json").mode("overwrite").option("path", "/user/itv020752/writerOutputData/json").save()
    # 4 type of mode: Overwrite, ignore, append, errorifexists

    #!hadoop fs -ls -h writerOutputData/json
    # will create a 12 files in this case, with RF = 3, 11 files are having a size of 18 to 19.6 MB, and 1 files having size of 4.1MB

   customer_df.write.format("orc").mode("overwrite").option("path", "/user/itv020752/writerOutputData/orc").save()
    # 4 type of mode: Overwrite, ignore, append, errorifexists

    #!hadoop fs -ls -h writerOutputData/orc
    # will create a 12 files in this case, with RF = 3, 11 files are having a size of 406 to 418 KB, and 1 files having size of 90KB