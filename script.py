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






    ###################################
    ### Additional Informations #######

        # [itv020752@g01 ~]$ hadoop fsck  data/students.csv -files -blocks -locations
        # WARNING: Use of this script to execute fsck is deprecated.
        # WARNING: Attempting to execute replacement "hdfs fsck" instead.

        # Connecting to namenode via http://m01.itversity.com:9870/fsck?ugi=itv020752&files=1&blocks=1&locations=1&path=%2Fuser%2Fitv020752%2Fdata%2F
        # students.csv
        # FSCK started by itv020752 (auth:SIMPLE) from /172.16.1.101 for path /user/itv020752/data/students.csv at Thu Jul 17 11:27:30 EDT 2025

        # /user/itv020752/data/students.csv 60201900 bytes, replicated: replication=3, 1 block(s):  OK
        # 0. BP-1685381103-172.16.1.103-1609223169030:blk_1100842491_27121754 len=60201900 Live_repl=3  [DatanodeInfoWithStorage[172.16.1.105:9866,DS
        # -cd1d8ab0-7d77-4607-98bf-961a7ad81f45,DISK], DatanodeInfoWithStorage[172.16.1.106:9866,DS-b1aa8def-bcd8-4514-8697-29c2f7fd008d,DISK], Datan
        # odeInfoWithStorage[172.16.1.107:9866,DS-53639da4-6786-42af-a4a6-5021150dddf3,DISK]]


        # Status: HEALTHY
        # Number of data-nodes:  3
        # Number of racks:               1
        # Total dirs:                    0
        # Total symlinks:                0

        # Replicated Blocks:
        # Total size:    60201900 B
        # Total files:   1
        # Total blocks (validated):      1 (avg. block size 60201900 B)
        # Minimally replicated blocks:   1 (100.0 %)
        # Over-replicated blocks:        0 (0.0 %)
        # Under-replicated blocks:       0 (0.0 %)
        # Mis-replicated blocks:         0 (0.0 %)
        # Default replication factor:    3
        # Average block replication:     3.0
        # Missing blocks:                0
        # Corrupt blocks:                0
        # Missing replicas:              0 (0.0 %)
        # Blocks queued for replication: 0

        # Erasure Coded Block Groups:
        # Total size:    0 B
        # Total files:   0
        # Total block groups (validated):        0
        # Minimally erasure-coded block groups:  0
        # Over-erasure-coded block groups:       0
        # Under-erasure-coded block groups:      0
        # Unsatisfactory placement block groups: 0
        # Average block group size:      0.0
        # Missing block groups:          0
        # Corrupt block groups:          0
        # Missing internal blocks:       0
        # Blocks queued for replication: 0
        # FSCK ended at Thu Jul 17 11:27:30 EDT 2025 in 1 milliseconds

        # itv020752@g01 ~]$ hadoop fs -du -h data
        # 348.1 M  1.0 G    data/bigLog.txt
        # 57.4 M   172.2 M  data/students.csv

        # 60201900B / 1024 /1024 = 57.4 MB