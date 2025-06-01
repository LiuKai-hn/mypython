#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 10:42
# Description:
# Question:
import sys
from imp import reload
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

# 检查参数是否存在
if len(sys.argv) < 2:
    print("参数缺失，请输入 orcale 表名 和 hive表名 ，eg: jcsj.DM_SJZD tmp_lk.DM_SJZD")
    sys.exit(1)

# 获取参数
oracle_table_name = sys.argv[1]
hive_table_name = sys.argv[2]

hdfs_path = "/apps/hive/warehouse/tmp_lk.db/" + hive_table_name.split('.')[1]

print('oracle_table_name=====>', oracle_table_name)
print('hive_table_name=====>', hive_table_name)
print('hdfs_path=====>', hdfs_path)

conf = SparkConf()
sc = SparkContext(conf=conf)

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

url = "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ"
# table = "JCSJ.SJ_PHBDFB"
properties = {
    "user": "hjc",
    "password": "hjcCETC15",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# 读取Oracle数据库中的数据
df = spark.read.jdbc(url=url, table=oracle_table_name, properties=properties)

# df.coalesce(4).write \
#     .mode("overwrite") \
#     .option("delimiter", "\t") \
#     .option("header", "false") \
#     .csv(hdfs_path)


df.coalesce(4)\
    .write\
    .mode("overwrite")\
    .option("delimiter", "\t")\
    .option("header", "false")\
    .option("compression", "none")\
    .format("text").saveAsTable(hive_table_name)
# 关闭SparkSession
spark.stop()
