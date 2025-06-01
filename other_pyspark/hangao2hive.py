#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2024/05/08 9:51
# Description:
# Question: 迁移瀚高库2hive库
'''
-- 瀚高获取表注释sql
SELECT
    c.relname AS "表名",
    d.description AS "表注释"
FROM
    pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
WHERE
    n.nspname ='jcsjk' and c.relkind = 'r' -- 只查询普通表


-- 表字段注释
SELECT
    a.attname AS "字段名",
    d.description AS "字段注释"
FROM
    pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE
    n.nspname ='jcsjk'
    AND a.attnum > 0
    AND NOT a.attisdropped
    and c.relname='sat_cc_basic_info'
'''



import traceback
import sys
from pyspark.sql import SparkSession



# 封装方法，用于截取数组
# 检查参数是否存在
if len(sys.argv) < 1:
    print("please input table_name")
    sys.exit(1)

table_name = sys.argv[1]

spark = SparkSession.builder \
    .appName("oracle2hive") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    data_df = spark.read.format("jdbc")\
        .option("url", "jdbc:highgo://172.18.13.11:5866/jcsjk500")\
        .option("dbtable", table_name)\
        .option("driver", "com.highgo.jdbc.Driver")\
        .option("user", "sysdba")\
        .option("password", r"Hello@123")\
        .load()

    data_df.createOrReplaceTempView("data_df")
    count_result = spark.sql("select count(1) as count_result from data_df ").collect()[0]["count_result"]
    print("table_name ===> " + str(count_result))
except Exception as e:
    traceback.print_exc()
    print("====> Error occurred for table: " + table_name + ". Skipping to the next table.")
