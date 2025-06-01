#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 10:42
# Description:
# Question:
from imp import reload

from pyspark.sql import SparkSession
import sys

from pyspark.sql.types import StructType, StructField, StringType

reload(sys)
sys.setdefaultencoding('utf-8')

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .enableHiveSupport() \
    .getOrCreate()


url = "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ"
table = "JCSJ.SJ_PHBDFB"
properties = {
    "user": "hjc",
    "password": "hjcCETC15",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# 读取Oracle数据库中的数据
df = spark.read.jdbc(url=url, table=table, properties=properties)
df.createOrReplaceTempView("sj_phbdfb")

bd_tblist=spark.sql(
    """select sjbm 
       from sj_phbdfb 
       where bdm like '%优秀中国特色社会主义建设者%'
       group by sjbm
    """).collect()


bd_url = "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ"

bd_properties = {
    "user": "bdmd",
    "password": "bdmdCETC15",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
# 将 DataFrame 转换为 Python 列表
table_names = [row[0] for row in bd_tblist]
# 定义需要读取的表名列表和字段列表
select_columns = ["xm", "qybm", "qymc"]
schema = StructType([
    StructField("xm", StringType(), True),  # 姓名
    StructField("qybm", StringType(), True),  # 企业编码
    StructField("qymc", StringType(), True)  # ,  # 企业名称
    # StructField("nd", StringType(), True)  # 年度
])
# 定义一个空的 DataFrame
all_bd_df = spark.createDataFrame([], schema=schema)

# 循环读取每个表，并选择需要的字段
for table_name in table_names:
    print("ready ====> " + table_name)
    table_df = spark.read.jdbc(url=bd_url, table=table_name, properties=bd_properties)
    # 输出表头
    print(table_name, table_df.columns)
    select_df = table_df.select("xm", "qybm", "qymc")
    all_bd_df = all_bd_df.union(select_df)

# 获取 DataFrame 的行数
count = all_bd_df.count()

# 打印行数
print("=======> DataFrame 的行数为：", count)

# 关闭SparkSession
spark.stop()
