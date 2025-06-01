#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/17 16:36
# Description:
# Question: 人员非榜单标签

'''
SAT_ENT_PARTNERS_INFO
LINK_PER_ENT
sat_person_basic_info_rd
'''

import sys
from imp import reload
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

# 创建 SparkSession
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .appName("qiye_jiben_xinxi") \
    .enableHiveSupport() \
    .config(conf=conf) \
    .getOrCreate()

url = "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ"
properties = {
    "user": "hjc",
    "password": "hjcCETC15",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

try:
    SAT_ENT_PARTNERS_INFO = spark.read.jdbc(url=url, table="JCSJ.SAT_ENT_PARTNERS_INFO", properties=properties)
    SAT_ENT_PARTNERS_INFO.createOrReplaceTempView("SAT_ENT_PARTNERS_INFO")

except Exception as e:
    print(" orcale 读取表 SAT_ENT_PARTNERS_INFO =====> 失败")
    print(e)
    spark.stop()

try:
    LINK_PER_ENT = spark.read.jdbc(url=url, table="JCSJ.LINK_PER_ENT", properties=properties)
    LINK_PER_ENT.createOrReplaceTempView("LINK_PER_ENT")

except Exception as e:
    print(" orcale 读取表 LINK_PER_ENT =====> 失败")
    print(e)
    spark.stop()

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01020202')
select t2.PERSON_HKEY,
       t3.name as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '股东' as tag_name,
       '是' as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from 
(select PER_ENT_HKEY from SAT_ENT_PARTNERS_INFO where STOCK_TYPE='自然人股') t1
join LINK_PER_ENT t2 on t1.PER_ENT_HKEY=t2.PER_ENT_HKEY
join tmp_lk.sat_person_basic_info_rd t3 on t2.person_hkey=t3.person_hkey 
''')
    print(" 股东 =====> 标签处理成功")

except Exception as e:
    print(" 股东 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

spark.stop()
