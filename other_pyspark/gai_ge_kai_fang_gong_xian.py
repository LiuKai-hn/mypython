#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 10:42
# Description:
# Question:
import sys
from imp import reload

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

reload(sys)
sys.setdefaultencoding('utf-8')

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .enableHiveSupport() \
    .getOrCreate()


# 需要检查每个表是否存在需要查询的字段
def check_column_exists(df, column_name):
    return column_name in df.columns


url = "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ"
table = "JCSJ.SJ_PHBDFB"
properties = {
    "user": "hjc",
    "password": "hjcCETC15",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
try:
    # 读取Oracle数据库中的数据
    sj_phbdfb = spark.read.jdbc(url=url, table=table, properties=properties)
    sj_phbdfb.createOrReplaceTempView("sj_phbdfb")
except Exception as e:
    print("===> 读取 sj_phbdfb 失败:", e)
finally:
    spark.stop()

try:
    bd_tblist = spark.sql(
        """select sjbm
           from sj_phbdfb 
           where bdm like '%优秀中国特色社会主义建设者%' or bdm like '%优秀中国特色社会主义建设者%'
           group by sjbm
        """).collect()

    bd_index_df = spark.sql(
        """select * 
           from sj_phbdfb 
           where bdm like '%优秀中国特色社会主义建设者%'
        """)

    bd_index_df.createOrReplaceTempView("bd_index_df")
    # 将 DataFrame 转换为 Python 列表
    table_names = [row[0] for row in bd_tblist]
    # 定义需要查询的字段列表
    columns = ["XM", "ZW", "QYBM", "QYMC", "BDBM", "ND", "SSSHI", "SSX"]
    # result_df
    schema = StructType([StructField(col, StringType(), True) for col in columns])
    bd_df = spark.createDataFrame([], schema=schema)
    # 循环遍历表
    for table_name in table_names:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
            .option("dbtable", table_name) \
            .option("user", "bdmd") \
            .option("password", "bdmdCETC15") \
            .load()
        print("==========>table_name " + table_name)

        select_columns = []
        for column in columns:
            if check_column_exists(df, column):
                select_columns.append(column)
            else:
                select_columns.append("NULL as " + column)
        new_df = df.selectExpr(select_columns)
        new_df.show()
        bd_df = bd_df.unionAll(new_df)

    # 显示查询结果

    bd_df.createOrReplaceTempView("bd_df")

    joined_df = spark.sql('''
       select t1.XM,
              t1.ZW,
              t1.QYBM,
              t1.QYMC,
              t1.BDBM,
              t2.DY,
              COALESCE(t1.ND,t2.ND, null) as nd,
              COALESCE(t2.DY||REPLACE(t1.SSSHI,t2.DY,'')||REPLACE(t1.SSX,t1.SSSHI,''),t2.DY, null) as tag_dist
       from bd_df t1 join bd_index_df t2 on t1.BDBM=t2.ID
       group by t1.XM,
                t1.ZW,
                t1.QYBM,
                t1.QYMC,
                t1.BDBM,
                t2.DY,
                COALESCE(t1.ND,t2.ND, null),
                COALESCE(t2.DY||REPLACE(t1.SSSHI,t2.DY,'')||REPLACE(t1.SSX,t1.SSSHI,''),t2.DY, null)
    ''')

    spark.sql("drop table if exists tmp_lk.bd_gai_ge_kai_fang_gong_xian")

    joined_df.write.mode("overwrite").saveAsTable("tmp_lk.bd_gai_ge_kai_fang_gong_xian")

    spark.sql('''
       insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01040101')
       select tt2.person_hkey,
              tt1.xm as nonpublic_name,
              current_timestamp() as tag_time, -- 标签产生时间
              '优秀中国特色社会主义建设者' as tag_name,
              '是' as tag_value, -- 标签值 
              tt1.dy as tag_level,
              tt1.tag_dist,
              tt1.nd as tag_year,
              current_timestamp() as change_time,
              null as id
       from tmp_lk.bd_gai_ge_kai_fang_gong_xian tt1 
       join tmp_person_info tt2 
       on tt1.xm=tt2.name and tt1.qymc=tt2.ENTERPRISE_NAME
    ''')
except Exception as e:
    print("===> 读取 sj_phbdfb 失败:", e)
finally:
    spark.stop()
