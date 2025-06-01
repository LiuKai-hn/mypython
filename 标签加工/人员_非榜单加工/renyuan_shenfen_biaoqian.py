#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/16 16:36
# Description:
# Question: 人员非榜单标签

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
    .appName("行业代表") \
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
    DM_SJZD = spark.read.jdbc(url=url, table="JCSJ.DM_SJZD", properties=properties)
    DM_SJZD.createOrReplaceTempView("DM_SJZD")

except Exception as e:
    print(" orcale 读取表 DM_SJZD =====> 失败")
    print(e)
    spark.stop()

try:
    SAT_EXE_COMMITTEE_INFO = spark.read.jdbc(url=url, table="JCSJ.SAT_EXE_COMMITTEE_INFO", properties=properties)
    SAT_EXE_COMMITTEE_INFO.createOrReplaceTempView("SAT_EXE_COMMITTEE_INFO")

except Exception as e:
    print(" orcale 读取表 SAT_EXE_COMMITTEE_INFO =====> 失败")
    print(e)
    spark.stop()

try:
    LINK_PER_EXE = spark.read.jdbc(url=url, table="JCSJ.LINK_PER_EXE", properties=properties)
    LINK_PER_EXE.createOrReplaceTempView("LINK_PER_EXE")

except Exception as e:
    print(" orcale 读取表 LINK_PER_EXE =====> 失败")
    print(e)
    spark.stop()

try:
    SAT_MEMBER_PERSON = spark.read.jdbc(url=url, table="JCSJ.SAT_MEMBER_PERSON", properties=properties)
    SAT_MEMBER_PERSON.createOrReplaceTempView("SAT_MEMBER_PERSON")

except Exception as e:
    print(" orcale 读取表 SAT_MEMBER_PERSON =====> 失败")
    print(e)
    spark.stop()

try:
    LINK_PER_MEM_P = spark.read.jdbc(url=url, table="JCSJ.LINK_PER_MEM_P", properties=properties)
    LINK_PER_MEM_P.createOrReplaceTempView("LINK_PER_MEM_P")

except Exception as e:
    print(" orcale 读取表 LINK_PER_MEM_P =====> 失败")
    print(e)
    spark.stop()

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01020101')
select PERSON_HKEY,
       nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '人员类型' as tag_name,
       tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from 
(
    select t1.PERSON_HKEY,
           t2.name as nonpublic_name,
           concat_ws(',',collect_set(t1.zdxz)) as tag_value
    from 
    (
        -- 执常委信息表
        select t2.PERSON_HKEY,
               d1.zdxz 
        from SAT_EXE_COMMITTEE_INFO t1 
        join DM_SJZD d1 on t1.COMMITTEE_PER_TYPE_CODE=d1.zdxbm
        join LINK_PER_EXE t2 on t1.EXECUTIVE_HKEY=t2.EXECUTIVE_HKEY
        union all 
        -- 个人会员信息表
        SELECT t2.PERSON_HKEY,
               d1.zdxz 
        FROM SAT_MEMBER_PERSON t1 
        join DM_SJZD d1 on t1.PERSON_TYPE=d1.zdxbm 
        join LINK_PER_MEM_P t2 on t1.MEM_PERSON_HKEY=t2.MEM_PERSON_HKEY
        -- 缺少 代表类型 表 
    ) t1 
    join tmp_lk.sat_person_basic_info_rd t2 on t1.person_hkey=t2.person_hkey
    group by t1.PERSON_HKEY,
             t2.name
) tt 
''')
    print(" 人员类型 =====> 标签处理成功")

except Exception as e:
    print(" 人员类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

print("========= 清除多余的 df,避免占用空间 =========")

# 清理 SAT_EXE_COMMITTEE_INFO
SAT_EXE_COMMITTEE_INFO.unpersist()
# 清理 SAT_EXE_COMMITTEE_INFO
spark.catalog.dropTempView("SAT_EXE_COMMITTEE_INFO")

# 清理 LINK_PER_EXE
LINK_PER_EXE.unpersist()
# 清理 LINK_PER_EXE
spark.catalog.dropTempView("LINK_PER_EXE")

# 清理 SAT_MEMBER_PERSON
SAT_MEMBER_PERSON.unpersist()
# 清理 SAT_MEMBER_PERSON
spark.catalog.dropTempView("SAT_MEMBER_PERSON")

# 清理 LINK_PER_MEM_P
LINK_PER_MEM_P.unpersist()
# 清理 LINK_PER_MEM_P
spark.catalog.dropTempView("LINK_PER_MEM_P")


print("==== 职位标签计算开始 =========")

try:
    SAT_FIC_DUTY_INFO = spark.read.jdbc(url=url, table="JCSJ.SAT_FIC_DUTY_INFO", properties=properties)
    SAT_FIC_DUTY_INFO.createOrReplaceTempView("SAT_FIC_DUTY_INFO")

except Exception as e:
    print(" orcale 读取表 SAT_FIC_DUTY_INFO =====> 失败")
    print(e)
    spark.stop()

try:
    LINK_ORG_PERSON = spark.read.jdbc(url=url, table="JCSJ.LINK_ORG_PERSON", properties=properties)
    LINK_ORG_PERSON.createOrReplaceTempView("LINK_ORG_PERSON")

except Exception as e:
    print(" orcale 读取表 LINK_ORG_PERSON =====> 失败")
    print(e)
    spark.stop()

try:
    sat_org_basic_info_rd = spark.read.jdbc(url=url, table="JCSJ.sat_org_basic_info_rd", properties=properties)
    sat_org_basic_info_rd.createOrReplaceTempView("sat_org_basic_info_rd")

except Exception as e:
    print(" orcale 读取表 sat_org_basic_info_rd =====> 失败")
    print(e)
    spark.stop()

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01020201')
select t1.person_hkey,
       t2.name as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '职位' as tag_name,
       t1.tag_value,
       t1.tag_level,
       t1.tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from 
(       
    select b.person_hkey,
           'T01020201' as tag_code, -- 
           d.zdxz as tag_value, -- 标签值 
           c.admini_level_code,
           d2.zdxz as tag_level,
           null as tag_year,
           c.ORG_NAME as tag_dist
    from (select * from SAT_FIC_DUTY_INFO where ON_THE_JOB='1') a  -- 这里只有在职
    left join LINK_ORG_PERSON b on a.org_per_hkey=b.org_per_hkey 
    left join sat_org_basic_info_rd c on b.org_hkey=c.org_hkey 
    left join DM_SJZD d on a.duty_code=d.zdxbm
    left join DM_SJZD d2 on c.admini_level_code=d2.zdxbm
) t1  
left join tmp_lk.sat_person_basic_info_rd t2 on t1.person_hkey=t2.person_hkey 
    ''')
    print(" 职位类型 =====> 标签处理完成")
except Exception as e:
    print(" 职位类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

spark.stop()
