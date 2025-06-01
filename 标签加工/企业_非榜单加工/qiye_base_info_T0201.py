#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/17 16:36
# Description:
# Question: 人员非榜单标签

'''
sat_ent_basic_info_rd
DM_SJZD
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
    .appName("qiye_base_info_T0201") \
    .enableHiveSupport() \
    .config(conf=conf) \
    .getOrCreate()

qiye_base_info = spark.sql('''
select a.ENTERPRISE_HKEY,
       a.ENTERPRISE_NAME,
       -- a.ESTABLISH_TIME, -- 成立时间
       year(current_date())-year(from_unixtime(unix_timestamp(a.ESTABLISH_TIME, 'yyyy/MM/dd'), 'yyyy-MM-dd'))
       +if(datediff(concat(year(current_date()),substr(from_unixtime(unix_timestamp(a.ESTABLISH_TIME, 'yyyy/MM/dd'), 'yyyy-MM-dd'),5,6)),current_date())<0,1,0) as ESTABLISH_YEARS,
       a.REGISTERED_FUND, -- 注册资金
       a.OPERATING_STATUS, -- 经营状态
       d1.zdxz as hang_ye_lei_xing, -- 行业类型
       -- a.ENTERPRISE_BASISTYPE, -- 企业分类  企业基础分类   还是个空字段 ,以后会加进来  
       d2.zdxz as qi_ye_xing_zhi-- 企业性质   企业类型
from tmp_lk.SAT_ENT_BASIC_INFO_rd a 
join tmp_lk.DM_SJZD d1 on a.INDUSTRY = d1.zdxbm 
join tmp_lk.DM_SJZD d2 on a.BUSINESS_REGISTRATION_TYPE = d2.zdxbm 
''')

qiye_base_info.createOrReplaceTempView("qiye_base_info")

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02010101')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '成立年限' as tag_name,
       concat(floor(ESTABLISH_YEARS/5)*5,"-",ceil(ESTABLISH_YEARS/5)*5,'年') as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 成立年限 =====> 标签处理成功")

except Exception as e:
    print(" 成立年限 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

# 由于单位不统一：万元、美元、港元、欧元、壹亿 ... 还有没有单位的，还有地址信息的
try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02010102')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '注册资本规模' as tag_name,
       concat(floor(REGISTERED_FUND/500)*500,"-",ceil(REGISTERED_FUND/500)*500,'万元') as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 注册资本规模 =====> 标签处理成功")

except Exception as e:
    print(" 注册资本规模 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

# 字段为null 无法判断 在营、注销……
try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02010103')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '经营状态' as tag_name,
       OPERATING_STATUS as tag_value, -- 字段为null 无法判断 在营、注销……
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 经营状态 =====> 标签处理成功")

except Exception as e:
    print(" 经营状态 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02010201')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '企业性质' as tag_name,
       qi_ye_xing_zhi as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 企业性质 =====> 标签处理成功")

except Exception as e:
    print(" 企业性质 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

# 企业分类 目前没有这个字段


try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02010203')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '行业类型' as tag_name,
       hang_ye_lei_xing as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 行业类型 =====> 标签处理成功")

except Exception as e:
    print(" 行业类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

spark.stop()
