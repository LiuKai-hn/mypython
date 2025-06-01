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
from pyspark.sql.types import StructType, StructField, StringType

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


try:
    # 人员基本信息 数据写入hive
    person_base_info_df = spark.sql('''
    select person_hkey, -- hash主键
           name, -- 姓名 
           d1.zdxz as gender, -- 性别 
           d7.zdxz as countries_regions, -- 国籍
           d2.zdxz as nation, -- 民族 
           d3.zdxz as native_place, -- 籍贯 
           d4.zdxz as birth_place, -- 出生地 
           year(current_date())-year(from_unixtime(unix_timestamp(birthdate, 'yyyy/MM/dd'), 'yyyy-MM-dd'))
           +if(datediff(concat(year(current_date()),substr(from_unixtime(unix_timestamp(birthdate, 'yyyy/MM/dd'), 'yyyy-MM-dd'),5,6)),current_date())<0,1,0) as age,
           d5.zdxz as politics_status, -- 政治面貌 
           d6.zdxz as hk_and_macaoidentity, -- 港澳身份 
           id_card -- 身份证号
    from  tmp_lk.sat_person_basic_info_rd t1 
    left join tmp_lk.DM_SJZD d1 on t1.gender_code=d1.zdxbm
    left join tmp_lk.DM_SJZD d2 on t1.nation_code=d2.zdxbm
    left join tmp_lk.DM_SJZD d3 on t1.native_place_code=d3.zdxbm
    left join tmp_lk.DM_SJZD d4 on t1.birth_place_code=d4.zdxbm
    left join tmp_lk.DM_SJZD d5 on t1.politics_status_code=d5.zdxbm
    left join tmp_lk.DM_SJZD d6 on t1.hk_and_macaoidentity_code=d6.zdxbm
    left join tmp_lk.DM_SJZD d7 on t1.countries_regions_code=d7.zdxbm
    ''')
    person_base_info_df.createOrReplaceTempView("person_base_info_df")
    print(" =====>人员基本信息临时表创建成功 ")
except Exception as e:
    print(" =====>人员基本信息 数据写入hive 失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010101')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '性别' as tag_name,
       gender, -- 性别代码
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 性别 =====> 标签处理成功")
except Exception as e:
    print(" 性别 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010102')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '国籍' as tag_name,
       countries_regions, -- 国籍
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 国籍 =====> 标签处理成功")

except Exception as e:
    print(" 国籍 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010103')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '民族' as tag_name,
       nation, -- 民族
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 民族 =====> 标签处理成功")

except Exception as e:
    print(" 民族 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010104')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '籍贯' as tag_name,
       native_place, -- 籍贯
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 籍贯 =====> 标签处理成功")

except Exception as e:
    print(" 籍贯 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010105')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '出生地' as tag_name,
       birth_place, -- 出生地 
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 出生地 =====> 标签处理成功")

except Exception as e:
    print(" 出生地 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010106')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '年龄段' as tag_name,
       -- age,
       concat(floor(age/5)*5,"-",ceil(age/5)*5) as tag_value, -- 年龄段  
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 年龄段 =====> 标签处理成功")

except Exception as e:
    print(" 年龄段 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010107')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '政治面貌' as tag_name,
       politics_status, -- 政治面貌 
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 政治面貌 =====> 标签处理成功")

except Exception as e:
    print(" 政治面貌 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T01010108')
select person_hkey as nonpublic_id, -- hash主键
       name as nonpublic_name, -- 姓名 
       current_timestamp() as tag_time, -- 标签产生时间
       '港澳身份' as tag_name,
       hk_and_macaoidentity, -- 港澳身份 
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id
from person_base_info_df
''')
    print(" 港澳身份 =====> 标签处理成功")

except Exception as e:
    print(" 港澳身份 =====> 标签处理失败")
    print(e)
    sys.exit(1)

spark.stop()
