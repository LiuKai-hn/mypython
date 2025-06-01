#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/17 16:36
# Description:
# Question: 人员非榜单标签

'''
SAT_MEMBER_ENT_RD
LINK_ENT_MEM_ENT
sat_ent_basic_info_rd
SAT_FIC_DUTY_INFO
LINK_ORG_PERSON
DM_SJZD
tmp_person_info
SAT_ENT_ESTABLISHORG_INFO


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


try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020101')
select b.ENTERPRISE_HKEY,
       c.ENTERPRISE_NAME,
       current_timestamp() as tag_time, -- 标签产生时间
       '会员状态' as tag_name,
       '是' as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from tmp_lk.SAT_MEMBER_ENT_RD a 
join tmp_lk.LINK_ENT_MEM_ENT b on a.MEM_ENTERPRISE_HKEY=b.MEM_ENTERPRISE_HKEY
join tmp_lk.sat_ent_basic_info_rd c on b.ENTERPRISE_HKEY = c.ENTERPRISE_HKEY
where a.MEMBER_TYPE='1'
    ''')
    print(" 会员状态 =====> 标签处理成功")

except Exception as e:
    print(" 会员状态 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)




try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020102')
select per_ent.ENTERPRISE_HKEY,
       per_ent.ENTERPRISE_NAME,
       current_timestamp() as tag_time, -- 标签产生时间
       '会长企业' as tag_name,
       '是' as tag_value, -- 标签值 
       t1.tag_level,
       t1.tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from (select * from tmp_lk.SAT_FIC_DUTY_INFO where ON_THE_JOB='1') a  -- 这里只有在职
join tmp_lk.LINK_ORG_PERSON b on a.org_per_hkey=b.org_per_hkey 
join (select * from tmp_lk.DM_SJZD where zdxz like '%会长') d on a.duty_code=d.zdxbm
join tmp_lk.tmp_person_info per_ent on a.person_hkey=per_ent.person_hkey
    ''')
    print(" 会长企业 =====> 标签处理成功")

except Exception as e:
    print(" 会长企业 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)


try:
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020201')
select b.ENTERPRISE_HKEY,
       c.ENTERPRISE_NAME,
       current_timestamp() as tag_time, -- 标签产生时间
       '商会会员类型' as tag_name,
       '商会会员' as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from tmp_lk.SAT_MEMBER_ENT_RD a 
join tmp_lk.LINK_ENT_MEM_ENT b on a.MEM_ENTERPRISE_HKEY=b.MEM_ENTERPRISE_HKEY
join tmp_lk.sat_ent_basic_info_rd c on b.ENTERPRISE_HKEY = c.ENTERPRISE_HKEY
where a.MEMBER_TYPE='2'
 ''')
    print(" 商会会员类型 =====> 标签处理成功")

except Exception as e:
    print(" 商会会员类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)


try:
    qiye_base_info = spark.sql('''
select a.ENTERPRISE_HKEY,
       b.enterprise_name,
       case when PARTY_STRUCTURE='0' then '未知'
            when PARTY_STRUCTURE='1' then '党委'
            when PARTY_STRUCTURE='2' then '党总支'
            else '党支部' end as PARTY_STRUCTURE, -- 党组织结构
       concat(floor(PARTY_MEMBER_NUM/5)*5,"-",ceil(PARTY_MEMBER_NUM/5)*5) as PARTY_MEMBER_NUM, -- 党员人数
       IS_LABOR_UNION, -- 是否建立工会组织
       concat(floor(LABOR_UNION_MEMBER_NUM/5)*5,"-",ceil(LABOR_UNION_MEMBER_NUM/5)*5) as LABOR_UNION_MEMBER_NUM, -- 工会会员人数
       IS_LEAGUE_ORGANIZATION, -- 是否建立共青团组织
       concat(floor(LEAGUE_MEMBER_NUM/5)*5,"-",ceil(LEAGUE_MEMBER_NUM/5)*5) as LEAGUE_MEMBER_NUM -- 共青团团员人数
from tmp_lk.SAT_ENT_ESTABLISHORG_INFO a 
join tmp_lk.sat_ent_basic_info_rd b on a.ENTERPRISE_HKEY=b.ENTERPRISE_HKEY
''')
    print(" 党组织类型 =====> 标签处理成功")

except Exception as e:
    print(" 党组织类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

qiye_base_info.createOrReplaceTempView("qiye_base_info")

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020301')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '党组织类型' as tag_name,
       PARTY_STRUCTURE as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 党组织类型 =====> 标签处理成功")

except Exception as e:
    print(" 党组织类型 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

# 由于单位不统一：万元、美元、港元、欧元、壹亿 ... 还有没有单位的，还有地址信息的
try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020302')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '党员人数规模' as tag_name,
       PARTY_MEMBER_NUM as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 党员人数规模 =====> 标签处理成功")

except Exception as e:
    print(" 党员人数规模 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

# 字段为null 无法判断 在营、注销……
try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020401')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '建立工会组织' as tag_name,
       '是' as tag_value, -- 字段为null 无法判断 在营、注销……
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
where IS_LABOR_UNION='1'
''')
    print(" 建立工会组织 =====> 标签处理成功")

except Exception as e:
    print(" 建立工会组织 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020402')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '会员人数规模' as tag_name,
       LABOR_UNION_MEMBER_NUM as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 会员人数规模 =====> 标签处理成功")

except Exception as e:
    print(" 会员人数规模 =====> 标签处理失败")
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
       '建立团组织' as tag_name,
       '是' as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
where IS_LEAGUE_ORGANIZATION='1'
''')
    print(" 建立团组织 =====> 标签处理成功")

except Exception as e:
    print(" 建立团组织 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

try:
    # 数据写入指定分区
    spark.sql('''
insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02020502')
select ENTERPRISE_HKEY,
       ENTERPRISE_NAME as nonpublic_name,
       current_timestamp() as tag_time, -- 标签产生时间
       '团员人数规模' as tag_name,
       LEAGUE_MEMBER_NUM as tag_value,
       null as tag_level,
       null as tag_dist,
       null as tag_year,
       current_timestamp() as change_time,
       null as id 
from qiye_base_info
''')
    print(" 团员人数规模 =====> 标签处理成功")

except Exception as e:
    print(" 团员人数规模 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

spark.stop()
