#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/21 16:36
# Description:
# Question: 企业标签 龙盾加工逻辑

import sys
from imp import reload
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

# 创建 SparkSession
conf = SparkConf()
conf.set("hive.exec.dynamic.partition", "true")
conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .appName("long_dun_jia_gong") \
    .enableHiveSupport() \
    .config(conf=conf) \
    .getOrCreate()

try:
    diqu_df = spark.sql('''
     select ENTERPRISE_HKEY,
            ENTERPRISE_NAME,
            concat_ws(',',collect_set(tag_value_jiangzhehu)) AS tag_value_jiangzhehu,
            concat_ws(',',collect_set(tag_value_jingjinji)) AS tag_value_jingjinji,
            concat_ws(',',collect_set(tag_value_zhusanjiao)) AS tag_value_zhusanjiao,
            concat_ws(',',collect_set(tag_value_changsanjiao)) AS tag_value_changsanjiao,
            tag_value_diqu
     from 
     (
         select ENTERPRISE_HKEY,
                ENTERPRISE_NAME,
                if(substr(REGION_CODE,1,2) in ('31','32','33'), '江浙沪',null) as tag_value_jiangzhehu,
                if(substr(REGION_CODE,1,2) in ('11','12')  
                          or substr(REGION_CODE,1,4) in ('1306','1310','1302','1307','1308','1303','1309','1311','1305','1304','1301'),'京津冀',null) as tag_value_jingjinji,
                if(substr(REGION_CODE,1,4) in ('4401','4403','4406','4407','4404','4419','4420','4413','4412'), '珠三角',null) as tag_value_zhusanjiao,
                if(substr(REGION_CODE,1,2) in ('31','32','33'), '长三角',null) as tag_value_changsanjiao,
                case when substr(REGION_CODE,1,2) in ('11','12','31','13','32','33','37','35','44','46') then '东部地区'
                     when substr(REGION_CODE,1,2) in ('14','41','42','43','34','36') then '中部地区'
                     when substr(REGION_CODE,1,2) in ('50','51','61','62','63','53','52','45','15','64','65','54') 
                          or substr(REGION_CODE,1,4) in ('4331','4228') then '西部地区'
                     when substr(REGION_CODE,1,2) in ('21','22','23') then '东北地区'
                     else REGION_CODE
                     end as tag_value_diqu
         from 
         (
                select ENTERPRISE_HKEY,
                       ENTERPRISE_NAME,
                       ADMINISTRATIVE_REGION_CODE
                from tmp_lk.SAT_ENT_BASIC_INFO_RD
                where ADMINISTRATIVE_REGION_CODE is not null and length(ADMINISTRATIVE_REGION_CODE)!=0
         ) t 
         lateral view explode(split(ADMINISTRATIVE_REGION_CODE,',')) tmp as REGION_CODE
     ) tt 
     group by ENTERPRISE_HKEY,
              ENTERPRISE_NAME,
              tag_value_diqu
    ''')

    diqu_df.createOrReplaceTempView("diqu_df")

    spark.sql('''
    insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02070201')
    select ENTERPRISE_HKEY,
           ENTERPRISE_NAME,
           current_timestamp() as tag_time, -- 标签产生时间
           '江浙沪' as tag_name,
           '是' as tag_value,
           null as tag_level,
           null as tag_dist,
           null as tag_year,
           current_timestamp() as change_time,
           null as id 
    from diqu_df
    where tag_value_jiangzhehu is not null and length(tag_value_jiangzhehu)!=0
    ''')
    print(" 江浙沪 =====> 处理成功")

    spark.sql('''
    insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02070202')
    select ENTERPRISE_HKEY,
           ENTERPRISE_NAME,
           current_timestamp() as tag_time, -- 标签产生时间
           '京津冀' as tag_name,
           '是' as tag_value,
           null as tag_level,
           null as tag_dist,
           null as tag_year,
           current_timestamp() as change_time,
           null as id 
    from diqu_df
    where tag_value_jingjinji is not null and length(tag_value_jingjinji)!=0
    ''')
    print(" 京津冀 =====> 处理成功")

    spark.sql('''
    insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02070203')
    select ENTERPRISE_HKEY,
           ENTERPRISE_NAME,
           current_timestamp() as tag_time, -- 标签产生时间
           '珠三角' as tag_name,
           '是' as tag_value,
           null as tag_level,
           null as tag_dist,
           null as tag_year,
           current_timestamp() as change_time,
           null as id 
    from diqu_df
    where tag_value_zhusanjiao is not null and length(tag_value_zhusanjiao)!=0
    ''')
    print(" 珠三角 =====> 处理成功")

    spark.sql('''
    insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02070204')
    select ENTERPRISE_HKEY,
           ENTERPRISE_NAME,
           current_timestamp() as tag_time, -- 标签产生时间
           '长三角' as tag_name,
           '是' as tag_value,
           null as tag_level,
           null as tag_dist,
           null as tag_year,
           current_timestamp() as change_time,
           null as id 
    from diqu_df
    where tag_value_changsanjiao is not null and length(tag_value_changsanjiao)!=0
    ''')
    print(" 长三角 =====> 处理成功")

    spark.sql('''
    insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='T02070205')
    select ENTERPRISE_HKEY,
           ENTERPRISE_NAME,
           current_timestamp() as tag_time, -- 标签产生时间
           '企业所属地区' as tag_name,
           tag_value_diqu as tag_value,
           null as tag_level,
           null as tag_dist,
           null as tag_year,
           current_timestamp() as change_time,
           null as id 
    from diqu_df
    where tag_value_diqu is not null and length(tag_value_diqu)!=0
    ''')
    print(" 企业所属地区 =====> 处理成功")

    # 清理 diqu_df
    diqu_df.unpersist()
    # 清理 diqu_tb
    spark.catalog.dropTempView("zuzhi_base_info_df")
    print("diqu_df ===> 清理完毕 ")


except Exception as e:
    print(" diqu_df =====> 处理失败")
    print(e)
    spark.stop()

try:
    # 数据写入指定分区
    spark.sql('''
     insert overwrite table tmp_lk.nonpublic_tag partition(tag_code)
     select ENTERPRISE_HKEY,
            ENTERPRISE_NAME,
            current_timestamp() as tag_time, -- 标签产生时间
            tag_name,
            '是' as tag_value,
            null as tag_level,
            null as tag_dist,
            null as tag_year,
            current_timestamp() as change_time,
            null as id,
            tag_code
     from tmp_lk.longdun_tag_code_name_map t1 
     join tmp_lk.SAT_ENT_BASIC_INFO_RD t2 on t1.hang_ye_zhi_biao=t2.INDUSTRY 
     where t1.tag_code in ('T02070107',
                        'T02070108',
                        'T02070109',
                        'T02070110',
                        'T02070111',
                        'T02070112',
                        'T02070113',
                        'T02070114',
                        'T02070115',
                        'T02070116',
                        'T02070117')
''')
    print(" 产业标签 =====> 标签处理成功")

except Exception as e:
    print(" 产业标签 =====> 标签处理失败")
    print(e)
    spark.stop()
    sys.exit(1)

spark.stop()
