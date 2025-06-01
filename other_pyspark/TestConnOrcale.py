#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 9:51
# Description:
# Question:



import sys
from imp import reload

from pyspark.sql import SparkSession

reload(sys)
sys.setdefaultencoding('utf-8')

# 检查参数是否存在
if len(sys.argv) < 3:
    print("参数缺失，请输入 orcale username、password、数据集 参数")
    sys.exit(1)

# 获取参数
orcale_user = sys.argv[1]
orcale_passwd = sys.argv[2]
orcale_database = sys.argv[3]


# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .enableHiveSupport() \
    .getOrCreate()

# 创建 SparkSession
spark = SparkSession.builder.appName("Read Oracle Tables").getOrCreate()



# 定义需要查询的表名列表
table_names = ["BQ_FL_FLXX", "BQ_FL_BQGLGX", "BQ_BQDY"]
BQ_FL_FLXX = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/"+orcale_database) \
    .option("dbtable", "JCSJR.BQ_FL_FLXX") \
    .option("user", orcale_user) \
    .option("password", orcale_passwd) \
    .load()

BQ_FL_FLXX.createOrReplaceTempView("BQ_FL_FLXX")

BQ_FL_BQGLGX = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/"+orcale_database) \
    .option("dbtable", "JCSJR.BQ_FL_BQGLGX") \
    .option("user", orcale_user) \
    .option("password", orcale_passwd) \
    .load()

BQ_FL_BQGLGX.createOrReplaceTempView("BQ_FL_BQGLGX")

BQ_BQDY = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/"+orcale_database) \
    .option("dbtable", "JCSJR.BQ_BQDY") \
    .option("user", orcale_user) \
    .option("password", orcale_passwd) \
    .load()

BQ_BQDY.createOrReplaceTempView("BQ_BQDY")

SJ_PHBDFB = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/"+orcale_database) \
    .option("dbtable", "JCSJR.SJ_PHBDFB") \
    .option("user", orcale_user) \
    .option("password", orcale_passwd) \
    .load()

SJ_PHBDFB.createOrReplaceTempView("SJ_PHBDFB")

tag_name_sjbm_df = spark.sql("""
select BQFLMC,
       SJBM 
from
(
    SELECT XH,
           BQFLMC,
           BQMC,
           BQBM,
           BDM,
           BDBM,
           SJBM 
    FROM 
    (
        SELECT EJ.ID,
               EJ.BQFLMC,
               EJ.XH,
               BQ.BQMC,
               BQ.BQBM,
               FB.ID AS BDBM,
               FB.BDM,
               FB.SJBM 
        FROM BQ_FL_FLXX EJ,BQ_FL_BQGLGX BQGX ,BQ_BQDY BQ ,SJ_PHBDFB FB
        WHERE EJ.ID = BQGX.FLID AND BQGX.BQID = BQ.ID AND BQ.LYBM = FB.ID
        AND EJ.ZT ='非公经济优秀企业家-分类标签'
        AND EJ.JB ='二级'
        AND EJ.SCBZ ='1'
        AND EJ.XH IS NOT NULL
    ) t1 
) t2 
WHERE XH IN ('1','2','3','4','5','6','7','8','13','14','15','16','38','39','40','83','84','85','86','87','88','89','90','91','92','93','94','95','96','124','125','126','127','128','129','130','131','132','133','134','135','136','137','138','139','140','141','142','143','144','145','146','147','148','149','151','152','153','154','155','156','158','165','166','185','186','187','188','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228')
group by BQFLMC,SJBM
""")

tag_name_sjbm_df.write.mode("overwrite").saveAsTable("tmp_lk.tag_name_sjbm_df")

spark.stop()
