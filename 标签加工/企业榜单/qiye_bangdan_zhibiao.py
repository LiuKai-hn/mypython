# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 10:42
# Description:
# Question:
import sys
from imp import reload

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

reload(sys)
sys.setdefaultencoding('utf-8')

# 检查参数是否存在
if len(sys.argv) < 2:
    print("参数缺失，请输入1/2: (1：人/2:企业) 和 where条件两个参数... eg: tag_code in ('1','2','3')")
    sys.exit(1)

# 获取参数
flag = sys.argv[1]
where_str = sys.argv[2]

# 创建 SparkSession
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .appName("行业代表") \
    .enableHiveSupport() \
    .config(conf=conf) \
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
# # 读取Oracle数据库中的数据
# sj_phbdfb = spark.read.jdbc(url=url, table=table, properties=properties)
# sj_phbdfb = sj_phbdfb.withColumn("DY", trim(sj_phbdfb["DY"]))
# sj_phbdfb.createOrReplaceTempView("sj_phbdfb")
# sj_phbdfb.write.format("parquet").mode("overwrite").saveAsTable("tmp_lk.sj_phbdfb")
# lable_likes_list = ["优秀中国特色社会主义建设者", "改革开放"]

# 获取 榜单配置表
lable_likes_df = spark.sql("""
select likes,
       tag_code,
       tag_name
from tmp_lk.nonpublic_tag_level 
where (likes is not null and length(likes)!=0) and flag='$flag$' and $where_str$
""".replace("$flag$", flag)
                           .replace("$where_str$", where_str))
# 获取 tag_name--榜单关系表
tag_code_name_map_tmp = spark.sql("select tag_name,bd_tb_name from tmp_lk.tag_code_name_map_tmp")
# 注册临时表
tag_code_name_map_tmp.createOrReplaceTempView("tag_code_name_map_tmp")
# 获取 所有的 likes 并将其转化成list
tag_codes = [row[0] for row in lable_likes_df.select("tag_code").collect()]
if len(tag_codes) == 0:
    print(where_str + "=====>  没有任何匹配的 tag_code 信息")
    sys.exit(1)

lable_likes_df.createOrReplaceTempView("lable_likes_df")
for tag_code in tag_codes:

    # 筛选出 likes 字段等于 lable_like_name 的记录
    tag_tuple_sql = "select tag_code,tag_name,likes from lable_likes_df  where tag_code ='" + tag_code + "'"
    print("tag_tuple_sql =====>" + tag_tuple_sql.replace("lable_likes_df", "tmp_lk.nonpublic_tag_level"))
    result_df = spark.sql(tag_tuple_sql)

    result_df.show()
    result = result_df.collect()
    # 将 tag_code 和 tag_name 分别赋值给两个变量名
    tag_code = result[0]["tag_code"]
    tag_name = result[0]["tag_name"]
    likes = result[0]["likes"]
    print("tag_code ===>" + tag_code)
    print("tag_name ===>" + tag_name)
    print("likes ===>" + likes)

    if len(likes) == 0 or likes is None:
        print("=====> 当前 likes 没有相关榜单匹配项 ")
        continue

    try:
        print("准备处理标签 ========> " + tag_name)
        bd_tblist = spark.sql(
            """ select bd_tb_name as sjbm
                from tag_code_name_map_tmp 
                where lable_like_name
                group by bd_tb_name
            """.replace("lable_like_name", likes)).collect()

        # 将 DataFrame 转换为 Python 列表
        bd_table_names = [row[0] for row in bd_tblist]
        if len(bd_table_names) == 0:
            print(tag_code + "===> 没有匹配的榜单")
            continue
        print("当前 所用到的榜单名称 ====> " + str(bd_table_names))
        # 定义需要查询的字段列表
        columns = ["QYBM", "QYMC", "BDBM", "ND"]
        # result_df
        schema = StructType([StructField(col, StringType(), True) for col in columns])
        bd_df = spark.createDataFrame([], schema=schema)
        # 循环遍历榜单表
        for bd_table_name in bd_table_names:
            try:
                df = spark.read.format("jdbc") \
                    .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
                    .option("dbtable", bd_table_name) \
                    .option("user", "bdmd") \
                    .option("password", "bdmdCETC15") \
                    .option("ignoreMissingTable", "true") \
                    .load()
                print("榜单 table_name==========> " + bd_table_name)
                # 判断当前表是否存在该字段
                select_columns = []
                for column in columns:
                    if check_column_exists(df, column):
                        select_columns.append(column)
                    else:
                        select_columns.append("NULL as " + column)
                new_df = df.selectExpr(select_columns)
                # 榜单数据union all 进行汇总
                bd_df = bd_df.unionAll(new_df)
                # 清理 diqu_df
                new_df.unpersist()
                print("new_df ===> 清理完毕 ")
            except:
                print(bd_table_name + " ==========> 榜单 不存在")
        print(tag_code + "=====> 榜单 union all 完毕 ")
        # 显示查询结果
        bd_df.createOrReplaceTempView("bd_df")
        # 落地 hive 方便debug
        bd_df.write.format("parquet").mode("overwrite").saveAsTable("tmp_lk.bd_df_" + tag_code)
        print("====> tmp_lk.bd_df_" + tag_code + "   hive 入库成功")
        joined_df = spark.sql('''
              select 
                     t1.QYBM,
                     t1.QYMC,
                     t1.BDBM,
                     CASE
                       WHEN instr(t2.DY , '省') > 0 THEN '省级'
                       when t2.DY  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区') then '省级'
                       when t2.DY ='全国' then  tag_level
                       else t2.DY 
                       END AS tag_level,
                     COALESCE(t1.ND,t2.ND, null) as nd,
                     t2.DY as tag_dist
              from bd_df t1 join tmp_lk.sj_phbdfb t2 on t1.BDBM=t2.ID
              group by 
                       t1.QYBM,
                       t1.QYMC,
                       t1.BDBM,
                       CASE
                         WHEN instr(t2.DY , '省') > 0 THEN '省级'
                         when t2.DY  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区') then '省级'
                         when t2.DY ='全国' then  tag_level
                         else t2.DY 
                       END,
                       COALESCE(t1.ND,t2.ND, null)
        ''')

        joined_df.createOrReplaceTempView("renyuan_qiye_xinxi_tb")
        # 落地 hive 方便debug
        joined_df.write.format("parquet").mode("overwrite").saveAsTable("tmp.renyuan_qiye_xinxi_tb_" + tag_code)

        # todo 标签层级 只有省市县区工信部等枚举，等处理完毕需要查询有多少个枚举，标签地区按地域来
        spark.sql("""
                insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='$tag_code$')
                select tt2.enterprise_hkey,
                      tt2.enterprise_name as nonpublic_name,
                      current_timestamp() as tag_time, -- 标签产生时间
                      '$tag_name$' as tag_name,
                      '是' as tag_value, -- 标签值 
                      tt1.tag_level,
                      tt1.tag_dist,
                      tt1.nd as tag_year,
                      current_timestamp() as change_time,
                      null as id
                from renyuan_qiye_xinxi_tb tt1 
                join tmp_lk.sat_ent_basic_info_rd tt2 
                on tt1.qymc=tt2.enterprise_name
                """.replace("$tag_code$", tag_code)
                  .replace("$tag_name$", tag_name))
        print(tag_code + "=====> 标签处理完毕 ")

    except Exception as e:
        print(tag_code + "=====> 标签处理失败 ")
        print(e)
        spark.stop()
spark.stop()

print("========= all over ==========")
