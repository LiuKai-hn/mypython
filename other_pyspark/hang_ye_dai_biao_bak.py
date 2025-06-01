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
if len(sys.argv) < 1:
    print("参数缺失，请输入where条件参数... eg: tag_code in ('1','2','3')")
    sys.exit(1)

# 获取参数
where_str = sys.argv[1]

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
# lable_likes_list = ["优秀中国特色社会主义建设者", "改革开放"]

# 获取 榜单配置表
lable_likes_df = spark.sql("""
select likes,
       tag_code,
       tag_name
from tmp_lk.nonpublic_tag_level 
where (likes is not null or length(likes)!=0) and $where_str$
""".replace("$where_str$", where_str))

lable_likes_df.show()
# 获取 所有的 likes 并将其转化成list
likes_names = [row[0] for row in lable_likes_df.select("likes").collect()]

for lable_like_name in likes_names:
    # 筛选出 likes 字段等于 lable_like_name 的记录
    tag_tuple_sql = "select tag_code,tag_name from tmp_lk.nonpublic_tag_level  where likes ='" + lable_like_name + "'"
    print("tag_tuple_sql =====>" + tag_tuple_sql)
    result_df = spark.sql(tag_tuple_sql)

    result_df.show()
    result = result_df.collect()
    # 将 tag_code 和 tag_name 分别赋值给两个变量名
    tag_code = result[0]["tag_code"]
    tag_name = result[0]["tag_name"]
    print("tag_code ===>" + tag_code)
    print("tag_name ===>" + tag_name)

    try:
        print("准备处理标签 ========> " + lable_like_name)
        # 读取Oracle数据库中的数据
        sj_phbdfb = spark.read.jdbc(url=url, table=table, properties=properties)
        sj_phbdfb.createOrReplaceTempView("sj_phbdfb")

        bd_tblist = spark.sql(
            """ select sjbm
                from sj_phbdfb 
                where bdm like '%$lable_like_name$%'
                group by sjbm
            """.replace("$lable_like_name$", lable_like_name)).collect()

        bd_index_df = spark.sql(
            """select * 
               from sj_phbdfb 
               where bdm like '%$lable_like_name$%'
            """.replace("$lable_like_name$", lable_like_name))

        bd_index_df.createOrReplaceTempView("bd_index_df")
        # 将 DataFrame 转换为 Python 列表
        table_names = [row[0] for row in bd_tblist]
        # 定义需要查询的字段列表
        columns = ["XM", "ZW", "QYBM", "QYMC", "BDBM", "ND", "CITY", "COUNTY"]
        # result_df
        schema = StructType([StructField(col, StringType(), True) for col in columns])
        bd_df = spark.createDataFrame([], schema=schema)
        # 循环遍历榜单表
        for table_name in table_names:
            df = spark.read.format("jdbc") \
                .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
                .option("dbtable", table_name) \
                .option("user", "bdmd") \
                .option("password", "bdmdCETC15") \
                .load()
            print("榜单 table_name==========> " + table_name)
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
        print(lable_like_name + "=====> 榜单 union all 完毕 ")
        # 显示查询结果
        bd_df.createOrReplaceTempView("bd_df")
        # 指定每个人的city,county
        # select t1.XM,
        #         t1.ZW,
        #         t1.QYBM,
        #         t1.QYMC,
        #         t1.BDBM,
        #         t2.DY,
        #         COALESCE(t1.ND,t2.ND, null) as nd,
        #         COALESCE(t2.DY||REPLACE(t1.CITY,t2.DY,'')||REPLACE(t1.COUNTY,t1.CITY,''),t2.DY, null) as tag_dist
        #  from bd_df t1 join bd_index_df t2 on t1.BDBM=t2.ID
        #  group by t1.XM,
        #           t1.ZW,
        #           t1.QYBM,
        #           t1.QYMC,
        #           t1.BDBM,
        #           t2.DY,
        #           COALESCE(t1.ND,t2.ND, null),
        #           COALESCE(t2.DY||REPLACE(t1.CITY,t2.DY,'')||REPLACE(t1.COUNTY,t1.CITY,''),t2.DY, null)

        joined_df = spark.sql('''
              select t1.XM,
                     t1.ZW,
                     t1.QYBM,
                     t1.QYMC,
                     t1.BDBM,
                     t2.DY,
                     COALESCE(t1.ND,t2.ND, null) as nd,
                     COALESCE(t2.DY,t2.DY, null) as tag_dist
              from bd_df t1 join bd_index_df t2 on t1.BDBM=t2.ID
              group by t1.XM,
                       t1.ZW,
                       t1.QYBM,
                       t1.QYMC,
                       t1.BDBM,
                       t2.DY,
                       COALESCE(t1.ND,t2.ND, null),
                       COALESCE(t2.DY,t2.DY, null)
        ''')
        spark.sql("drop table if exists tmp_lk.hang_ye_dai_biao")
        # 将榜单表写入到hive
        joined_df.write.mode("overwrite").saveAsTable("tmp_lk.hang_ye_dai_biao")
        print(lable_like_name + "=====> joined_df 入 hive 完毕 ")
        xm_notnull_cnt = spark.sql("select count(XM) from tmp_lk.hang_ye_dai_biao").collect()[0][0]
        print("xm_notnull_cnt====>" + str(xm_notnull_cnt))
        if xm_notnull_cnt == 0:
            join_on_str = "tt1.qymc=tt2.enterprise_name"
        else:
            join_on_str = "tt1.xm=tt2.person_name and tt1.qymc=tt2.enterprise_name"
        print("join_on_str====>" + join_on_str)
        # todo 标签层级 只有省市县区工信部等枚举，等处理完毕需要查询有多少个枚举，标签地区按地域来
        spark.sql("""
                insert overwrite table tmp_lk.nonpublic_tag partition(tag_code='$tag_code$')
                select tt2.person_hkey,
                      tt2.person_name as nonpublic_name,
                      current_timestamp() as tag_time, -- 标签产生时间
                      '$tag_name$' as tag_name,
                      '是' as tag_value, -- 标签值 
                      tt1.dy as tag_level,
                      tt1.tag_dist,
                      tt1.nd as tag_year,
                      current_timestamp() as change_time,
                      null as id
                from tmp_lk.hang_ye_dai_biao tt1 
                join tmp_lk.tmp_person_info tt2 
                on $join_on_str$
                """.replace("$join_on_str$", join_on_str)
                  .replace("$tag_code$", tag_code)
                  .replace("$tag_name$", tag_name))
        print(lable_like_name + "=====> 标签处理完毕 ")
    except Exception as e:
        print(lable_like_name + "=====> 标签处理失败 ：" + str(e))
        spark.stop()
spark.stop()

print("========= all over ==========")
