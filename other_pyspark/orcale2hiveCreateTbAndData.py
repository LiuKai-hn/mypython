#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 9:51
# Description:
# Question: 该表主要用于确定每个标签对应的榜单名
import traceback
import sys
from pyspark.sql.functions import concat_ws
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list


# 封装方法，用于截取数组
def extract_array(filename, start_index, end_index):
    # 打开文件
    with open(filename, 'r') as file:
        # 读取文件的每一行并存储到数组中，去掉每行末尾的换行符
        lines = [line.strip() for line in file.readlines()]

    # 检查 start_index 是否超出数组范围
    if start_index >= len(lines):
        print('start_index must be less than the length of the array .....')
        return None

    # 如果结束索引大于数组长度，则将其设为数组长度
    if end_index > len(lines):
        end_index = len(lines)

    # 如果开始索引大于等于结束索引，则返回空数组
    if start_index >= end_index:
        print('start_index must be less than end_index .....')
        return []

    # 截取数组并返回
    return lines[start_index:end_index]


# 检查参数是否存在
if len(sys.argv) < 3:
    print("please input 3 args, like file_name, start_index, end_index")
    sys.exit(1)

# 获取参数
oracle_table_file = sys.argv[1]

# 文件开始行
start_index = int(sys.argv[2])
if start_index < 1:
    print('start_index must > 1 .....')
    sys.exit(1)
# 文件开始行
start_index = start_index - 1
# 文件结束行 + sys.argv[3] 每次读取数组的长度
end_index = start_index + int(sys.argv[3])

# 需要迁移的表 调用封装的方法截取数组
table_names = extract_array(oracle_table_file, start_index, end_index)
# 打印本批次的表
print('print table list ===> ' + ', '.join(table_names))
# 创建 SparkSession
spark = SparkSession.builder \
    .appName("oracle2hive") \
    .enableHiveSupport() \
    .getOrCreate()

try:

    # 读取Oracle表格,主要获取表字段信息
    all_col_comments = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
        .option("dbtable", "all_col_comments") \
        .option("user", "jcsjr") \
        .option("password", "jcsjrCETC15") \
        .load()
    all_col_comments.createOrReplaceTempView("all_col_comments")

    # 读取Oracle表格,主要获取表字段信息
    all_tab_comments = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
        .option("dbtable", "all_tab_comments") \
        .option("user", "jcsjr") \
        .option("password", "jcsjrCETC15") \
        .load()
    all_tab_comments.createOrReplaceTempView("all_tab_comments")

    # 循环遍历表
    for table_name in table_names:

        try:
            # 读取 oracle 源表的数据
            data_df = spark.read.format("jdbc") \
                .option("url", "jdbc:oracle:thin:@//172.16.16.205:1521/JCSJJ") \
                .option("dbtable", table_name) \
                .option("user", "bdmd") \
                .option("password", "bdmdCETC15") \
                .option("ignoreMissingTable", "true") \
                .load()

            # 执行SQL查询字段注释
            column_sql_str = '''
            SELECT concat('`',column_name, '`', ' String comment "', nvl(comments,'-'), '",') AS column_info
            FROM bdmd.all_col_comments
            WHERE table_name = '$table_name$' AND OWNER = 'BDMD'
            '''.replace('$table_name$', table_name)
            # 打印sql语句
            # print("column_sql_str ===== ", column_sql_str)
            # 执行
            column_comment_result_df = spark.sql(column_sql_str)
            # 将字段注释汇总为一个字符串
            column_comment_result_str = \
                column_comment_result_df.agg(concat_ws('', collect_list("column_info")).alias("column_info")).collect()[0][
                    "column_info"]

            # 执行SQL查询表注释
            tb_comment_rows = spark.sql('''
            SELECT comments
            FROM all_tab_comments
            WHERE table_name = "$table_name$" AND owner = "BDMD"
            '''.replace('$table_name$', table_name)).collect()
            print('==== 获取表注释 ===')
            # 如果结果为空，则设置为空字符串
            if len(tb_comment_rows) == 0:
                tb_comment = ""
            else:
                tb_comment = tb_comment_rows[0]["comments"]
            # 先清空表
            spark.sql('drop table if exists BDMD.' + table_name)
            # 拼装建表语句
            tab_pre_str = 'create table if not exists BDMD.' + table_name
            tab_format_str = '''
             comment '$tb_comment$'
             row format delimited fields terminated by '\t'
              lines terminated by '\n'
              stored as textfile
            '''.replace('$tb_comment$', tb_comment)
            # column_comment_result_str[:-1] 标识 去掉最后一个逗号
            create_tb_sql = tab_pre_str + '(' + column_comment_result_str[:-1] + ')' + tab_format_str
            # 打印结果
            print(" print create_sql ========> ", create_tb_sql)
            # 创建表
            spark.sql(create_tb_sql)
            print("=====>create table is ok ===" + table_name)

            data_df.createOrReplaceTempView('data_df')
            spark.sql('''
            INSERT overwrite table BDMD.$table_name$ 
            select * from data_df
            '''.replace('$table_name$', table_name))

            count_result = spark.sql("select count(1) as count_result from BDMD.$table_name$ ".replace('$table_name$', table_name)).collect()[0]["count_result"]
            print(table_name + "oracle count ===>" + str(data_df.count()) + " ,hive count ===> " + str(count_result))
            print("==========> " + table_name + " over <============")
        except Exception as e:
            traceback.print_exc()
            print("====> Error occurred for table: " + table_name + ". Skipping to the next table.")
            continue

except Exception as e:
    traceback.print_exc()
finally:
    spark.stop()
