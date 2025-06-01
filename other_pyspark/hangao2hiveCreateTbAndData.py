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


'''
-- 先在瀚高查询出结果，分别将结果导入hive表的tmp_lk.hangao_tbcomm 、tmp_lk.hangao_col_comm
-- 获取 highgo 表注释 tmp_lk.hangao_tbcomm
    SELECT
        c.relname AS tb_name,
        d.description AS tb_name_omm
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
    WHERE n.nspname ='jcsjk'
-- 获取 highgo 字段注释 tmp_lk.hangao_col_comm
                SELECT 
                    c.relname AS tb_name,
                    a.attname AS column_name,
                    d.description AS comments
                FROM
                    pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
                WHERE
                    n.nspname ='jcsjk'
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    order by c.relname,
                    a.attname,
                    d.description
'''

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
if len(sys.argv) < 4:
    print("please input 4 args, like file_name, start_index, end_index, target_db")
    sys.exit(1)

# 获取参数
hangao_tablelist_file = sys.argv[1]

# 文件开始行
start_index = int(sys.argv[2])
if start_index < 1:
    print('start_index must > 1 .....')
    sys.exit(1)
# 文件开始行
start_index = start_index - 1
# 文件结束行 + sys.argv[3] 每次读取数组的长度
end_index = start_index + int(sys.argv[3])
target_db = sys.argv[4]
# 需要迁移的表 调用封装的方法截取数组
table_names = extract_array(hangao_tablelist_file, start_index, end_index)
# 打印本批次的表
print('print table list ===> ' + ', '.join(table_names))
# 创建 SparkSession
spark = SparkSession.builder \
    .appName("hangao2hive") \
    .enableHiveSupport() \
    .getOrCreate()


# 读取瀚高表数据
def load_hangao_table_func(tableName):
    try:
        hangaoTableName = spark.read.format("jdbc")\
            .option("url", "jdbc:highgo://172.18.13.11:5866/jcsjk500")\
            .option("dbtable", "jcsjk." + tableName)\
            .option("driver", "com.highgo.jdbc.Driver")\
            .option("user", "sysdba")\
            .option("password", r"Hello@123")\
            .load()
        hangaoTableName.createOrReplaceTempView(tableName)
    except Exception as e:
        traceback.print_exc()

try:
    # 读取瀚高系统表,主要获取表及字段注释信息
    # load_hangao_table_func("pg_class")
    # load_hangao_table_func("pg_namespace")
    # load_hangao_table_func("pg_description")
    # load_hangao_table_func("pg_attribute")

    # 将本批次需要查询的表进行拼接，用于 where in
    quoted_arr = ['"{}"'.format(item) for item in table_names]
    arr_str = ",".join(quoted_arr)
    # 获取所有表注释
    all_tab_comments_df = spark.sql('''
    SELECT
        tb_name,
        tb_name_omm as tb_name_comm
    FROM tmp_lk.hangao_tbcomm 
    WHERE tb_name in ($tableList$)
    '''.replace("$tableList$", arr_str))
    # all_tab_comments_df.show()
    # 将DataFrame转换为字典列表
    # 创建一个空字典来存储表名和表注释的对应关系
    tab_comments_dict = {}
    all_tab_comments_list = all_tab_comments_df.collect()
    # 遍历Row对象列表，将表名和表注释存储到字典中
    for row in all_tab_comments_list:
        tab_comments_dict[row[0]] = row[1]
    # 循环遍历表
    for table_name in table_names:
        try:
            # 读取 瀚高 源表的数据
            df = spark.read.format("jdbc") \
                .option("url", "jdbc:highgo://172.18.13.11:5866/jcsjk500") \
                .option("dbtable", "jcsjk." + table_name) \
                .option("driver", "com.highgo.jdbc.Driver") \
                .option("user", "sysdba") \
                .option("password", r"Hello@123") \
                .load()
            df.createOrReplaceTempView(table_name)
            # 执行SQL查询字段注释
            column_sql_str = '''
            SELECT concat_ws('', collect_list(concat('`',column_name, '`', ' String comment "', nvl(comments,'-'), '",'))) AS column_info
                   ,concat_ws(',', collect_list(column_name)) as column_names
            FROM tmp_lk.hangao_col_comm
            where tb_name="$table_name$"
            '''.replace('$table_name$', table_name)
            # 打印sql语句
            # print("column_sql_str ===== ", column_sql_str)
            # 执行
            column_comment_result_df = spark.sql(column_sql_str)
            # 将字段注释汇总为一个字符串
            column_info_list = column_comment_result_df.collect()
            column_comment_result_str = column_info_list[0]["column_info"]
            column_names = column_info_list[0]["column_names"]
            # 获取表注释
            tb_comment = tab_comments_dict.get(table_name)
            # 如果结果为空，则设置为空字符串
            if len(tb_comment) == 0:
                tb_comment = ""
            # 先清空表
            spark.sql('drop table if exists ' + target_db + "." + table_name)
            # 拼装建表语句
            tab_pre_str = 'create table if not exists ' + target_db + "." + table_name
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

            insert_sql = '''
            INSERT overwrite table $target_db$.$table_name$ 
            select $column_names$ from $table_name$
            '''.replace('$column_names$', column_names).replace('$target_db$', target_db).replace('$table_name$', table_name)

            print(" insert_sql =======> "+insert_sql)
            # 插入数据
            spark.sql(insert_sql)

            #count_result = spark.sql("select count(1) as count_result from $target_db$.$table_name$ ".replace('$target_db$', target_db)
            #                         .replace('$table_name$',table_name)).collect()[0]["count_result"]
            #print(table_name + "hangao count ===> " + str(table_name.count()) + " ,hive count ===> " + str(count_result))
            print("==========> " + table_name + " over <============")
        except Exception as e:
            traceback.print_exc()
            print("====> Error occurred for table: " + table_name + ". Skipping to the next table.")
            continue

except Exception as e:
    traceback.print_exc()
finally:
    spark.stop()
