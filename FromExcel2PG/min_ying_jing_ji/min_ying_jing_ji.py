#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/26 11:27
# Description:
# Question:

import csv
import time

from GSL.FromExcel2PG.shi_jie_jing_ji.ShiJieJingJiDataInsert import getConn


def UpsertData(file_path):
    # 创建游标对象
    conn = getConn()
    cur = conn.cursor()

    tag_code_lists = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            tag_code_lists.append("'" + row[2] + "'")
        tag_codes = ','.join(tag_code_lists)

    # 删除F_CODE原有的数据，避免多次插入
    delete_sql = "DELETE from " + "macro_eco_index where f_code in ($F_CODES$);".replace('$F_CODES$', tag_codes)
    print("execute sql ==>" + delete_sql)
    # 执行查询语句
    cur.execute(delete_sql)
    # 提交事务
    conn.commit()
    time.sleep(1)

    # 执行copy from语句
    print("UpsertData copy from ==> " + file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        # next(f)  # 跳过标题行
        columns = ('f_typecode', 'f_type', 'f_code', 'f_name', 'f_time', 'f_data', 'f_area', 'status')
        cur.copy_from(f, 'macro_eco_index', sep=',', columns=columns)  # 将数据从文件中复制到数据库表中
    # 提交事务
    conn.commit()
    time.sleep(1)
    cur.close()
    conn.close()


if __name__ == "__main__":
    UpsertData(r"E:\work\gsl\民营经济数据.csv")
