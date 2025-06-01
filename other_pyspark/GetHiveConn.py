#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/7 9:51
# Description:
# Question:
from pyhive import hive

# 连接Hive服务器
conn = hive.Connection(host='172.18.13.19', port=10000, username='hive', database='tmp_lk')

# 创建游标对象
cursor = conn.cursor()

# 执行查询
cursor.execute(" select * from SJ_PHBDFB where sjbm='PH_ZGZJYXLWSSJLXB'")

# 获取结果
results = cursor.fetchall()

# 打印结果
for row in results:
    print(row)

# 关闭连接
conn.close()






