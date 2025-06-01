#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2024/3/11 9:23
# Description:
# Question:

import sys


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


if __name__ == '__main__':
    # 检查参数是否存在

    # 获取参数
    oracle_table_file = r"E:\work\IdeaProjects\TestPython\GSL\other_pyspark\榜单名单表名.txt"
    # 文件开始索引
    start_index = int(21)
    if start_index < 1:
        print('start_index must > 1 .....')
        sys.exit(1)
    # 文件开始索引
    start_index = start_index - 1
    # 文件结束索引
    end_index = start_index + int(20)

    # 需要迁移的表 调用封装的方法截取数组
    table_names = extract_array(oracle_table_file, start_index, end_index)
    # 打印本批次的表
    print(table_names)
