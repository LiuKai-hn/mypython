#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------
# Author: liu kai
# CreateTime: 2023/9/15 15:09
# Description:
# Question:


import os
import io
import csv
import xlrd2 as xlrd
def get_sheet3_data(file_path):
    # 打开文件，xlrd.open_workbook()，函数中参数为文件路径，分为相对路径和绝对路径
    work_book = xlrd.open_workbook(file_path)
    # 获取sheet内容
    # 按索引号获取sheet内容
    worksheet = work_book.sheet_by_index(2)  # sheet索引从0开始
    # 获取 sheet3下第二行第一列
    file_code = worksheet.cell_value(1, 0)

    return file_code


# 替换字典表中无用的字符
def replace_key(key):
    return key.replace(" ", "").replace("，", "").replace("_", "").replace("/", "")


def rename_files_func(base_floder, F_LEVEL_THR_NAME):
    file_names = os.listdir(base_floder)
    for file_name in file_names:
        # 源文件路径
        src_file = base_floder + "\\" + file_name
        # 根据 sheet3 下的data修改文件名称 去掉空格并将中文括号转换成英文括号，中文逗号的
        file_code = get_sheet3_data(src_file)
        # 获取原始的key，配置表中的值
        print(F_LEVEL_THR_NAME+"," + file_code + "," + file_name.replace(".xls",""))
        # print(file_code)


if __name__ == '__main__':

    sub_paths = ['公共部门', '基础设施', '教育', '金融部门', '经济与增长', '科学技术', '能源与矿产', '农业与农村发展', '人口与就业', '私营部门', '外债']
    # sub_paths = ['基础设施']
    for sub_path in sub_paths:

        try:
            # 先将该目录下的文件根据配置表进行重命名
            rename_files_func(r"E:\work\gsl\202304\$sub_path$".replace("$sub_path$", sub_path),sub_path)
        except Exception as e:
            print("==================> " + sub_path + " 失败 <=========================")
            print(e)

    # rename_files_func(r"E:\work\gsl\宏观经济hive\202304\公共部门", "./mic_index_con.csv", "公共部门")
