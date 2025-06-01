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


# 获取 指标配置表
def get_file_code_name_map(indexPath, F_LEVEL_THR_NAME):
    # 打开CSV文件
    with io.open(indexPath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        # 创建空字典
        data_dict = {}
        # 读取每一行数据
        for row in reader:
            FILE_F_LEVEL_THR_NAME = row[0]  # 第一列
            FILE_CODE = row[1]  # 第一列
            FILE_NAME = row[2]  # 第一列
            if F_LEVEL_THR_NAME == FILE_F_LEVEL_THR_NAME:
                data_dict[FILE_CODE] = FILE_NAME

    return data_dict


def get_sheet3_data(file_path):
    # 打开文件，xlrd.open_workbook()，函数中参数为文件路径，分为相对路径和绝对路径
    work_book = xlrd.open_workbook(file_path)
    # 获取sheet内容
    # 按索引号获取sheet内容
    worksheet = work_book.sheet_by_index(2)  # sheet索引从0开始
    # 获取 sheet3下第二行第一列
    file_code = worksheet.cell_value(1, 0)

    return file_code


def rename_files_func(base_floder, indexFilePath, F_LEVEL_THR_NAME):
    file_code_name_map = get_file_code_name_map(indexFilePath, F_LEVEL_THR_NAME)
    # 打印新字典
    print(file_code_name_map)
    file_names = os.listdir(base_floder)
    for file_name in file_names:
        # 源文件路径
        src_file = base_floder + "\\" + file_name
        # 根据 sheet3 下的code
        sheet_3_code = str(get_sheet3_data(src_file))
        try:
            # 获取原始的key，配置表中的值
            real_file_name = file_code_name_map.get(sheet_3_code)
            # 命名后的文件路径
            new_file_name = base_floder + "\\" + real_file_name + ".xls"
            os.rename(src_file, new_file_name)
            print(new_file_name + " ===> 修改完毕")
            # print(F_LEVEL_THR_NAME + "," + real_file_name + "," + file_name.replace(".xls", ""))
        except Exception as e:
            print("====>" + file_name + "在index_dict中不存在")
            print(e)


if __name__ == '__main__':

    # sub_paths = ['公共部门', '基础设施', '教育', '金融部门', '经济与增长', '科学技术', '能源与矿产', '农业与农村发展', '人口与就业', '私营部门', '外债']
    sub_paths = ['金融部门']
    for sub_path in sub_paths:

        try:
            # 先将该目录下的文件根据配置表进行重命名
            rename_files_func(r"E:\work\gsl\202304\$sub_path$".replace("$sub_path$", sub_path),
                              "./file_name_code_map.csv",
                              sub_path)
        except Exception as e:
            print("==================> " + sub_path + " 失败 <=========================")
            print(e)


    # rename_files_func(r"E:\work\gsl\202304\公共部门", "./file_name_code_map.csv", "公共部门")
