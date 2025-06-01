# encoding=utf-8
# --------------------------------
# Author: liu kai
# CreateTime: 2023/6/30 16:05
# Description:
# Question:
import csv
import importlib
import io
import os
import sys
import time

import xlrd2 as xlrd

importlib.reload(sys)

# 由于手动命名不正确，现需要根据sheet3下第二行第二列的内容来重命名 获取 sheet3下第二行第二列的内容
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
            FILE_CODE = row[1]  # code
            FILE_NAME = row[2]  # code 对应 fileName
            if F_LEVEL_THR_NAME == FILE_F_LEVEL_THR_NAME:
                data_dict[FILE_CODE] = FILE_NAME

    return data_dict


# 读取 sheet3下第二行第一列的内容 code
def get_sheet3_data(file_path):
    # 打开文件，xlrd.open_workbook()，函数中参数为文件路径，分为相对路径和绝对路径
    work_book = xlrd.open_workbook(file_path)
    # 获取sheet内容
    # 按索引号获取sheet内容
    worksheet = work_book.sheet_by_index(2)  # sheet索引从0开始
    # 获取 sheet3下第二行第一列
    file_code = worksheet.cell_value(1, 0)

    return file_code


# 根据sheet3中的code字段修改文件名称
def rename_files_func(base_floder, indexFilePath, F_LEVEL_THR_NAME):
    file_code_name_map = get_file_code_name_map(indexFilePath, F_LEVEL_THR_NAME)
    # 打印新字典
    # print(file_code_name_map)
    file_names = os.listdir(base_floder)
    for file_name in file_names:
        # 源文件路径
        src_file = base_floder + file_name
        # 根据 sheet3 下的code
        sheet_3_code = str(get_sheet3_data(src_file))
        try:
            # 获取原始的key，配置表中的值
            real_file_name = file_code_name_map.get(sheet_3_code)
            # 命名后的文件路径
            new_file_name = base_floder + real_file_name + ".xls"
            os.rename(src_file, new_file_name)
            print(new_file_name + " ===> 修改完毕")
            # print(F_LEVEL_THR_NAME + "," + real_file_name + "," + file_name.replace(".xls", ""))
        except Exception as e:
            print("====>" + file_name + "在index_dict中不存在")
            print(e)
            sys.exit()

# 将科学计数法转换成真实数字
def parse_scientific_notation(scientific_notation):
    try:
        return float(scientific_notation)
    except ValueError:
        # 如果无法转换，返回原始字符串
        return scientific_notation



# 将xls 中的数据按年份炸裂成多行
def toCSVs(file_path, F_TYPECODE, F_TYPE, F_NAME, F_CODE, output_path):
    # 打开文件，xlrd.open_workbook()，函数中参数为文件路径，分为相对路径和绝对路径
    work_book = xlrd.open_workbook(file_path)
    # 获取sheet内容
    # 按索引号获取sheet内容
    sheet1_content1 = work_book.sheet_by_index(0)  # sheet索引从0开始
    # 获取表头
    header = sheet1_content1.row_values(3)  # 获取第四行内容

    # 定义一个空列表，将读取的每一行数据保存到该列表中
    lists = []
    for i in range(4, sheet1_content1.nrows):
        row = sheet1_content1.row_values(i)
        for y in range(4, len(row)):
            # line =""+
            #     F_TYPECODE + ","   # 数据类型代码
            #     F_TYPE + ","   # 数据类型
            #     F_CODE + ","   # 指标代码
            #     F_NAME + ","   # 指标代码中文名称
            #     header[y] + ","# 指标时间
            #     row[y] + ","   # 指标数据
            #     row[0] + ","   # 地区
            #     "1"  # 状态
            # value = sheet1_content1.cell(i, y).value
            F_DATA = parse_scientific_notation(row[y])  # 处理第6列的科学计数法
            line = F_TYPECODE + "," + F_TYPE + "," + F_CODE + "," + F_NAME + ","  + header[y] + "," +  str(F_DATA) + "," + row[0] + ",,1"

            lists.append(line)

    # 将数据写入文件
    with io.open(output_path, 'a', encoding='utf-8') as f:
        f.write('\n'.join(lists))
        f.write(u'\n')
        print("===>csv转换结果: " + file_path + "====>  共有 ：" + str(len(lists)) + "数据,并写入 ===> " + output_path)
    time.sleep(0.5)


# 获取hive入库前所有的世界经济数据格式
def getExcel2CSVRealData(folder_path, F_LEVEL_THR_NAME, indexFilePath, outfile_folder_path):
    # 获取指标配置表dict
    index_dict = get_index_map(indexFilePath, F_LEVEL_THR_NAME)
    # 使用os.listdir()函数获取文件夹下的所有文件名
    print("当前需要处理的目录 ==>" + folder_path)

    file_names = os.listdir(folder_path)
    # print(str(file_names).decode("string-escape"))

    # 判断 路径是否存在，若不存在，则创建
    if not os.path.exists(outfile_folder_path):
        os.makedirs(outfile_folder_path)

    outfile_path = outfile_folder_path + "/" + F_LEVEL_THR_NAME + ".csv"
    # 转换该目录下所有 excel 文件为 csv
    for file_name in file_names:
        if file_name.split('.')[1] == 'csv':
            continue
        else:
            abs_path = folder_path + file_name
            print("当前处理文件==>" + abs_path + "将其转换成csv")
            F_NAME = file_name.replace(" ", "").split('.')[0]
            # 因为 文件名不能有特殊字符，需要单独处理
            if F_NAME == 'GDP单位能源使用量（购买力平价美元千克石油当量）':
                F_NAME = 'GDP单位能源使用量（购买力平价美元/千克石油当量）'
            try:
                F_CODE = index_dict.get(F_NAME).get('F_CODE')
                print("======= F_CODE ==========>"+F_CODE)
                toCSVs(abs_path, "C", "年度数据", F_NAME, F_CODE, outfile_path)
                print("=====>" + abs_path + " 处理完毕")
            except Exception as e:
                print(abs_path + "==> 转换csv失败,找不到对应的 F_CODE ")
                print(str(e))
                sys.exit()


# 获取 指标配置表
def get_index_map(indexPath, F_LEVEL_THR_NAME):
    # 打开CSV文件
    with io.open(indexPath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        #next(reader)  # 跳过标题行
        # 创建空字典
        data_dict = {}
        # 读取每一行数据
        for row in reader:
            THR_CODE = row[2]  # 第一列
            THR_NAME = row[3]  # 第一列
            F_CODE = row[6]  # 第四列
            F_NAME = row[7]
            F_CODE_FROM_LD_CODE = row[8]  # 第五列

            if F_LEVEL_THR_NAME == THR_NAME:
                # 将数据封装为字典
                data_dict[F_NAME] = {'F_CODE': F_CODE,
                                     'F_LEVEL_THR_NAME': F_LEVEL_THR_NAME,
                                     'F_LEVEL_THR_CODE': THR_CODE,
                                     'F_CODE_FROM_LD_CODE': F_CODE_FROM_LD_CODE}

    # 打印结果
    print(str(data_dict))

    return data_dict


if __name__ == "__main__":
    sub_paths = ['公共部门', '基础设施', '教育', '金融部门', '经济与增长', '科学技术', '能源与矿产', '农业与农村发展', '人口与就业', '私营部门', '外债']
    #sub_paths = ['公共部门']

    # 检查参数是否存在
    if len(sys.argv) < 3:
        print("参数缺失，请输入 输入路径、指标文件路径、输出路径 ... 其中 输入和输出路径为路径，不是到文件")
        sys.exit(1)

    # 获取参数
    input_floder = sys.argv[1]
    index_file_path = sys.argv[2]
    output_floder = sys.argv[3]

    for sub_path in sub_paths:
        # getExcel2CSVRealData(r"E:\work\gsl\202304\$sub_path$\\".replace("$sub_path$", '人口与就业'),
        #                      sub_path,
        #                      r'E:\work\IdeaProjects\TestPython\GSL\FromExcel2PG\index.csv',
        #                      r"E:\work\gsl\output")
        try:
            # 先将该目录下的文件根据配置表进行重命名
            rename_files_func(input_floder + "/$sub_path$/".replace("$sub_path$", sub_path),
                              "./index_files/file_name_code_map.csv",
                              sub_path
                              )
            # 处理当前目录，将其格式转换并保存到csv
            getExcel2CSVRealData(input_floder + "/$sub_path$/".replace("$sub_path$", sub_path),
                                 sub_path,
                                 index_file_path,
                                 output_floder)
            print("==================> " + sub_path + " success <=======================")
        except Exception as e:
            print("==================> " + sub_path + " 失败 <=========================")
            print(str(e))
            sys.exit()

print("======== all over =======")
