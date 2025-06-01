import os
import xlrd

'''
本程序主要将目录下的xls文件类型转换成csv类型
需要传参：xls所在的目录 
'''


def read_excel(file_path):
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
            str_data = str(row[y])
            lists.append(row[0] + "," +
                         header[y] + "," +
                         str_data)

    # 将数据写入文件
    with open(file_path.split('.')[0] + '.csv', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lists))


def toCSVs(folder_path):
    # 要获取文件名的文件夹路径
    # 使用os.listdir()函数获取文件夹下的所有文件名
    file_names = os.listdir(folder_path)

    for file_name in file_names:
        abs_path = folder_path + file_name
        if abs_path.endswith('.csv'):
            os.remove(abs_path)

    # 转换该目录下所有 excel 文件为 csv
    for file_name in file_names:
        abs_path = folder_path + file_name
        if abs_path.endswith('.xls'):
            read_excel(abs_path)


# if __name__ == '__main__':
#
#     toCSVs(r"E:\work\gsl\202304\教育\\")
#     print("ok")

