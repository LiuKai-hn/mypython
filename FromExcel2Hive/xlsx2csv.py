import openpyxl

# 打开Excel文件
workbook = openpyxl.load_workbook(r'E:\work\gsl\宏观经济hive\私营企业法人.xlsx')

# 选择sheet1页
sheet = workbook['Sheet1']


# 写入表头
header = ['2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014']

area_list = ['', '110000', '120000', '130000', '140000', '150000', '210000', '220000', '230000', '310000', '320000',
             '330000', '340000', '350000', '360000', '370000', '410000', '420000', '430000', '440000', '450000',
             '460000', '500000', '510000', '520000', '530000', '540000', '610000', '620000', '630000', '640000',
             '650000']

# area_list = ['110000', '120000']

lists = []


for area in area_list:
    # 逐行读取数据并写入csv文件
    count = 0
    for row in sheet.iter_rows(min_row=3, values_only=True):
        year = 2021
        for cell_value in row[1:]:
            line = "F,分省年度数据,A02010301,私营工业企业单位数," + str(year) + "," + str(cell_value) + "," + area
            print(line)
            if year < 2014:
                break
            lists.append(line)
            year = year - 1
            count = count + 1
        if count == 8:
            break
# 将数据写入文件
with open(r'E:\work\gsl\宏观经济hive\私营企业法人.csv', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lists))

'''
print("转换完成！")
'''
