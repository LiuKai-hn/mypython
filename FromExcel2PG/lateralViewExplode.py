import csv

with open('../../testPy/example.csv', 'r', encoding='utf-8') as csvfile:
    # 创建CSV读取器
    reader = csv.reader(csvfile)
    # 创建一个空列表
    rows = []
    header = next(reader)
    # 表头
    rows.append("country_name,year_date,data")

    # 遍历CSV文件中的每一行
    for row in reader:
        for i in range(1, len(row)):

            # print(row[0]+","+header[i]+","+row[i])
            # 将每一行炸开：
            # 以原来
            #     country_name	1960	1961	1962
            #     安哥拉 8.403292626,8.142251934,10.68105437
            # 改成
            #     country_name  year_date  data
            #     安哥拉           1960    8.403292626
            #     安哥拉           1961    8.142251934
            #     安哥拉           1962    10.68105437
            rows.append(row[0]+","+header[i]+","+row[i])


# 将列表写入文件
with open('../../testPy/output.csv', 'w', encoding='utf-8') as file:
    for row in rows:
        file.write(row + '\n')
    file.close()