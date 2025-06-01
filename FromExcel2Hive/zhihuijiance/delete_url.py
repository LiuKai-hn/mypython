import csv
import io
import re

def get_file_code_name_map(inputfile):
    # 打开CSV文件
    with io.open(inputfile, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        # 读取每一行数据
        count = 0
        for row in reader:
            # ent_id = row[0]  # 第一列
            news_context = row[0]  # code
            print(re.sub(r'https://\S+', '', news_context))
            count=count+1
        print(count)



if __name__ == "__main__":
    get_file_code_name_map(r"E:\work\IdeaProjects\TestPython\GSL\FromExcel2Hive\zhihuijiance\news_ent_id_info_limit1000.csv")





