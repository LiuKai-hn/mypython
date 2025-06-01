import csv
import os

import psycopg2 as psycopg2
import time


from GSL.FromExcel2PG.shi_jie_jing_ji.ReadExcel2CSV import toCSVs


def getConn():
    # 连接数据库
    conn = psycopg2.connect(
        host="localhost",
        database="test_idea",
        user="postgres",
        password="123456",
        port='5432'
    )

    conn.set_client_encoding('utf-8')

    return conn


# 将文件转化为入库格式
def getRightDataFunc(infile_path, F_LEVEL_TWO_NAME, outfile_path, F_TYPECODE, F_TYPE, F_LEVEL_ONE_NAME, F_NAME):

    F_CODE = get_F_CODE_F_NAME(F_LEVEL_ONE_NAME, F_LEVEL_TWO_NAME, F_NAME)

    if not F_CODE:
        print(infile_path + " ==> F_CODE 指标代码为空，请认真检查是否输入有误")
        return
    with open(infile_path, 'r', encoding='utf-8') as csvfile:
        # 创建CSV读取器
        reader = csv.reader(csvfile)
        # 创建一个空列表
        rows = []
        # 表头
        rows.append("F_TYPECODE," +
                    "F_TYPE," +
                    "F_CODE," +
                    "F_NAME," +
                    "F_TIME," +
                    "F_DATA," +
                    "F_AREA," +
                    "STATUS")

        # 遍历CSV文件中的每一行
        for row in reader:
            # line =""+
            #     F_TYPECODE + ","   # 数据类型代码
            #     F_TYPE + ","   # 数据类型
            #     F_CODE + ","   # 指标代码
            #     F_NAME + ","   # 指标代码中文名称
            #     row[1] + ","   # 指标时间
            #     row[2] + ","   # 指标数据
            #     row[0] + ","   # 地区
            #     "1"  # 状态

            line = F_TYPECODE + "," + F_TYPE + "," + F_CODE + "," + F_NAME + "," + row[1] + "," + row[2] + "," + row[0] + ",1"
            # print(line)
            rows.append(line)

    # 将列表写入文件
    outfile_path = r"" + outfile_path + F_CODE + ".csv"
    # 判断文件是否存在 ,如果存在则删除
    if os.path.exists(outfile_path):
        os.remove(outfile_path)
    with open(outfile_path, 'w', encoding='utf-8') as file:
        file.write('\n'.join(rows))
        file.close()

    result = [F_CODE, outfile_path]
    return result


def get_F_CODE_F_NAME(F_LEVEL_ONE_NAME, F_LEVEL_TWO_NAME, f_name):
    # 连接数据库
    conn = getConn()

    # 因为 文件名不能有特殊字符，需要单独处理
    if f_name == 'GDP单位能源使用量（购买力平价美元千克石油当量）':
        f_name = 'GDP单位能源使用量（购买力平价美元/千克石油当量）'

    # 创建游标对象
    cur = conn.cursor()

    # 执行查询语句
    cur.execute("select F_CODE,F_NAME " +
                "from " +
                "( "
                "    select F_CODE,F_NAME " +
                "    from mic_index_con " +
                "    where F_LEVEL_ONE_NAME = '$F_LEVEL_ONE_NAME$' and F_LEVEL_TWO_NAME='$F_LEVEL_TWO_NAME$'"
                ") t "
                "where F_NAME ='$F_NAME$'"
                .replace('$F_LEVEL_ONE_NAME$', F_LEVEL_ONE_NAME)
                .replace('$F_LEVEL_TWO_NAME$', F_LEVEL_TWO_NAME)
                .replace('$F_NAME$', f_name)
                )

    # 获取查询结果
    rows = cur.fetchall()
    # 如果输入的 指标中文名称有误，则退出
    if len(rows) == 0:
        return

    print("F_CODE==>" + rows[0][0])

    # 关闭游标和连接
    cur.close()
    conn.close()

    return rows[0][0]


def UpsertData(F_CODE, abs_file_path):
    # 创建游标对象
    conn = getConn()
    cur = conn.cursor()
    # 删除F_CODE原有的数据，避免多次插入
    delete_sql = "DELETE from " + "macro_eco_index where f_code='$F_CODE$';".replace('$F_CODE$', F_CODE)
    print("execute sql ==>" + delete_sql)
    # 执行查询语句
    cur.execute(delete_sql)
    # 提交事务
    conn.commit()
    time.sleep(1)

    # 执行copy from语句
    print("UpsertData copy from ==> " + abs_file_path)
    with open(abs_file_path, 'r', encoding='utf-8') as f:
        next(f)  # 跳过标题行
        columns = ('f_typecode', 'f_type', 'f_code', 'f_name', 'f_time', 'f_data', 'f_area', 'status')
        cur.copy_from(f, 'macro_eco_index', sep=',', columns=columns)  # 将数据从文件中复制到数据库表中
    # 提交事务
    conn.commit()
    time.sleep(1)
    cur.close()
    conn.close()


def getRealFileData(folder_path, F_LEVEL_TWO_NAME, output_folder_path):
    """

    :rtype: 将数据转换成入库的形式
    """
    # 将当前目录下的xls文件转化成csv
    toCSVs(folder_path)

    # 使用os.listdir()函数获取文件夹下的所有文件名
    file_names = os.listdir(folder_path)
    # 转换该目录下所有 excel 文件为 csv
    for file_name in file_names:
        abs_path = folder_path + file_name
        F_NAME = file_name.split('.')[0]
        if abs_path.endswith('.csv'):
            print("正在处理当前文件==>" + abs_path)
            arrays = getRightDataFunc(abs_path, F_LEVEL_TWO_NAME, output_folder_path, "C", "年度数据", "世界经济数据", F_NAME)
            UpsertData(arrays[0], arrays[1])


if __name__ == "__main__":

    # sub_paths = ['公共部门', '基础设施', '教育', '金融部门', '经济与增长', '科学技术', '能源与矿产', '农业与农村发展', '人口与就业', '私营部门', '外债']
    sub_paths = ['人口与就业']
    for sub_path in sub_paths:
        getRealFileData(r"E:\work\gsl\202304\$sub_path$\\".replace("$sub_path$", sub_path), sub_path, r'E:\work\gsl\output\\')

    # UpsertData('A030502401', r'E:\work\gsl\output\A030502401.csv')
