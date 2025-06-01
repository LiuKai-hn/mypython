import requests


def get_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("请求失败，状态码：", response.status_code)
    except requests.exceptions.RequestException as e:
        print("请求发生异常：", e)


# 接口地址
api_url = "http://172.16.87.208:10087/cloudservice/api/hive_20230818153043"

# 调用函数获取数据
params = {
    "page": 1
}
data = get_data_from_api(api_url)

# 打印数据
print(data)
