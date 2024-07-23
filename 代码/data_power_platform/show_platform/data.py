import json
import pymysql

from impala.dbapi import connect

# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234',
               auth_mechanism='PLAIN')

cursor = conn.cursor()

class SourceDataDemo:

    def __init__(self):
        self.title = 'SPEC 数据中台可视化大屏'
        value_cpu = value_company = 0
        cursor.execute("select count(*) from tdm_cpu_score_table;")
        for result1 in cursor.fetchall():
            value_cpu = int(result1[0])
        cursor.execute("select count(*) from tdm_company_score_table;")
        for result2 in cursor.fetchall():
            value_company = int(result2[0])
        self.counter = {'name': 'cpu提交总量', 'value': value_cpu}
        self.counter2 = {'name': '公司测试总量', 'value': value_company}

    @property
    def echart1(self):

        tempt_dict = dict()
        benchmark_count_sql = ["select table_type, count(*) from dw_cpu_table group by table_type;",
                               "select table_type, count(*) from dw_jbb_table group by table_type;",
                               "select table_type, count(*) from dw_power_ssj_table group by table_type;",
                               "select table_type, count(*) from dw_jvm_table group by table_type;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] == 'cpu2017_int_speed':
                    tempt_dict['cpu17_is'] = int(result[1])
                elif result[0] == 'cpu2017_int_rates':
                    tempt_dict['cpu17_ir'] = int(result[1])
                elif result[0] == 'cpu2017_float_speed':
                    tempt_dict['cpu17_fs'] = int(result[1])
                elif result[0] == 'cpu2017_float_rates':
                    tempt_dict['cpu17_fr'] = int(result[1])
                elif result[0] == 'cpu2006_int_speed':
                    tempt_dict['cpu06_is'] = int(result[1])
                elif result[0] == 'cpu2006_int_rates':
                    tempt_dict['cpu06_ir'] = int(result[1])
                elif result[0] == 'cpu2006_float_speed':
                    tempt_dict['cpu06_fs'] = int(result[1])
                elif result[0] == 'cpu2006_float_rates':
                    tempt_dict['cpu06_fr'] = int(result[1])
                else:
                    tempt_dict[result[0]] = int(result[1])

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        echart = {
            'title': '每个benchmark提交数据量',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict],
            'series': [one_tuple[1] for one_tuple in tempt_dict],
        }
        return echart

    @property
    def echart2(self):

        tempt_dict = dict()
        benchmark_count_sql = ["select test_location, count(*) from dw_jbb_table group by test_location;",
                               "select test_location, count(*) from dw_power_ssj_table group by test_location;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        echart = {
            'title': '提交测试地区分布前十（全球）',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict][0:10],
            'series': [one_tuple[1] for one_tuple in tempt_dict][0:10]
        }

        return echart

    @property
    def echarts3_3(self):
        tempt_dict = dict()
        benchmark_count_sql = ["select test_location, count(*) from dw_jbb_table group by test_location;",
                               "select test_location, count(*) from dw_power_ssj_table group by test_location;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        data = []
        for one_tuple in tempt_dict:
            one_dict = dict()
            one_dict['name'] = one_tuple[0]
            one_dict['value'] = one_tuple[1]
            data.append(one_dict)

        echart = {
            'title': '地区分布前五',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict][0:5],
            'data': data[0:5]
        }
        return echart

    @property
    def echarts3_1(self):
        tempt_dict = dict()
        benchmark_count_sql = ["select cpu_name, count(*) from dw_cpu_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_jbb_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_power_ssj_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_jvm_table group by cpu_name;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        data = []
        for one_tuple in tempt_dict:
            one_dict = dict()
            one_dict['name'] = one_tuple[0]
            one_dict['value'] = one_tuple[1]
            data.append(one_dict)

        echart = {
            'title': 'cpu提交前五',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict][0:5],
            'data': data[0:5]
        }
        return echart

    @property
    def echarts3_2(self):
        tempt_dict = dict()
        benchmark_count_sql = ["select company, count(*) from dw_cpu_table group by company;",
                               "select company, count(*) from dw_jbb_table group by company;",
                               "select company, count(*) from dw_power_ssj_table group by company;",
                               "select company, count(*) from dw_jvm_table group by company;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        data = []
        for one_tuple in tempt_dict:
            one_dict = dict()
            one_dict['name'] = one_tuple[0]
            one_dict['value'] = one_tuple[1]
            data.append(one_dict)

        echart = {
            'title': '公司提交前五',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict][0:5],
            'data': data[0:5]
        }
        return echart

    @property
    def echart4(self):

        tempt_dict = dict()
        benchmark_count_sql = ["select Publish_Date,count(*) from dw_cpu_table GROUP BY Publish_Date;",
                               "select Publish_Date,count(*) from dw_jbb_table GROUP BY Publish_Date;",
                               "select Publish_Date,count(*) from dw_jvm_table GROUP BY Publish_Date;",
                               "select Publish_Date,count(*) from dw_power_ssj_table GROUP BY Publish_Date;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] is not None:
                    if i == 0:  # cpu
                        date_year = int(result[0].split("-")[1])
                    elif i == 1:  # jbb
                        if ',' in result[0]:
                            date_year = int(result[0].split(",")[1])
                        else:
                            date_year = int(result[0].split("-")[2])
                    elif i == 2:  # jvm
                        date_year = int('20' + result[0].split(" ")[2])
                    else:
                        date_year = int(result[0].split(",")[1])

                    if date_year not in tempt_dict.keys():
                        tempt_dict[date_year] = int(result[1])
                    else:
                        tempt_result = tempt_dict[date_year] + int(result[1])
                        tempt_dict[date_year] = tempt_result

        ten_date_dict = {}
        for one_year in tempt_dict.keys():
            if int(one_year) <= 2023 and int(one_year) >= 2013:
                ten_date_dict[one_year] = tempt_dict[one_year]

        data_key = sorted(ten_date_dict)
        data_value = []
        for one_key in data_key:
            data_value.append(int(ten_date_dict[one_key]))

        data_dict = []
        tempt = dict()
        tempt['name'] = '测试数据量'
        tempt['value'] = data_value
        data_dict.append(tempt)

        echart = {
            'title': '过去十年的年提交数据量',
            'names': ['年提交数据量'],
            'xAxis': data_key,
            'data': data_dict,
        }

        return echart

    @property
    def echart5(self):

        tempt_dict = dict()
        benchmark_count_sql = ["select cpu_name, count(*) from dw_cpu_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_jbb_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_power_ssj_table group by cpu_name;",
                               "select cpu_name, count(*) from dw_jvm_table group by cpu_name;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        echart = {
            'title': 'cpu型号提交数量排名前十',
            'xAxis': [one_tuple[0] for one_tuple in tempt_dict][0:10],
            'series': [one_tuple[1] for one_tuple in tempt_dict][0:10]
        }

        return echart

    @property
    def echart6(self):

        tempt_dict = dict()
        benchmark_count_sql = ["select company, count(*) from dw_cpu_table group by company;",
                               "select company, count(*) from dw_jbb_table group by company;",
                               "select company, count(*) from dw_power_ssj_table group by company;",
                               "select company, count(*) from dw_jvm_table group by company;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        x_data = []
        for one_tuple in tempt_dict:
            if one_tuple[0] == "Lenovo Global Technology":
                x_data.append("Lenovo")
            elif one_tuple[0] == "Hewlett-Packard Company":
                x_data.append("Hewlett-Packard")
            else:
                x_data.append(one_tuple[0])


        echart = {
            'title': '公司提交数量排名前十',
            'xAxis': x_data[0:10],
            'series': [one_tuple[1] for one_tuple in tempt_dict][0:10]
        }

        return echart

    @property
    def map_1(self):

        location_dict = dict()
        location_dict['Beijing'] = '北京'
        location_dict['Taipei'] = '台湾'
        location_dict['Jinan'] = '济南'
        location_dict['Shen Zhen'] = '深圳'
        location_dict['Hang Zhou'] = '杭州'
        location_dict['Taoyuan'] = '台湾'
        location_dict['Zhonghe Dist'] = '台湾'
        location_dict['New Taipei City'] = '台湾'
        location_dict['Nangang'] = '台湾'
        location_dict['XinTien'] = '台湾'

        tempt_dict = dict()
        benchmark_count_sql = ["select test_location, count(*) from dw_jbb_table group by test_location;",
                               "select test_location, count(*) from dw_power_ssj_table group by test_location;"]
        for i in range(len(benchmark_count_sql)):
            cursor.execute(benchmark_count_sql[i])
            for result in cursor.fetchall():
                if result[0] not in tempt_dict.keys():
                    tempt_dict[result[0]] = int(result[1])
                else:
                    tempt_result = tempt_dict[result[0]] + int(result[1])
                    tempt_dict[result[0]] = tempt_result

        tempt_dict = sorted(tempt_dict.items(), key=lambda x: x[1], reverse=True)

        data_dict = dict()
        for one_tuple in tempt_dict:
            if one_tuple[0] in location_dict.keys():
                if location_dict[one_tuple[0]] in data_dict.keys():
                    tempt = data_dict[location_dict[one_tuple[0]]]
                    data_dict[location_dict[one_tuple[0]]] = tempt + int(one_tuple[1])
                else:
                    data_dict[location_dict[one_tuple[0]]] = int(one_tuple[1])

        data = []
        for one_key in data_dict.keys():
            one_dict = dict()
            one_dict['name'] = one_key
            one_dict['value'] = data_dict[one_key]
            data.append(one_dict)

        echart = {
            'symbolSize': 20,
            'data': data,
        }
        return echart


class SourceData(SourceDataDemo):

    def __init__(self):
        """
        按照 SourceDataDemo 的格式覆盖数据即可
        """
        super().__init__()
        self.title = 'SPEC 数据中台可视化大屏'
