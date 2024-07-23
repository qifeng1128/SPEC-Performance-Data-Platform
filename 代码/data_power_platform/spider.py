from bs4 import BeautifulSoup
import requests
import pandas as pd
from impala.dbapi import connect
from pyspark import SparkContext
from pyspark.sql import SparkSession
import numpy as np

# 设置http请求头伪装成浏览器
send_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
        }

def get_html_content(url):
    # requests获取查找页面所有可以寻找的数据
    r = requests.get(url, headers=send_headers,verify=False)
    r.encoding = "utf-8"
    html = r.text

    # 将获取到的html送入bs4进行解析
    soup = BeautifulSoup(html, "html.parser")   # 获得解析后对象
    return soup

def get_jbb_extra_data(total_extra_url):
    extra_data_dict = dict()
    soup = get_html_content(total_extra_url)
    # 获取测试时间和地点
    table_content = soup.find_all(class_="section mainDesc")
    table_content = str(table_content).split("</td>")
    for one_table_line in table_content:
        if "Test location" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Test_Location"] = one_table_line[index + 1:]
        if "Publication" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Publish_Date"] = one_table_line[index + 1:]
    # 获取processor相关信息
    table_content = soup.find_all(class_="verticalDelim")
    table_content = str(table_content).split("</td></tr>")
    for one_table_line in table_content:
        if "CPU Name" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Name"] = one_table_line[index+1:]
        if "CPU Frequency (MHz)" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Frequency"] = one_table_line[index + 1:]
        if "Nodes Per System" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Nodes"] = one_table_line[index + 1:]
        if "Cores Per System" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Processor_Cores"] = one_table_line[index + 1:]
        if "Chips Per System" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Processor_Chips"] = one_table_line[index + 1:]
        if "Threads Per System" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Processor_Threads"] = one_table_line[index + 1:]
        if "Primary Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Primary_Cache"] = one_table_line[index + 1:]
        if "Secondary Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Secondary_Cache"] = one_table_line[index + 1:]
        if "Tertiary Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Tertiary_Cache"] = one_table_line[index + 1:]
        if "Disk" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Disk"] = one_table_line[index + 1:]
        if "Memory" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Memory"] = one_table_line[index + 1:] + "(GB)"

    return list(extra_data_dict.keys()), list(extra_data_dict.values())

def get_power_ssj_extra_data(total_extra_url):
    extra_data_dict = dict()
    soup = get_html_content(total_extra_url)
    # 获取测试时间和地点
    table_content = soup.find_all(class_="resultHeader")
    table_content = str(table_content).split("<td class=")
    for one_table_line in table_content:
        if "Test Location" in one_table_line:
            index = one_table_line.rfind("<td>")
            extra_data_dict["Test_Location"] = one_table_line[index + 4:].split("<")[0]
        if "Publication" in one_table_line:
            index = one_table_line.rfind("<td>")
            extra_data_dict["Publish_Date"] = one_table_line[index + 4:].split("<")[0]
    # 获取processor相关信息
    table_content = soup.find_all(class_="configSection")
    table_content = str(table_content).split("</tr>")
    for one_table_line in table_content:
        if "Primary Cache" in one_table_line:
            index = one_table_line[:-5].rfind(">")
            extra_data_dict["Primary_Cache"] = one_table_line[index + 1:-6]
        if "Secondary Cache" in one_table_line:
            index = one_table_line[:-5].rfind(">")
            extra_data_dict["Secondary_Cache"] = one_table_line[index + 1:-6]
        if "Tertiary Cache" in one_table_line:
            index = one_table_line[:-5].rfind(">")
            extra_data_dict["Tertiary_Cache"] = one_table_line[index + 1:-6]
        if "Disk Drive" in one_table_line:
            index = one_table_line[:-5].rfind(">")
            extra_data_dict["Disk"] = one_table_line[index + 1:-6]
        if "Memory Amount (GB)" in one_table_line:
            index = one_table_line[:-5].rfind(">")
            extra_data_dict["Memory"] = one_table_line[index + 1:-6] + "(GB)"

    return list(extra_data_dict.keys()), list(extra_data_dict.values())

def get_jvm_extra_data(total_extra_url):
    extra_data_dict = dict()
    soup = get_html_content(total_extra_url)
    # 获取测试时间和地点
    table_content = str(soup).split("<td>")
    flag = 0
    for one_table_content in table_content:
        if "Test date" in one_table_content and flag == 0:
            total_content = one_table_content.split("</tr>")
            for the_content in total_content:
                if "Test date" in the_content:
                    index = the_content.rfind("Test date:")
                    extra_data_dict["Publish_Date"] = the_content[index + 10:-13]
                    flag = 1
                    break
        elif "CPU name" in one_table_content:
            total_content = one_table_content.split("</tr>")
            for the_content in total_content:
                if "CPU name" in the_content:
                    index = the_content.replace("</td>","").rfind(">")
                    extra_data_dict["cpu_name"] = the_content[index + 6:-6]
                elif "CPU frequency" in the_content:
                    index = the_content.replace("</td>","").rfind(">")
                    extra_data_dict["CPU_Frequency"] = the_content[index + 6:-6]
                elif "Primary cache" in the_content:
                    index = the_content.replace("</td>","").rfind(">")
                    extra_data_dict["Primary_Cache"] = the_content[index + 6:-6]
                elif "Secondary cache" in the_content:
                    index = the_content.replace("</td>","").rfind(">")
                    extra_data_dict["Secondary_Cache"] = the_content[index + 6:-6]
                elif "Memory size" in the_content:
                    index = the_content.replace("</td>","").rfind(">")
                    extra_data_dict["Memory"] = the_content[index + 6:-6]

    return list(extra_data_dict.keys()), list(extra_data_dict.values())

def get_cpu2017_extra_data(total_extra_url):
    extra_data_dict = dict()
    soup = get_html_content(total_extra_url)
    # 获取测试时间和地点
    table_content = soup.find_all(class_="datebar")
    table_content = str(table_content).split("</td>")
    for one_table_line in table_content:
        if "Test Date" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Publish_Date"] = one_table_line[index + 1:]
    # 获取processor相关信息
    table_content = soup.find_all(class_="infobox")
    table_content = str(table_content).split("</td>")
    for one_table_line in table_content:
        if "CPU Name" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Name"] = one_table_line[index + 1:]
        if "Max MHz" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Frequency"] = one_table_line[index + 1:]
        if "Cache L1" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Primary_Cache"] = one_table_line[index + 1:]
        if "L2" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Secondary_Cache"] = one_table_line[index + 1:]
        if "L3" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Tertiary_Cache"] = one_table_line[index + 1:]
        if "Storage" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Disk"] = one_table_line[index + 1:]
        if "Memory" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Memory"] = one_table_line[index + 1:] + "(GB)"

    return list(extra_data_dict.keys()), list(extra_data_dict.values())

def get_cpu2006_extra_data(total_extra_url):
    extra_data_dict = dict()
    soup = get_html_content(total_extra_url)
    # 获取测试时间和地点
    table_content = soup.find_all(class_="datebar")
    table_content = str(table_content).split("</td>")
    for one_table_line in table_content:
        if "Test date" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Publish_Date"] = one_table_line[index + 1:]
    # 获取processor相关信息
    table_content = soup.find_all(class_="infobox")
    table_content = str(table_content).split("</td>")
    for one_table_line in table_content:
        if "CPU Name" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Name"] = one_table_line[index + 1:]
        if "CPU MHz" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["CPU_Frequency"] = one_table_line[index + 1:]
        if "Primary Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Primary_Cache"] = one_table_line[index + 1:]
        if "Secondary Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Secondary_Cache"] = one_table_line[index + 1:]
        if "L3 Cache" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Tertiary_Cache"] = one_table_line[index + 1:]
        if "Disk Subsystem" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Disk"] = one_table_line[index + 1:]
        if "Memory" in one_table_line:
            index = one_table_line.rfind(">")
            extra_data_dict["Memory"] = one_table_line[index + 1:] + "(GB)"

    return list(extra_data_dict.keys()), list(extra_data_dict.values())

# 获取spark的上下文
sc = SparkContext('local', 'spark_file_conversion')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

url = ["https://www.spec.org/jbb2015/results/jbb2015.html",                          # Java Client/Server
       "https://www.spec.org/power_ssj2008/results/power_ssj2008.html",              # Power
       "https://www.spec.org/jvm2008/results/jvm2008.html",                          # Java Client/Server
       "https://www.spec.org/cpu2017/results/cpu2017.html",                          # CPU
       "https://www.spec.org/cpu2006/results/cpu2006.html"]                          # CPU
front_url = ["https://www.spec.org/jbb2015/results/",                          # Java Client/Server
             "https://www.spec.org/power_ssj2008/results/",                    # Power
             "https://www.spec.org/jvm2008/results/",                          # Java Client/Server
             "https://www.spec.org/cpu2017/results/",                          # CPU
             "https://www.spec.org/cpu2006/results/"]
change_table_name = [False, True, True, True, True]
drop_table_name = ['Tester Name', 'Hardware Vendor  Test Sponsor', 'Tester', 'Test Sponsor', 'Test Sponsor']
csv_name = [['ods_html_jbb2015_composite', 'ods_html_jbb2015_distributed', 'ods_html_jbb2015_multijvm'],
            ['ods_html_power_ssj2008'],
            ['ods_html_jvm2008'],
            ['ods_html_cpu2017_floating_point_rates', 'ods_html_cpu2017_floating_point_speed', 'ods_html_cpu2017_integer_rates', 'ods_html_cpu2017_integer_speed'],
            ['ods_html_cpu2006_integer_speed', 'ods_html_cpu2006_floating_point_speed', 'ods_html_cpu2006_integer_rates', 'ods_html_cpu2006_floating_point_rates']]
jvm_path = [".base/SPECjvm2008.base.html",
            ".base/SPECjvm2008.base.html",
            ".base/SPECjvm2008.base.html",
            ".peak/SPECjvm2008.peak.html",
            ".peak/SPECjvm2008.peak.html",
            ".peak/SPECjvm2008.peak.html",
            ".peak/SPECjvm2008.peak.html",
            ".peak/SPECjvm2008.peak.html",
            ".peak/SPECjvm2008.peak.html",
            ".base/SPECjvm2008.base.html",
            ".base/SPECjvm2008.base.html",
            ".base/SPECjvm2008.base.html"]
for i in range(len(url)):
    # 获取网页上的所以表格内容
    soup = get_html_content(url[i])
    # 通过表格中的超链接获取额外的信息
    extra_columns = []
    extra_data = []
    hyperlink = soup.find_all('a')  # 获取网页中的所有超链接
    extra_url_count = 0
    for h in hyperlink:
        if "HTML" in h:         # 包含HTML字段为表格内的链接
            extra_url = h.get('href')
            total_extra_url = front_url[i] + extra_url
            # 获取额外的信息
            one_extra_data = []
            if i == 0:
                extra_columns, one_extra_data = get_jbb_extra_data(total_extra_url)    # 获取表格中每一行数据的额外信息
            elif i == 1:
                extra_columns, one_extra_data = get_power_ssj_extra_data(total_extra_url)
            elif i == 2:
                total_extra_url = total_extra_url[:-5] + jvm_path[extra_url_count]
                extra_columns, one_extra_data = get_jvm_extra_data(total_extra_url)
                extra_url_count = extra_url_count + 1
            elif i == 3:
                extra_columns, one_extra_data = get_cpu2017_extra_data(total_extra_url)
            elif i == 4:
                extra_columns, one_extra_data = get_cpu2006_extra_data(total_extra_url)
            extra_data.append(one_extra_data)
    # 获取网页中表格内相关信息
    tables = pd.read_html(soup.prettify())
    print(len(tables))
    # 存在一个页面具有多张表格的情况，根据每张表的数据量大小对额外信息进行划分
    total_table_count = 0
    # 处理每一个表格的信息
    for j in range(len(tables)):
        df = tables[j]
        if change_table_name[i]:
            # 将html上两行表格头进行合并
            change_column_name = []
            for k in range(len(df.columns)):
                old_column_name = df.columns[k][1]
                if df.columns[k][0] != df.columns[k][1]:
                    new_column_name = df.columns[k][0] + " # " + df.columns[k][1]
                else:
                    new_column_name = df.columns[k][0]
                change_column_name.append(new_column_name)
            df.columns = change_column_name
        # 将html上重复表格头进行删除
        df_clear = df.drop(df[df[drop_table_name[i]] == drop_table_name[i]].index)
        # 根据每张表的数据量大小对额外信息进行划分
        tempt = total_table_count + len(df_clear)
        # 增加额外信息
        for t in range(len(extra_columns)):
            df_clear[extra_columns[t]] = np.array(extra_data)[total_table_count:tempt, t]
        total_table_count = tempt

        # 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
        data_values = df_clear.values.tolist()
        data_columns = list(df_clear.columns)
        df_new = spark.createDataFrame(data_values, data_columns)
        # 将数据存储至hdfs中
        filepath = csv_name[i][j] + '.csv'
        df_new.write.format("csv").options(header='true', inferschema='true', sep='$').save(
            'hdfs://localhost:9000/user/hive/csv_data/' + filepath)


#, sep=';'

        # # 存储数据至hive中
        # pars = df_clear.values.tolist()
        # pars = list(map(tuple, pars))
        # cursor = conn.cursor()
        # cursor.executemany(insert_query[i][j], pars)



