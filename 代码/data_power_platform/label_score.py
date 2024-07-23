import re

import numpy as np
import pandas as pd
from impala.dbapi import connect
from pyspark import SparkContext
from pyspark.sql import SparkSession
import statsmodels.api as sm
import pymysql


def normalization(data):
    _range = np.max(data) - np.min(data)
    return (data - np.min(data)) / _range


def standardization(data):
    mu = np.mean(data, axis=0)
    sigma = np.std(data, axis=0)
    return list((data - mu) / sigma)

# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234',
               auth_mechanism='PLAIN')


cursor = conn.cursor()

# 对不同benchmark的数据进行标准化
data_table = ["dw_jbb_table", "dw_jvm_table", "dw_power_ssj_table", "dw_cpu_table"]
data_benchmark = [["distributed", "multijvm", "composite"], ["Jvm"], ["Power"], ["cpu2017_float_rates", "cpu2017_float_speed", "cpu2017_int_rates", "cpu2017_int_speed",
                                                                                 "cpu2006_float_rates", "cpu2006_float_speed", "cpu2006_int_rates", "cpu2006_int_speed"]]
for i in range(len(data_table)):
    for j in range(len(data_benchmark[i])):
        avg_data = std_data = 0
        sql = "select avg(total_result), std(total_result) from %s where table_type = '%s'"
        data = (data_table[i], data_benchmark[i][j])
        cursor.execute(sql % data)
        for result in cursor.fetchall():
            avg_data = float(result[0])
            std_data = float(result[1])
        # 进行标准化
        update_sql = "update %s set total_result = 1.0 * (total_result - %f) / %f WHERE table_type =  '%s'"
        update_data = (data_table[i], avg_data, std_data, data_benchmark[i][j])
        cursor.execute(update_sql % update_data)
        conn.commit()

# 计算不同benchmark和不同评价指标下cpu的打分均值
# (某种型号的cpu在某种benchmark下的均分值，对应benchmark在性能下的占比，以及对应benchmark所属的评价指标，若单个benchmark属于多类评价指标，则增加多条数据)
tdm_benchmark_indicator_table = "create table IF NOT EXISTS tdm_benchmark_indicator_table(" \
                    "Indicator_Type VARCHAR(255)," \
                    "Benchmark VARCHAR(255)," \
                    "Cpu_Name VARCHAR(255)," \
                    "Percentage VARCHAR(255)," \
                    "Total_Result VARCHAR(255)," \
                    "Percentage_Result VARCHAR(255))"

cursor.execute(tdm_benchmark_indicator_table)

benchmark_percentage = [[0.3, 0.3, 0.3], [0.1], [1], [0.3, 0.3, 0.3, 0.3, 0.2, 0.2, 0.2, 0.2]]
indicator = [[["JAVA"], ["JAVA"], ["JAVA"]], [["JAVA"]], [["POWER"]], [["FLOAT", "MULTI"], ["FLOAT", "SINGLE"], ["INT", "MULTI"], ["INT", "SINGLE"],
                                                                       ["FLOAT", "MULTI"], ["FLOAT", "SINGLE"], ["INT", "MULTI"], ["INT", "SINGLE"]]]

for i in range(len(data_table)):
    # 获取某种型号的cpu在某种benchmark下的均分值
    benchmark_score_sql = "select cpu_name, table_type, avg(total_result)  from %s group by cpu_name, table_type"
    benchmark_score_data = (data_table[i])
    cursor.execute(benchmark_score_sql % benchmark_score_data)
    for result in cursor.fetchall():
        cpu_name = result[0]
        table_type = result[1]
        avg_score = float(result[2])
        percentage_index = data_benchmark[i].index(table_type)
        for j in range(len(indicator[i][percentage_index])):
            # 写入表格中
            insert_sql = "insert into tdm_benchmark_indicator_table values('%s','%s','%s',%f,%f,%f)"
            insert_data = (indicator[i][percentage_index][j], table_type, cpu_name, benchmark_percentage[i][percentage_index], avg_score, benchmark_percentage[i][percentage_index] * avg_score)
            cursor.execute(insert_sql % insert_data)
            conn.commit()

# 计算不同类型的cpu在六种评价指标下分别的成绩
cpu_show_score_sql1 = "SELECT cpu_name,Indicator_Type,avg(Percentage_Result) cpu_indicator_score FROM `tdm_benchmark_indicator_table` group by cpu_name,Indicator_Type ORDER BY cpu_name;"

# 计算不同类型的cpu的总成绩（假定六种评价指标的权重相同，即同等重要）
cpu_show_score_sql2 = "CREATE VIEW tempt_cpu_indicator_score AS SELECT cpu_name,Indicator_Type,avg(Percentage_Result) cpu_indicator_score FROM `tdm_benchmark_indicator_table` group by cpu_name,Indicator_Type ORDER BY cpu_name;"
cpu_show_score_sql3 = "SELECT cpu_name, sum(cpu_indicator_score) cpu_score from tempt_cpu_indicator_score group by cpu_name;"

tdm_cpu_score_table = "create table IF NOT EXISTS tdm_cpu_score_table(" \
                    "Cpu_Name VARCHAR(255)," \
                    "Cpu_Score VARCHAR(255))"

cursor.execute(tdm_cpu_score_table)

cursor.execute(cpu_show_score_sql2)
cursor.execute(cpu_show_score_sql3)
for result in cursor.fetchall():
    insert_sql = "insert into tdm_cpu_score_table values('%s', %f)"
    insert_data = result
    cursor.execute(insert_sql % insert_data)
conn.commit()

# 获取某个公司测试的某个cpu型号的总分，用于后续公司成绩的打分
tdm_company_cpu_score_table = "create table IF NOT EXISTS tdm_company_cpu_score_table(" \
                    "Company_Name VARCHAR(255)," \
                    "Cpu_Name VARCHAR(255)," \
                    "Cpu_Score VARCHAR(255))"

cursor.execute(tdm_company_cpu_score_table)

for i in range(len(data_table)):
    company_cpu_score_sql1 = "SELECT t1.Company, t1.cpu_name, t2.cpu_score from %s t1, tdm_cpu_score_table t2 where t1.cpu_name = t2.cpu_name;"
    company_cpu_score_data = (data_table[i])
    cursor.execute(company_cpu_score_sql1 % company_cpu_score_data)
    for result in cursor.fetchall():
        insert_sql = "insert into tdm_company_cpu_score_table values('%s', '%s', '%s')"
        if "'" in result[0]:
            tempt_list = [result[0].replace("'", " "), result[1], result[2]]
            result = tuple(tempt_list)
        insert_data = result
        cursor.execute(insert_sql % insert_data)
conn.commit()

# 计算公司的成绩（主要以公司测评的cpu性能成绩和提交测评的次数为指标）
company_cpu_score_sql2 = "CREATE VIEW tempt_company_test_count AS SELECT company_name, count(*) company_test_count FROM `tdm_company_cpu_score_table` group by company_name;"
company_cpu_score_sql3 = "CREATE VIEW tempt_company_cpu_score AS SELECT company_name, avg(cpu_score) company_cpu_score FROM `tdm_company_cpu_score_table` group by company_name;"

cursor.execute(company_cpu_score_sql2)
cursor.execute(company_cpu_score_sql3)

tdm_company_score_table = "create table IF NOT EXISTS tdm_company_score_table(" \
                    "Company_Name VARCHAR(255)," \
                    "Company_Score VARCHAR(255))"

cursor.execute(tdm_company_score_table)

company_score_sql1 = "SELECT t1.Company_name, t1.company_test_count, t2.company_cpu_score from tempt_company_test_count t1, tempt_company_cpu_score t2 where t1.company_name = t2.company_name;"
cursor.execute(company_score_sql1)
for result in cursor.fetchall():
    log_count = 0
    count = int(result[1])
    score = float(result[2])
    if count == 1:
        log_count = 0.1
    else:
        log_count = np.log10(count)
    final_score = score * log_count
    tempt_list = [result[0], final_score]
    result = tuple(tempt_list)
    insert_sql = "insert into tdm_company_score_table values('%s',%f)"
    insert_data = result
    cursor.execute(insert_sql % insert_data)
conn.commit()

company_show_score_sql2 = "SELECT company_name,company_score FROM `tdm_company_score_table` ORDER BY company_score desc;"

# 给影响cpu性能的不同因素进行打分
# JAVA性能
java_jbb_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_jbb_table.chips, dw_jbb_table.cores, dw_jbb_table.threads, dw_jbb_table.cpu_frequency, dw_jbb_table.Primary_Instruction_Cache, dw_jbb_table.Primary_Data_Cache, dw_jbb_table.Secondary_Cache, dw_jbb_table.Memory, dw_jbb_table.Total_Result from dw_jbb_table, tdm_cpu_score_table where dw_jbb_table.cpu_name = tdm_cpu_score_table.cpu_name;"
java_jvm_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_jvm_table.chips, dw_jvm_table.cores, dw_jvm_table.threads, dw_jvm_table.cpu_frequency, dw_jvm_table.Primary_Instruction_Cache, dw_jvm_table.Primary_Data_Cache, dw_jvm_table.Secondary_Cache, dw_jvm_table.Memory, dw_jvm_table.Total_Result from dw_jvm_table, tdm_cpu_score_table where dw_jvm_table.cpu_name = tdm_cpu_score_table.cpu_name;"
java_indicator = ["cpu_score", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Memory"]

# jbb 每种 benchmark 数据权重均为 0.3
jbb_x_data = []
jbb_y_data = []
cursor.execute(java_jbb_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    jbb_x_data.append(tempt_x_data)
    jbb_y_data.append(float(result[-1]) * 0.3)
# 对每个影响因素和最后打分的值进行标准化
jbb_x_data = standardization(jbb_x_data)
jbb_y_data = standardization(jbb_y_data)
jbb_x_data = np.array(jbb_x_data)

# jvm 每种 benchmark 数据权重均为 0.1
jvm_x_data = []
jvm_y_data = []
cursor.execute(java_jvm_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    jvm_x_data.append(tempt_x_data)
    jvm_y_data.append(float(result[-1]) * 0.1)
# 对每个影响因素和最后打分的值进行标准化
jvm_x_data = standardization(jvm_x_data)
jvm_y_data = standardization(jvm_y_data)
jvm_x_data = np.array(jvm_x_data)

java_x_data = np.vstack((jvm_x_data, jbb_x_data))
java_y_data = jvm_y_data + jbb_y_data

df_java_x_data = pd.DataFrame(java_x_data,columns=java_indicator)

model = sm.OLS(java_y_data, df_java_x_data)     #生成模型
result = model.fit()                            #模型拟合
print(result.summary())
java_weight = result.params
with open("JAVA_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(java_indicator)):
        variable_name.write(str(java_indicator[i]) + "\t" + str(java_weight[i]))
        variable_name.write("\n")



# 能耗性能
power_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_power_ssj_table.nodes, dw_power_ssj_table.chips, dw_power_ssj_table.cores, dw_power_ssj_table.threads, dw_power_ssj_table.cpu_frequency, dw_power_ssj_table.Primary_Instruction_Cache, dw_power_ssj_table.Primary_Data_Cache, dw_power_ssj_table.Secondary_Cache, dw_power_ssj_table.tertiary_cache, dw_power_ssj_table.Memory, dw_power_ssj_table.Total_Result from dw_power_ssj_table, tdm_cpu_score_table where dw_power_ssj_table.cpu_name = tdm_cpu_score_table.cpu_name and dw_power_ssj_table.cores is not Null;"
power_indicator = ["cpu_score", "nodes", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory"]

# power_ssj 每种 benchmark 数据权重均为 1
power_x_data = []
power_y_data = []
cursor.execute(power_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    power_x_data.append(tempt_x_data)
    power_y_data.append(float(result[-1]))
# 对每个影响因素和最后打分的值进行标准化
power_x_data = standardization(power_x_data)
power_y_data = standardization(power_y_data)

df_power_x_data = pd.DataFrame(power_x_data, columns=power_indicator)

model = sm.OLS(power_y_data, df_power_x_data) #生成模型
result = model.fit()     #模型拟合
print(result.summary())   #模型描述

power_weight = result.params
with open("POWER_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(power_indicator)):
        variable_name.write(str(power_indicator[i]) + "\t" + str(power_weight[i]))
        variable_name.write("\n")




# 浮点数运算性能
cpu2017_float_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2017_float_rates' or dw_cpu_table.table_type = 'cpu2017_float_speed');"
cpu2006_float_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2006_float_rates' or dw_cpu_table.table_type = 'cpu2006_float_speed');"
cpu_float_indicator = ["cpu_score", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory"]

# cpu2017 每种 benchmark 数据权重均为 0.3
cpu2017_float_x_data = []
cpu2017_float_y_data = []
cursor.execute(cpu2017_float_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2017_float_x_data.append(tempt_x_data)
    cpu2017_float_y_data.append(float(result[-1]) * 0.3)
# 对每个影响因素和最后打分的值进行标准化
cpu2017_float_x_data = standardization(cpu2017_float_x_data)
cpu2017_float_y_data = standardization(cpu2017_float_y_data)
cpu2017_float_x_data = np.array(cpu2017_float_x_data)

# cpu2006 每种 benchmark 数据权重均为 0.2
cpu2006_float_x_data = []
cpu2006_float_y_data = []
cursor.execute(cpu2006_float_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2006_float_x_data.append(tempt_x_data)
    cpu2006_float_y_data.append(float(result[-1]) * 0.2)
# 对每个影响因素和最后打分的值进行标准化
cpu2006_float_x_data = standardization(cpu2006_float_x_data)
cpu2006_float_y_data = standardization(cpu2006_float_y_data)
cpu2006_float_x_data = np.array(cpu2006_float_x_data)

cpu_float_x_data = np.vstack((cpu2006_float_x_data, cpu2017_float_x_data))
cpu_float_y_data = cpu2006_float_y_data + cpu2017_float_y_data

cpu_float_x_data = pd.DataFrame(cpu_float_x_data,columns=cpu_float_indicator)

model = sm.OLS(cpu_float_y_data, cpu_float_x_data)     #生成模型
result = model.fit()                                   #模型拟合
print(result.summary())                                #模型描述

cpu_float_weight = result.params
with open("CPU_FLOAT_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(cpu_float_indicator)):
        variable_name.write(str(cpu_float_indicator[i]) + "\t" + str(cpu_float_weight[i]))
        variable_name.write("\n")



# 整数运算性能
cpu2017_int_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2017_int_rates' or dw_cpu_table.table_type = 'cpu2017_int_speed');"
cpu2006_int_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2006_int_rates' or dw_cpu_table.table_type = 'cpu2006_int_speed');"
cpu_int_indicator = ["cpu_score", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory"]

# cpu2017 每种 benchmark 数据权重均为 0.3
cpu2017_int_x_data = []
cpu2017_int_y_data = []
cursor.execute(cpu2017_int_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2017_int_x_data.append(tempt_x_data)
    cpu2017_int_y_data.append(float(result[-1]) * 0.3)
# 对每个影响因素和最后打分的值进行标准化
cpu2017_int_x_data = standardization(cpu2017_int_x_data)
cpu2017_int_y_data = standardization(cpu2017_int_y_data)
cpu2017_int_x_data = np.array(cpu2017_int_x_data)

# cpu2006 每种 benchmark 数据权重均为 0.2
cpu2006_int_x_data = []
cpu2006_int_y_data = []
cursor.execute(cpu2006_int_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2006_int_x_data.append(tempt_x_data)
    cpu2006_int_y_data.append(float(result[-1]) * 0.2)
# 对每个影响因素和最后打分的值进行标准化
cpu2006_int_x_data = standardization(cpu2006_int_x_data)
cpu2006_int_y_data = standardization(cpu2006_int_y_data)
cpu2006_int_x_data = np.array(cpu2006_int_x_data)

cpu_int_x_data = np.vstack((cpu2006_int_x_data, cpu2017_int_x_data))
cpu_int_y_data = cpu2006_int_y_data + cpu2017_int_y_data

cpu_int_x_data = pd.DataFrame(cpu_int_x_data,columns=cpu_int_indicator)

model = sm.OLS(cpu_int_y_data, cpu_int_x_data)     #生成模型
result = model.fit()                                   #模型拟合
print(result.summary())                                #模型描述


cpu_int_weight = result.params
with open("CPU_INT_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(cpu_int_indicator)):
        variable_name.write(str(cpu_int_indicator[i]) + "\t" + str(cpu_int_weight[i]))
        variable_name.write("\n")



# 单核性能
cpu2017_single_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2017_float_speed' or dw_cpu_table.table_type = 'cpu2017_int_speed');"
cpu2006_single_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2006_float_speed' or dw_cpu_table.table_type = 'cpu2006_int_speed');"
cpu_single_indicator = ["cpu_score", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory"]

# cpu2017 每种 benchmark 数据权重均为 0.3
cpu2017_single_x_data = []
cpu2017_single_y_data = []
cursor.execute(cpu2017_single_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2017_single_x_data.append(tempt_x_data)
    cpu2017_single_y_data.append(float(result[-1]) * 0.3)
# 对每个影响因素和最后打分的值进行标准化
cpu2017_single_x_data = standardization(cpu2017_single_x_data)
cpu2017_single_y_data = standardization(cpu2017_single_y_data)
cpu2017_single_x_data = np.array(cpu2017_single_x_data)

# cpu2006 每种 benchmark 数据权重均为 0.2
cpu2006_single_x_data = []
cpu2006_single_y_data = []
cursor.execute(cpu2006_single_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2006_single_x_data.append(tempt_x_data)
    cpu2006_single_y_data.append(float(result[-1]) * 0.2)
# 对每个影响因素和最后打分的值进行标准化
cpu2006_single_x_data = standardization(cpu2006_single_x_data)
cpu2006_single_y_data = standardization(cpu2006_single_y_data)
cpu2006_single_x_data = np.array(cpu2006_single_x_data)

cpu_single_x_data = np.vstack((cpu2006_single_x_data, cpu2017_single_x_data))
cpu_single_y_data = cpu2006_single_y_data + cpu2017_single_y_data

cpu_single_x_data = pd.DataFrame(cpu_single_x_data,columns=cpu_single_indicator)

model = sm.OLS(cpu_single_y_data, cpu_single_x_data)     #生成模型
result = model.fit()                                     #模型拟合
print(result.summary())                                  #模型描述

cpu_single_weight = result.params
with open("CPU_SINGLE_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(cpu_single_indicator)):
        variable_name.write(str(cpu_single_indicator[i]) + "\t" + str(cpu_single_weight[i]))
        variable_name.write("\n")


# 多核性能
cpu2017_multi_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2017_float_speed' or dw_cpu_table.table_type = 'cpu2017_int_speed');"
cpu2006_multi_indicator_sql = "select tdm_cpu_score_table.cpu_score, dw_cpu_table.chips, dw_cpu_table.cores, dw_cpu_table.threads, dw_cpu_table.cpu_frequency, dw_cpu_table.Primary_Instruction_Cache, dw_cpu_table.Primary_Data_Cache, dw_cpu_table.Secondary_Cache, dw_cpu_table.tertiary_cache, dw_cpu_table.Memory, dw_cpu_table.Total_Result from dw_cpu_table, tdm_cpu_score_table where dw_cpu_table.cpu_name = tdm_cpu_score_table.cpu_name and (dw_cpu_table.table_type = 'cpu2006_float_speed' or dw_cpu_table.table_type = 'cpu2006_int_speed');"
cpu_multi_indicator = ["cpu_score", "chips", "cores", "threads", "cpu_frequency", "primary_instruction_cache", "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory"]

# cpu2017 每种 benchmark 数据权重均为 0.3
cpu2017_multi_x_data = []
cpu2017_multi_y_data = []
cursor.execute(cpu2017_multi_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2017_multi_x_data.append(tempt_x_data)
    cpu2017_multi_y_data.append(float(result[-1]) * 0.3)
# 对每个影响因素和最后打分的值进行标准化
cpu2017_multi_x_data = standardization(cpu2017_multi_x_data)
cpu2017_multi_y_data = standardization(cpu2017_multi_y_data)
cpu2017_multi_x_data = np.array(cpu2017_multi_x_data)

# cpu2006 每种 benchmark 数据权重均为 0.2
cpu2006_multi_x_data = []
cpu2006_multi_y_data = []
cursor.execute(cpu2006_multi_indicator_sql)
for result in cursor.fetchall():
    tempt_x_data = []
    for i in range(len(result) - 1):
        tempt_x_data.append(float(result[i]))
    cpu2006_multi_x_data.append(tempt_x_data)
    cpu2006_multi_y_data.append(float(result[-1]) * 0.1)
# 对每个影响因素和最后打分的值进行标准化
cpu2006_multi_x_data = standardization(cpu2006_multi_x_data)
cpu2006_multi_y_data = standardization(cpu2006_multi_y_data)
cpu2006_multi_x_data = np.array(cpu2006_multi_x_data)

cpu_multi_x_data = np.vstack((cpu2006_multi_x_data, cpu2017_multi_x_data))
cpu_multi_y_data = cpu2006_multi_y_data + cpu2017_multi_y_data

cpu_multi_x_data = pd.DataFrame(cpu_multi_x_data,columns=cpu_multi_indicator)

model = sm.OLS(cpu_multi_y_data, cpu_multi_x_data)       #生成模型
result = model.fit()                                     #模型拟合
print(result.summary())                                  #模型描述

cpu_multi_weight = result.params
with open("CPU_multi_INDICATOR_WEIGHT.txt","w") as variable_name:
    for i in range(len(cpu_multi_indicator)):
        variable_name.write(str(cpu_multi_indicator[i]) + "\t" + str(cpu_multi_weight[i]))
        variable_name.write("\n")




# 关闭连接
# cursor.close()
# conn.close()
