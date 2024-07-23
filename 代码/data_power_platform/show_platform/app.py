#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2020/8/26 14:48
# @Author : way
# @Site :
# @Describe:

from flask import Flask, render_template
from data import *
from impala.dbapi import connect
from flask import Flask,render_template,request,url_for
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import *


app = Flask(__name__)

# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234',
               auth_mechanism='PLAIN')

cursor = conn.cursor()

@app.route('/')
def home():
    data = SourceData()
    return render_template('home.html', form=data, title=data.title)


@app.route('/big_screen')
def big_screen():
    data = SourceData()
    return render_template('big_screen.html', form=data, title=data.title)

@app.route('/cpu2017')
def cpu2017():
    cpu2017_sql = "select * from dw_cpu_table where table_type in ('cpu2017_float_rates', 'cpu2017_int_rates', 'cpu2017_float_speed', 'cpu2017_int_speed');"
    column_name = ['table_type', 'company', 'system_name', 'cores', 'chips', 'threads', 'publish_date', 'cpu_name', 'cpu_frequency',
                   'primary_instruction_cache', 'primary_data_cache', 'secondary_cache', 'tertiary_cache', 'memory', 'result']
    cursor.execute(cpu2017_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='dw_cpu2017详细数据集')

@app.route('/cpu2006')
def cpu2006():
    cpu2006_sql = "select * from dw_cpu_table where table_type in ('cpu2006_float_rates', 'cpu2006_int_rates', 'cpu2006_float_speed', 'cpu2006_int_speed');"
    column_name = ['table_type', 'company', 'system_name', 'cores', 'chips', 'threads', 'publish_date', 'cpu_name', 'cpu_frequency',
                   'primary_instruction_cache', 'primary_data_cache', 'secondary_cache', 'tertiary_cache', 'memory', 'result']
    cursor.execute(cpu2006_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='dw_cpu2006详细数据集')

@app.route('/jvm2008')
def jvm2008():
    jvm2008_sql = "select * from dw_jvm_table;"
    column_name = ['table_type', 'company', 'system_name', 'cores', 'chips', 'threads', 'jvm_name', 'jvm_version', 'publish_date',
                   'cpu_name', 'cpu_frequency', 'primary_instruction_cache', 'primary_data_cache', 'secondary_cache',
                   'memory', 'result']
    cursor.execute(jvm2008_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='dw_jvm2008详细数据集')

@app.route('/jbb2015')
def jbb2015():
    jbb2015_sql = "select * from dw_jbb_table;"
    column_name = ['table_type', 'company', 'system_name', 'jvm_name', 'jvm_version', 'test_location', 'publish_date', 'cpu_name',
                   'nodes', 'chips', 'cores', 'threads', 'cpu_frequency', 'primary_instruction_cache', 'primary_data_cache',
                   'secondary_cache', 'tertiary_cache', 'memory', 'result']
    cursor.execute(jbb2015_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='dw_jbb2015详细数据集')

@app.route('/power_ssj2008')
def power_ssj2008():
    power_ssj2008_sql = "select * from dw_power_ssj_table;"
    column_name = ['table_type', 'company', 'system_name', 'nodes', 'jvm_company', 'cpu_name', 'cpu_frequency', 'chips', 'cores',
                   'threads', 'memory', 'test_location', 'publish_date', 'primary_instruction_cache', 'primary_data_cache',
                   'secondary_cache', 'tertiary_cache', 'result']
    cursor.execute(power_ssj2008_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='dw_power_ssj2008详细数据集')

@app.route('/cpu_score')
def cpu_score():
    cpu_score_sql = "select * from tdm_cpu_score_table;"

    column_name = ['CPU_NAME', 'FLOAT', 'MULTI', 'SINGLE', 'INT', 'POWER', 'JAVA', 'AVERAGE']
    cursor.execute(cpu_score_sql)
    data = []
    columns = []
    for one_column_name in column_name:
        one_dict = dict()
        one_dict['data'] = one_column_name
        one_dict['title'] = one_column_name
        columns.append(one_dict)
    for result in cursor.fetchall():
        one_dict = dict()
        cpu_name = result[0]
        one_dict['CPU_NAME'] = cpu_name
        one_dict['AVERAGE'] = float(result[1])
        cpu_other_score_sql = "select indicator_type, cpu_indicator_score from tempt_cpu_indicator_score where cpu_name = '" + cpu_name + "';"
        cursor.execute(cpu_other_score_sql)
        for one_result in cursor.fetchall():
            one_dict[one_result[0]] = float(one_result[1])
        # 空白分值填充为0
        for one_column_name in column_name:
            if one_column_name not in one_dict.keys():
                one_dict[one_column_name] = 0
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='不同cpu型号在六种评价指标下的分数以及综合打分')

@app.route('/company_score')
def company_score():
    company_score_sql = "SELECT tdm_company_cpu_score_table.Company_Name, tdm_company_cpu_score_table.Cpu_Name, tdm_company_cpu_score_table.Cpu_Score, tdm_company_score_table.Company_Score FROM `tdm_company_cpu_score_table`,tdm_company_score_table where tdm_company_cpu_score_table.Company_Name = tdm_company_score_table.Company_Name;"

    column_name = ['company_name', 'cpu_name', 'cpu_score', 'company_score']

    cursor.execute(company_score_sql)
    data = []
    columns = []
    for result in cursor.fetchall():
        columns = []
        one_dict = dict()
        for i in range(len(column_name)):
            an_dict = dict()
            an_dict['data'] = column_name[i]
            an_dict['title'] = column_name[i]
            one_dict[column_name[i]] = result[i]
            columns.append(an_dict)
        data.append(one_dict)

    return render_template('total_data.html', data=data, columns=columns, title='不同厂商使用的cpu型号以及厂商的综合打分')

@app.route('/weight_java')
def weight_java():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\JAVA_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='JAVA性能评价指标下的硬件环境配置权重')

@app.route('/weight_power')
def weight_power():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\POWER_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='POWER性能评价指标下的硬件环境配置权重')

@app.route('/weight_cpu_float')
def weight_cpu_float():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\CPU_FLOAT_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='CPU FLOAT性能评价指标下的硬件环境配置权重')

@app.route('/weight_cpu_int')
def weight_cpu_int():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\CPU_INT_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='CPU INT性能评价指标下的硬件环境配置权重')

@app.route('/weight_cpu_multi')
def weight_cpu_multi():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\CPU_MULTI_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='CPU MULTI性能评价指标下的硬件环境配置权重')

@app.route('/weight_cpu_single')
def weight_cpu_single():
    indicator = []
    weight = []
    with open("D:\数据中台\code_data_power\CPU_SINGLE_INDICATOR_WEIGHT.txt", 'r') as file:
        data_list = file.read().split("\n")
        for one_data in data_list:
            if len(one_data) > 0:
                tempt = one_data.split("\t")
                indicator.append(tempt[0])
                weight.append(tempt[1])

    columns = []
    data = []
    one_dict = dict()
    for i in range(len(indicator)):
        an_dict = dict()
        an_dict['data'] = indicator[i]
        an_dict['title'] = indicator[i]
        one_dict[indicator[i]] = weight[i]
        columns.append(an_dict)
    data.append(one_dict)


    return render_template('total_data.html', data=data, columns=columns, title='CPU SINGLE性能评价指标下的硬件环境配置权重')


if __name__ == "__main__":
    app.run(host='127.0.0.1', debug=False)
