import re

import pandas as pd
from impala.dbapi import connect
from pyspark import SparkContext
from pyspark.sql import SparkSession

class JBB:
    def __init__(self, data_list, class_type):
        self.type = class_type               # 数据内容类别
        self.company = data_list[0]
        self.system_name = data_list[1]
        self.jvm_name = data_list[2]
        self.jvm_version = data_list[3]
        self.max_jops = data_list[4]
        self.critical_jops = data_list[5]
        self.test_location = data_list[6]
        self.publish_date = data_list[7]
        self.cpu_name = data_list[8]
        self.nodes = data_list[9]
        self.chips = data_list[10]
        self.cores = data_list[11]
        self.threads = data_list[12]
        self.cpu_frequency = data_list[13]
        self.primary_cache = data_list[14]
        self.secondary_cache = data_list[15]
        self.tertiary_cache = data_list[16]
        self.memory = data_list[18]

    def process_data(self):

        # 处理公司名(将缩写Inc. Co. LTD等内容删除)
        self.company = self.company.replace(" Inc.", "")
        self.company = self.company.replace(", Inc", "")
        self.company = self.company.replace(" CO., LTD.", "")
        self.company = self.company.replace(" Co., Ltd.", "")
        self.company = self.company.replace(".", "")
        self.company = self.company.replace(",", "")
        # 将不同形式却相同的公司名进行一致化
        if self.company == "Hewlett Packard Enterprise" or self.company == "Hewlett-Packard Company":
            self.company = "Hewlett Packard"
        if self.company == "Huawei Company" or self.company == "Huawei Technologies" or self.company == "Huawei Technologies Co Ltd":
            self.company = "Huawei"
        if self.company == "Supermicro":
            self.company = "Super Micro"

        # 处理系统名(将爬取的HTML字段内容删除)
        self.system_name = self.system_name.replace("  HTML", "")

        # 处理地点名（进行一致化）
        self.test_location = self.test_location.split(",")[0]
        if self.test_location == "Hangzhou":
            self.test_location = "Hang Zhou"
        if self.test_location == "Shenzhen":
            self.test_location = "Shen Zhen"

        # 处理日期（进行一致化）
        if "EDT" in self.publish_date or "EST" in self.publish_date:
            total_date = self.publish_date.split(" ")
            if '' in total_date:
                total_date.remove('')
            year = total_date[-1]
            month = total_date[1]
            date = total_date[2]
            self.publish_date = date + "-" + month + "-" + year
        elif self.publish_date == "MMM DD, YYYY":
            self.publish_date = ""

        # 将字符串转为数字
        self.nodes = float(self.nodes)
        self.chips = float(self.chips)
        self.cores = float(self.cores)
        self.threads = float(self.threads)
        self.max_jops = float(self.max_jops)
        self.critical_jops = float(self.critical_jops)

        # 处理cpu速度（两者取最大值）
        if "(typical to max)" in self.cpu_frequency:
            self.cpu_frequency = float(re.findall(r"\d+\.?\d*", self.cpu_frequency)[-1])
        else:
            self.cpu_frequency = float(self.cpu_frequency)

        # 处理一级缓存（均以KB per core为单位）
        instruction_cache = self.primary_cache.split("+")[0]
        data_cache = self.primary_cache.split("+")[1]
        if "KB" in self.primary_cache:
            self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
            self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
        else:
            self.primary_instruction_cache = self.primary_cache
            self.primary_data_cache = self.primary_cache

        # 处理二级缓存（均以KB per core为单位）
        if "I+D" in self.secondary_cache:
            self.secondary_cache = self.secondary_cache.split("I+D")[0]
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache or "M" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024
        # 例子：2 MB I on chip per chip (256 KB / 4 cores); 4 MB D on chip per chip (256 KB / 2 cores)
        elif "I" in self.secondary_cache or "D" in self.secondary_cache:
            p1 = re.compile(r'[(](.*?)[)]', re.S)    # 匹配括号内的内容
            secondary_instruction_cache = re.findall(p1, self.secondary_cache)[0]
            secondary_data_cache = re.findall(p1, self.secondary_cache)[1]
            instruction1 = instruction2 = data1 = data2 = 0
            if "KB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0])
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0])
            elif "MB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0]) * 1024
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0]) * 1024
            if "KB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0])
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0])
            elif "MB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0]) * 1024
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0]) * 1024
            self.secondary_cache = instruction1 / instruction2 + data1 / data2
        else:
            # 例子：256KB per core
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024

        # 处理三级缓存
        # 例子：60MB (I+D) on chip per chip
        if "I+D" in self.tertiary_cache:
            self.tertiary_cache = self.tertiary_cache.split("I+D")[0]
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache or "M" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024
        elif "None" in self.tertiary_cache:
            self.tertiary_cache = 0
        else:
            # 例子：8MB L3 cache per 4 core
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024

        # 处理内存大小
        self.memory = self.memory.replace("(GB)","")
        # 例子：DDR4 U-dimm 8GB 2133MHz(GB)
        if "DDR4 U-dimm " in self.memory:
            self.memory = self.memory.replace("DDR4 U-dimm ","")
        # 例子：DDR4 R-dimm 16GB 2400MHz(GB)
        if "DDR4 R-dimm " in self.memory:
            self.memory = self.memory.replace("DDR4 R-dimm ","")
        if self.memory == "64G 4Rx4 PC4-2666V-L(GB)":
            self.memory = "64GB 4Rx4 PC4-2666V-L(GB)"
        if self.memory == "":
            self.memory = 0
        else:
            self.memory = self.memory.split("GB")[0]
            # 例子：12 x 16 GB 2Rx4 PC4-2933Y-R, running at 2666(GB)
            if "x" in self.memory:
                memory1 = float(re.findall(r"\d+\.?\d*", self.memory)[0])
                memory2 = float(re.findall(r"\d+\.?\d*", self.memory)[1])
                self.memory = memory1 * memory2
            else:
                self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0])

        self.total_result = 0.3 * self.max_jops + 0.7 * self.critical_jops

        return [self.type, self.company, self.system_name, self.jvm_name, self.jvm_version, self.test_location,
                self.publish_date, self.cpu_name, self.nodes, self.chips, self.cores, self.threads, self.cpu_frequency, self.primary_instruction_cache,
                self.primary_data_cache, self.secondary_cache, self.tertiary_cache, self.memory, self.total_result]


class JVM:
    def __init__(self, data_list):
        self.type = "Jvm"  # 数据内容类别
        self.company = data_list[0]
        self.system_name = data_list[1]
        self.cores = data_list[2]
        self.chips = data_list[3]
        self.threads = data_list[4]
        self.jvm_name = data_list[6]
        self.jvm_version = data_list[7]
        self.result_base = data_list[8]
        self.result_peak = data_list[9]
        self.publish_date = data_list[10]
        self.cpu_name = data_list[11]
        self.cpu_frequency = data_list[12]
        self.primary_cache = data_list[13]
        self.secondary_cache = data_list[14]
        self.memory = data_list[15]

    def process_data(self):

        # 处理公司名(将缩写Inc. Co. LTD等内容删除)
        self.company = self.company.replace(" Inc.", "")
        self.company = self.company.replace(", Inc", "")
        self.company = self.company.replace(" CO., LTD.", "")
        self.company = self.company.replace(" Co., Ltd.", "")
        self.company = self.company.replace(".", "")
        self.company = self.company.replace(",", "")

        # 将不同形式却相同的公司名进行一致化
        if self.company == "Hewlett Packard Enterprise" or self.company == "Hewlett-Packard Company":
            self.company = "Hewlett Packard"
        if self.company == "Huawei Company" or self.company == "Huawei Technologies" or self.company == "Huawei Technologies Co Ltd":
            self.company = "Huawei"
        if self.company == "Supermicro":
            self.company = "Super Micro"

        # 处理系统名(将爬取的HTML字段内容删除)
        self.system_name = self.system_name.replace("  HTML", "")

        # 将字符串转为数字
        self.cores = float(self.cores)
        self.chips = float(self.chips)

        # 处理线程数
        if "(" in self.threads:
            self.threads = self.threads.split("(")[0].replace(" ","")
        self.threads = float(self.threads)

        # 处理打分
        if self.result_peak == "--":
            self.result_peak = 0.0
        else:
            self.result_peak = float(self.result_peak)
        if self.result_base == "--":
            self.result_base = 0.0
        else:
            self.result_base = float(self.result_base)

        # 处理cpu速度
        self.cpu_frequency = self.cpu_frequency.replace(" GHz","")
        self.cpu_frequency = float(self.cpu_frequency)

        # 处理一级缓存（均以KB per core为单位）
        if "+" not in self.primary_cache:
            self.primary_instruction_cache = self.primary_data_cache = float(re.findall(r"\d+\.?\d*", self.primary_cache)[0]) / 2
        else:
            instruction_cache = self.primary_cache.split("+")[0]
            data_cache = self.primary_cache.split("+")[1]
            if "KB" in self.primary_cache:
                self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
                self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            else:
                self.primary_instruction_cache = self.primary_cache
                self.primary_data_cache = self.primary_cache

        # 处理二级缓存（均以KB per core为单位）
        if "KB" in self.secondary_cache:
            self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
        elif "MB" in self.secondary_cache:
            self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024

        # 处理内存大小
        self.memory = self.memory.replace("MB","")
        if "GB" in self.memory:
            self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0]) * 1024
        else:
            self.memory = float(self.memory)

        self.total_result = max(self.result_base, self.result_peak)

        return [self.type, self.company, self.system_name, self.cores, self.chips, self.threads, self.jvm_name, self.jvm_version,
                self.publish_date, self.cpu_name, self.cpu_frequency, self.primary_instruction_cache,
                self.primary_data_cache, self.secondary_cache, self.memory, self.total_result]

class Power:
    def __init__(self,data_list):
        self.type = "Power"            # 数据内容类别
        self.company = data_list[0]
        self.system_name = data_list[1]
        self.nodes = data_list[2]
        self.jvm_company = data_list[3]
        self.cpu_name = data_list[4]
        self.cpu_frequency = data_list[5]
        self.chips = data_list[6]
        self.cores = data_list[7]
        self.threads = data_list[8]
        self.memory = data_list[9]
        self.result = data_list[13]
        self.test_location = data_list[14]
        self.primary_cache = data_list[16]
        self.secondary_cache = data_list[17]
        self.tertiary_cache = data_list[18]
        self.publish_date = 0

    def process_data(self):

        # 处理公司名(将缩写Inc. Co. LTD等内容删除)
        self.company = self.company.split("Parts Built")[0]
        self.company = self.company.split("  ")[0]
        self.company = self.company.replace(" Inc.", "")
        self.company = self.company.replace(", Inc", "")
        self.company = self.company.replace(", Ltd.", "")
        self.company = self.company.replace(" CO., LTD.", "")
        self.company = self.company.replace(" Co., Ltd.", "")
        self.company = self.company.replace("  Intel Corp.", "")
        self.company = self.company.replace(".", "")
        self.company = self.company.replace(",", "")
        self.company = self.company.replace("Incoporated", "Incorporated")
        # 将不同形式却相同的公司名进行一致化
        if self.company == "GIGA-BYTE Technology Co" or self.company == "GIGA-BYTE TECHNOLOGY":
            self.company = "GIGA-BYTE Technology"
        if self.company.count("Hewlett") == 1 or self.company.count("Hewlett-Packard") == 1:
            self.company = "Hewlett Packard"
        if self.company == "Huawei Technologies Co Ltd":
            self.company = "Huawei"
        if self.company == "Super Micro Computer" or self.company == "Supermicro Computer" or self.company == "Supermicro Computer":
            self.company = "Super Micro Computer"
        if self.company == "SuperMicro" or self.company == "Supermicro" or self.company == "Supermicro Inc":
            self.company = "Super Micro"
        if self.company == "Tyan Computer Corp" or self.company == "Tyan Computer Corporation":
            self.company = "Tyan Computer Corporation"

        # 处理系统名(将爬取的HTML字段内容删除) 并获取时间字段
        self.publish_date = self.system_name.split("HTML")[0]
        self.system_name = self.publish_date
        date_list = ["Jan ", "Feb ", "Mar ", "Apr ", "May ", "Jun ", "Jul ", "Aug ", "Sep ", "Oct ", "Nov ", "Dec "]
        for the_date in date_list:
            if the_date in self.publish_date:
                self.system_name = self.publish_date.split(the_date)[0]
                break
        self.publish_date = self.publish_date.replace(self.system_name, "").replace("|", "")
        self.system_name = self.system_name.replace("None", "")

        # 处理jvm公司名
        self.jvm_company = self.jvm_company.replace(", Inc.", "")
        self.jvm_company = self.jvm_company.replace(" Inc.", "")
        self.jvm_company = self.jvm_company.replace(", Inc", "")
        self.jvm_company = self.jvm_company.replace("Corp.", "Corporation")
        self.jvm_company = self.jvm_company.replace("Corparation", "Corporation")
        self.jvm_company = self.jvm_company.replace(".", "")
        self.jvm_company = self.jvm_company.replace(",", "")
        # 将不同形式却相同的公司名进行一致化
        if self.jvm_company == "BEA Systems" or self.jvm_company == "BEA SystemsInc" or self.jvm_company == "Bea Systems":
            self.jvm_company = "BEA System"
        if self.jvm_company == "Oracle":
            self.jvm_company = "Oracle Corporation"

        # 将字符串转为数字
        if len(self.cpu_frequency) == 0:
            self.cpu_frequency = 0.0
        else:
            self.cpu_frequency = float(self.cpu_frequency)
        if len(self.chips) == 0:
            self.chips = 0.0
        else:
            self.chips = float(self.chips)
        self.cores = float(self.cores)
        if len(self.threads) == 0:
            self.threads = 0.0
        else:
            self.threads = float(self.threads)
        self.memory = float(self.memory)
        if self.result == "NC":
            self.result = 0.0
        else:
            self.result = float(self.result)

        # 处理地点名（进行一致化）
        self.test_location = self.test_location.split(",")[0]
        if self.test_location == "Hangzhou" or self.test_location == "HangZhou":
            self.test_location = "Hang Zhou"
        if self.test_location == "Shenzhen":
            self.test_location = "Shen Zhen"
        if self.test_location == "JINAN" or self.test_location == "Ji'nan":
            self.test_location = "Jinan"

        # 处理一级缓存（均以KB per core为单位）
        if "I+D" in self.primary_cache:
            instruction_cache = float(re.findall(r"\d+\.?\d*", self.primary_cache)[0])
            self.primary_instruction_cache = self.primary_data_cache = float(instruction_cache) / 2
        else:
            instruction_cache = self.primary_cache.split("+")[0]
            data_cache = self.primary_cache.split("+")[1]
            if "KB" in self.primary_cache or "K" in self.primary_cache:
                self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
                self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            else:
                self.primary_instruction_cache = self.primary_cache
                self.primary_data_cache = self.primary_cache

        # 处理二级缓存（均以KB per core为单位）
        if "I+D" in self.secondary_cache:
            self.secondary_cache = self.secondary_cache.split("I+D")[0]
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache or "M" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024
        # 例子：2 MB I on chip per chip (256 KB / 4 cores); 4 MB D on chip per chip (256 KB / 2 cores)
        elif "I" in self.secondary_cache or "D" in self.secondary_cache:
            p1 = re.compile(r'[(](.*?)[)]', re.S)    # 匹配括号内的内容
            secondary_instruction_cache = re.findall(p1, self.secondary_cache)[0]
            secondary_data_cache = re.findall(p1, self.secondary_cache)[1]
            instruction1 = instruction2 = data1 = data2 = 0
            if "KB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0])
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0])
            elif "MB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0]) * 1024
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0]) * 1024
            if "KB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0])
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0])
            elif "MB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0]) * 1024
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0]) * 1024
            self.secondary_cache = instruction1 / instruction2 + data1 / data2
        else:
            # 例子：256KB per core
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024

        # 处理三级缓存
        # 例子：60MB (I+D) on chip per chip
        if "I+D" in self.tertiary_cache:
            self.tertiary_cache = self.tertiary_cache.split("I+D")[0]
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache or "M" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024
        elif "None" in self.tertiary_cache:
            self.tertiary_cache = 0
        else:
            # 例子：8MB L3 cache per 4 core
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024

        self.total_result = self.result

        return [self.type, self.company, self.system_name, self.nodes, self.jvm_company, self.cpu_name, self.cpu_frequency, self.chips, self.cores,
                self.threads, self.memory, self.test_location, self.publish_date, self.primary_instruction_cache, self.primary_data_cache,
                self.secondary_cache, self.tertiary_cache, self.total_result]

class CPU2017:
    def __init__(self, data_list, class_type):
        self.type = class_type                       # 数据内容类别
        self.company = data_list[0]
        self.system_name = data_list[1]
        if "speed" in self.type:
            self.cores = data_list[4]
            self.chips = data_list[5]
            self.threads_per_core = data_list[6]
            self.base_result = data_list[7]
            self.peak_result = data_list[8]
            self.base_energy = data_list[9]
            self.peak_energy = data_list[10]
            self.publish_date = data_list[11]
            self.cpu_name = data_list[12]
            self.cpu_frequency = data_list[13]
            self.primary_cache = data_list[14]
            self.secondary_cache = data_list[15]
            self.tertiary_cache = data_list[16]
            self.memory = data_list[17]
        else:
            self.cores = data_list[3]
            self.chips = data_list[4]
            self.threads_per_core = data_list[5]
            self.base_result = data_list[6]
            self.peak_result = data_list[7]
            self.base_energy = data_list[8]
            self.peak_energy = data_list[9]
            self.publish_date = data_list[10]
            self.cpu_name = data_list[11]
            self.cpu_frequency = data_list[12]
            self.primary_cache = data_list[13]
            self.secondary_cache = data_list[14]
            self.tertiary_cache = data_list[15]
            self.memory = data_list[16]

    def process_data(self):

        # 处理公司名(将缩写Inc. Co. LTD等内容删除)
        self.company = self.company.replace(" Co., Ltd.", "")
        self.company = self.company.replace(" Inc.", "")
        self.company = self.company.replace(" Inc", "")
        self.company = self.company.replace(" Corp., Ltd.", "")
        self.company = self.company.replace(".", "")
        self.company = self.company.replace(",", "")
        self.company = self.company.replace(";", "")
        # 将不同形式却相同的公司名进行一致化
        if self.company == "GIGA-BYTE TECHNOLOGY CO LTD":
            self.company = "GIGA-BYTE Technology"
        if self.company == "Supermicro":
            self.company = "Super Micro"

        # 处理系统名(将爬取的HTML字段内容删除)
        self.system_name = self.system_name.split(" HTML")[0]

        # 将字符串转为数字
        self.cores = float(self.cores)
        self.chips = float(self.chips)
        threads = float(self.threads_per_core) * self.cores
        self.cpu_frequency = float(self.cpu_frequency)


        # 处理结果
        if self.base_result == "NC":
            self.base_result = 0.0
        else:
            self.base_result = float(self.base_result)
        if self.peak_result == "Not Run" or self.peak_result == "NC":
            self.peak_result = 0.0
        else:
            self.peak_result = float(self.peak_result)
        if self.base_energy == "--" or self.base_energy == "NC":
            self.base_energy = 0.0
        else:
            self.base_energy = float(self.base_energy)
        if self.peak_energy == "--" or self.peak_energy == "Not Run" or self.peak_energy == "NC":
            self.peak_energy = 0.0
        else:
            self.peak_energy = float(self.peak_energy)

        # 处理一级缓存（均以KB per core为单位）
        if "I+D" in self.primary_cache:
            instruction_cache = float(re.findall(r"\d+\.?\d*", self.primary_cache)[0])
            self.primary_instruction_cache = self.primary_data_cache = float(instruction_cache) / 2
        elif self.primary_cache == "redacted":
            self.primary_instruction_cache = self.primary_data_cache = 0.0
        else:
            instruction_cache = self.primary_cache.split("+")[0]
            data_cache = self.primary_cache.split("+")[1]
            if "KB" in self.primary_cache or "K" in self.primary_cache:
                self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
                self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            else:
                self.primary_instruction_cache = self.primary_cache
                self.primary_data_cache = self.primary_cache

        # 处理二级缓存（均以KB per core为单位）
        if "I+D" in self.secondary_cache:
            self.secondary_cache = self.secondary_cache.split("I+D")[0]
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache or "M" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024
        # 例子：2 MB I on chip per chip (256 KB / 4 cores); 4 MB D on chip per chip (256 KB / 2 cores)
        elif "I" in self.secondary_cache or "D" in self.secondary_cache:
            p1 = re.compile(r'[(](.*?)[)]', re.S)  # 匹配括号内的内容
            secondary_instruction_cache = re.findall(p1, self.secondary_cache)[0]
            secondary_data_cache = re.findall(p1, self.secondary_cache)[1]
            instruction1 = instruction2 = data1 = data2 = 0
            if "KB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0])
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0])
            elif "MB" in secondary_instruction_cache:
                instruction1 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[0])[0]) * 1024
                instruction2 = float(re.findall(r"\d+\.?\d*", secondary_instruction_cache.split("/")[1])[0]) * 1024
            if "KB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0])
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0])
            elif "MB" in secondary_data_cache:
                data1 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[0])[0]) * 1024
                data2 = float(re.findall(r"\d+\.?\d*", secondary_data_cache.split("/")[1])[0]) * 1024
            self.secondary_cache = instruction1 / instruction2 + data1 / data2
        elif self.secondary_cache == "redacted":
            self.secondary_cache = 0.0
        else:
            # 例子：256KB per core
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024

        # 处理三级缓存
        # 例子：60MB (I+D) on chip per chip
        if "I+D" in self.tertiary_cache:
            self.tertiary_cache = self.tertiary_cache.split("I+D")[0]
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache or "M" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024
        elif "None" in self.tertiary_cache:
            self.tertiary_cache = 0.0
        elif self.tertiary_cache == "redacted":
            self.tertiary_cache = 0.0
        else:
            # 例子：8MB L3 cache per 4 core
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024

        # 处理内存大小(GB为单位)
        self.memory = self.memory.replace("(GB)", "").replace("(TB)", "")
        if "GB" in self.memory:
            self.memory = self.memory.split("GB")[0]
            self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0])
        elif "TB" in self.memory:
            self.memory = self.memory.split("TB")[0]
            self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0])

        self.total_result = 0.3 * self.peak_result + 0.7 * self.base_result

        return [self.type, self.company, self.system_name, self.cores, self.chips, threads,
                self.publish_date, self.cpu_name, self.cpu_frequency, self.primary_instruction_cache,
                self.primary_data_cache, self.secondary_cache, self.tertiary_cache, self.memory, self.total_result]

class CPU2006:
    def __init__(self, data_list, class_type):
        self.type = class_type                       # 数据内容类别
        self.company = data_list[0]
        self.system_name = data_list[1]
        self.cores = data_list[3]
        self.chips = data_list[4]
        self.threads_per_core = data_list[6]
        self.base_result = data_list[7]
        self.peak_result = data_list[8]
        self.publish_date = data_list[9]
        self.cpu_name = data_list[10]
        self.cpu_frequency = data_list[11]
        self.primary_cache = data_list[12]
        self.secondary_cache = data_list[13]
        self.tertiary_cache = data_list[14]
        self.memory = data_list[15]

    def process_data(self):

        # 处理公司名(将缩写Inc. Co. LTD等内容删除)
        self.company = self.company.replace(" Co., Ltd.", "")
        self.company = self.company.replace(" Inc.", "")
        self.company = self.company.replace(" Inc", "")
        self.company = self.company.replace(" Corp., Ltd.", "")
        self.company = self.company.replace(".", "")
        self.company = self.company.replace(",", "")
        self.company = self.company.replace(";", "")
        # 将不同形式却相同的公司名进行一致化
        if self.company == "GIGA-BYTE TECHNOLOGY CO LTD":
            self.company = "GIGA-BYTE Technology"
        if self.company == "Supermicro":
            self.company = "Super Micro"
        if self.company == "Acerorporation":
            self.company = "Acerorporated"
        if self.company == "Apple":
            self.company = "Apple Computer"
        if self.company == "Fujitsu Limited" or self.company == "Fujitsu Siemens Computers":
            self.company = "Fujitsu"

        # 处理系统名(将爬取的HTML字段内容删除)
        self.system_name = self.system_name.split(" HTML")[0]

        # 将字符串转为数字
        self.cores = float(self.cores)
        self.chips = float(self.chips)
        threads = float(self.threads_per_core) * self.cores
        self.cpu_frequency = float(self.cpu_frequency)


        # 处理结果
        if self.base_result == "NC" or self.base_result == "NaN":
            self.base_result = 0.0
        else:
            self.base_result = float(self.base_result)
        if self.peak_result == "Not Run" or self.peak_result == "NC" or self.peak_result == "NaN" or self.peak_result == "--":
            self.peak_result = 0.0
        else:
            self.peak_result = float(self.peak_result)

        # 处理一级缓存（均以KB per core为单位）
        if "+" in self.primary_cache:
            instruction_cache = self.primary_cache.split("+")[0]
            data_cache = self.primary_cache.split("+")[1]
            if "KB" in self.primary_cache or "K" in self.primary_cache:
                self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
                self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            else:
                self.primary_instruction_cache = self.primary_cache
                self.primary_data_cache = self.primary_cache
        else:
            instruction_cache = self.primary_cache.split(";")[0]
            data_cache = self.primary_cache.split(";")[1]
            if "KB" in self.primary_cache or "K" in self.primary_cache:
                self.primary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
                self.primary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            else:
                self.primary_instruction_cache = self.primary_cache
                self.primary_data_cache = self.primary_cache

        # # 处理二级缓存（均以KB per core为单位）
        self.secondary_instruction_cache = self.secondary_data_cache = 0
        if "I+D" in self.secondary_cache:
            self.secondary_cache = self.secondary_cache.split("I+D")[0]
            if "KB" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0])
            elif "MB" in self.secondary_cache or "M" in self.secondary_cache:
                self.secondary_cache = float(re.findall(r"\d+\.?\d*", self.secondary_cache)[0]) * 1024
        # 例子：1 MB I + 256 KB D on chip per core
        elif "+" in self.secondary_cache:
            instruction_cache = self.secondary_cache.split("+")[0]
            data_cache = self.secondary_cache.split("+")[1]
            self.secondary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
            self.secondary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            if "MB" in instruction_cache:
                self.secondary_instruction_cache = self.secondary_instruction_cache * 1024
            elif "MB" in data_cache:
                self.secondary_data_cache = self.secondary_data_cache * 1024
            self.secondary_cache = self.secondary_instruction_cache + self.secondary_data_cache
        # 例子：2 MB I on chip per chip (256 KB / 4 cores); 4 MB D on chip per chip (256 KB / 2 cores)
        else:
            instruction_cache = self.secondary_cache.split(";")[0]
            data_cache = self.secondary_cache.split(";")[1]
            self.secondary_instruction_cache = float(re.findall(r"\d+\.?\d*", instruction_cache)[0])
            self.secondary_data_cache = float(re.findall(r"\d+\.?\d*", data_cache)[0])
            if "MB" in instruction_cache:
                self.secondary_instruction_cache = self.secondary_instruction_cache * 1024
            elif "MB" in data_cache:
                self.secondary_data_cache = self.secondary_data_cache * 1024
            self.secondary_cache = self.secondary_instruction_cache + self.secondary_data_cache

        # 处理三级缓存
        # 例子：60MB (I+D) on chip per chip
        if "I+D" in self.tertiary_cache:
            self.tertiary_cache = self.tertiary_cache.split("I+D")[0]
            if "KB" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0])
            elif "MB" in self.tertiary_cache or "M" in self.tertiary_cache:
                self.tertiary_cache = float(re.findall(r"\d+\.?\d*", self.tertiary_cache)[0]) * 1024
        elif "None" in self.tertiary_cache:
            self.tertiary_cache = 0.0

        # 处理内存大小(GB为单位)
        self.memory = self.memory.replace("(GB)", "").replace("(TB)", "")
        if "GB" in self.memory:
            self.memory = self.memory.split("GB")[0]
            self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0])
        elif "TB" in self.memory:
            self.memory = self.memory.split("TB")[0]
            self.memory = float(re.findall(r"\d+\.?\d*", self.memory)[0])

        self.total_result = 0.3 * self.peak_result + 0.7 * self.base_result

        return [self.type, self.company, self.system_name, self.cores, self.chips, threads,
                self.publish_date, self.cpu_name, self.cpu_frequency, self.primary_instruction_cache, self.primary_data_cache,
                self.secondary_cache, self.tertiary_cache, self.memory, self.total_result]

# 获取spark的上下文
sc = SparkContext('local', 'spark_file_conversion')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234',
               auth_mechanism='PLAIN')
cursor = conn.cursor()

# Java_Client
jbb2015_select_sql = ["select * from ods_html_jbb2015_distributed","select * from ods_html_jbb2015_multijvm","select * from ods_html_jbb2015_composite"]
jbb2015_class_type = ["distributed", "multijvm", "composite"]
jbb2015_columns = ["Table_Type", "Company", "System_Name", "Jvm_Name", "Jvm_Version", "Test_Location",
                   "Publish_Date", "Cpu_Name", "Nodes", "Chips", "Cores", "Threads", "Cpu_Frequency", "Primary_Instruction_Cache",
                   "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory", "Total_Result"]

jbb2015_data = []
for i in range (len(jbb2015_select_sql)):
    # 获取jbb2015表中的每行数据
    cursor.execute(jbb2015_select_sql[i])
    for result in cursor.fetchall():
        jbb = JBB(result, jbb2015_class_type[i])
        process_data = jbb.process_data()
        jbb2015_data.append(process_data)
jbb2015_data = pd.DataFrame(jbb2015_data, columns=jbb2015_columns)

jbb2015_data.to_csv("dw_jbb_table.csv")

# 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
data_values = jbb2015_data.values.tolist()
data_columns = list(jbb2015_data.columns)
df_new = spark.createDataFrame(data_values, data_columns)
# 将数据存储至hdfs中
filepath = 'dw_jbb.csv'
df_new.write.format("csv").options(header='true', inferschema='true', sep='$').save(
    'hdfs://localhost:9000/user/hive/csv_data/' + filepath)

# JVM
jvm2008_select_sql = ["select * from ods_html_jvm2008_new"]
jvm2008_data = []
jvm2008_columns = ["Table_Type", "Company", "System_Name", "Cores", "Chips", "Threads", "Jvm_Name", "Jvm_Version",
                   "Publish_Date", "Cpu_Name", "Cpu_Frequency", "Primary_Instruction_Cache",
                   "Primary_Data_Cache", "Secondary_Cache", "Memory", "Total_Result"]

for one_jvm2008_sql in jvm2008_select_sql:
    # 获取jvm2008表中的每行数据
    cursor.execute(one_jvm2008_sql)
    for result in cursor.fetchall():
        jvm = JVM(result)
        process_data = jvm.process_data()
        jvm2008_data.append(process_data)
jvm2008_data = pd.DataFrame(jvm2008_data, columns=jvm2008_columns)

jvm2008_data.to_csv("dw_jvm_table.csv")

# 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
data_values = jvm2008_data.values.tolist()
data_columns = list(jvm2008_data.columns)
df_new = spark.createDataFrame(data_values, data_columns)
# 将数据存储至hdfs中
filepath = 'dw_jvm.csv'
df_new.write.format("csv").options(header='true', inferschema='true', sep='$').save(
    'hdfs://localhost:9000/user/hive/csv_data/' + filepath)

# Power
power_ssj2008_select_sql = ["select * from ods_html_power2008"]
power_ssj2008_data = []
power_ssj2008_columns = ["Table_Type", "Company", "System_Name", "Nodes", "Jvm_Company", "Cpu_Name", "Cpu_Frequency", "Chips", "Cores",
                         "Threads", "Memory", "Test_Location", "Publish_Date", "Primary_Instruction_Cache",
                         "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Total_Result"]

for one_power_ssj2008_sql in power_ssj2008_select_sql:
    # 获取power_ssj2008表中的每行数据
    cursor.execute(one_power_ssj2008_sql)
    for result in cursor.fetchall():
        power = Power(result)
        process_data = power.process_data()
        power_ssj2008_data.append(process_data)
power_ssj2008_data = pd.DataFrame(power_ssj2008_data, columns=power_ssj2008_columns)

power_ssj2008_data.to_csv("dw_power_ssj_table.csv")

# 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
data_values = power_ssj2008_data.values.tolist()
data_columns = list(power_ssj2008_data.columns)
df_new = spark.createDataFrame(data_values, data_columns)
# 将数据存储至hdfs中
filepath = 'dw_power_ssj.csv'
df_new.write.format("csv").options(header='true', inferschema='true', sep='$').save(
    'hdfs://localhost:9000/user/hive/csv_data/' + filepath)


# CPU
cpu2017_select_sql = ["select * from ods_html_cpu2017_floating_point_rates", "select * from ods_html_cpu2017_floating_point_speed",
                      "select * from ods_html_cpu2017_integer_rates", "select * from ods_html_cpu2017_integer_speed"]
cpu2017_class_type = ["cpu2017_float_rates", "cpu2017_float_speed", "cpu2017_int_rates", "cpu2017_int_speed"]
cpu_columns = ["Table_Type", "Company", "System_Name", "Cores", "Chips", "Threads",
                   "Publish_Date", "Cpu_Name", "Cpu_Frequency", "Primary_Instruction_Cache",
                   "Primary_Data_Cache", "Secondary_Cache", "Tertiary_Cache", "Memory", "Total_Result"]

cpu_data = []
for i in range(len(cpu2017_select_sql)):
    # 获取cpu2017表中的每行数据
    cursor.execute(cpu2017_select_sql[i])
    for result in cursor.fetchall():
        cpu = CPU2017(result, cpu2017_class_type[i])
        process_data = cpu.process_data()
        cpu_data.append(process_data)

cpu2006_select_sql = ["select * from ods_html_cpu2006_floating_point_rates", "select * from ods_html_cpu2006_floating_point_speed",
                      "select * from ods_html_cpu2006_integer_rates", "select * from ods_html_cpu2006_integer_speed"]
cpu2006_class_type = ["cpu2006_float_rates", "cpu2006_float_speed", "cpu2006_int_rates", "cpu2006_int_speed"]

for i in range(len(cpu2006_select_sql)):
    # 获取sfs2014表中的每行数据
    cursor.execute(cpu2006_select_sql[i])
    for result in cursor.fetchall():
        cpu = CPU2006(result, cpu2006_class_type[i])
        process_data = cpu.process_data()
        cpu_data.append(process_data)
cpu_data = pd.DataFrame(cpu_data, columns=cpu_columns)

cpu_data.to_csv("dw_cpu_table.csv")

# 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
data_values = cpu_data.values.tolist()
data_columns = list(cpu_data.columns)
df_new = spark.createDataFrame(data_values, data_columns)
# 将数据存储至hdfs中
filepath = 'dw_cpu.csv'
df_new.write.format("csv").options(header='true', inferschema='true', sep='$').save(
    'hdfs://localhost:9000/user/hive/csv_data/' + filepath)


# cursor.execute("select * from dw_cpu_table")
# for result in cursor.fetchall():
#     print(result)
#     break
# 关闭连接
cursor.close()
conn.close()
