from impala.dbapi import connect

# cpu2017_floating_point_rates
ods_html_cpu2017_fpr_table = "create table IF NOT EXISTS ods_html_cpu2017_floating_point_rates(" \
                             "Test_Sponsor String," \
                             "System_Name String," \
                             "Base_Copies String," \
                             "Processor1Enabled_Cores String," \
                             "Processor1Enabled_Chips String," \
                             "Processor1Threads_Core String," \
                             "Results1Base String," \
                             "Results1Peak String," \
                             "Energy1Base String," \
                             "Energy1Peak String," \
                             "Publish_Date String," \
                             "CPU_Name String," \
                             "CPU_Frequency String," \
                             "Primary_Cache String," \
                             "Secondary_Cache String," \
                             "Tertiary_Cache String," \
                             "Memory String," \
                             "Disk String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2017_floating_point_speed
ods_html_cpu2017_fps_table = "create table IF NOT EXISTS ods_html_cpu2017_floating_point_speed(" \
                             "Test_Sponsor String," \
                             "System_Name String," \
                             "Parallel String,"\
                             "Base_Copies String," \
                             "Processor1Enabled_Cores String," \
                             "Processor1Enabled_Chips String," \
                             "Processor1Threads_Core String," \
                             "Results1Base String," \
                             "Results1Peak String," \
                             "Energy1Base String," \
                             "Energy1Peak String," \
                             "Publish_Date String," \
                             "CPU_Name String," \
                             "CPU_Frequency String," \
                             "Primary_Cache String," \
                             "Secondary_Cache String," \
                             "Tertiary_Cache String," \
                             "Memory String," \
                             "Disk String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2017_integer_rates
ods_html_cpu2017_ir_table = "create table IF NOT EXISTS ods_html_cpu2017_integer_rates(" \
                            "Test_Sponsor String," \
                            "System_Name String," \
                            "Base_Copies String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Energy1Base String," \
                            "Energy1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String," \
                            "Disk String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2017_integer_speed
ods_html_cpu2017_is_table = "create table IF NOT EXISTS ods_html_cpu2017_integer_speed(" \
                            "Test_Sponsor String," \
                            "System_Name String," \
                            "Parallel String," \
                            "Base_Copies String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Energy1Base String," \
                            "Energy1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String," \
                            "Disk String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jbb2015_distributed
ods_html_jbb2015_distributed_table = "create table IF NOT EXISTS ods_html_jbb2015_distributed(" \
                                     "Tester_Name String," \
                                     "System_Name String," \
                                     "JVM_Name String," \
                                     "JVM_Version String," \
                                     "max_jOPS String," \
                                     "critical_jOPS String," \
                                     "Test_Location String," \
                                     "Publish_Date String," \
                                     "CPU_Name String," \
                                     "Nodes String," \
                                     "Processor_Chips String," \
                                     "Processor_Cores String," \
                                     "Processor_Threads String," \
                                     "CPU_Frequency String," \
                                     "Primary_Cache String," \
                                     "Secondary_Cache String," \
                                     "Tertiary_Cache String," \
                                     "Disk String," \
                                     "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jbb2015_composite
ods_html_jbb2015_composite_table = "create table IF NOT EXISTS ods_html_jbb2015_composite(" \
                                    "Tester_Name String," \
                                    "System_Name String," \
                                    "JVM_Name String," \
                                    "JVM_Version String," \
                                    "max_jOPS String," \
                                    "critical_jOPS String," \
                                    "Test_Location String," \
                                    "Publish_Date String," \
                                    "CPU_Name String," \
                                    "Nodes String," \
                                    "Processor_Chips String," \
                                    "Processor_Cores String," \
                                    "Processor_Threads String," \
                                    "CPU_Frequency String," \
                                    "Primary_Cache String," \
                                    "Secondary_Cache String," \
                                    "Tertiary_Cache String," \
                                    "Disk String," \
                                    "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jbb2015_multijvm
ods_html_jbb2015_multijvm_table = "create table IF NOT EXISTS ods_html_jbb2015_multijvm(" \
                                  "Tester_Name String," \
                                  "System_Name String," \
                                  "JVM_Name String," \
                                  "JVM_Version String," \
                                  "max_jOPS String," \
                                  "critical_jOPS String," \
                                  "Test_Location String," \
                                  "Publish_Date String," \
                                  "CPU_Name String," \
                                  "Nodes String," \
                                  "Processor_Chips String," \
                                  "Processor_Cores String," \
                                  "Processor_Threads String," \
                                  "CPU_Frequency String," \
                                  "Primary_Cache String," \
                                  "Secondary_Cache String," \
                                  "Tertiary_Cache String," \
                                  "Disk String," \
                                  "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# power2008
ods_html_power2008_table = "create table IF NOT EXISTS ods_html_power2008(" \
                               "Hardware_Vendor_Test_Sponsor String," \
                               "System_Enclosure_if_applicable String," \
                               "Nodes String," \
                               "JVM_Vendor String," \
                               "Processor1CPU_Description String," \
                               "Processor1MHz String," \
                               "Processor1Chips String," \
                               "Processor1Cores String," \
                               "Processor1Total_Threads String," \
                               "Total_Memory_GB String," \
                               "Submeasurements_ssj_ops String," \
                               "Submeasurements_avg_watts String," \
                               "Submeasurements_avg_watts_active_idle String," \
                               "Result_overall_ssj_ops_watt String," \
                               "Test_Location String," \
                               "Publish_Date String," \
                               "Primary_Cache String," \
                               "Secondary_Cache String," \
                               "Tertiary_Cache String," \
                               "Memory String," \
                               "Disk String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jvm2008
ods_html_jvm2008_table = "create table IF NOT EXISTS ods_html_jvm2008_new(" \
                                "Tester String," \
                                "System_Name String," \
                                "Processor_Cores String," \
                                "Processor_Chips String," \
                                "Processor_HW_Threads String," \
                                "Processor_Threads_Core String," \
                                "JVM_Name String," \
                                "JVM_Version String," \
                                "Results_Base String," \
                                "Results_Peak String," \
                                "Publish_Date String," \
                                "CPU_Name String," \
                                "CPU_Frequency String," \
                                "Primary_Cache String," \
                                "Secondary_Cache String," \
                                "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"


# cpu2006_floating_point_rates
ods_html_cpu2006_fpr_table = "create table IF NOT EXISTS ods_html_cpu2006_floating_point_rates(" \
                             "Test_Sponsor String," \
                            "System_Name String," \
                            "Base_Copies String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Cores_Chip	String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2006_floating_point_speed
ods_html_cpu2006_fps_table = "create table IF NOT EXISTS ods_html_cpu2006_floating_point_speed(" \
                             "Test_Sponsor String," \
                            "System_Name String," \
                            "Auto_Parallel String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Cores_Chip	String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2006_integer_rates
ods_html_cpu2006_ir_table = "create table IF NOT EXISTS ods_html_cpu2006_integer_rates(" \
                            "Test_Sponsor String," \
                            "System_Name String," \
                            "Base_Copies String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Cores_Chip	String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2006_integer_speed
ods_html_cpu2006_is_table = "create table IF NOT EXISTS ods_html_cpu2006_integer_speed(" \
                            "Test_Sponsor String," \
                            "System_Name String," \
                            "Auto_Parallel String," \
                            "Processor1Enabled_Cores String," \
                            "Processor1Enabled_Chips String," \
                            "Processor1Cores_Chip	String," \
                            "Processor1Threads_Core	String," \
                            "Results1Base String," \
                            "Results1Peak String," \
                            "Publish_Date String," \
                            "CPU_Name String," \
                            "CPU_Frequency String," \
                            "Primary_Cache String," \
                            "Secondary_Cache String," \
                            "Tertiary_Cache String," \
                            "Memory String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jbb2015
dw_jbb_table = "create table IF NOT EXISTS dw_jbb_table(" \
                    "Table_Type String," \
                    "Company String," \
                    "System_Name String," \
                    "Jvm_Name String," \
                    "Jvm_Version String," \
                    "Test_Location	String," \
                    "Publish_Date	String," \
                    "Cpu_Name String," \
                    "Nodes String," \
                    "Chips String," \
                    "Cores String," \
                    "Threads String," \
                    "Cpu_Frequency String," \
                    "Primary_Instruction_Cache String," \
                    "Primary_Data_Cache String," \
                    "Secondary_Cache String," \
                    "Tertiary_Cache String," \
                    "Memory String," \
                    "Total_Result String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# jvm2008
dw_jvm_table = "create table IF NOT EXISTS dw_jvm_table(" \
                    "Table_Type String," \
                    "Company String," \
                    "System_Name String," \
                    "Cores String," \
                    "Chips String," \
                    "Threads String," \
                    "Jvm_Name String," \
                    "Jvm_Version String," \
                    "Publish_Date	String," \
                    "Cpu_Name String," \
                    "Cpu_Frequency String," \
                    "Primary_Instruction_Cache String," \
                    "Primary_Data_Cache String," \
                    "Secondary_Cache String," \
                    "Memory String," \
                    "Total_Result String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# power_ssj2008
dw_power_ssj_table = "create table IF NOT EXISTS dw_power_ssj_table(" \
                    "Table_Type String," \
                    "Company String," \
                    "System_Name String," \
                    "Nodes String," \
                    "Jvm_Company String," \
                    "Cpu_Name String," \
                    "Cpu_Frequency String," \
                    "Chips String," \
                    "Cores String," \
                    "Threads String," \
                    "Memory String," \
                    "Test_Location String," \
                    "Publish_Date String," \
                    "Primary_Instruction_Cache String," \
                    "Primary_Data_Cache String," \
                    "Secondary_Cache String," \
                    "Tertiary_Cache String," \
                    "Total_Result String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"

# cpu2017 & cpu2006
dw_cpu_table = "create table IF NOT EXISTS dw_cpu_table(" \
                    "Table_Type String," \
                    "Company String," \
                    "System_Name String," \
                    "Cores String," \
                    "Chips String," \
                    "Threads String," \
                    "Publish_Date String," \
                    "Cpu_Name String," \
                    "Cpu_Frequency String," \
                    "Primary_Instruction_Cache String," \
                    "Primary_Data_Cache String," \
                    "Secondary_Cache String," \
                    "Tertiary_Cache String," \
                    "Memory String," \
                    "Total_Result String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '$' STORED AS TEXTFILE TBLPROPERTIES('skip.header.line.count'='1')"



# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234', auth_mechanism ='PLAIN')
cursor = conn.cursor()

# 创建html爬取数据和下载数据表
cursor.execute(ods_html_cpu2017_fpr_table)
cursor.execute(ods_html_cpu2017_fps_table)
cursor.execute(ods_html_cpu2017_ir_table)
cursor.execute(ods_html_cpu2017_is_table)

cursor.execute(ods_html_jbb2015_composite_table)
cursor.execute(ods_html_jbb2015_distributed_table)
cursor.execute(ods_html_jbb2015_multijvm_table)

cursor.execute(ods_html_power2008_table)

cursor.execute(ods_html_jvm2008_table)

cursor.execute(ods_html_cpu2006_fpr_table)
cursor.execute(ods_html_cpu2006_fps_table)
cursor.execute(ods_html_cpu2006_ir_table)
cursor.execute(ods_html_cpu2006_is_table)

# 创建统一数仓层表格
cursor.execute(dw_jbb_table)
cursor.execute(dw_power_ssj_table)
cursor.execute(dw_jvm_table)
cursor.execute(dw_cpu_table)

# 关闭连接
cursor.close()
conn.close()