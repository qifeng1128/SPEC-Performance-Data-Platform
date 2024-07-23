from impala.dbapi import connect

# cpu2017_floating_point_rates
cpu2017_fpr = "load data inpath '/user/hive/csv_data/ods_html_cpu2017_floating_point_rates.csv' into table ods_html_cpu2017_floating_point_rates"

# cpu2017_floating_point_speed
cpu2017_fps = "load data inpath '/user/hive/csv_data/ods_html_cpu2017_floating_point_speed.csv' into table ods_html_cpu2017_floating_point_speed"

# cpu2017_integer_rates
cpu2017_ir = "load data inpath '/user/hive/csv_data/ods_html_cpu2017_integer_rates.csv' into table ods_html_cpu2017_integer_rates"

# cpu2017_integer_speed
cpu2017_is = "load data inpath '/user/hive/csv_data/ods_html_cpu2017_integer_speed.csv' into table ods_html_cpu2017_integer_speed"

# jbb2015_distributed
jbb2015_distributed = "load data inpath '/user/hive/csv_data/ods_html_jbb2015_distributed.csv' into table ods_html_jbb2015_distributed"

# jbb2015_composite
jbb2015_composite = "load data inpath '/user/hive/csv_data/ods_html_jbb2015_composite.csv' into table ods_html_jbb2015_composite"

# jbb2015_multijvm
jbb2015_multijvm = "load data inpath '/user/hive/csv_data/ods_html_jbb2015_multijvm.csv' into table ods_html_jbb2015_multijvm"

# power_ssj2008
power_ssj2008 = "load data inpath '/user/hive/csv_data/ods_html_power_ssj2008.csv' into table ods_html_power2008"

# jvm2008
jvm2008 = "load data inpath '/user/hive/csv_data/ods_html_jvm2008.csv' into table ods_html_jvm2008_new"

# cpu2006_floating_point_rates
cpu2006_fpr = "load data inpath '/user/hive/csv_data/ods_html_cpu2006_floating_point_rates.csv' into table ods_html_cpu2006_floating_point_rates"

# cpu2006_floating_point_speed
cpu2006_fps = "load data inpath '/user/hive/csv_data/ods_html_cpu2006_floating_point_speed.csv' into table ods_html_cpu2006_floating_point_speed"

# cpu2006_integer_rates
cpu2006_ir = "load data inpath '/user/hive/csv_data/ods_html_cpu2006_integer_rates.csv' into table ods_html_cpu2006_integer_rates"

# cpu2006_integer_speed
cpu2006_is = "load data inpath '/user/hive/csv_data/ods_html_cpu2006_integer_speed.csv' into table ods_html_cpu2006_integer_speed"


# cpu
cpu = "load data inpath '/user/hive/csv_data/dw_cpu.csv' into table dw_cpu_table"

# jbb
jbb = "load data inpath '/user/hive/csv_data/dw_jbb.csv' into table dw_jbb_table"

# jvm
jvm = "load data inpath '/user/hive/csv_data/dw_jvm.csv' into table dw_jvm_table"

# power_ssj
power_ssj = "load data inpath '/user/hive/csv_data/dw_power_ssj.csv' into table dw_power_ssj_table"



# 创建hive连接
conn = connect(host='127.0.0.1', port=10000, user='Yiqi Shen', database='ods_data_17new2', password='1234', auth_mechanism ='PLAIN')
cursor = conn.cursor()

# 创建html爬取数据和下载数据表
cursor.execute(cpu2017_fpr)
cursor.execute(cpu2017_fps)
cursor.execute(cpu2017_ir)
cursor.execute(cpu2017_is)

cursor.execute(jbb2015_multijvm)
cursor.execute(jbb2015_composite)
cursor.execute(jbb2015_distributed)

cursor.execute(power_ssj2008)

cursor.execute(jvm2008)

cursor.execute(cpu2006_fpr)
cursor.execute(cpu2006_fps)
cursor.execute(cpu2006_ir)
cursor.execute(cpu2006_is)

cursor.execute(jbb)
cursor.execute(jvm)
cursor.execute(power_ssj)
cursor.execute(cpu)

# 关闭连接
cursor.close()
conn.close()