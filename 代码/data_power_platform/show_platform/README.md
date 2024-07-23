#### 1、文件内容说明

- data文件夹：存储贴源数据层和统一数仓层的数据（由于这两层的数据量较大，直接加载至hive数据库中花费时间较长，因此选择上传至hadoop后再加载进hive数据库，因此data文件夹中的数据为直接从hadoop上get下来，而标签数据层的数据量不是很大，因此直接存储入hive数据库中，无法从hadoop上get下来）

- show_platform文件夹：用于应用数据层的可视化展示平台

- txt文件：分别存储不同评价指标下的硬件环境影响因素的权重
- create_table.py：用于建表

- data_process.py：用于统一数仓层的数据清洗和整合

- hdfs_to_hive.py：用于将hadoop上的文件加载至hive中

- label_score.py：用于获得标签数据层的处理器、生产厂商、影响因素的标签

- spider.py：用于爬取SPEC网页数据，获得贴源数据层的数据，加载至hadoop上

#### 2、如何运行该项目

- 运行spider.py执行贴源数据层，即爬取SPEC网页的数据加载至hadoop上

- 运行create_table.py构建hive上的表格

- 运行hdfs_to_hive.py将hadoop数据加载至hive中

- 运行data_process.py执行统一数仓层，进行数据清洗和整合

- 运行label_score.py执行标签数据层，对处理器、厂商、影响因素进行打标签

- 运行show_platform文件中的app.py执行应用数据层，进行数据可视化和推荐分析服务

