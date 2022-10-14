# SUSTech_DataBase_Project01_SUSTC

## 项目基本信息

### 项目作者
- 项目作者01
  - 姓名：罗嘉诚（Jiacheng Luo）
  - 学号：12112910
  - 实验课：Lab2
- 项目作者02
  - 姓名：伦天乐（Tianle Lun）
  - 学号：12113019
  - 实验课：Lab2
### 各成员项目贡献

根据小组内部讨论并达成一致，建议最终项目贡献比：
- 罗嘉诚：50%
- 伦天乐：50%

各任务详细贡献如下：
1. 绘制 E-R 图：罗嘉诚。
2. 根据 E-R 图创建数据表：伦天乐。
3. 项目报告撰写：罗嘉诚（50%）、伦天乐（50%）。
## Task 1： E-R 图

根据项目说明中提供的逻辑，迭代多次版本，使用 `亿图图示` 图形绘制软件，绘制项目的 E-R 图，提供截图如下：

![](/SUSTech_databse_Project01_SUSTC/doc/ER%20diagram.png)

##  Task 2： 数据库设计

使用附录中 `table_maker.sql` 文件使用 `DDL` 创建数据表。

1. 使用 `Show Visualization` 显示表设计及关系。
在 `DataGrip` 中创建数据表后并全选，右键 `Diagram > Show Visualization` 保存图片如下：

![](/SUSTech_databse_Project01_SUSTC/doc/database%20design.png)

2. 数据库设计合理性的说明

在数据表构建中，我们创建了 $9$ 个数据表，使得构建的数据库满足三大范式：

- 第一范式：1NF（第一范式）是指数据库表的每一列都是不可分割的基本数据项，同一列中不能有多个值。
- 第二范式：2NF（第二范式）是指每个表必须有主关键字,其他数据元素与主关键字一一对应。
- 第三范式：3NF（第三范式）是指表中的所有数据元素不但要能唯一地被主关键字所标识,而且它们之间还必须相互独立,不存在其他的函数关系。

我们采用自顶向下的方式依次介绍所构建的表格。

- 表格 `item`：用来存储项目的基本信息，包括`项目名称 name`、`项目类型 type`、`项目价格 price`、`项目状态最新更新时间 log_time`，这四个简单的直接属性信息，还包括`检索信息 retrieval_information`、`出口信息 export_information`、`打包 container`、`装船 ship`、`进口信息 import_information`、`寄送信息 delivery_information` 六种运输过程的不同阶段（使用外键链接到相关数据表，还需要考虑它们之间地限制关系）。其中主键是`项目名称 name`，因为它对于不同的项目，他是独特的。表中四个直接属性信息的数据类型较好地贴合给出数据的实际，`项目名称 name`、`项目类型 type` 采用 `varchar(32)` 类型存储，`项目价格 price` 采用 `integer` 类型存储，`项目更新时间 log_time` 采用 `timestamp` 类型存储。
- 表格 `retrieval_information` 用来存储运输的第一阶段，`检索 retrieval` 的相关信息，包括`自增数字id`、`城市名称 name`、`检索时间 start_time` 三个简单的直接属性信息，还包括`检索快递员 retrieval_courier`的相关信息。其中主键是`自增数字id`，这是由于一个不同的项目，应该由唯一的检索信息与之对应，这样一一对应的关系。表中的三个直接属性信息的数据类型较好地贴合给出数据的实际，`城市名称 city`、`项目类型 type` 采用 `varchar(32)` 类型存储，`项目价格 price` 采用 `integer` 类型存储，`项目更新时间 log_time` 采用 `timestamp` 类型存储。
