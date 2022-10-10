# SUSTech_DataBase_Project01_SUSTC
有任何想法，可以在这个地方使用`Markdown`写出。

## 项目信息

### 项目架构

#### doc 文档

保存项目需要用到的资源，包括：

1. `CS307-2022Fall_Project1.pdf` 项目要求（官方英文版）。
2. `CS307-2022Fall_Project01_translation.md` 项目要求（中文翻译版）。

## 项目思路

### 数据表设计 (15%)

- 第一范式：1NF（第一范式）是指数据库表的每一列都是不可分割的基本数据项，同一列中不能有多个值，即实体中的某个属性不能有多个值或者不能有重复的属性。如果出现重复的属性，就可能需要定义一个新的实体，新的实体由重复的属性构成，新实体与原实体之间为一对多关系。在第一范式1NF中表的每一行只包含一个实例的信息。简而言之，第一范式就是无重复的列。

- 第二范式：2NF（第二范式）是指每个表必须有主关键字(Primary key),其他数据元素与主关键字一一对应。通常称这种关系为函数依赖(Functional dependence)关系，即表中其他数据元素都依赖于主关键字,或称该数据元素惟一地被主关键字所标识。第二范式是数据库规范化中所使用的一种正规形式。它的规则是要求数据表里的所有非主属性都要和该数据表的主键有完全依赖关系；如果有哪些非主属性只和主键的一部份有关的话，它就不符合第二范式。同时可以得出：如果一个数据表的主键只有单一一个字段的话，它就一定符合第二范式(前提是该数据表符合第一范式)。

- 第三范式：3NF（第三范式）是指表中的所有数据元素不但要能唯一地被主关键字所标识,而且它们之间还必须相互独立,不存在其他的函数关系。也就是说，对于一个满足2nd NF 的数据结构来说，表中有可能存在某些数据元素依赖于其他非关键字数据元素的现象,必须消除。

