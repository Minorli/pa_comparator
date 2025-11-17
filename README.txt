一个完整的Python程序包，包含三个部分：
1. 用法说明 (README)：如何安装、配置和运行。
2. 配置文件 (db.ini)：需要您来填写的模板。
3. 规则文件 (remap_rules.txt)：一个示例。
4. Python 脚本 (db_comparator.py)：程序的核心代码。
1.  用法说明 (README)
目的：
本程序用于对比 Oracle (源) 和 OceanBase Oracle兼容租户 (目标) 之间的 TABLE 和 VIEW 对象。
它会严格按照您的配置（db.ini）和规则（remap_rules.txt）生成一个“最终校验清单”，然后逐个检查：
• VIEW (视图): 只检查在目标端是否存在。
• TABLE (表): 检查是否存在，并且所有列名是否与源端完全一致。
安装依赖：
本程序最大限度地使用了Python标准库。唯一需要安装的外部库是 oracledb (用于连接源端Oracle)。
pip install oracledb

以下是运行程序的步骤：
创建文件： 将以下三个文件（db.ini, remap_rules.txt, db_comparator.py）保存在同一个目录中。
配置 db.ini： 详细填写您的Oracle和OceanBase连接信息。
配置 remap_rules.txt： 填写所有需要 "remap" 的对象。
执行程序：
python db_comparator.py

v9 版本只检查表及视图差异，不提供其他对象的检查和修复
v10 版本提供表，视图，触发器等其他对象的检查，不提供修复
v12 版本提供所有类型对象的检查，同时提供 fixup
--
基于 v10 迭代检查，基于 v12 版本迭代检查及修复逻辑
