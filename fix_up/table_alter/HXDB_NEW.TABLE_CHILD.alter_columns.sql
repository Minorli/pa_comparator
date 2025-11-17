-- 基于列差异的 ALTER TABLE 修补脚本: HXDB_NEW.TABLE_CHILD (源: HR.TABLE_CHILD)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 源端存在而目标端缺失的列，将通过 ALTER TABLE ADD 补齐：
ALTER TABLE HXDB_NEW.TABLE_CHILD ADD (NAME2 VARCHAR2(120));