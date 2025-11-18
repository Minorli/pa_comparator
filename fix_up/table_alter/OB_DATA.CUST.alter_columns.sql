-- 基于列差异的 ALTER TABLE 修补脚本: OB_DATA.CUST (源: ORA_APP.CUSTOMERS)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_DATA.CUST MODIFY (STATUS VARCHAR(3)); -- 源长度: 2, 目标长度: 2, 期望长度: 3;
