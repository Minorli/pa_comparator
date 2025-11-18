-- 基于列差异的 ALTER TABLE 修补脚本: OB_STAGE.DEPT (源: ORA_HR.DEPARTMENTS)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_STAGE.DEPT MODIFY (DEPT_NAME VARCHAR(135)); -- 源长度: 90, 目标长度: 90, 期望长度: 135;
