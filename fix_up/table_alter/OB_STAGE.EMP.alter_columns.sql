-- 基于列差异的 ALTER TABLE 修补脚本: OB_STAGE.EMP (源: ORA_HR.EMPLOYEES)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_STAGE.EMP MODIFY (EMP_NAME VARCHAR(120)); -- 源长度: 80, 目标长度: 80, 期望长度: 120
ALTER TABLE OB_STAGE.EMP MODIFY (JOB_ID VARCHAR(30)); -- 源长度: 20, 目标长度: 20, 期望长度: 30
ALTER TABLE OB_STAGE.EMP MODIFY (EMAIL VARCHAR(90)); -- 源长度: 60, 目标长度: 60, 期望长度: 90;
