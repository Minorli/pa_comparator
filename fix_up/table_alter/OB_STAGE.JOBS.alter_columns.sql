-- 基于列差异的 ALTER TABLE 修补脚本: OB_STAGE.JOBS (源: ORA_HR.JOBS)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_STAGE.JOBS MODIFY (JOB_ID VARCHAR(30)); -- 源长度: 20, 目标长度: 20, 期望长度: 30
ALTER TABLE OB_STAGE.JOBS MODIFY (JOB_NAME VARCHAR(150)); -- 源长度: 100, 目标长度: 100, 期望长度: 150;
