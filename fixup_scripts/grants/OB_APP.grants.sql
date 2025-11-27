-- OB_APP 授予跨 schema 依赖对象所需权限
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

ALTER SESSION SET CURRENT_SCHEMA = OB_APP;
GRANT EXECUTE ON OB_APP.FN_ORDER_TOTAL TO OB_ANALYTICS;
