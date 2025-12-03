# 数据库对象对比工具设计文档

本文档描述最新版 OceanBase Comparator Toolkit 的设计思路。新版本在原有“Oracle vs OceanBase” 元数据对比基础上，加入了依赖分析、授权推导、dbcat DDL 提取、Rich 报告与全量 fix-up 管道。
> 当前版本：V0.8（Dump-Once, Compare-Locally + 依赖分析 + ALTER 级修补 + 注释校验；支持交互式配置向导与运行前自检）

## 1. 核心目标

1. **准确识别差异**：覆盖 TABLE/VIEW/MATERIALIZED VIEW/PLSQL 对象/TYPE/JOB/SCHEDULE 等主对象，并扩展校验 INDEX/CONSTRAINT/SEQUENCE/TRIGGER。
2. **自动化修补**：对缺失对象、列差异、授权缺口生成结构化 SQL，支持自动执行。
3. **可追踪的报告**：通过 Rich 报表与文本快照输出，记录所有差异、依赖状态、无效 Remap 等，使迁移过程可审计。
4. **高性能 & 低负载**：仍坚持“一次转储、本地对比”，避免循环访问数据库。

## 2. 总体架构

```
config.ini + remap_rules.txt
        │
        ▼
Oracle Thick Mode (DBA_OBJECTS / DBA_DEPENDENCIES / DBMS_METADATA)
        │
        │        obclient (DBA_OBJECTS / DBA_TAB_COLUMNS / … / DBA_DEPENDENCIES)
        ▼                           │
   源对象映射 + 依赖映射 + 目标元数据集
        │
        ├── 主对象 / 扩展对象对比
        ├── 依赖核对 & GRANT 计算
        ├── dbcat DDL 提取 + fix-up 生成
        └── Rich 报告 + 文本快照 + fixup_scripts 目录
```

关键阶段：

1. **配置验证**：校验 `source_schemas`、Remap、Instant Client 路径等，一旦发现致命问题立即退出。
2. **元数据缓存**：Oracle 侧使用 Thick Mode + 批量查询；OceanBase 侧使用 obclient 执行预设 SQL，所有数据一次性加载到内存。
3. **差异分析**：依赖 `master_list`（源→目标）完成表/列校验；索引/约束/触发器使用 Oracle/OB 双缓存进行集合比对；序列按 schema 级别比较。
4. **依赖 & 授权**：`DBA_DEPENDENCIES` 映射后生成期望依赖集合，与目标库实际依赖比较并给出原因，同时计算跨 schema 所需的 `GRANT`。
5. **修补脚本**：构建 dbcat 请求（含 schema→对象类型的任务集合），复用 `dbcat_output` 缓存，对 DDL 做 schema remap、语法清理、授权插入，并按对象类型输出到 `fixup_scripts/`（含依赖重编译/授权脚本）。
6. **报告与执行**：Rich 控制台输出+落地文件；`run_fixup.py` 负责在 OceanBase 上顺序执行 SQL 并回写结果。

## 3. 配置与 Remap 驱动

- `config.ini` 完全驱动运行参数：除了连接信息外，还包含 `fixup_dir`、`report_dir`、`generate_fixup`、`obclient_timeout`、`cli_timeout`（dbcat）、`dbcat_*` 等。  
- `remap_rules.txt` 控制源/目标对象映射，支持 `PACKAGE BODY` 特殊写法及注释。加载时会验证：
  - 源对象是否存在；
  - 是否出现“多对一”目标对象（直接报错）。
- 程序会构建两种映射：
  - `master_list`: 仅包含主对象（TABLE/VIEW/PLSQL/TYPE等）用于主校验；
  - `full_object_mapping`: 包含所有受管对象（含 TRIGGER/SEQUENCE/INDEX），供依赖分析与脚本生成共享。

## 4. 元数据采集

### Oracle

- 通过 `oracledb` Thick Mode 连接，确保能使用 `DBMS_METADATA` 与 `DBA_*` 视图。
- 运行账号需具备跨 schema 查询 `DBA_*` 的权限（如 SYS/SYSDBA 或 `SELECT_CATALOG_ROLE`/`SELECT ANY DICTIONARY`），否则只能看到自身对象。
- 程序启动后会显式提醒需要上述权限，避免因权限不足导致元数据缺失。
- 读取内容：
  - `DBA_OBJECTS`：源对象全集；
  - `DBA_TAB_COLUMNS`、`DBA_INDEXES/DBA_IND_COLUMNS`、`DBA_CONSTRAINTS/DBA_CONS_COLUMNS`、`DBA_TRIGGERS`、`DBA_SEQUENCES`；
  - `DBA_DEPENDENCIES`；
  - `DBA_TAB_COMMENTS` / `DBA_COL_COMMENTS`（按待校验表分批获取注释，默认开启，可通过 `check_comments` 关闭）。
  - DDL 提取阶段调用 dbcat（内部仍读取 Oracle 数据字典）以便批量生成标准化 DDL。
- 把读取结果缓存到 `OracleMetadata`（按 schema+对象名称索引）。

### OceanBase

- 使用一次性 obclient 调用（带 `-ss` + `timeout`）拉取相同的 `DBA_*` 视图。
- 结果存放在 `ObMetadata` 中，结构与 Oracle 侧对应，便于纯 Python 内存对比。

该“批量转储”架构保证比较阶段再无网络往返，提高性能和可重复性。

## 5. 对比策略

1. **主对象**  
   - TABLE：检查存在性、列名集合（过滤 `OMS_*` 内部列）、`VARCHAR/VARCHAR2` 列长度是否落在 `[ceil(1.5 * 源长度), ceil(2.5 * 源长度)]` 区间；不足则生成 ALTER，过大则给出 WARNING。
   - VIEW/MVIEW/类型/PLSQL/SYNONYM/JOB/SCHEDULE 等：验证存在性即可。
2. **扩展对象**  
   - INDEX：按列序列+唯一性匹配；多余或缺少的索引列集合会在报告中详细列出；若源端缺失元数据，也会打印目标端现存索引列表。
   - CONSTRAINT：区分 PK/UK/FK，比较列组合和定义；目标端名称包含 `_OMS_ROWID` 的约束会被忽略；源端元数据缺失时同样输出目标端现存约束列表。
   - TRIGGER：考虑 remap 后的触发器名称，检查目标端是否缺失或多余。
   - SEQUENCE：按源 schema → 目标 schema 映射逐个确认；源端元数据缺失时会提示并列出目标端已有序列。
3. **数量汇总**  
   - 在报告中附带 Oracle vs OceanBase 的对象数量对比，快速观察整体迁移完成度。

所有差异会被写入 `tv_results`/`extra_results`，供后续报告和修补模块复用。

## 6. 依赖分析与授权

- 使用 `DBA_DEPENDENCIES` 构建 `{依赖对象 -> 被依赖对象}` 的全集，对应的对象类型限定在 `ALL_TRACKED_OBJECT_TYPES`。
- 应用 `full_object_mapping` 后得到目标端“期望依赖集合”，再与 OceanBase 实际依赖差集：
  - **缺失依赖**：指出是依赖对象缺失、被依赖对象缺失还是单纯未编译。
  - **额外依赖**：提示目标端存在额外耦合，需人工判断。
  - **跳过项**：源/目标缺少 remap 信息时记录原因但不报错。
- 对于缺失依赖但目标对象已存在的情况，会生成 `ALTER ... COMPILE` 脚本以尝试重编译。
- 基于期望依赖自动推导跨 schema 所需权限：例如 PROC 调用其他 schema 的 TABLE/SYNONYM → 输出 `GRANT SELECT ...`，PLSQL 调用包/类型 → `GRANT EXECUTE ...`。这些脚本写入 `fixup_scripts/grants/` 并在报告中展示。

## 7. 修补脚本生成

### dbcat 集成

- 根据差异收集“需要 DDL 的对象集合”，以 schema 为维度构造 dbcat 命令：
  - 可复用 `dbcat_output/<schema>/...` 缓存，未命中的对象再触发 dbcat。
  - 运行 dbcat 需要 `JAVA_HOME` 与 `dbcat_from/dbcat_to` profile，超时时间由 `cli_timeout` 控制。
- DDL 后处理：
  - `adjust_ddl_for_object`：根据 Remap 替换 schema/name，支持额外引用对象的重写。
  - `cleanup_dbcat_wrappers`：移除 `DELIMITER/$$` 包裹。
  - `normalize_ddl_for_ob`：剔除 OceanBase 不支持的 `USING INDEX (...) ENABLE` 等片段。
  - `enforce_schema_for_ddl`：必要时插入 `ALTER SESSION SET CURRENT_SCHEMA`。

### 输出顺序与目录

生成顺序遵循依赖关系：SEQUENCE → TABLE（CREATE + ALTER）→ 代码对象 → INDEX → CONSTRAINT → TRIGGER → 依赖重编译（COMPILE）→ GRANT → 其他对象。所有文件位于 `fixup_scripts/<object_type>/`，并带有头部注释（源/目标信息、审核提示）。  

表列差异专门写入 `fixup_scripts/table_alter/`，对缺失列生成 `ADD COLUMN`，对长度不足生成 `MODIFY`，长度过大的列以 WARNING 形式提示人工评估；多余列仅以注释形式提示 `DROP`。  

缺失依赖时生成的 `ALTER ... COMPILE` 脚本集中在 `fixup_scripts/compile/`，便于在 GRANT 前后分批执行。  

若某些对象类型 dbcat 暂不支持，生成阶段会给出 warning，提示需要手动处理。

## 8. 报告与执行

- **Rich 控制台报告**：包含综合概要、表级差异、索引/约束/序列/触发器明细、依赖缺口、授权脚本、无效 remap 等；每个章节都带计数和色彩区分。
- **文本快照 (`main_reports/report_<timestamp>.txt`)**：通过 `Console(record=True)` 同步导出，方便归档或发给其他团队，并在开头展示源/目标数据库的版本与连接概览。
- **fixup_scripts 指南**：报告结尾展示各子目录含义，提醒人工审核。
- **`run_fixup.py` 执行器**：
  - 读取 `fixup_scripts/` 第一层子目录下的 SQL。
  - 通过 obclient 顺序执行，成功后移动到 `fixup_scripts/done/<subdir>/`，失败则保留原地并打印错误。
  - 输出明细表与累计结果，便于反复执行。

## 9. 健壮性设计

- **超时控制**：所有 obclient 调用使用 `obclient_timeout`；dbcat 调用使用 `cli_timeout`；超时会记录 SQL 并立即退出。
- **配置前置校验**：缺失 Instant Client / dbcat / JAVA_HOME / Remap 异常会直接报错，避免运行到一半失败。
- **错误提示**：对每一步都提供结构化日志，方便定位：例如 Remap 无效、Oracle 元数据为空、dbcat 缺文件等。
- **缓存与重用**：dbcat 输出保存在 `dbcat_output`，避免重复扫描；`generate_fixup=false` 时跳过 fix-up 阶段以加快巡检。
- **并行友好**：所有输出目录（`fixup_scripts/`, `main_reports/`, `dbcat_output/`）都显式创建且清理旧结果，确保自动化流水线能够多次运行。

综上，该工具以配置驱动、一次转储、本地对比为核心；辅以依赖图与 dbcat 脚本生成，形成“发现问题 → 生成方案 → 执行验证”的闭环，满足大规模 Oracle → OceanBase 迁移过程的验证与修复需求。

## 10. 配套工具与资产

- `run_fixup.py`：在 OceanBase 上顺序执行 `fixup_scripts/` 第一层子目录的 SQL，支持 `--only-dirs/--only-types/--glob` 过滤，并将成功文件搬运到 `fixup_scripts/done/` 便于幂等重跑。  
- `init_test.py`：读取 `config.ini`，对 `test_scenarios/gorgon_knot_case` 的 Oracle/OB SQL 进行分号与 `/` 划分后逐条执行，快速搭建冒烟环境。  
- `test_scenarios/*`：三套场景覆盖不同特性：`labyrinth_case`（默认配置、列长度/依赖/GRANT）、`hydra_matrix_case`（多 schema 网格与复杂 remap）、`gorgon_knot_case`（多对一/一对多映射与名称冲突）。每套都附带 remap 与 README。  
- `dbcat_output/`：缓存最近的 dbcat 提取结果（当前为 Labyrinth 表/MV），在下次生成时可复用以减少对源库的扫描。  
- `fixup_scripts/` 与 `main_reports/`：仓库中留存的一次 Labyrinth 演练输出，展示脚本/报告格式；实际使用时可删除后由新一轮对比生成。  
- `history/`：V8–V12 的迭代版本，展示从基础列对比到 ALTER 级修补的演进，可用于排查回归或参考旧逻辑。
