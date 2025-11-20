# OceanBase Comparator Toolkit

This toolkit automates end-to-end validation for Oracle → OceanBase migrations that run in Oracle compatibility mode. It performs a single metadata dump on both sides, compares every managed object, flags dependency gaps, and—when asked—generates ready-to-review fix-up scripts plus the `obclient` runner that applies them.

## Highlights

- 一次转储本地对比：Oracle 使用 Thick Mode + `DBMS_METADATA`，OceanBase 通过几次 `obclient` 调用批量拉取 `ALL_*` 视图，避免循环调库。
- 覆盖的对象类型包括 `TABLE/VIEW/MATERIALIZED VIEW/PROCEDURE/FUNCTION/PACKAGE/PACKAGE BODY/SYNONYM/JOB/SCHEDULE/TYPE/TYPE BODY`，并扩展检查 `INDEX/CONSTRAINT/SEQUENCE/TRIGGER`。
- 表校验除了存在性外，还会校验列名集合与 `VARCHAR/VARCHAR2` 长度（目标端需 ≥ `ceil(1.5 * 源端长度)`），并生成 `ALTER TABLE` 修补建议。
- 自动收集 `ALL_DEPENDENCIES` 并映射到目标 schema，输出缺失/多余依赖和所需 `GRANT` 脚本。
- 基于 dbcat 导出的 DDL + 本地修补器生成结构化的 `fix_up/` 目录，含 SEQUENCE/TABLE/代码对象/INDEX/CONSTRAINT/TRIGGER/GRANT/TABLE_ALTER 等脚本。
- `final_fix.py` 可按顺序执行这些脚本，并把成功的文件移动到 `fix_up/done/...` 目录，方便二次运行。

## Repository Layout

| Path | Description |
| --- | --- |
| `db_comparator_fixup_release.py` | 主脚本，负责加载配置、Remap、元数据转储、差异对比、依赖分析、报告生成与 fix-up 输出。 |
| `final_fix.py` | 在 OceanBase 上批量执行 `fix_up/` 子目录中的 SQL，打印成功/失败摘要。 |
| `db.ini` | 样例配置（Oracle/OceanBase 连接、Instant Client、dbcat、输出目录等）。 |
| `remap_rules.txt` | 对象级 remap 文件。`remap_rules_old.txt` 保留历史示例。 |
| `fix_up/` | 最近一次校验生成的修补脚本（含 `grants/`, `table_alter/`, 以及各对象类型子目录）。 |
| `history/` | 旧版本脚本 & `dbcat_output/` 缓存（避免重复导出 DDL）。 |
| `reports/` | `rich` 渲染的文本报告，文件名格式 `report_<timestamp>.txt`。 |
| `test_scenarios/` | `hydra_matrix_case` 样例（包含 DDL、Remap、场景说明）。 |
| `requirements.txt` | Python 依赖（`oracledb`, `rich`）。 |
| `DESIGN.md` | 设计/架构说明。 |

## Requirements

### Runtime & external tools

1. Linux + Python 3.8+（已在 3.11 上验证）。
2. Oracle Instant Client 19c+，并设置 `LD_LIBRARY_PATH` 指向解压目录（`oracle_client_lib_dir` 也需配置）。
3. `obclient` 客户端以及访问 Oracle/OceanBase 的网络。
4. `dbcat` CLI（例如 `dbcat-2.5.0-SNAPSHOT`），以及可用的 `JAVA_HOME`。dbcat 用于批量导出源端 DDL，是修补脚本生成的核心。

### Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`db_comparator_fixup_release.py` 会在启动时校验 `oracledb` 与 `rich` 是否可用。

## Configuration (`db.ini`)

- **`[ORACLE_SOURCE]`**  
  `user`, `password`, `dsn` (`host:port/service_name`)。脚本使用 Thick Mode，所以必须能够找到 Instant Client。

- **`[OCEANBASE_TARGET]`**  
  `executable`（obclient 路径）、`host`, `port`, `user_string`（完整 `-u` 参数）、`password`。所有转储和 `final_fix.py` 都依赖此配置。

- **`[SETTINGS]`**  
  - `source_schemas`：逗号分隔的 Oracle schema 列表。必须与 Remap 源对象一致。
  - `remap_file`：对象 remap 文件路径。
  - `oracle_client_lib_dir`：Instant Client 目录（用于 `oracledb.init_oracle_client`）。
  - `fixup_dir`：修补脚本输出目录（默认 `fix_up`）。
  - `report_dir`：报告输出目录（默认 `reports`）。
  - `generate_fixup`：`true/false`，允许只跑对比不生成脚本。
  - `obclient_timeout`：每次 `obclient` 调用的超时（秒，默认 60）。
  - `cli_timeout`：shell 工具（如 dbcat）超时，默认 600 秒。
  - `dbcat_bin`：dbcat 根目录或 `bin/dbcat` 可执行文件路径。
  - `dbcat_from` / `dbcat_to`：dbcat 的源/目标 profile（例如 `oracle19c` → `oboracle420`）。
  - `dbcat_output_dir`：dbcat 输出根目录，默认 `history/dbcat_output`，支持缓存复用。
  - `java_home`：可选。如果留空则回退到环境变量 `JAVA_HOME`。

### Remap rules

`remap_rules.txt` 每行格式 `SRC_SCHEMA.OBJECT = TGT_SCHEMA.OBJECT`，支持注释（`#` 开头）和空行。  
特殊处理：

- `PACKAGE BODY` 可以使用 `PACKAGE_NAME BODY = ...` 写法。
- 工具会验证源对象是否真实存在；无效条目会在报告中单独列出。
- 检测“多对一”映射（同一个目标对象被多个源对象映射）并立即终止，防止后续差异混乱。

## Running the comparator

```bash
export LD_LIBRARY_PATH="/path/to/instantclient:${LD_LIBRARY_PATH}"
python3 db_comparator_fixup_release.py
```

运行过程概览：

1. **配置与 Remap 校验**：加载 `db.ini`、`remap_rules.txt`，确认所有源对象存在。
2. **Oracle Thick Mode 初始化**：`oracledb` 以 Thick Mode 连接，批量读取 `ALL_OBJECTS/ALL_*`、`ALL_DEPENDENCIES`，并缓存表/索引/约束/触发器/序列元数据。
3. **构建主校验清单**：生成源→目标映射，记录依赖、统计对象数量。
4. **OceanBase 元数据转储**：通过少量 `obclient` 调用一次性拉取 `ALL_OBJECTS/COLUMNS/INDEXES/CONSTRAINTS/CONS_COLUMNS/TRIGGERS/SEQUENCES/DEPENDENCIES`。
5. **对比阶段**  
   - 主对象：逐个校验存在性与表列/长度差异（忽略 `OMS_*` 列）。  
   - 扩展对象：对每个表比对索引/约束/触发器；按 schema 比对序列。  
   - 依赖：把 Oracle 依赖映射到目标 schema，核对 OceanBase 的 `ALL_DEPENDENCIES`，得出缺失/额外/跳过项，并计算跨 schema 所需的 `GRANT`。
6. **修补脚本（可选）**：若 `generate_fixup=true`，按以下顺序生成脚本：
   1. 缺失的 SEQUENCE（dbcat DDL）
   2. 缺失的 TABLE（CREATE）与 `table_alter/` 中的列修补
   3. VIEW/MVIEW/PLSQL/SYNONYM/JOB/SCHEDULE/TYPE/TYPE BODY
   4. INDEX / CONSTRAINT / TRIGGER
   5. `grants/`：依赖所需授权  
   生成前会清空旧的 `fix_up/` 内容，并尽量复用 `history/dbcat_output` 缓存。
7. **报告输出**：使用 `rich` 打印彩色摘要（对象数量、缺失/不匹配列表、依赖状态、GRANT 建议、无效 remap 等），同时写入 `reports/report_<timestamp>.txt`。

建议每次应用修补脚本后再次运行主脚本，确认所有对象与依赖均为绿色。

## Output artifacts

- **`reports/report_<timestamp>.txt`**  
  控制台同款报告（Rich 表格），包含：
  - 主对象汇总（OK/缺失/不匹配/无效 remap）
  - 扩展对象（索引/约束/序列/触发器）状态
  - 依赖缺失/额外/跳过原因以及所需 GRANT
  - Oracle vs OceanBase 数量对比和 fix_up 指南

- **`fix_up/`**（当 `generate_fixup=true`）  
  - `table/`, `view/`, `materialized_view/`, `procedure/`, `function/`, `package/`, `package_body/`, `synonym/`, `job/`, `schedule/`, `type/`, `type_body/`：缺失对象的 CREATE DDL。
  - `sequence/`, `trigger/`, `index/`, `constraint/`：针对相应差异的脚本。
  - `table_alter/`: 针对列缺失/长度不足生成的 `ALTER TABLE` 脚本（多余列仅给出注释版 DROP 建议）。
  - `grants/`: `GRANT <priv> ON <schema.object> TO <schema>`，确保跨 schema 依赖可编译。
  - `done/`: 由 `final_fix.py` 创建，用于存放已执行成功的脚本副本。

- **`history/dbcat_output/`**  
  缓存最近一次 dbcat 导出的 DDL（按 schema 存放），下一次运行会优先复用缓存，只有需要的新对象才会重新导出，避免反复扫描 Oracle。

## Applying fix-up scripts

在人工审核 `fix_up/` 中的 SQL 后，可用 `final_fix.py` 自动执行：

```bash
python3 final_fix.py [optional/path/to/db.ini]
```

行为：
1. 读取 `db.ini` 并定位 `fixup_dir`。
2. 遍历第一层子目录的 `*.sql` 文件，按字母顺序执行。
3. 通过 `obclient` 执行脚本；成功的脚本会移动到 `fix_up/done/<subdir>/`。
4. 输出详细表格，总结成功/失败/跳过原因，便于重跑。

如需按场景分批执行，可在 `fix_up/` 中保留多个子目录或手动挑选脚本。

## Sample scenarios & history

- `test_scenarios/hydra_matrix_case`：多 schema、多 remap 的组合案例，模拟企业级项目。
- `history/db_comparator_*.py`：旧版本脚本留档，可参考排查差异。
（当前仓库仅包含 Hydra 场景，Spiderweb 场景未随仓库提供。）

---

欢迎根据自身需求扩展 remap 规则、接入 CI、或把工具集成到更大的迁移流水线中。若需了解内部实现和设计动机，请继续阅读 `DESIGN.md`。祝迁移顺利!
