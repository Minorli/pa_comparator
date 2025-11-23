# OceanBase Comparator Toolkit

🚀 **极简必看用法**  
1. 先在目标机准备好 Python 3.7（3.6 也可）、Oracle Instant Client、obclient、JDK+dbcat，设置好 `LD_LIBRARY_PATH` / `JAVA_HOME`。  
2. `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`  
3. 配置 `config.ini`（路径用绝对路径，`generate_fixup` 默认开启会生成修补脚本）。  
4. `python schema_diff_reconciler.py` → 输出 `main_reports/` 和 `fixup_scripts/`。  
5. 审核后执行 `python run_fixup.py` 自动跑脚本，可多次重试。

## Highlights

- 一次转储本地对比：Oracle 使用 Thick Mode + `DBMS_METADATA`，OceanBase 通过几次 `obclient` 调用批量拉取 `ALL_*` 视图，避免循环调库。
- 覆盖的对象类型包括 `TABLE/VIEW/MATERIALIZED VIEW/PROCEDURE/FUNCTION/PACKAGE/PACKAGE BODY/SYNONYM/JOB/SCHEDULE/TYPE/TYPE BODY`，并扩展检查 `INDEX/CONSTRAINT/SEQUENCE/TRIGGER`。
- 表校验除了存在性外，还会校验列名集合与 `VARCHAR/VARCHAR2` 长度（目标端需在 `[ceil(1.5*x), ceil(2.5*x)]` 区间），并生成 `ALTER TABLE` 修补建议。
- 自动收集 `ALL_DEPENDENCIES` 并映射到目标 schema，输出缺失/多余依赖、依赖重编译脚本和所需 `GRANT`。
- 基于 dbcat 导出的 DDL + 本地修补器生成结构化的 `fixup_scripts/` 目录，含 SEQUENCE/TABLE/代码对象/INDEX/CONSTRAINT/TRIGGER/COMPILE/GRANT/TABLE_ALTER 等脚本。
- `run_fixup.py` 可按顺序执行这些脚本，并把成功的文件移动到 `fixup_scripts/done/...` 目录，方便二次运行。

## Repository Layout

| Path | Description |
| --- | --- |
| `schema_diff_reconciler.py` | 主脚本，负责加载配置、Remap、元数据转储、差异对比、依赖分析、报告生成与 fix-up 输出。 |
| `run_fixup.py` | 在 OceanBase 上批量执行 `fixup_scripts/` 子目录中的 SQL，打印成功/失败摘要。 |
| `config.ini` | 样例配置（Oracle/OceanBase 连接、Instant Client、dbcat、输出目录等）。 |
| `remap_rules.txt` | 对象级 remap 文件。`remap_rules_old.txt` 保留历史示例。 |
| `fixup_scripts/` | 最近一次校验生成的修补脚本（含 `grants/`, `table_alter/`, 以及各对象类型子目录）。 |
| `dbcat_output/` | dbcat 导出的 DDL 缓存（避免重复导出）。 |
| `main_reports/` | `rich` 渲染的文本报告，文件名格式 `report_<timestamp>.txt`。 |
| `history/` | 旧版本脚本留档。 |
| `test_scenarios/` | `hydra_matrix_case` 样例（包含 DDL、Remap、场景说明）。 |
| `requirements.txt` | Python 依赖（`oracledb`, `rich`）。 |
| `DESIGN.md` | 设计/架构说明。 |

## Requirements

### Runtime & external tools

1. Linux + Python 3.7（3.6 也可；更高版本如 3.11 亦已验证）。
2. Oracle Instant Client 19c+，并设置 `LD_LIBRARY_PATH` 指向解压目录（`oracle_client_lib_dir` 也需配置）。
3. `obclient` 客户端以及访问 Oracle/OceanBase 的网络。
4. `dbcat` CLI（例如 `dbcat-2.5.0-SNAPSHOT`），以及可用的 `JAVA_HOME`。dbcat 用于批量导出源端 DDL，是修补脚本生成的核心。
5. 运行账号需能查询目标 schema 的 `ALL_*` 视图（推荐 SYS/SYSDBA 或拥有 `SELECT_CATALOG_ROLE`/`SELECT ANY DICTIONARY`；OceanBase 侧建议用 SYS/root 级账号），否则只会看到自身 schema 的对象。

### Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`schema_diff_reconciler.py` 会在启动时校验 `oracledb` 与 `rich` 是否可用。

## Configuration (`config.ini`)

- **`[ORACLE_SOURCE]`**  
  `user`, `password`, `dsn` (`host:port/service_name`)。脚本使用 Thick Mode，所以必须能够找到 Instant Client。

- **`[OCEANBASE_TARGET]`**  
  `executable`（obclient 路径）、`host`, `port`, `user_string`（完整 `-u` 参数）、`password`。所有转储和 `run_fixup.py` 都依赖此配置。

- **`[SETTINGS]`**  
  - `source_schemas`：逗号分隔的 Oracle schema 列表。必须与 Remap 源对象一致。
  - `remap_file`：对象 remap 文件路径。
  - `oracle_client_lib_dir`：Instant Client 目录（用于 `oracledb.init_oracle_client`）。
  - `fixup_dir`：修补脚本输出目录（默认 `fixup_scripts`）。
  - `report_dir`：报告输出目录（默认 `main_reports`）。
  - `generate_fixup`：`true/false`，允许只跑对比不生成脚本。
  - `check_primary_types`：限制本次主对象校验的类型，逗号分隔（如 `TABLE,VIEW`，留空为全量）。
  - `check_extra_types`：限制扩展校验的模块，默认 `index,constraint,sequence,trigger`，可按需删减。
  - `check_dependencies`：`true/false`，关闭后跳过依赖校验与授权建议。
  - `obclient_timeout`：每次 `obclient` 调用的超时（秒，默认 60）。
  - `cli_timeout`：shell 工具（如 dbcat）超时，默认 600 秒。
  - `dbcat_bin`：dbcat 根目录或 `bin/dbcat` 可执行文件路径。
- `dbcat_from` / `dbcat_to`：dbcat 的源/目标 profile（例如 `oracle19c` → `oboracle420`）。
- `dbcat_output_dir`：dbcat 输出根目录，默认 `dbcat_output`，支持缓存复用。
- `java_home`：可选。如果留空则回退到环境变量 `JAVA_HOME`。

常用配置组合示例（可按需写入 `config.ini`）：

- 仅校验表（列名 + VARCHAR 长度区间），不生成修补脚本、跳过依赖：
  - `check_primary_types=TABLE`
  - `check_extra_types=`（留空表示跳过索引/约束/序列/触发器）
  - `check_dependencies=false`
  - `generate_fixup=false`

- 校验表 + 索引/约束/序列/触发器（默认值）：
  - `check_primary_types=TABLE`
  - `check_extra_types=index,constraint,sequence,trigger`
  - `check_dependencies=true`

- 全量检查（所有受管类型 + 依赖）：
  - `check_primary_types=` 留空或填全（如 `TABLE,VIEW,...`）
  - `check_extra_types=` 留空或默认值
  - `check_dependencies=true`

运行时控制台会打印“本次启用的主对象类型/扩展校验模块/是否跳过依赖”，便于确认范围。

### Remap rules

`remap_rules.txt` 每行格式 `SRC_SCHEMA.OBJECT = TGT_SCHEMA.OBJECT`，支持注释（`#` 开头）和空行。  
特殊处理：

- `PACKAGE BODY` 可以使用 `PACKAGE_NAME BODY = ...` 写法。
- 工具会验证源对象是否真实存在；无效条目会在报告中单独列出。
- 检测“多对一”映射（同一个目标对象被多个源对象映射）并立即终止，防止后续差异混乱。

## Running the comparator

```bash
export LD_LIBRARY_PATH="/path/to/instantclient:${LD_LIBRARY_PATH}"
python3 schema_diff_reconciler.py
```

运行过程概览：

1. **配置与 Remap 校验**：加载 `config.ini`、`remap_rules.txt`，确认所有源对象存在。
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
   5. 依赖重编译（`compile/` 中的 `ALTER ... COMPILE`）
   6. `grants/`：依赖所需授权  
   生成前会清空旧的 `fixup_scripts/` 内容，并尽量复用 `dbcat_output` 缓存。
7. **报告输出**：使用 `rich` 打印彩色摘要（对象数量、缺失/不匹配列表、依赖状态、GRANT 建议、无效 remap 等），同时写入 `main_reports/report_<timestamp>.txt`。

建议每次应用修补脚本后再次运行主脚本，确认所有对象与依赖均为绿色。

## Output artifacts

- **`main_reports/report_<timestamp>.txt`**  
  控制台同款报告（Rich 表格），包含：
  - 源/目标数据库的版本、容器/用户/连接概要
  - 主对象汇总（OK/缺失/不匹配/无效 remap）
  - 扩展对象（索引/约束/序列/触发器）状态
  - 依赖缺失/额外/跳过原因以及所需 GRANT
  - Oracle vs OceanBase 数量对比和 fixup 指南

- **`fixup_scripts/`**（当 `generate_fixup=true`）  
  - `table/`, `view/`, `materialized_view/`, `procedure/`, `function/`, `package/`, `package_body/`, `synonym/`, `job/`, `schedule/`, `type/`, `type_body/`：缺失对象的 CREATE DDL。
  - `sequence/`, `trigger/`, `index/`, `constraint/`：针对相应差异的脚本。
  - `table_alter/`: 针对列缺失/长度不足生成的 `ALTER TABLE` 脚本（多余列仅给出注释版 DROP 建议；长度过大的列会以 WARNING 提示人工收敛）。
  - `compile/`: 针对缺失依赖的对象生成 `ALTER ... COMPILE` 重编译脚本。
  - `grants/`: `GRANT <priv> ON <schema.object> TO <schema>`，确保跨 schema 依赖可编译。
  - `done/`: 由 `run_fixup.py` 创建，用于存放已执行成功的脚本副本。

- **`dbcat_output/`**  
  缓存最近一次 dbcat 导出的 DDL（按 schema 存放），下一次运行会优先复用缓存，只有需要的新对象才会重新导出，避免反复扫描 Oracle。

## Applying fix-up scripts

在人工审核 `fixup_scripts/` 中的 SQL 后，可用 `run_fixup.py` 自动执行：

```bash
python3 run_fixup.py [optional/path/to/config.ini]
# 仅跑指定目录或类型：
# python3 run_fixup.py --only-dirs table,table_alter
# python3 run_fixup.py --only-types TABLE,VIEW
# 只跑匹配的文件名：
# python3 run_fixup.py --glob \"*202512*.sql\"
```

行为：
1. 读取 `config.ini` 并定位 `fixup_dir`。
2. 遍历第一层子目录的 `*.sql` 文件，按优先级顺序执行；可用 `--only-dirs/--only-types/--exclude-dirs/--glob` 过滤。
3. 通过 `obclient` 执行脚本；成功的脚本会移动到 `fixup_scripts/done/<subdir>/`。
4. 输出详细表格，总结成功/失败/跳过原因，便于重跑。

如需按场景分批执行，可在 `fixup_scripts/` 中保留多个子目录或手动挑选脚本。

## Sample scenarios & history

- `test_scenarios/hydra_matrix_case`：多 schema、多 remap 的组合案例，模拟企业级项目。
- `history/db_comparator_*.py`：旧版本脚本留档，可参考排查差异。
（当前仓库仅包含 Hydra 场景，Spiderweb 场景未随仓库提供。）

---

欢迎根据自身需求扩展 remap 规则、接入 CI、或把工具集成到更大的迁移流水线中。若需了解内部实现和设计动机，请继续阅读 `DESIGN.md`。祝迁移顺利!
