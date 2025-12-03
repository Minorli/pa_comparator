# 数据库对象对比工具 - 跨平台打包与执行指南 (Wheelhouse 版)

> 适用场景：需要把当前目录下的 Python 程序（保持源码不改）打包到另一台机器运行，目标 Python 可能是 3.6/3.7.8，且目标机可能无法联网。本文只使用 wheelhouse+venv，不用 PyInstaller。
> 适用版本：V0.8（保持“一次转储，本地对比”，命令行可传入自定义的 config.ini；新增表/列注释比对开关）。

## 0. 环境与外部依赖清单（目标机必须具备）
- **Python 解释器**：与打包时的版本和架构匹配（建议提前在目标机装好 3.6.x 或 3.7.x，对应 cp36/cp37）。  
- **Oracle Instant Client**：供 `oracledb` Thick Mode 使用，版本建议 19c+，架构需匹配目标机。  
- **obclient**：可直连目标 OceanBase 并有足够权限。  
- **JDK + dbcat**：dbcat 依赖 JDK（8/11/17 均可），需准备 dbcat 可执行目录（如 `dbcat-2.5.0-SNAPSHOT`）。  
- **网络/权限**：能访问源 Oracle、目标 OceanBase；目标目录需写权限（生成报告/脚本/缓存）。  
- **系统库**：Linux 下需要可用的 glibc（oracledb manylinux 轮子要求），并能设置 `LD_LIBRARY_PATH`。

## 1. 在构建机准备 wheelhouse（与目标机 Python/架构匹配）
1) 安装与目标一致的 Python 版本（尽量同一个小版本，例如目标 3.7.8→构建机用 3.7.x）。  
2) 创建虚拟环境：
```bash
python3 -m venv .venv
source .venv/bin/activate
```
3) 生成 wheelhouse（默认当前平台/当前 Python）：
```bash
mkdir -p wheelhouse
pip wheel --wheel-dir=./wheelhouse -r requirements.txt
```
4) 若需为其他平台/小版本打包（例如构建机 macOS，目标 Linux x86_64 + Python 3.6/3.7），先在目标机跑：
```bash
pip debug --verbose | grep -E "py.implementation|platforms"
```
然后在构建机用对应标签生成轮子（示例：Linux x86_64 + Python 3.7）：
```bash
pip wheel --wheel-dir=./wheelhouse -r requirements.txt \
  --python-version 37 \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --only-binary :all:
```
> 提示：oracledb 轮子只提供受支持的平台标签；macOS→Linux/aarch64 通常不可行，尽量在目标同架构的机器生成。

## 2. 打包部署物料（在构建机）
整理一个可直接拷贝的目录，例如 `deployment_package/`：
```
deployment_package/
├── schema_diff_reconciler.py
├── run_fixup.py
├── requirements.txt
├── config.ini                 # 可留模板，目标机再改绝对路径
├── remap_rules.txt
├── wheelhouse/                # pip wheel 输出
├── dbcat-2.5.0-SNAPSHOT/      # 完整 dbcat 目录
├── instantclient_19_28/       # Oracle Instant Client
└── setup_env.sh               # （可选）封装环境变量的脚本
```
将目录打包为 `.tar.gz`/`.zip`，拷贝到目标机。

`setup_env.sh` 示例：
```bash
export JAVA_HOME=/home/user/comparator_deploy/jdk-11
export LD_LIBRARY_PATH=/home/user/comparator_deploy/instantclient_19_28:${LD_LIBRARY_PATH}
export PATH=/home/user/comparator_deploy/dbcat-2.5.0-SNAPSHOT/bin:${PATH}
```
> obclient 若不在 PATH，直接在 `config.ini` 里写绝对路径。

## 3. 在目标机解压与离线安装依赖
```bash
cd /path/to/deployment_package   # 例如 /home/user/comparator_deploy
tar -xzf comparator_package.tar.gz  # 或 unzip

# 创建/激活 venv（确保用目标 Python 3.6/3.7）
python3 -m venv .venv
source .venv/bin/activate

# 离线安装依赖
pip install --no-index --find-links=./wheelhouse -r requirements.txt
```
检查：
```bash
python -V            # 确认版本
pip list | grep -E "oracledb|rich"
```

## 4. 配置 `config.ini`（全部改为绝对路径）
关键项示例：
```ini
[ORACLE_SOURCE]
user=...
password=...
dsn=host:port/service

[OCEANBASE_TARGET]
executable=/home/user/obclient/bin/obclient
host=...
port=...
user_string=...
password=...

[SETTINGS]
source_schemas=...
remap_file=/home/user/comparator_deploy/remap_rules.txt
oracle_client_lib_dir=/home/user/comparator_deploy/instantclient_19_28
dbcat_bin=/home/user/comparator_deploy/dbcat-2.5.0-SNAPSHOT
dbcat_output_dir=/home/user/comparator_deploy/dbcat_output
java_home=/home/user/comparator_deploy/jdk-11
fixup_dir=/home/user/comparator_deploy/fixup_scripts
report_dir=/home/user/comparator_deploy/main_reports
```
> 端口/用户信息按实际填写；确保目标目录存在写权限。

## 5. 运行步骤（目标机）
```bash
cd /home/user/comparator_deploy
source setup_env.sh           # 设置 JAVA_HOME / LD_LIBRARY_PATH / PATH
source .venv/bin/activate     # 进入对应 Python 版本的 venv

# 运行对比
python schema_diff_reconciler.py [config.ini]            # 不传时默认读取当前目录的 config.ini
python schema_diff_reconciler.py --wizard [config.ini]   # 缺项时启动交互式向导并写回配置

# 查看输出
ls main_reports
ls fixup_scripts

# 执行修补脚本（可多次重跑）
python run_fixup.py           # 同样可传 config.ini 路径
```

## 6. 检查点与常见问题
- **版本匹配**：`python -V` 与 wheelhouse 标签（cp36/cp37）一致；架构与 wheelhouse 平台一致。  
- **Instant Client**：`echo $LD_LIBRARY_PATH` 含 instantclient 路径；缺库时报 `libclntsh.so` 找不到。  
- **JDK/dbcat**：`java -version` 正常；`$JAVA_HOME` 与 `dbcat_bin` 可执行。  
- **obclient**：可单独执行一条 `obclient -h ... -P ... -u ... -p... -e "select 1;"` 进行连通性验证。  
- **权限**：输出目录（`fixup_scripts/`, `main_reports/`, `dbcat_output/`）需可写；`config.ini` 含明文密码需妥善保护。  
- **离线安装失败**：确认 `pip install --no-index --find-links=./wheelhouse` 使用的 Python 版本正确；轮子平台标签是否覆盖目标机；必要时在目标架构重新生成 wheelhouse。  
- **依赖不足**：oracledb Thick Mode 还依赖 `libaio` 等系统库，请在目标机提前安装（依据操作系统包管理器）。

## 7. 附带资料与可选内容
- **必须携带**：`schema_diff_reconciler.py`、`run_fixup.py`、`requirements.txt`、不含敏感信息的 `config.ini` 模板、Remap 文件、`wheelhouse/`、dbcat 目录、Instant Client。  
- **推荐附带**：`test_scenarios/` 与 `init_test.py`（便于离线冒烟）、`README.md`/`README_CROSS_PLATFORM.md`/`DESIGN.md`、`dbcat_output/`（可选缓存，避免目标机重复抽取）。  
- **可清理后再打包**：旧的 `fixup_scripts/` 与 `main_reports/`（避免混淆生成物）、`history/`（纯参考）。敏感账号信息请务必在打包前去除或脱敏。

## 8. 小结
- 不修改源码，只通过 wheelhouse + venv 迁移。  
- 构建机与目标机的 Python/架构要匹配；必要时在目标架构生成 wheelhouse。  
- 运行前先设置 `JAVA_HOME`/`LD_LIBRARY_PATH` 等外部依赖，再激活 venv 并执行脚本。  
