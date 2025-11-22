# 数据库对象对比工具 - 跨平台打包与执行指南

本文档旨在指导用户如何将本 Python 程序打包，并移植到另一台（目标）机器上执行。目标平台可能是 Python 3.6 或 3.7 的环境。

## 1. 简介

本工具套件包含两个核心程序：
1.  `schema_diff_reconciler.py`: 用于对比源端 Oracle 数据库与目标端 OceanBase 数据库的 Schema 对象差异，并生成修复用的 SQL 脚本。
2.  `run_fixup.py`: 用于自动执行 `schema_diff_reconciler.py` 生成的修复脚本。

为了在没有预装 Python 环境或依赖库的目标机器上运行，我们使用 `PyInstaller` 进行打包。

## 2. 核心依赖

在**目标机器**上，即使有了打包好的程序，仍然必须准备好以下**外部依赖**：

- **Oracle Instant Client**:
  - 这是 `oracledb` 库在 "Thick Mode" 下运行所必需的。
  - 下载地址：[Oracle Instant Client Downloads](https://www.oracle.com/database/technologies/instant-client/downloads.html)
  - **注意**: 请下载与目标机器操作系统和架构匹配的版本（如 `Linux x86-64`）。版本建议 19c 或更高。

- **OceanBase Client (`obclient`)**:
  - 用于连接目标端 OceanBase 并执行查询和脚本。
  - 需要确保其版本与目标 OceanBase 集群兼容。

- **Java Development Kit (JDK)**:
  - 这是 `dbcat` 工具运行所必需的。`dbcat` 用于从源端 Oracle 提取 DDL。
  - 建议安装 JDK 8 或更高版本。

## 3. 打包步骤（在开发机上操作）

以下步骤描述了如何利用 `PyInstaller` 将 Python 程序打包成可执行文件。

### 3.1. 创建虚拟环境

为了隔离依赖，强烈建议在项目根目录下创建一个虚拟环境。

```bash
# 创建名为 .venv 的虚拟环境
python3 -m venv .venv

# 激活虚拟环境 (Linux/macOS)
source .venv/bin/activate

# 如果是 Windows
# .venv\Scripts\activate
```

### 3.2. 安装依赖

在激活的虚拟环境中，安装所有必要的 Python 库。

```bash
pip install -r requirements.txt

# 同时安装 PyInstaller
pip install pyinstaller
```

### 3.3. 使用 PyInstaller 打包

我们将同时打包两个主脚本。PyInstaller 会为每个脚本生成一个对应的可执行文件。

```bash
# 在项目根目录下执行
pyinstaller --clean --onefile \
  schema_diff_reconciler.py \
  run_fixup.py
```

- `--onefile`: 将所有依赖打包进一个单独的可执行文件中。
- `--clean`: 在打包前清理 PyInstaller 的缓存。

打包成功后，你会在项目根目录下看到一个 `dist` 目录，其中包含两个可执行文件：`schema_diff_reconciler` 和 `run_fixup`。

### 3.4. 准备部署压缩包

为了方便移植，请创建一个压缩包，包含所有需要部署的文件：

- `dist/` 目录下的两个可执行文件。
- `config.ini` 配置文件。
- `remap_rules.txt` 规则文件（如果用到）。
- `dbcat` 工具的完整目录。
- `Oracle Instant Client` 的完整目录。

例如，创建一个部署包结构如下：

```
deployment_package/
├── schema_diff_reconciler      # 打包后的可执行文件
├── run_fixup                   # 打包后的可执行文件
├── config.ini                  # 配置文件
├── remap_rules.txt             # 映射规则
├── dbcat-2.5.0-SNAPSHOT/       # dbcat 工具目录
└── instantclient_19_28/        # Oracle Instant Client 目录
```

将 `deployment_package` 目录压缩成 `tar.gz` 或 `.zip` 文件，然后拷贝到目标机器。

## 4. 移植与部署（在目标机上操作）

### 4.1. 解压与放置文件

将上一步创建的压缩包上传到目标机器并解压。所有文件将存在于一个独立的目录中，例如 `/home/user/comparator_deploy`。

### 4.2. 设置环境变量

这是**至关重要**的一步。程序依赖环境变量来找到 `Oracle Instant Client` 和 `Java`。

在执行程序前，请在当前 shell 会话中设置以下环境变量：

```bash
# 假设你的文件解压在 /home/user/comparator_deploy

# 1. 设置 JAVA_HOME，指向你的 JDK 安装目录
export JAVA_HOME=/path/to/your/jdk-11.0.2

# 2. 设置 LD_LIBRARY_PATH，指向 Oracle Instant Client 目录
#    这使得 oracledb 库能找到 .so 共享库文件
export LD_LIBRARY_PATH=/home/user/comparator_deploy/instantclient_19_28:${LD_LIBRARY_PATH}
```

**强烈建议**将这些 `export` 命令写入一个 `setup_env.sh` 脚本，每次运行前先 `source setup_env.sh`。

## 5. 配置

在执行前，必须修改 `config.ini` 文件，以适配目标机器的环境。**所有路径都应使用绝对路径**。

请重点检查并修改以下路径相关的配置项：

```ini
[SETTINGS]
# remap 规则文件路径
remap_file              = /home/user/comparator_deploy/remap_rules.txt

# Oracle Instant Client 目录 (必须包含 libclntsh.so)
# 这个路径必须与 LD_LIBRARY_PATH 中设置的路径一致
oracle_client_lib_dir   = /home/user/comparator_deploy/instantclient_19_28

# dbcat 配置
dbcat_bin               = /home/user/comparator_deploy/dbcat-2.5.0-SNAPSHOT
dbcat_output_dir        = /home/user/comparator_deploy/dbcat_output
# Java Home (可选，但建议设置，如果 JAVA_HOME 环境变量未全局设置)
java_home               = /path/to/your/jdk-11.0.2


[OCEANBASE_TARGET]
# obclient 可执行文件的绝对路径
executable  = /path/to/your/obclient
```

同时，请确保 `[ORACLE_SOURCE]` 和 `[OCEANBASE_TARGET]` 中的数据库连接信息（用户、密码、IP、端口等）是正确的。

## 6. 执行

完成所有配置后，可以开始执行程序。

### 6.1. 设置环境

```bash
# 激活环境变量
source setup_env.sh
```

### 6.2. 运行对比程序

```bash
# 切换到部署目录
cd /home/user/comparator_deploy

# 运行 schema 对比
./schema_diff_reconciler
```

程序会连接数据库，进行对比，并将报告输出到 `main_reports` 目录，修复脚本输出到 `fixup_scripts` 目录。

### 6.3. 运行修复脚本

对比完成后，可以检查 `fixup_scripts` 目录中的 SQL 文件，然后使用 `run_fixup` 执行它们。

```bash
# 运行修复脚本
./run_fixup
```

此脚本会自动查找 `fixup_scripts` 目录下的 SQL 文件并按顺序在 OceanBase 中执行。

## 7. 注意事项

- **平台兼容性**: PyInstaller 打包的可执行文件**不跨平台**。例如，在 Linux 上打包的文件不能在 Windows 上运行。请确保打包环境的操作系统和架构（如 x86-64）与目标机一致。
- **Python 版本**: PyInstaller 会将打包时使用的 Python 解释器也打包进去。虽然程序本身兼容 Python 3.6/3.7，但建议打包环境的 Python 版本不要高于目标环境太多，以避免潜在的 glibc 版本不兼容问题。
- **权限**: 确保 `obclient` 和打包后的两个可执行文件都有执行权限 (`chmod +x <filename>`)。同时确保程序对 `fixup_scripts`, `main_reports`, `dbcat_output` 等目录有写入权限。
- **安全**: `config.ini` 中包含数据库明文密码，请严格控制该文件的访问权限，避免泄露。
- **问题排查**: 如果执行出错，请首先检查：
  1. `LD_LIBRARY_PATH` 和 `JAVA_HOME` 是否已正确设置。
  2. `config.ini` 中的所有路径是否都正确无误。
  3. 数据库连接信息是否正确，网络是否通畅。
  4. `obclient` 是否能手动正常连接到 OceanBase。
