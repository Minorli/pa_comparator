#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库对象对比工具 (V0.1 - Dump-Once, Compare-Locally + 扩展对象 + ALTER 修补)
---------------------------------------------------------------------------
功能概要：
1. 对比 Oracle (源) 与 OceanBase (目标) 的：
   - TABLE, VIEW
   - INDEX, CONSTRAINT (PK/UK/FK)
   - SEQUENCE, TRIGGER
   - PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM

2. 对比规则：
   - TABLE：只对比“列名集合”，忽略以 OMS_ 开头的列，不对比数据类型/长度。
   - VIEW / SEQUENCE / TRIGGER / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM：
       只对比“是否存在”。
   - INDEX / CONSTRAINT：
       对比“是否存在”及“构成”（索引列集合、有无/列顺序；约束类型和约束列集合）。

3. 性能架构 (V0.1 核心)：
   - OceanBase 侧采用“一次转储，本地对比”：
       使用少量 obclient 调用，分别 dump：
         ALL_OBJECTS
         ALL_TAB_COLUMNS
         ALL_INDEXES / ALL_IND_COLUMNS
         ALL_CONSTRAINTS / ALL_CONS_COLUMNS
         ALL_TRIGGERS
         ALL_SEQUENCES
       后续所有对比均在 Python 内存数据结构中完成。
   - 避免 V12 中在循环中大量调用 obclient 的性能黑洞。

4. 修补脚本生成：
   - 缺失对象：
       TABLE / VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM /
       INDEX / CONSTRAINT / SEQUENCE / TRIGGER
       → 生成对应的 CREATE 语句脚本。
   - TABLE 列不匹配：
       → 生成 ALTER TABLE ADD 列的脚本；
       → 对“多余列”生成注释掉的 DROP COLUMN 建议语句。
   - 所有脚本写入 fix_up 目录下相应子目录，需人工审核后在 OceanBase 执行。

5. 健壮性：
   - 所有 obclient 调用增加 timeout（从 db.ini 的 [SETTINGS] -> obclient_timeout 读取，默认 60 秒）。
"""

import configparser
import subprocess
import sys
import logging
import re
from collections import defaultdict, OrderedDict
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional, NamedTuple

# 尝试导入 oracledb，如果失败则提示安装
try:
    import oracledb
except ImportError:
    print("错误: 未找到 'oracledb' 库。", file=sys.stderr)
    print("请先安装: pip install oracledb", file=sys.stderr)
    sys.exit(1)



# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    stream=sys.stderr
)
log = logging.getLogger(__name__)

# --- 类型别名 ---
OraConfig = Dict[str, str]
ObConfig = Dict[str, str]
RemapRules = Dict[str, str]
SourceObjectMap = Dict[str, str]  # {'OWNER.OBJ': 'TYPE'}
MasterCheckList = List[Tuple[str, str, str]]  # [(src_name, tgt_name, type)]
ReportResults = Dict[str, List]

# --- 全局 obclient timeout（秒），由配置初始化 ---
OBC_TIMEOUT: int = 60


# --- 扩展检查结果结构 ---
class IndexMismatch(NamedTuple):
    table: str
    missing_indexes: Set[str]
    extra_indexes: Set[str]
    detail_mismatch: List[str]


class ConstraintMismatch(NamedTuple):
    table: str
    missing_constraints: Set[str]
    extra_constraints: Set[str]
    detail_mismatch: List[str]


class SequenceMismatch(NamedTuple):
    src_schema: str
    tgt_schema: str
    missing_sequences: Set[str]
    extra_sequences: Set[str]
    note: Optional[str] = None


class TriggerMismatch(NamedTuple):
    table: str
    missing_triggers: Set[str]
    extra_triggers: Set[str]
    detail_mismatch: List[str]


ExtraCheckResults = Dict[str, List]

class ObMetadata(NamedTuple):
    """
    一次性从 OceanBase dump 出来的元数据，用于本地对比。
    """
    objects_by_type: Dict[str, Set[str]]                 # OBJECT_TYPE -> {OWNER.OBJ}
    tab_columns: Dict[Tuple[str, str], Dict[str, Dict]]   # (OWNER, TABLE_NAME) -> {COLUMN_NAME: {type, length, etc.}}
    indexes: Dict[Tuple[str, str], Dict[str, Dict]]      # (OWNER, TABLE_NAME) -> {INDEX_NAME: {uniqueness, columns[list]}}
    constraints: Dict[Tuple[str, str], Dict[str, Dict]]  # (OWNER, TABLE_NAME) -> {CONS_NAME: {type, columns[list]}}
    triggers: Dict[Tuple[str, str], Dict[str, Dict]]     # (OWNER, TABLE_NAME) -> {TRG_NAME: {event, status}}
    sequences: Dict[str, Set[str]]                       # SEQUENCE_OWNER -> {SEQUENCE_NAME}


class OracleMetadata(NamedTuple):
    """
    源端 Oracle 的元数据缓存，避免在循环中重复查询。
    """
    table_columns: Dict[Tuple[str, str], Dict[str, Dict]]   # (OWNER, TABLE_NAME) -> 列定义
    indexes: Dict[Tuple[str, str], Dict[str, Dict]]        # (OWNER, TABLE_NAME) -> 索引
    constraints: Dict[Tuple[str, str], Dict[str, Dict]]    # (OWNER, TABLE_NAME) -> 约束
    triggers: Dict[Tuple[str, str], Dict[str, Dict]]       # (OWNER, TABLE_NAME) -> 触发器
    sequences: Dict[str, Set[str]]                         # OWNER -> {SEQUENCE_NAME}


# ====================== 通用配置和基础函数 ======================

def load_config(config_file: str) -> Tuple[OraConfig, ObConfig, Dict]:
    """读取 db.ini 配置文件"""
    log.info(f"正在加载配置文件: {config_file}")
    config = configparser.ConfigParser()
    if not config.read(config_file):
        log.error(f"严重错误: 配置文件 {config_file} 未找到或无法读取。")
        sys.exit(1)

    try:
        ora_cfg = dict(config['ORACLE_SOURCE'])
        ob_cfg = dict(config['OCEANBASE_TARGET'])
        settings = dict(config['SETTINGS'])

        schemas_raw = settings.get('source_schemas', '')
        schemas_list = [s.strip().upper() for s in schemas_raw.split(',') if s.strip()]
        if not schemas_list:
            log.error(f"严重错误: [SETTINGS] 中的 'source_schemas' 未配置或为空。")
            sys.exit(1)
        settings['source_schemas_list'] = schemas_list

        # fix_up 目录
        settings.setdefault('fixup_dir', 'fix_up')
        # obclient 超时时间 (秒)
        settings.setdefault('obclient_timeout', '60')

        global OBC_TIMEOUT
        try:
            OBC_TIMEOUT = int(settings['obclient_timeout'])
        except ValueError:
            OBC_TIMEOUT = 60

        log.info(f"成功加载配置，将扫描 {len(schemas_list)} 个源 schema。")
        log.info(f"obclient 超时时间: {OBC_TIMEOUT} 秒")
        return ora_cfg, ob_cfg, settings
    except KeyError as e:
        log.error(f"严重错误: 配置文件中缺少必要的部分: {e}")
        sys.exit(1)


def load_remap_rules(file_path: str) -> RemapRules:
    """从 txt 文件加载 remap 规则"""
    log.info(f"正在加载 Remap 规则文件: {file_path}")
    rules: RemapRules = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                if '=' not in line:
                    log.warning(f"  [规则警告] 第 {i+1} 行格式错误，已跳过: {line}")
                    continue

                try:
                    src_obj, tgt_obj = line.split('=', 1)
                    src_obj = src_obj.strip().upper()
                    tgt_obj = tgt_obj.strip().upper()
                    if not src_obj or not tgt_obj or '.' not in src_obj or '.' not in tgt_obj:
                        log.warning(f"  [规则警告] 第 {i+1} 行格式无效 (必须为 'SCHEMA.OBJ')，已跳过: {line}")
                        continue
                    rules[src_obj] = tgt_obj
                except Exception:
                    log.warning(f"  [规则警告] 第 {i+1} 行解析失败，已跳过: {line}")

    except FileNotFoundError:
        log.warning(f"  [警告] Remap 文件 {file_path} 未找到。将按 1:1 规则继续。")
        return {}

    log.info(f"加载了 {len(rules)} 条 Remap 规则。")
    return rules


def get_source_objects(ora_cfg: OraConfig, schemas_list: List[str]) -> SourceObjectMap:
    """
    从 Oracle 源端获取所有需要对比的“主对象”：
      TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, SYNONYM
    """
    log.info(f"正在连接 Oracle 源端: {ora_cfg['dsn']}...")

    placeholders = ','.join([f":{i+1}" for i in range(len(schemas_list))])

    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OWNER IN ({placeholders})
          AND OBJECT_TYPE IN (
              'TABLE','VIEW',
              'PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','SYNONYM'
          )
    """

    source_objects: SourceObjectMap = {}

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            log.info("Oracle 连接成功。正在查询源对象列表...")
            with connection.cursor() as cursor:
                cursor.execute(sql, schemas_list)
                for row in cursor:
                    owner, obj_name, obj_type = row
                    full_name = f"{owner}.{obj_name}"
                    source_objects[full_name] = obj_type.upper()
    except oracledb.Error as e:
        log.error(f"严重错误: 连接或查询 Oracle 失败: {e}")
        sys.exit(1)

    log.info(
        "从 Oracle 成功获取 %d 个主对象 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM)。",
        len(source_objects)
    )
    return source_objects


def validate_remap_rules(remap_rules: RemapRules, source_objects: SourceObjectMap) -> List[str]:
    """检查 remap 规则中的源对象是否存在于 Oracle source_objects 中。"""
    log.info("正在验证 Remap 规则...")
    remap_keys = set(remap_rules.keys())
    source_keys = set(source_objects.keys())

    extraneous_keys = sorted(list(remap_keys - source_keys))

    if extraneous_keys:
        log.warning(f"  [规则警告] 在 remap_rules.txt 中发现了 {len(extraneous_keys)} 个无效的源对象。")
        log.warning("  (这些对象在源端 Oracle (db.ini 中配置的 schema) 中未找到)")
        for key in extraneous_keys:
            log.warning(f"    - 无效条目: {key}")
    else:
        log.info("Remap 规则验证通过，所有规则中的源对象均存在。")

    return extraneous_keys


def generate_master_list(source_objects: SourceObjectMap, remap_rules: RemapRules) -> MasterCheckList:
    """
    生成“最终校验清单”并检测 "多对一" 映射。
    包含主对象类型：TABLE / VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM
    """
    log.info("正在生成主校验清单 (应用 Remap 规则)...")
    master_list: MasterCheckList = []

    target_tracker: Dict[str, str] = {}

    for src_name, obj_type in source_objects.items():
        if src_name in remap_rules:
            tgt_name = remap_rules[src_name]
        else:
            tgt_name = src_name

        if tgt_name in target_tracker:
            existing_src = target_tracker[tgt_name]
            log.error(f"{'='*80}")
            log.error(f"                 !!! 致命配置错误 !!!")
            log.error(f"发现“多对一”映射。同一个目标对象 '{tgt_name}' 被映射了多次：")
            log.error(f"  1. 源: '{existing_src}' -> 目标: '{tgt_name}'")
            log.error(f"  2. 源: '{src_name}' -> 目标: '{tgt_name}'")
            log.error("这会导致校验逻辑混乱。请检查您的 remap_rules.txt 文件，")
            log.error("确保每一个目标对象只被一个源对象所映射。")
            log.error(f"{'='*80}")
            sys.exit(1)

        target_tracker[tgt_name] = src_name
        master_list.append((src_name, tgt_name, obj_type))

    log.info(f"主校验清单生成完毕，共 {len(master_list)} 个待校验项。")
    return master_list


# ====================== obclient + 一次性元数据转储 ======================

def obclient_run_sql(ob_cfg: ObConfig, sql_query: str) -> Tuple[bool, str, str]:
    """运行 obclient CLI 命令并返回 (Success, stdout, stderr)，带 timeout。"""
    command_args = [
        ob_cfg['executable'],
        '-h', ob_cfg['host'],
        '-P', ob_cfg['port'],
        '-u', ob_cfg['user_string'],
        '-p' + ob_cfg['password'],
        '-ss',  # Silent 模式
        '-e', sql_query
    ]

    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=OBC_TIMEOUT
        )

        if result.returncode != 0 or (result.stderr and "Warning" not in result.stderr):
            log.error(f"  [OBClient 错误] SQL: {sql_query.strip()} | 错误: {result.stderr.strip()}")
            return False, "", result.stderr.strip()

        return True, result.stdout.strip(), ""

    except subprocess.TimeoutExpired:
        log.error(f"严重错误: obclient 执行超时 (>{OBC_TIMEOUT} 秒)。请检查网络/OB 状态或调大 obclient_timeout。")
        return False, "", "TimeoutExpired"
    except FileNotFoundError:
        log.error(f"严重错误: 未找到 obclient 可执行文件: {ob_cfg['executable']}")
        log.error("请检查 db.ini 中的 [OCEANBASE_TARGET] -> executable 路径。")
        sys.exit(1)
    except Exception as e:
        log.error(f"严重错误: 执行 subprocess 时发生未知错误: {e}")
        return False, "", str(e)


def dump_ob_metadata(ob_cfg: ObConfig, target_schemas: Set[str]) -> ObMetadata:
    """
    一次性从 OceanBase dump 所有需要的元数据，返回 ObMetadata。
    如果任何关键视图查询失败，则视为致命错误并退出。
    """
    if not target_schemas:
        log.warning("目标 schema 集合为空，OB 元数据转储将返回空结构。")
        return ObMetadata(
            objects_by_type={},
            tab_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={}
        )

    owners_in = ",".join(f"'{s}'" for s in sorted(target_schemas))

    # --- 1. ALL_OBJECTS ---
    objects_by_type: Dict[str, Set[str]] = {}
    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OWNER IN ({owners_in})
          AND OBJECT_TYPE IN (
              'TABLE','VIEW',
              'PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','SYNONYM',
              'SEQUENCE','TRIGGER','INDEX'
          )
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_OBJECTS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            owner, name, obj_type = parts[0].strip().upper(), parts[1].strip().upper(), parts[2].strip().upper()
            full = f"{owner}.{name}"
            objects_by_type.setdefault(obj_type, set()).add(full)

    # --- 2. ALL_TAB_COLUMNS ---
    tab_columns: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sql = f"""
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, NULLABLE, DATA_DEFAULT
        FROM ALL_TAB_COLUMNS
        WHERE OWNER IN ({owners_in})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_TAB_COLUMNS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 7:
                continue
            owner, table, col, dtype, char_len, nullable, default = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip().upper(),
                parts[4].strip(),
                parts[5].strip(),
                parts[6].strip()
            )
            key = (owner, table)
            tab_columns.setdefault(key, {})[col] = {
                "data_type": dtype,
                "char_length": int(char_len) if char_len.isdigit() else None,
                "nullable": nullable,
                "data_default": default
            }

    # --- 3. ALL_INDEXES ---
    indexes: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sql = f"""
        SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, UNIQUENESS
        FROM ALL_INDEXES
        WHERE TABLE_OWNER IN ({owners_in})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_INDEXES，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 4:
                continue
            t_owner, t_name, idx_name, uniq = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip().upper()
            )
            key = (t_owner, t_name)
            indexes.setdefault(key, {})[idx_name] = {
                "uniqueness": uniq,
                "columns": []
            }

    # --- 4. ALL_IND_COLUMNS ---
    sql = f"""
        SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME, COLUMN_POSITION
        FROM ALL_IND_COLUMNS
        WHERE TABLE_OWNER IN ({owners_in})
        ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_IND_COLUMNS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 5:
                continue
            t_owner, t_name, idx_name, col_name = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip().upper()
            )
            key = (t_owner, t_name)
            if key not in indexes:
                indexes[key] = {}
            if idx_name not in indexes[key]:
                indexes[key][idx_name] = {"uniqueness": "UNKNOWN", "columns": []}
            indexes[key][idx_name]["columns"].append(col_name)

    # --- 5. ALL_CONSTRAINTS (P/U/R) ---
    constraints: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sql = f"""
        SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
        FROM ALL_CONSTRAINTS
        WHERE OWNER IN ({owners_in})
          AND CONSTRAINT_TYPE IN ('P','U','R')
          AND STATUS = 'ENABLED'
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_CONSTRAINTS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 4:
                continue
            owner, table, cons_name, ctype = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip().upper()
            )
            key = (owner, table)
            constraints.setdefault(key, {})[cons_name] = {
                "type": ctype,
                "columns": []
            }

    # --- 6. ALL_CONS_COLUMNS ---
    sql = f"""
        SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, POSITION
        FROM ALL_CONS_COLUMNS
        WHERE OWNER IN ({owners_in})
        ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME, POSITION
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_CONS_COLUMNS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 5:
                continue
            owner, table, cons_name, col_name = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip().upper()
            )
            key = (owner, table)
            if key not in constraints:
                constraints[key] = {}
            if cons_name not in constraints[key]:
                constraints[key][cons_name] = {"type": "UNKNOWN", "columns": []}
            constraints[key][cons_name]["columns"].append(col_name)

    # --- 7. ALL_TRIGGERS ---
    triggers: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sql = f"""
        SELECT TABLE_OWNER, TABLE_NAME, TRIGGER_NAME, TRIGGERING_EVENT, STATUS
        FROM ALL_TRIGGERS
        WHERE TABLE_OWNER IN ({owners_in})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_TRIGGERS，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 5:
                continue
            t_owner, t_name, trg_name, ev, status = (
                parts[0].strip().upper(),
                parts[1].strip().upper(),
                parts[2].strip().upper(),
                parts[3].strip(),
                parts[4].strip()
            )
            key = (t_owner, t_name)
            triggers.setdefault(key, {})[trg_name] = {
                "event": ev,
                "status": status
            }

    # --- 8. ALL_SEQUENCES ---
    sequences: Dict[str, Set[str]] = {}
    sql = f"""
        SELECT SEQUENCE_OWNER, SEQUENCE_NAME
        FROM ALL_SEQUENCES
        WHERE SEQUENCE_OWNER IN ({owners_in})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_SEQUENCES，程序退出。")
        sys.exit(1)

    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 2:
                continue
            owner, seq_name = parts[0].strip().upper(), parts[1].strip().upper()
            sequences.setdefault(owner, set()).add(seq_name)

    log.info("OceanBase 元数据转储完成 (ALL_OBJECTS/COLUMNS/INDEXES/CONSTRAINTS/TRIGGERS/SEQUENCES)。")
    return ObMetadata(
        objects_by_type=objects_by_type,
        tab_columns=tab_columns,
        indexes=indexes,
        constraints=constraints,
        triggers=triggers,
        sequences=sequences
    )


# ====================== Oracle 侧辅助函数 ======================

def dump_oracle_metadata(
    ora_cfg: OraConfig,
    master_list: MasterCheckList,
    settings: Dict
) -> OracleMetadata:
    """
    预先加载 Oracle 端所需的所有元数据，避免在校验/修补阶段频繁查询。
    """
    table_pairs: Set[Tuple[str, str]] = set()
    owner_set: Set[str] = set()
    for src_name, _, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        try:
            schema, table = src_name.split('.')
        except ValueError:
            continue
        schema = schema.upper()
        table = table.upper()
        owner_set.add(schema)
        table_pairs.add((schema, table))

    owners = sorted(owner_set)
    seq_owners = sorted({s.upper() for s in settings.get('source_schemas_list', [])})

    if not owners and not seq_owners:
        log.warning("未检测到需要加载的 Oracle schema，返回空元数据。")
        return OracleMetadata(
            table_columns={},
            indexes={},
            constraints={},
            triggers={},
            sequences={}
        )

    def _make_in_clause(values: List[str]) -> str:
        return ",".join(f":{i+1}" for i in range(len(values)))

    log.info("正在批量加载 Oracle 元数据 (ALL_TAB_COLUMNS/INDEXES/CONSTRAINTS/TRIGGERS/SEQUENCES)...")
    table_columns: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    indexes: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    constraints: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    triggers: Dict[Tuple[str, str], Dict[str, Dict]] = {}
    sequences: Dict[str, Set[str]] = {}

    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as ora_conn:
            if owners:
                owners_clause = _make_in_clause(owners)

                # 列定义
                sql = f"""
                    SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                           DATA_LENGTH, DATA_PRECISION, DATA_SCALE,
                           NULLABLE, DATA_DEFAULT, CHAR_USED, CHAR_LENGTH
                    FROM ALL_TAB_COLUMNS
                    WHERE OWNER IN ({owners_clause})
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql, owners)
                    for row in cursor:
                        owner, table, col = row[0].upper(), row[1].upper(), row[2].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        table_columns.setdefault(key, {})[col] = {
                            "data_type": row[3],
                            "data_length": row[4],
                            "data_precision": row[5],
                            "data_scale": row[6],
                            "nullable": row[7],
                            "data_default": row[8],
                            "char_used": row[9],
                            "char_length": row[10],
                        }

                # 索引
                sql_idx = f"""
                    SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, UNIQUENESS
                    FROM ALL_INDEXES
                    WHERE TABLE_OWNER IN ({owners_clause})
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_idx, owners)
                    for row in cursor:
                        owner, table = row[0].upper(), row[1].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        idx_name = row[2].upper()
                        indexes.setdefault(key, {})[idx_name] = {
                            "uniqueness": (row[3] or "").upper(),
                            "columns": []
                        }

                sql_idx_cols = f"""
                    SELECT TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_NAME
                    FROM ALL_IND_COLUMNS
                    WHERE TABLE_OWNER IN ({owners_clause})
                    ORDER BY TABLE_OWNER, TABLE_NAME, INDEX_NAME, COLUMN_POSITION
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_idx_cols, owners)
                    for row in cursor:
                        owner, table = row[0].upper(), row[1].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        idx_name = row[2].upper()
                        col_name = row[3].upper()
                        indexes.setdefault(key, {}).setdefault(
                            idx_name, {"uniqueness": "UNKNOWN", "columns": []}
                        )["columns"].append(col_name)

                # 约束
                sql_cons = f"""
                    SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
                    FROM ALL_CONSTRAINTS
                    WHERE OWNER IN ({owners_clause})
                      AND CONSTRAINT_TYPE IN ('P','U','R')
                      AND STATUS = 'ENABLED'
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_cons, owners)
                    for row in cursor:
                        owner, table = row[0].upper(), row[1].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        name = row[2].upper()
                        constraints.setdefault(key, {})[name] = {
                            "type": (row[3] or "").upper(),
                            "columns": []
                        }

                sql_cons_cols = f"""
                    SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
                    FROM ALL_CONS_COLUMNS
                    WHERE OWNER IN ({owners_clause})
                    ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME, POSITION
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_cons_cols, owners)
                    for row in cursor:
                        owner, table = row[0].upper(), row[1].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        cons_name = row[2].upper()
                        col_name = row[3].upper()
                        constraints.setdefault(key, {}).setdefault(
                            cons_name, {"type": "UNKNOWN", "columns": []}
                        )["columns"].append(col_name)

                # 触发器
                sql_trg = f"""
                    SELECT TABLE_OWNER, TABLE_NAME, TRIGGER_NAME, TRIGGERING_EVENT, STATUS
                    FROM ALL_TRIGGERS
                    WHERE TABLE_OWNER IN ({owners_clause})
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_trg, owners)
                    for row in cursor:
                        owner, table = row[0].upper(), row[1].upper()
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        trg_name = row[2].upper()
                        triggers.setdefault(key, {})[trg_name] = {
                            "event": row[3],
                            "status": row[4]
                        }

            if seq_owners:
                seq_clause = _make_in_clause(seq_owners)
                sql_seq = f"""
                    SELECT SEQUENCE_OWNER, SEQUENCE_NAME
                    FROM ALL_SEQUENCES
                    WHERE SEQUENCE_OWNER IN ({seq_clause})
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_seq, seq_owners)
                    for row in cursor:
                        owner = row[0].upper()
                        seq_name = row[1].upper()
                        sequences.setdefault(owner, set()).add(seq_name)

    except oracledb.Error as e:
        log.error(f"严重错误: 批量获取 Oracle 元数据失败: {e}")
        sys.exit(1)

    log.info(
        "Oracle 元数据加载完成：列=%d, 索引表=%d, 约束表=%d, 触发器表=%d, 序列schema=%d",
        len(table_columns), len(indexes), len(constraints), len(triggers), len(sequences)
    )

    return OracleMetadata(
        table_columns=table_columns,
        indexes=indexes,
        constraints=constraints,
        triggers=triggers,
        sequences=sequences
    )


# ====================== TABLE / VIEW / 其他主对象校验 ======================

def check_primary_objects(
    master_list: MasterCheckList,
    extraneous_rules: List[str],
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata
) -> ReportResults:
    """
    核心主对象校验：
      - TABLE: 存在性 + 列名集合 (忽略 OMS_ 列)
      - VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM: 只校验存在性
    """
    results: ReportResults = {
        "missing": [],
        "mismatched": [],
        "ok": [],
        "extraneous": extraneous_rules
    }

    if not master_list:
        log.info("主校验清单为空，没有需要校验的对象。")
        return results

    log.info("--- 开始执行主对象批量验证 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM) ---")

    total = len(master_list)
    for i, (src_name, tgt_name, obj_type) in enumerate(master_list):

        if (i + 1) % 100 == 0:
            log.info(f"  主对象校验进度: {i+1} / {total} ...")

        obj_type_u = obj_type.upper()
        try:
            src_schema, src_obj = src_name.split('.')
            tgt_schema, tgt_obj = tgt_name.split('.')
        except ValueError:
            log.warning(f"  [跳过] 对象名格式不正确: src='{src_name}', tgt='{tgt_name}'")
            continue

        src_schema_u = src_schema.upper()
        src_obj_u = src_obj.upper()
        tgt_schema_u = tgt_schema.upper()
        tgt_obj_u = tgt_obj.upper()
        full_tgt = f"{tgt_schema_u}.{tgt_obj_u}"

        if obj_type_u == 'TABLE':
            # 1) OB 是否存在 TABLE
            ob_tables = ob_meta.objects_by_type.get('TABLE', set())
            if full_tgt not in ob_tables:
                results['missing'].append(('TABLE', full_tgt, src_name))
                continue

            # 2) 列级别详细对比 (包括 VARCHAR/VARCHAR2 长度 * 1.5)
            src_cols_details = oracle_meta.table_columns.get((src_schema_u, src_obj_u))
            tgt_cols_details = ob_meta.tab_columns.get((tgt_schema_u, tgt_obj_u), {})

            if src_cols_details is None:
                results['mismatched'].append((
                    'TABLE',
                    f"{full_tgt} (源端列信息获取失败)",
                    set(),
                    set(),
                    []
                ))
                continue

            src_col_names = set(src_cols_details.keys())
            tgt_col_names_raw = set(tgt_cols_details.keys())

            # 过滤 'OMS_' 列
            tgt_col_names = {
                col for col in tgt_col_names_raw
                if not col.upper().startswith('OMS_')
            }

            missing_in_tgt = src_col_names - tgt_col_names
            extra_in_tgt = tgt_col_names - src_col_names
            length_mismatches = []

            # 检查公共列的长度
            common_cols = src_col_names & tgt_col_names
            for col_name in common_cols:
                src_info = src_cols_details[col_name]
                tgt_info = tgt_cols_details[col_name]

                src_dtype = (src_info.get("data_type") or "").upper()

                if src_dtype in ('VARCHAR2', 'VARCHAR'):
                    src_len = src_info.get("char_length") or src_info.get("data_length")
                    tgt_len = tgt_info.get("char_length")

                    if src_len is not None and tgt_len is not None:
                        expected_tgt_len = int(src_len * 1.5)
                        if tgt_len != expected_tgt_len:
                            length_mismatches.append(
                                (col_name, src_len, tgt_len, expected_tgt_len)
                            )

            if not missing_in_tgt and not extra_in_tgt and not length_mismatches:
                results['ok'].append(('TABLE', full_tgt))
            else:
                results['mismatched'].append((
                    'TABLE',
                    full_tgt,
                    missing_in_tgt,
                    extra_in_tgt,
                    length_mismatches
                ))

        elif obj_type_u in ('VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'SYNONYM'):
            ob_set = ob_meta.objects_by_type.get(obj_type_u, set())
            if full_tgt in ob_set:
                results['ok'].append((obj_type_u, full_tgt))
            else:
                results['missing'].append((obj_type_u, full_tgt, src_name))

        else:
            # 不在主对比范围的类型直接忽略
            continue

    return results


# ====================== 扩展：索引 / 约束 / 序列 / 触发器 ======================

def build_schema_mapping(master_list: MasterCheckList) -> Dict[str, str]:
    """
    基于 master_list 中 TABLE 映射，推导 schema 映射：
      如果同一 src_schema 只映射到唯一一个 tgt_schema，则使用该映射；
      否则 (映射到多个目标 schema)，退回 src_schema 本身 (1:1)。
    """
    mapping_tmp: Dict[str, Set[str]] = {}
    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue
        try:
            src_schema, _ = src_name.split('.')
            tgt_schema, _ = tgt_name.split('.')
        except ValueError:
            continue
        mapping_tmp.setdefault(src_schema.upper(), set()).add(tgt_schema.upper())

    final_mapping: Dict[str, str] = {}
    for src_schema, tgt_set in mapping_tmp.items():
        if len(tgt_set) == 1:
            final_mapping[src_schema] = next(iter(tgt_set))
        else:
            final_mapping[src_schema] = src_schema
    return final_mapping


def compare_indexes_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[IndexMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_idx = oracle_meta.indexes.get(src_key)
    if src_idx is None:
        return False, IndexMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_indexes=set(),
            extra_indexes=set(),
            detail_mismatch=[
                "无法比较：源端 Oracle 未提供该表的索引元数据 (ALL_INDEXES/ALL_IND_COLUMNS dump 为空)。"
            ]
        )

    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_idx = ob_meta.indexes.get(tgt_key, {})

    def canonicalize(entries: Dict[str, Dict]) -> List[Tuple[str, str, Tuple[str, ...]]]:
        result: List[Tuple[str, str, Tuple[str, ...]]] = []
        for name, info in entries.items():
            cols = tuple(info.get("columns") or [])
            uniq = (info.get("uniqueness") or "").upper()
            result.append((name, uniq, cols))
        return result

    src_entries = canonicalize(src_idx)
    tgt_entries = canonicalize(tgt_idx)

    missing: Set[str] = set()
    tgt_used = [False] * len(tgt_entries)
    detail_mismatch: List[str] = []

    for src_name, src_unique, src_cols in src_entries:
        matched_index = None
        for idx, (tgt_name, tgt_unique, tgt_cols) in enumerate(tgt_entries):
            if tgt_used[idx]:
                continue
            if src_cols == tgt_cols:
                matched_index = idx
                tgt_used[idx] = True
                if src_unique != tgt_unique:
                    detail_mismatch.append(
                        f"索引列 {list(src_cols)} 唯一性不一致 (源 {src_unique}, 目标 {tgt_unique})。"
                    )
                break
        if matched_index is None:
            missing.add(src_name)
            detail_mismatch.append(
                f"索引 {src_name} (列 {list(src_cols)}, 唯一性 {src_unique}) 在目标端未找到。"
            )

    extra: Set[str] = set()
    for idx, used in enumerate(tgt_used):
        if not used:
            tgt_name, tgt_unique, tgt_cols = tgt_entries[idx]
            extra.add(tgt_name)
            detail_mismatch.append(
                f"目标端存在额外索引 {tgt_name} (列 {list(tgt_cols)}, 唯一性 {tgt_unique})。"
            )

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, IndexMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_indexes=missing,
            extra_indexes=extra,
            detail_mismatch=detail_mismatch
        )


def compare_constraints_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[ConstraintMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_cons = oracle_meta.constraints.get(src_key)
    if src_cons is None:
        return False, ConstraintMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_constraints=set(),
            extra_constraints=set(),
            detail_mismatch=[
                "无法比较：源端 Oracle 未提供该表的约束元数据 (ALL_CONSTRAINTS/ALL_CONS_COLUMNS dump 为空)。"
            ]
        )

    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_cons = ob_meta.constraints.get(tgt_key, {})

    detail_mismatch: List[str] = []
    missing: Set[str] = set()
    extra: Set[str] = set()

    def bucket_constraints(cons_dict: Dict[str, Dict]) -> Dict[str, List[Tuple[Tuple[str, ...], str]]]:
        buckets: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {'P': [], 'U': [], 'R': []}
        for name, info in cons_dict.items():
            ctype = (info.get("type") or "").upper()
            if ctype not in buckets:
                continue
            cols = tuple(info.get("columns") or [])
            buckets[ctype].append((cols, name))
        return buckets

    grouped_src = bucket_constraints(src_cons)
    grouped_tgt = bucket_constraints(tgt_cons)

    def match_constraints(label: str, src_list: List[Tuple[Tuple[str, ...], str]], tgt_list: List[Tuple[Tuple[str, ...], str]]):
        tgt_used = [False] * len(tgt_list)
        for cols, name in src_list:
            found_idx = None
            for idx, (t_cols, t_name) in enumerate(tgt_list):
                if tgt_used[idx]:
                    continue
                if cols == t_cols:
                    found_idx = idx
                    tgt_used[idx] = True
                    break
            if found_idx is None:
                missing.add(name)
                detail_mismatch.append(
                    f"{label}: 源约束 {name} (列 {list(cols)}) 在目标端未找到。"
                )
        for idx, used in enumerate(tgt_used):
            if not used:
                extra_name = tgt_list[idx][1]
                extra.add(extra_name)
                detail_mismatch.append(
                    f"{label}: 目标端存在额外约束 {extra_name} (列 {list(tgt_list[idx][0])})。"
                )

    match_constraints("PRIMARY KEY", grouped_src.get('P', []), grouped_tgt.get('P', []))
    match_constraints("UNIQUE KEY", grouped_src.get('U', []), grouped_tgt.get('U', []))
    match_constraints("FOREIGN KEY", grouped_src.get('R', []), grouped_tgt.get('R', []))

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, ConstraintMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_constraints=missing,
            extra_constraints=extra,
            detail_mismatch=detail_mismatch
        )


def compare_sequences_for_schema(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    tgt_schema: str
) -> Tuple[bool, Optional[SequenceMismatch]]:
    src_seqs = oracle_meta.sequences.get(src_schema.upper())
    if src_seqs is None:
        log.warning(f"[序列检查] 未找到 {src_schema} 的 Oracle 序列元数据。")
        return False, SequenceMismatch(
            src_schema=src_schema,
            tgt_schema=tgt_schema,
            missing_sequences=set(),
            extra_sequences=set(),
            note=f"Oracle 用户已成功查询，但在 schema {src_schema} 的 ALL_SEQUENCES 未返回任何记录，请检查该 schema 是否确实存在序列。"
        )

    tgt_seqs = ob_meta.sequences.get(tgt_schema.upper(), set())

    missing = src_seqs - tgt_seqs
    extra = tgt_seqs - src_seqs
    all_good = (not missing) and (not extra)
    if all_good:
        return True, None
    else:
        return False, SequenceMismatch(
            src_schema=src_schema,
            tgt_schema=tgt_schema,
            missing_sequences=missing,
            extra_sequences=extra,
            note=None
        )


def compare_triggers_for_table(
    oracle_meta: OracleMetadata,
    ob_meta: ObMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str
) -> Tuple[bool, Optional[TriggerMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_trg = oracle_meta.triggers.get(src_key)
    if src_trg is None:
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_triggers=set(),
            extra_triggers=set(),
            detail_mismatch=[
                "Oracle 用户已成功查询，但 ALL_TRIGGERS 在该表没有返回记录，请确认源端是否确实存在触发器。"
            ]
        )

    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_trg = ob_meta.triggers.get(tgt_key, {})

    src_names = set(src_trg.keys())
    tgt_names = set(tgt_trg.keys())

    missing = src_names - tgt_names
    extra = tgt_names - src_names
    detail_mismatch: List[str] = []

    common = src_names & tgt_names
    for name in common:
        s = src_trg[name]
        t = tgt_trg[name]
        if (s["event"] or "").strip() != (t.get("event") or "").strip():
            detail_mismatch.append(
                f"{name}: 触发事件不一致 (src={s['event']}, tgt={t.get('event')})"
            )
        if (s["status"] or "").strip() != (t.get("status") or "").strip():
            detail_mismatch.append(
                f"{name}: 状态不一致 (src={s['status']}, tgt={t.get('status')})"
            )

    all_good = (not missing) and (not extra) and (not detail_mismatch)
    if all_good:
        return True, None
    else:
        return False, TriggerMismatch(
            table=f"{tgt_schema}.{tgt_table}",
            missing_triggers=missing,
            extra_triggers=extra,
            detail_mismatch=detail_mismatch
        )


def check_extra_objects(
    settings: Dict,
    master_list: MasterCheckList,
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata
) -> ExtraCheckResults:
    """
    基于 master_list (TABLE 映射) 检查：
      - 索引
      - 约束 (PK/UK/FK)
      - 触发器
    基于 schema 映射检查：
      - 序列
    """
    extra_results: ExtraCheckResults = {
        "index_ok": [],
        "index_mismatched": [],
        "constraint_ok": [],
        "constraint_mismatched": [],
        "sequence_ok": [],
        "sequence_mismatched": [],
        "trigger_ok": [],
        "trigger_mismatched": [],
    }

    if not master_list:
        return extra_results

    log.info("--- 开始执行扩展对象校验 (索引/约束/序列/触发器) ---")

    schema_map = build_schema_mapping(master_list)

    # 1) 针对每个 TABLE 做索引/约束/触发器校验
    total_tables = sum(1 for _, _, t in master_list if t.upper() == 'TABLE')
    done_tables = 0

    for src_name, tgt_name, obj_type in master_list:
        if obj_type.upper() != 'TABLE':
            continue

        done_tables += 1
        if done_tables % 100 == 0:
            log.info(f"  扩展校验 (索引/约束/触发器) 进度: {done_tables} / {total_tables} ...")

        try:
            src_schema, src_table = src_name.split('.')
            tgt_schema, tgt_table = tgt_name.split('.')
        except ValueError:
            continue

        # 索引
        ok_idx, idx_mis = compare_indexes_for_table(
            oracle_meta, ob_meta,
            src_schema, src_table,
            tgt_schema, tgt_table
        )
        if ok_idx:
            extra_results["index_ok"].append(tgt_name)
        elif idx_mis:
            extra_results["index_mismatched"].append(idx_mis)

        # 约束
        ok_cons, cons_mis = compare_constraints_for_table(
            oracle_meta, ob_meta,
            src_schema, src_table,
            tgt_schema, tgt_table
        )
        if ok_cons:
            extra_results["constraint_ok"].append(tgt_name)
        elif cons_mis:
            extra_results["constraint_mismatched"].append(cons_mis)

        # 触发器
        ok_trg, trg_mis = compare_triggers_for_table(
            oracle_meta, ob_meta,
            src_schema, src_table,
            tgt_schema, tgt_table
        )
        if ok_trg:
            extra_results["trigger_ok"].append(tgt_name)
        elif trg_mis:
            extra_results["trigger_mismatched"].append(trg_mis)

    # 2) 按 schema 做序列校验
    for src_schema in settings['source_schemas_list']:
        src_schema_u = src_schema.upper()
        tgt_schema = schema_map.get(src_schema_u, src_schema_u)
        ok_seq, seq_mis = compare_sequences_for_schema(
            oracle_meta, ob_meta,
            src_schema_u, tgt_schema
        )
        mapping_label = f"{src_schema_u}->{tgt_schema}"
        if ok_seq:
            extra_results["sequence_ok"].append(mapping_label)
        elif seq_mis:
            extra_results["sequence_mismatched"].append(seq_mis)

    return extra_results


# ====================== DDL 抽取 & ALTER 级别修补 ======================

def setup_metadata_session(ora_conn):
    plsql = """
    BEGIN
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',TRUE);
    END;
    """
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(plsql)
    except oracledb.Error as e:
        log.warning(f"[DDL] 设置 DBMS_METADATA transform 失败: {e}")


DDL_OBJ_TYPE_MAPPING = {
    'PACKAGE BODY': 'PACKAGE_BODY'
}


def oracle_get_ddl(ora_conn, obj_type: str, owner: str, name: str) -> Optional[str]:
    sql = "SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL"
    obj_type_norm = DDL_OBJ_TYPE_MAPPING.get(obj_type.upper(), obj_type.upper())
    try:
        with ora_conn.cursor() as cursor:
            cursor.execute(sql, [obj_type_norm, name.upper(), owner.upper()])
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            return str(row[0])
    except oracledb.Error as e:
        log.warning(f"[DDL] 获取 {obj_type} {owner}.{name} DDL 失败: {e}")
        return None


def adjust_ddl_for_object(
    ddl: str,
    src_schema: str,
    src_name: str,
    tgt_schema: str,
    tgt_name: str,
    extra_identifiers: Optional[List[Tuple[Tuple[str, str], Tuple[str, str]]]] = None
) -> str:
    """
    依据 remap 结果调整 DBMS_METADATA 生成的 DDL：
      - 先替换主对象 (schema+name)
      - 再按需替换依赖对象 (如索引/触发器引用的表)
    extra_identifiers: [ ((src_schema, src_name), (tgt_schema, tgt_name)), ... ]
    """

    def replace_identifier(text: str, src_s: str, src_n: str, tgt_s: str, tgt_n: str) -> str:
        if not src_s or not src_n or not tgt_s or not tgt_n:
            return text
        src_s_u = src_s.upper()
        src_n_u = src_n.upper()
        tgt_s_u = tgt_s.upper()
        tgt_n_u = tgt_n.upper()

        pattern_quoted = re.compile(
            rf'"{re.escape(src_s_u)}"\."{re.escape(src_n_u)}"',
            re.IGNORECASE
        )
        pattern_unquoted = re.compile(
            rf'\b{re.escape(src_s_u)}\.{re.escape(src_n_u)}\b',
            re.IGNORECASE
        )

        text = pattern_quoted.sub(f'"{tgt_s_u}"."{tgt_n_u}"', text)
        text = pattern_unquoted.sub(f'{tgt_s_u}.{tgt_n_u}', text)
        return text

    result = replace_identifier(ddl, src_schema, src_name, tgt_schema, tgt_name)

    if extra_identifiers:
        for (src_pair, tgt_pair) in extra_identifiers:
            result = replace_identifier(
                result,
                src_pair[0],
                src_pair[1],
                tgt_pair[0],
                tgt_pair[1]
            )

    return result


USING_INDEX_PATTERN_WITH_OPTIONS = re.compile(
    r'USING\s+INDEX\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*(ENABLE|DISABLE)',
    re.IGNORECASE
)
USING_INDEX_PATTERN_SIMPLE = re.compile(
    r'USING\s+INDEX\s+(ENABLE|DISABLE)',
    re.IGNORECASE
)


def normalize_ddl_for_ob(ddl: str) -> str:
    """
    清理 DBMS_METADATA 的输出，使其更适合在 OceanBase (Oracle 模式) 上执行：
      - 移除 "USING INDEX ... ENABLE/DISABLE" 之类 Oracle 专有语法
    未来如有更多不兼容语法，可在此扩展。
    """
    ddl = USING_INDEX_PATTERN_WITH_OPTIONS.sub(lambda m: m.group(1), ddl)
    ddl = USING_INDEX_PATTERN_SIMPLE.sub(lambda m: m.group(1), ddl)
    return ddl


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_fixup_file(base_dir: Path, subdir: str, filename: str, content: str, header_comment: str):
    target_dir = base_dir / subdir
    ensure_dir(target_dir)
    file_path = target_dir / filename
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(f"-- {header_comment}\n")
        f.write("-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。\n\n")
        f.write(content.strip())
        if not content.strip().endswith(';'):
            f.write(';\n')
    log.info(f"[FIXUP] 生成修补脚本: {file_path}")


def format_oracle_column_type(info: Dict) -> str:
    dt = (info.get("data_type") or "").upper()
    prec = info.get("data_precision")
    scale = info.get("data_scale")
    length = info.get("data_length")
    char_len = info.get("char_length")

    if dt in ("NUMBER", "FLOAT"):
        if prec is not None:
            if scale is not None:
                return f"{dt}({int(prec)},{int(scale)})"
            else:
                return f"{dt}({int(prec)})"
        else:
            return dt

    if dt in ("CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"):
        ln = char_len or length
        if ln is not None:
            return f"{dt}({int(ln)})"
        else:
            return dt

    return dt


def generate_alter_for_table_columns(
    oracle_meta: OracleMetadata,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    missing_cols: Set[str],
    extra_cols: Set[str],
    length_mismatches: List[Tuple[str, int, int, int]]
) -> Optional[str]:
    """
    为一个具体的表生成 ALTER TABLE 脚本：
      - 对 missing_cols 生成 ADD COLUMN
      - 对 extra_cols 生成注释掉的 DROP COLUMN 建议
      - 对 length_mismatches 生成 MODIFY COLUMN
    """
    if not missing_cols and not extra_cols and not length_mismatches:
        return None

    col_details = oracle_meta.table_columns.get((src_schema.upper(), src_table.upper()))
    if col_details is None:
        log.warning(f"[ALTER] 未找到 {src_schema}.{src_table} 的列元数据，跳过 ALTER 生成。")
        return None

    lines: List[str] = []
    tgt_schema_u = tgt_schema.upper()
    tgt_table_u = tgt_table.upper()

    # 缺失列：ADD
    if missing_cols:
        lines.append(f"-- 源端存在而目标端缺失的列，将通过 ALTER TABLE ADD 补齐：")
        for col in sorted(missing_cols):
            info = col_details.get(col)
            if not info:
                lines.append(f"-- WARNING: 源端未找到列 {col} 的详细定义，需手工补充。")
                continue

            col_u = col.upper()
            col_type = format_oracle_column_type(info)
            default_clause = ""
            default_val = info.get("data_default")
            if default_val is not None:
                default_str = str(default_val).strip()
                if default_str:
                    default_clause = f" DEFAULT {default_str}"

            nullable_clause = " NOT NULL" if (info.get("nullable") == "N") else ""

            lines.append(
                f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"ADD ({col_u} {col_type}{default_clause}{nullable_clause});"
            )
    
    # 长度不匹配：MODIFY
    if length_mismatches:
        lines.append("")
        lines.append("-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：")
        for col_name, src_len, tgt_len, expected_len in length_mismatches:
            info = col_details.get(col_name)
            if not info:
                continue
            
            # 在 OB 中，VARCHAR2 等同于 VARCHAR
            modified_type = format_oracle_column_type(info).replace("VARCHAR2", "VARCHAR").replace(f"({src_len})", f"({expected_len})")

            lines.append(
                f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"MODIFY ({col_name.upper()} {modified_type}); "
                f"-- 源长度: {src_len}, 目标长度: {tgt_len}, 期望长度: {expected_len}"
            )

    # 多余列：DROP（注释掉，供人工评估）
    if extra_cols:
        lines.append("")
        lines.append("-- 目标端存在而源端不存在的列，以下 DROP COLUMN 为建议操作，请谨慎执行：")
        for col in sorted(extra_cols):
            col_u = col.upper()
            lines.append(
                f"-- ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                f"DROP COLUMN {col_u};"
            )

    return "\n".join(lines) if lines else None


def generate_fixup_scripts(
    ora_cfg: OraConfig,
    settings: Dict,
    tv_results: ReportResults,
    extra_results: ExtraCheckResults,
    master_list: MasterCheckList,
    oracle_meta: OracleMetadata,
    remap_rules: RemapRules
):
    """
    基于校验结果生成 fix_up DDL 脚本，并按依赖顺序排列：
      1. SEQUENCE
      2. TABLE (CREATE)
      3. TABLE (ALTER - for column diffs)
      4. VIEW
      5. INDEX
      6. CONSTRAINT
      7. TRIGGER
      8. PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM
    """
    base_dir = Path(settings.get('fixup_dir', 'fix_up'))

    if not master_list:
        log.info("[FIXUP] master_list 为空，未生成修补脚本。")
        return

    base_dir = Path(settings.get('fixup_dir', 'fix_up'))
    ensure_dir(base_dir)
    log.info(f"[FIXUP] 修补脚本将生成到目录: {base_dir.resolve()}")

    table_map: Dict[str, str] = {
        tgt_name: src_name
        for (src_name, tgt_name, obj_type) in master_list
        if obj_type.upper() == 'TABLE'
    }
    object_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    table_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    for src_name, tgt_name, obj_type in master_list:
        try:
            src_schema, src_object = src_name.split('.')
            tgt_schema, tgt_object = tgt_name.split('.')
        except ValueError:
            continue
        object_replacements.append(
            ((src_schema.upper(), src_object.upper()), (tgt_schema.upper(), tgt_object.upper()))
        )
        if obj_type.upper() == 'TABLE':
            table_replacements.append(
                ((src_schema.upper(), src_object.upper()), (tgt_schema.upper(), tgt_object.upper()))
            )
    remap_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    for src, tgt in remap_rules.items():
        try:
            src_schema, src_obj = src.split('.')
            tgt_schema, tgt_obj = tgt.split('.')
        except ValueError:
            continue
        remap_replacements.append(
            ((src_schema.upper(), src_obj.upper()), (tgt_schema.upper(), tgt_obj.upper()))
        )
    all_replacements = list(object_replacements)
    replacement_set = {
        (pair[0][0], pair[0][1], pair[1][0], pair[1][1]) for pair in object_replacements
    }
    for pair in remap_replacements:
        key = (pair[0][0], pair[0][1], pair[1][0], pair[1][1])
        if key not in replacement_set:
            all_replacements.append(pair)
            replacement_set.add(key)
    schema_map = build_schema_mapping(master_list)
    obj_type_to_dir = {
        'TABLE': 'table',
        'VIEW': 'view',
        'PROCEDURE': 'procedure',
        'FUNCTION': 'function',
        'PACKAGE': 'package',
        'PACKAGE BODY': 'package_body',
        'SYNONYM': 'synonym',
    }

    try:
        with oracledb.connect(
            user=ora_cfg['user'], password=ora_cfg['password'], dsn=ora_cfg['dsn']
        ) as ora_conn:
            setup_metadata_session(ora_conn)
            log.info("[FIXUP] 按依赖顺序开始生成脚本...")

            # 顺序 1: SEQUENCE 缺失
            log.info("[FIXUP] (1/8) 正在生成 SEQUENCE 脚本...")
            for seq_mis in extra_results.get('sequence_mismatched', []):
                src_schema = seq_mis.src_schema.upper()
                tgt_schema = seq_mis.tgt_schema.upper()
                for seq_name in seq_mis.missing_sequences:
                    ddl = oracle_get_ddl(ora_conn, 'SEQUENCE', src_schema, seq_name)
                    if ddl:
                        ddl_adj = adjust_ddl_for_object(
                            ddl, src_schema, seq_name, tgt_schema, seq_name,
                            extra_identifiers=all_replacements
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        filename = f"{tgt_schema}.{seq_name}.sql"
                        header = f"修补缺失的 SEQUENCE {tgt_schema}.{seq_name} (源: {src_schema}.{seq_name})"
                        write_fixup_file(base_dir, 'sequence', filename, ddl_adj, header)

            # 顺序 2: 主对象缺失 (TABLE, VIEW, PROC, etc.)
            log.info("[FIXUP] (2/8) 正在生成缺失的 TABLE CREATE 脚本...")
            missing_main_objects = tv_results.get('missing', [])
            for (obj_type, tgt_name, src_name) in missing_main_objects:
                if obj_type.upper() != 'TABLE': continue
                src_schema, src_obj = src_name.split('.')
                tgt_schema, tgt_obj = tgt_name.split('.')
                ddl = oracle_get_ddl(ora_conn, 'TABLE', src_schema, src_obj)
                if ddl:
                    ddl_adj = adjust_ddl_for_object(
                        ddl, src_schema, src_obj, tgt_schema, tgt_obj,
                        extra_identifiers=all_replacements
                    )
                    ddl_adj = normalize_ddl_for_ob(ddl_adj)
                    filename = f"{tgt_schema}.{tgt_obj}.sql"
                    header = f"修补缺失的 TABLE {tgt_schema}.{tgt_obj} (源: {src_schema}.{src_obj})"
                    write_fixup_file(base_dir, 'table', filename, ddl_adj, header)

            # 顺序 3: TABLE 列不匹配 (ALTER)
            log.info("[FIXUP] (3/8) 正在生成 TABLE ALTER 脚本...")
            for (obj_type, tgt_name, missing_cols, extra_cols, length_mismatches) in tv_results.get('mismatched', []):
                if obj_type.upper() != 'TABLE' or "获取失败" in tgt_name: continue
                src_name = table_map.get(tgt_name)
                if not src_name: continue
                src_schema, src_table = src_name.split('.')
                tgt_schema, tgt_table = tgt_name.split('.')
                alter_sql = generate_alter_for_table_columns(
                    oracle_meta, src_schema, src_table, tgt_schema, tgt_table,
                    missing_cols, extra_cols, length_mismatches
                )
                if alter_sql:
                    filename = f"{tgt_schema}.{tgt_table}.alter_columns.sql"
                    header = f"基于列差异的 ALTER TABLE 修补脚本: {tgt_schema}.{tgt_table} (源: {src_schema}.{src_table})"
                    write_fixup_file(base_dir, 'table_alter', filename, alter_sql, header)

            # 顺序 4: VIEW 缺失
            log.info("[FIXUP] (4/8) 正在生成缺失的 VIEW CREATE 脚本...")
            for (obj_type, tgt_name, src_name) in missing_main_objects:
                if obj_type.upper() != 'VIEW': continue
                src_schema, src_obj = src_name.split('.')
                tgt_schema, tgt_obj = tgt_name.split('.')
                ddl = oracle_get_ddl(ora_conn, 'VIEW', src_schema, src_obj)
                if ddl:
                    ddl_adj = adjust_ddl_for_object(
                        ddl, src_schema, src_obj, tgt_schema, tgt_obj,
                        extra_identifiers=all_replacements
                    )
                    ddl_adj = normalize_ddl_for_ob(ddl_adj)
                    filename = f"{tgt_schema}.{tgt_obj}.sql"
                    header = f"修补缺失的 VIEW {tgt_schema}.{tgt_obj} (源: {src_schema}.{src_obj})"
                    write_fixup_file(base_dir, 'view', filename, ddl_adj, header)

            # 顺序 5: INDEX 缺失
            log.info("[FIXUP] (5/8) 正在生成 INDEX 脚本...")
            for item in extra_results.get('index_mismatched', []):
                table_str = item.table.split()[0]
                if '.' not in table_str: continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name: continue
                src_schema, src_table = src_name.split('.')
                for idx_name in item.missing_indexes:
                    ddl = oracle_get_ddl(ora_conn, 'INDEX', src_schema, idx_name)
                    if ddl:
                        extra_ids = all_replacements + [
                            ((src_schema.upper(), src_table.upper()), (tgt_schema.upper(), tgt_table.upper()))
                        ]
                        ddl_adj = adjust_ddl_for_object(
                            ddl,
                            src_schema,
                            idx_name,
                            tgt_schema,
                            idx_name,
                            extra_identifiers=extra_ids
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        filename = f"{tgt_schema}.{idx_name}.sql"
                        header = f"修补缺失的 INDEX {idx_name} (表: {tgt_schema}.{tgt_table})"
                        write_fixup_file(base_dir, 'index', filename, ddl_adj, header)

            # 顺序 6: CONSTRAINT 缺失
            log.info("[FIXUP] (6/8) 正在生成 CONSTRAINT 脚本...")
            for item in extra_results.get('constraint_mismatched', []):
                table_str = item.table.split()[0]
                if '.' not in table_str: continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name: continue
                src_schema, src_table = src_name.split('.')
                constraint_meta = oracle_meta.constraints.get(
                    (src_schema.upper(), src_table.upper()), {}
                )
                for cons_name in item.missing_constraints:
                    obj_type_for_ddl = 'CONSTRAINT'
                    cons_info = constraint_meta.get(cons_name.upper())
                    if cons_info:
                        ctype = (cons_info.get("type") or "").upper()
                        if ctype == 'R':
                            obj_type_for_ddl = 'REF_CONSTRAINT'
                    else:
                        log.warning(
                            f"[DDL] 未在缓存中找到约束 {cons_name} 的类型，默认按 CONSTRAINT 获取 DDL。"
                        )
                    ddl = oracle_get_ddl(ora_conn, obj_type_for_ddl, src_schema, cons_name)
                    if ddl:
                        ddl_adj = adjust_ddl_for_object(
                            ddl,
                            src_schema,
                            cons_name,
                            tgt_schema,
                            cons_name,
                            extra_identifiers=all_replacements
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        if obj_type_for_ddl == 'REF_CONSTRAINT':
                            fk_match = re.search(
                                r'REFERENCES\s+"([^"]+)"\."([^"]+)"',
                                ddl_adj,
                                flags=re.IGNORECASE
                            )
                            if fk_match:
                                parent_schema = fk_match.group(1)
                                parent_table = fk_match.group(2)
                                grant_note = (
                                    f"\n\n-- 提示: 外键引用对象需要授权，"
                                    f"执行以下语句：\n"
                                    f"-- GRANT REFERENCES ON {parent_schema}.{parent_table} "
                                    f"TO {tgt_schema.upper()};"
                                )
                                ddl_adj = ddl_adj.rstrip() + grant_note
                        filename = f"{tgt_schema}.{cons_name}.sql"
                        header = f"修补缺失的约束 {cons_name} (表: {tgt_schema}.{tgt_table})"
                        write_fixup_file(base_dir, 'constraint', filename, ddl_adj, header)

            # 顺序 7: TRIGGER 缺失
            log.info("[FIXUP] (7/8) 正在生成 TRIGGER 脚本...")
            for item in extra_results.get('trigger_mismatched', []):
                table_str = item.table.split()[0]
                if '.' not in table_str: continue
                tgt_schema, tgt_table = table_str.split('.', 1)
                src_name = table_map.get(table_str)
                if not src_name: continue
                src_schema, src_table = src_name.split('.')
                for trg_name in item.missing_triggers:
                    ddl = oracle_get_ddl(ora_conn, 'TRIGGER', src_schema, trg_name)
                    if ddl:
                        extra_ids = all_replacements + [
                            ((src_schema.upper(), src_table.upper()), (tgt_schema.upper(), tgt_table.upper()))
                        ]
                        ddl_adj = adjust_ddl_for_object(
                            ddl,
                            src_schema,
                            trg_name,
                            tgt_schema,
                            trg_name,
                            extra_identifiers=extra_ids
                        )
                        ddl_adj = normalize_ddl_for_ob(ddl_adj)
                        filename = f"{tgt_schema}.{trg_name}.sql"
                        header = f"修补缺失的触发器 {trg_name} (表: {tgt_schema}.{tgt_table})"
                        write_fixup_file(base_dir, 'trigger', filename, ddl_adj, header)

            # 顺序 8: 其他代码对象 (PROCEDURE, FUNCTION, PACKAGE, SYNONYM)
            log.info("[FIXUP] (8/8) 正在生成其余代码对象脚本...")
            for (obj_type, tgt_name, src_name) in missing_main_objects:
                obj_type_u = obj_type.upper()
                if obj_type_u not in ('PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'SYNONYM'):
                    continue
                src_schema, src_obj = src_name.split('.')
                tgt_schema, tgt_obj = tgt_name.split('.')
                ddl = oracle_get_ddl(ora_conn, obj_type_u, src_schema, src_obj)
                if ddl:
                    ddl_adj = adjust_ddl_for_object(
                        ddl,
                        src_schema,
                        src_obj,
                        tgt_schema,
                        tgt_obj,
                        extra_identifiers=all_replacements
                    )
                    ddl_adj = normalize_ddl_for_ob(ddl_adj)
                    subdir = obj_type_to_dir[obj_type_u]
                    filename = f"{tgt_schema}.{tgt_obj}.sql"
                    header = f"修补缺失的 {obj_type_u} {tgt_schema}.{tgt_obj} (源: {src_schema}.{src_obj})"
                    write_fixup_file(base_dir, subdir, filename, ddl_adj, header)

    except oracledb.Error as e:
        log.error(f"[FIXUP] 生成修补脚本时 Oracle 连接出错: {e}")


# ====================== 报告输出 (Rich) ======================
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.theme import Theme
except ImportError:
    print("错误: 未找到 'rich' 库。", file=sys.stderr)
    print("请先安装: pip install rich", file=sys.stderr)
    sys.exit(1)

def print_final_report(
    tv_results: ReportResults,
    total_checked: int,
    extra_results: Optional[ExtraCheckResults] = None
):
    custom_theme = Theme({
        "ok": "green",
        "missing": "red",
        "mismatch": "yellow",
        "info": "cyan",
        "header": "bold magenta",
        "title": "bold white on blue"
    })
    console = Console(theme=custom_theme)

    if extra_results is None:
        extra_results = {
            "index_ok": [], "index_mismatched": [], "constraint_ok": [],
            "constraint_mismatched": [], "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }

    log.info("所有校验已完成。正在生成最终报告...")

    ok_count = len(tv_results['ok'])
    missing_count = len(tv_results['missing'])
    mismatched_count = len(tv_results['mismatched'])
    extraneous_count = len(tv_results['extraneous'])
    idx_ok_cnt = len(extra_results.get("index_ok", []))
    idx_mis_cnt = len(extra_results.get("index_mismatched", []))
    cons_ok_cnt = len(extra_results.get("constraint_ok", []))
    cons_mis_cnt = len(extra_results.get("constraint_mismatched", []))
    seq_ok_cnt = len(extra_results.get("sequence_ok", []))
    seq_mis_cnt = len(extra_results.get("sequence_mismatched", []))
    trg_ok_cnt = len(extra_results.get("trigger_ok", []))
    trg_mis_cnt = len(extra_results.get("trigger_mismatched", []))

    console.print(Panel.fit("[bold]数据库对象迁移校验报告 (V0.2 - Rich)[/bold]", style="title"))

    # --- 综合概要 ---
    summary_table = Table(title="[header]综合概要", show_header=False, box=None)
    summary_table.add_column("Category")
    summary_table.add_column("Details")

    primary_text = Text()
    primary_text.append(f"总计校验对象 (来自源库): {total_checked}\n")
    primary_text.append("一致: ", style="ok")
    primary_text.append(f"{ok_count}\n")
    primary_text.append("缺失: ", style="missing")
    primary_text.append(f"{missing_count}\n")
    primary_text.append("不匹配 (表列/长度): ", style="mismatch")
    primary_text.append(f"{mismatched_count}\n")
    primary_text.append("无效规则: ", style="mismatch")
    primary_text.append(f"{extraneous_count}")
    summary_table.add_row("[bold]主对象 (TABLE/VIEW/etc.)[/bold]", primary_text)

    ext_text = Text()
    ext_text.append("索引: ", style="info")
    ext_text.append(f"一致 {idx_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {idx_mis_cnt}\n", style="mismatch")
    ext_text.append("约束: ", style="info")
    ext_text.append(f"一致 {cons_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {cons_mis_cnt}\n", style="mismatch")
    ext_text.append("序列: ", style="info")
    ext_text.append(f"一致 {seq_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {seq_mis_cnt}\n", style="mismatch")
    ext_text.append("触发器: ", style="info")
    ext_text.append(f"一致 {trg_ok_cnt} / ", style="ok")
    ext_text.append(f"差异 {trg_mis_cnt}", style="mismatch")
    summary_table.add_row("[bold]扩展对象 (INDEX/SEQ/etc.)[/bold]", ext_text)
    console.print(summary_table)

    TYPE_COL_WIDTH = 16
    OBJECT_COL_WIDTH = 42
    DETAIL_COL_WIDTH = 90

    def summarize_actions() -> Panel:
        modify_counts = OrderedDict()
        modify_counts["TABLE (列差异修补)"] = len(tv_results.get('mismatched', []))

        addition_counts: Dict[str, int] = defaultdict(int)
        for obj_type, _, _ in tv_results.get('missing', []):
            addition_counts[obj_type.upper()] += 1
        for item in extra_results.get("index_mismatched", []):
            addition_counts["INDEX"] += len(item.missing_indexes)
        for item in extra_results.get("constraint_mismatched", []):
            addition_counts["CONSTRAINT"] += len(item.missing_constraints)
        for item in extra_results.get("sequence_mismatched", []):
            addition_counts["SEQUENCE"] += len(item.missing_sequences)
        for item in extra_results.get("trigger_mismatched", []):
            addition_counts["TRIGGER"] += len(item.missing_triggers)

        def format_block(title: str, data: OrderedDict) -> str:
            lines = [f"[bold]{title}[/bold]"]
            entries = [(k, v) for k, v in data.items() if v > 0]
            if not entries:
                lines.append("  - 无")
            else:
                for k, v in entries:
                    lines.append(f"  - {k}: {v}")
            return "\n".join(lines)

        def format_add_block(title: str, data_map: Dict[str, int]) -> str:
            lines = [f"[bold]{title}[/bold]"]
            entries = [(k, v) for k, v in sorted(data_map.items()) if v > 0]
            if not entries:
                lines.append("  - 无")
            else:
                for k, v in entries:
                    lines.append(f"  - {k}: {v}")
            return "\n".join(lines)

        text = "\n\n".join([
            format_block("需要在目标端修改的对象", modify_counts),
            format_add_block("需要在目标端新增的对象", addition_counts)
        ])
        return Panel.fit(text, title="[info]执行摘要", border_style="info")

    console.print(summarize_actions())

    # --- 1. 缺失的主对象 ---
    if tv_results['missing']:
        table = Table(title=f"[header]1. 缺失的主对象 (共 {missing_count} 个)", expand=True)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("目标对象 (应存在)", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("源对象", style="info", width=OBJECT_COL_WIDTH)
        for obj_type, tgt_name, src_name in tv_results['missing']:
            table.add_row(f"[{obj_type}]", tgt_name, src_name)
        console.print(table)

    # --- 2. 列不匹配的表 ---
    if tv_results['mismatched']:
        table = Table(title=f"[header]2. 不匹配的表 (共 {mismatched_count} 个)", expand=True)
        table.add_column("表名", style="info", width=OBJECT_COL_WIDTH)
        table.add_column("差异详情", width=DETAIL_COL_WIDTH)
        for obj_type, tgt_name, missing, extra, length_mismatches in tv_results['mismatched']:
            details = Text()
            if "获取失败" in tgt_name:
                details.append(f"源端列信息获取失败", style="missing")
            else:
                if missing:
                    details.append(f"- 缺失列: {sorted(list(missing))}\n", style="missing")
                if extra:
                    details.append(f"+ 多余列: {sorted(list(extra))}\n", style="mismatch")
                if length_mismatches:
                    details.append("* 长度不匹配 (VARCHAR/2):\n", style="mismatch")
                    for col, src_len, tgt_len, exp_len in length_mismatches:
                        details.append(f"    - {col}: 源={src_len}, 目标={tgt_len}, 期望={exp_len}\n")
            table.add_row(tgt_name, details)
        console.print(table)

    # --- 3. 扩展对象差异 ---
    def print_ext_mismatch_table(title, items, headers, render_func):
        if not items: return
        table = Table(title=f"[header]{title} (共 {len(items)} 项差异)", expand=True)
        table.add_column(headers[0], style="info", width=OBJECT_COL_WIDTH)
        table.add_column(headers[1], width=DETAIL_COL_WIDTH)
        for item in items:
            table.add_row(*render_func(item))
        console.print(table)

    print_ext_mismatch_table(
        "5. 索引一致性检查", extra_results["index_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            Text(f"- 缺失: {sorted(item.missing_indexes)}\n" if item.missing_indexes else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_indexes)}\n" if item.extra_indexes else "", style="mismatch") +
            Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )
    print_ext_mismatch_table(
        "6. 约束 (PK/UK/FK) 一致性检查", extra_results["constraint_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            Text(f"- 缺失: {sorted(item.missing_constraints)}\n" if item.missing_constraints else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_constraints)}\n" if item.extra_constraints else "", style="mismatch") +
            Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )
    print_ext_mismatch_table(
        "7. 序列 (SEQUENCE) 一致性检查", extra_results["sequence_mismatched"], ["Schema 映射", "差异详情"],
        lambda item: (
            Text(f"{item.src_schema}->{item.tgt_schema}"),
            Text(f"- 缺失: {sorted(item.missing_sequences)}\n" if item.missing_sequences else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_sequences)}\n" if item.extra_sequences else "", style="missing") +
            (Text(f"* {item.note}\n", style="missing") if item.note else Text(""))
        )
    )
    print_ext_mismatch_table(
        "8. 触发器 (TRIGGER) 一致性检查", extra_results["trigger_mismatched"], ["表名", "差异详情"],
        lambda item: (
            Text(item.table),
            Text(f"- 缺失: {sorted(item.missing_triggers)}\n" if item.missing_triggers else "", style="missing") +
            Text(f"+ 多余: {sorted(item.extra_triggers)}\n" if item.extra_triggers else "", style="mismatch") +
            Text('\n'.join([f"* {d}" for d in item.detail_mismatch]))
        )
    )

    # --- 4. 无效 Remap 规则 ---
    if tv_results['extraneous']:
        table = Table(title=f"[header]4. 无效的 Remap 规则 (共 {extraneous_count} 个)", expand=True)
        table.add_column("在 remap_rules.txt 中定义, 但在源端 Oracle 中未找到的对象", style="info", width=OBJECT_COL_WIDTH)
        for item in tv_results['extraneous']:
            table.add_row(item, style="mismatch")
        console.print(table)

    # --- 提示 ---
    fixup_panel = Panel.fit(
        "[bold]Fixup 脚本生成目录[/bold]\n\n"
        "fix_up/table         : 缺失 TABLE 的 CREATE 脚本\n"
        "fix_up/view          : 缺失 VIEW 的 CREATE 脚本\n"
        "fix_up/procedure     : 缺失 PROCEDURE 的 CREATE 脚本\n"
        "fix_up/function      : 缺失 FUNCTION 的 CREATE 脚本\n"
        "fix_up/package       : 缺失 PACKAGE 的 CREATE 脚本\n"
        "fix_up/package_body  : 缺失 PACKAGE BODY 的 CREATE 脚本\n"
        "fix_up/synonym       : 缺失 SYNONYM 的 CREATE 脚本\n"
        "fix_up/index         : 缺失 INDEX 的 CREATE 脚本\n"
        "fix_up/constraint    : 缺失约束的 CREATE 脚本\n"
        "fix_up/sequence      : 缺失 SEQUENCE 的 CREATE 脚本\n"
        "fix_up/trigger       : 缺失 TRIGGER 的 CREATE 脚本\n"
        "fix_up/table_alter   : 列不匹配 TABLE 的 ALTER 修补脚本\n\n"
        "[bold]请在 OceanBase 执行前逐一人工审核上述脚本。[/bold]",
        title="[info]提示",
        border_style="info"
    )
    console.print(fixup_panel)
    console.print(Panel.fit("[bold]报告结束[/bold]", style="title"))


# ====================== 主函数 ======================

def main():
    """主执行函数"""
    CONFIG_FILE = 'db.ini'

    # 1) 加载配置
    ora_cfg, ob_cfg, settings = load_config(CONFIG_FILE)

    # 2) 加载 Remap 规则
    remap_rules = load_remap_rules(settings['remap_file'])

    # 3) 加载源端主对象 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM)
    source_objects = get_source_objects(ora_cfg, settings['source_schemas_list'])

    # 4) 验证 Remap 规则
    extraneous_rules = validate_remap_rules(remap_rules, source_objects)

    # 5) 生成主校验清单
    master_list = generate_master_list(source_objects, remap_rules)

    if not master_list:
        log.info("主校验清单为空，程序结束。")
        tv_results: ReportResults = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "extraneous": extraneous_rules
        }
        extra_results: ExtraCheckResults = {
            "index_ok": [],
            "index_mismatched": [],
            "constraint_ok": [],
            "constraint_mismatched": [],
            "sequence_ok": [],
            "sequence_mismatched": [],
            "trigger_ok": [],
            "trigger_mismatched": [],
        }
        print_final_report(tv_results, 0, extra_results)
        return

    # 6) 计算目标端 schema 集合并一次性 dump OB 元数据
    target_schemas: Set[str] = set()
    for _, tgt_name, _ in master_list:
        try:
            schema, _ = tgt_name.split('.')
            target_schemas.add(schema.upper())
        except ValueError:
            continue

    ob_meta = dump_ob_metadata(ob_cfg, target_schemas)

    # 7) 主对象校验
    oracle_meta = dump_oracle_metadata(ora_cfg, master_list, settings)

    tv_results = check_primary_objects(
        master_list,
        extraneous_rules,
        ob_meta,
        oracle_meta
    )

    # 8) 扩展对象校验 (索引/约束/序列/触发器)
    extra_results = check_extra_objects(settings, master_list, ob_meta, oracle_meta)

    # 9) 生成修补脚本
    generate_fixup_scripts(ora_cfg, settings, tv_results, extra_results, master_list, oracle_meta, remap_rules)

    # 10) 输出最终报告
    print_final_report(tv_results, len(master_list), extra_results)


if __name__ == "__main__":
    main()
