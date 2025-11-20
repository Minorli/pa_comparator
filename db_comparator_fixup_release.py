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
   - TABLE：只对比“列名集合”，忽略 OceanBase 目标端自动生成的 4 个 OMS 列
     (OMS_OBJECT_NUMBER/OMS_RELATIVE_FNO/OMS_BLOCK_NUMBER/OMS_ROW_NUMBER)，不对比数据类型/长度。
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
import math
import re
import os
import uuid
import shutil
from collections import defaultdict, OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional, NamedTuple, Callable

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
SourceObjectMap = Dict[str, Set[str]]  # {'OWNER.OBJ': {'TYPE1', 'TYPE2'}}
FullObjectMapping = Dict[str, Dict[str, str]]  # {'OWNER.OBJ': {'TYPE': 'TGT_OWNER.OBJ'}}
MasterCheckList = List[Tuple[str, str, str]]  # [(src_name, tgt_name, type)]
ReportResults = Dict[str, List]
ObjectCountSummary = Dict[str, Dict[str, int]]

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


def normalize_column_sequence(columns: Optional[List[str]]) -> Tuple[str, ...]:
    if not columns:
        return tuple()
    seen: Set[str] = set()
    normalized: List[str] = []
    for col in columns:
        col_u = (col or '').upper()
        if not col_u:
            continue
        if col_u in seen:
            continue
        seen.add(col_u)
        normalized.append(col_u)
    return tuple(normalized)

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


class DependencyRecord(NamedTuple):
    owner: str
    name: str
    object_type: str
    referenced_owner: str
    referenced_name: str
    referenced_type: str


class DependencyIssue(NamedTuple):
    dependent: str
    dependent_type: str
    referenced: str
    referenced_type: str
    reason: str


DependencyReport = Dict[str, List[DependencyIssue]]


class ColumnLengthIssue(NamedTuple):
    column: str
    src_length: int
    tgt_length: int
    limit_length: int  # 下限或上限（根据 issue 标识）
    issue: str         # 'short' | 'oversize'


PRIMARY_OBJECT_TYPES: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'MATERIALIZED VIEW',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'SYNONYM',
    'JOB',
    'SCHEDULE',
    'TYPE',
    'TYPE BODY'
)

# 主对象中除 TABLE 外均做存在性验证
PRIMARY_EXISTENCE_ONLY_TYPES: Tuple[str, ...] = tuple(
    obj for obj in PRIMARY_OBJECT_TYPES if obj != 'TABLE'
)

# 额外纳入 remap/依赖但不做列级主检查的对象
DEPENDENCY_EXTRA_OBJECT_TYPES: Tuple[str, ...] = (
    'TRIGGER',
    'SEQUENCE',
    'INDEX'
)

ALL_TRACKED_OBJECT_TYPES: Tuple[str, ...] = tuple(
    sorted(set(PRIMARY_OBJECT_TYPES) | set(DEPENDENCY_EXTRA_OBJECT_TYPES))
)

# OceanBase 目标端自动生成且需在列对比中忽略的 OMS 列
IGNORED_OMS_COLUMNS: Tuple[str, ...] = (
    "OMS_OBJECT_NUMBER",
    "OMS_RELATIVE_FNO",
    "OMS_BLOCK_NUMBER",
    "OMS_ROW_NUMBER",
)

VARCHAR_LEN_MIN_MULTIPLIER = 1.5  # 目标端 VARCHAR/2 长度需 >= ceil(src * 1.5)
VARCHAR_LEN_OVERSIZE_MULTIPLIER = 2.5  # 超过该倍数认为“过大”，需要提示

OBJECT_COUNT_TYPES: Tuple[str, ...] = (
    'TABLE',
    'VIEW',
    'SYNONYM',
    'TRIGGER',
    'SEQUENCE',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY'
)

GRANT_PRIVILEGE_BY_TYPE: Dict[str, str] = {
    'TABLE': 'SELECT',
    'VIEW': 'SELECT',
    'MATERIALIZED VIEW': 'SELECT',
    'SYNONYM': 'SELECT',
    'SEQUENCE': 'SELECT',
    'TYPE': 'EXECUTE',
    'TYPE BODY': 'EXECUTE',
    'PROCEDURE': 'EXECUTE',
    'FUNCTION': 'EXECUTE',
    'PACKAGE': 'EXECUTE',
    'PACKAGE BODY': 'EXECUTE'
}

DDL_OBJECT_TYPE_OVERRIDE: Dict[str, Tuple[str, bool]] = {
    'PROCEDURE': ('PROCEDURE', True),
    'FUNCTION': ('FUNCTION', True),
    'PACKAGE': ('PACKAGE', True),
    'PACKAGE BODY': ('PACKAGE BODY', True),
    'TRIGGER': ('TRIGGER', True)
}
DBCAT_OPTION_MAP: Dict[str, str] = {
    'TABLE': '--table',
    'VIEW': '--view',
    'MATERIALIZED VIEW': '--mview',
    'PROCEDURE': '--procedure',
    'FUNCTION': '--function',
    'PACKAGE': '--package',
    'PACKAGE BODY': '--package-body',
    'SYNONYM': '--synonym',
    'SEQUENCE': '--sequence',
    'TRIGGER': '--trigger',
    'TYPE': '--type',
    'TYPE BODY': '--type-body',
    'MVIEW LOG': '--mview-log',
    'TABLEGROUP': '--tablegroup'
}

DBCAT_OUTPUT_DIR_HINTS: Dict[str, Tuple[str, ...]] = {
    'TABLE': ('TABLE',),
    'VIEW': ('VIEW',),
    'MATERIALIZED VIEW': ('MVIEW', 'MATERIALIZED VIEW'),
    'PROCEDURE': ('PROCEDURE',),
    'FUNCTION': ('FUNCTION',),
    'PACKAGE': ('PACKAGE',),
    'PACKAGE BODY': ('PACKAGE BODY', 'PACKAGE_BODY'),
    'SYNONYM': ('SYNONYM',),
    'SEQUENCE': ('SEQUENCE',),
    'TRIGGER': ('TRIGGER',),
    'TYPE': ('TYPE',),
    'TYPE BODY': ('TYPE BODY', 'TYPE_BODY'),
    'MVIEW LOG': ('MVIEW LOG', 'MVIEW_LOG'),
    'TABLEGROUP': ('TABLEGROUP',)
}

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
        # 报告输出目录
        settings.setdefault('report_dir', 'reports')
        # Oracle Instant Client 目录 (Thick Mode)
        settings.setdefault('oracle_client_lib_dir', '')
        # dbcat 相关配置
        settings.setdefault('dbcat_bin', '')
        settings.setdefault('dbcat_from', '')
        settings.setdefault('dbcat_to', '')
        settings.setdefault('dbcat_output_dir', 'history/dbcat_output')
        settings.setdefault('java_home', os.environ.get('JAVA_HOME', ''))

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


def init_oracle_client_from_settings(settings: Dict) -> None:
    """根据配置初始化 Oracle Thick Mode 并提示环境变量设置。"""
    client_dir = settings.get('oracle_client_lib_dir', '').strip()
    if not client_dir:
        log.error("严重错误: 未在 [SETTINGS] 中配置 oracle_client_lib_dir。")
        log.error("请在 db.ini 中添加例如: oracle_client_lib_dir = /home/user/instantclient_19_28")
        sys.exit(1)

    client_path = Path(client_dir).expanduser()
    if not client_path.exists():
        log.error(f"严重错误: 指定的 Oracle Instant Client 目录不存在: {client_path}")
        sys.exit(1)

    ld_path = os.environ.get('LD_LIBRARY_PATH') or '<未设置>'
    log.info(f"准备使用 Oracle Instant Client 目录: {client_path}")
    log.info("如遇 libnnz19.so 等库缺失，请先执行:")
    log.info(f"  export LD_LIBRARY_PATH=\"{client_path}:${{LD_LIBRARY_PATH}}\"")
    log.info(f"当前 LD_LIBRARY_PATH: {ld_path}")

    try:
        oracledb.init_oracle_client(lib_dir=str(client_path))
    except Exception as exc:
        log.error("严重错误: Oracle Thick Mode 初始化失败。")
        log.error("请确认 instant client 路径和 LD_LIBRARY_PATH 设置正确。")
        log.error(f"错误详情: {exc}")
        sys.exit(1)


def get_source_objects(ora_cfg: OraConfig, schemas_list: List[str]) -> SourceObjectMap:
    """
    从 Oracle 源端获取所有需要纳入 remap/依赖分析的对象：
      TABLE / VIEW / MATERIALIZED VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY /
      SYNONYM / JOB / SCHEDULE / TYPE / TYPE BODY / TRIGGER / SEQUENCE / INDEX
    """
    log.info(f"正在连接 Oracle 源端: {ora_cfg['dsn']}...")

    placeholders = ','.join([f":{i+1}" for i in range(len(schemas_list))])
    object_types_clause = ",".join(f"'{obj}'" for obj in ALL_TRACKED_OBJECT_TYPES)

    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OWNER IN ({placeholders})
          AND OBJECT_TYPE IN (
              {object_types_clause}
          )
    """

    source_objects: SourceObjectMap = defaultdict(set)
    mview_pairs: Set[Tuple[str, str]] = set()
    table_pairs: Set[Tuple[str, str]] = set()

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
                    owner = (row[0] or '').strip().upper()
                    obj_name = (row[1] or '').strip().upper()
                    obj_type = (row[2] or '').strip().upper()
                    if not owner or not obj_name or not obj_type:
                        continue
                    full_name = f"{owner}.{obj_name}"
                    source_objects[full_name].add(obj_type)
            # 精确认定物化视图集合，避免误删真实表
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT OWNER, MVIEW_NAME FROM ALL_MVIEWS WHERE OWNER IN ({placeholders})",
                    schemas_list
                )
                for row in cursor:
                    owner = (row[0] or '').strip().upper()
                    name = (row[1] or '').strip().upper()
                    if owner and name:
                        mview_pairs.add((owner, name))
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER IN ({placeholders})",
                    schemas_list
                )
                for row in cursor:
                    owner = (row[0] or '').strip().upper()
                    name = (row[1] or '').strip().upper()
                    if owner and name:
                        table_pairs.add((owner, name))
    except oracledb.Error as e:
        log.error(f"严重错误: 连接或查询 Oracle 失败: {e}")
        sys.exit(1)

    # Materialized View 在 ALL_OBJECTS 中通常会同时作为 TABLE 出现，去重以避免误将 MV 当成 TABLE 校验/抽取。
    mview_dedup = 0
    pure_tables = table_pairs - mview_pairs  # ALL_TABLES 也包含 MVIEW，这里只保留真实 TABLE
    for full_name, types in source_objects.items():
        if 'MATERIALIZED VIEW' in types and 'TABLE' in types:
            try:
                owner, name = full_name.split('.', 1)
            except ValueError:
                continue
            key = (owner.upper(), name.upper())
            # 只有确定该对象存在于 ALL_MVIEWS 且不在“纯表”列表时，才移除 TABLE 标记
            if key in mview_pairs and key not in pure_tables:
                types.discard('TABLE')
                mview_dedup += 1
    if mview_dedup:
        log.info(
            "检测到 %d 个 MATERIALIZED VIEW 同时出现在 TABLE 列表中，已移除重复的 TABLE 类型以使用 --mview 处理。",
            mview_dedup
        )

    total_objects = sum(len(types) for types in source_objects.values())
    log.info(
        "从 Oracle 成功获取 %d 个受管对象 (包含主对象与扩展对象)。",
        total_objects
    )
    return dict(source_objects)


def validate_remap_rules(remap_rules: RemapRules, source_objects: SourceObjectMap) -> List[str]:
    """检查 remap 规则中的源对象是否存在于 Oracle source_objects 中。"""
    log.info("正在验证 Remap 规则...")
    remap_keys = set(remap_rules.keys())
    source_keys = set(source_objects.keys())
    body_aliases = {
        f"{name} BODY"
        for name, obj_types in source_objects.items()
        if any(obj_type.upper() == 'PACKAGE BODY' for obj_type in obj_types)
    }
    source_keys_with_alias = source_keys | body_aliases

    extraneous_keys = sorted(list(remap_keys - source_keys_with_alias))

    if extraneous_keys:
        log.warning(f"  [规则警告] 在 remap_rules.txt 中发现了 {len(extraneous_keys)} 个无效的源对象。")
        log.warning("  (这些对象在源端 Oracle (db.ini 中配置的 schema) 中未找到)")
        for key in extraneous_keys:
            log.warning(f"    - 无效条目: {key}")
    else:
        log.info("Remap 规则验证通过，所有规则中的源对象均存在。")

    return extraneous_keys


def strip_body_suffix(name: str) -> str:
    text = name.rstrip()
    if text.upper().endswith(' BODY'):
        return text[:-5].rstrip()
    return text


def resolve_remap_target(
    src_name: str,
    obj_type: str,
    remap_rules: RemapRules
) -> Optional[str]:
    obj_type_u = obj_type.upper()
    candidate_keys: List[str] = [src_name]
    if obj_type_u == 'PACKAGE BODY':
        candidate_keys.insert(0, f"{src_name} BODY")
    for key in candidate_keys:
        if key in remap_rules:
            tgt = remap_rules[key].strip()
            if obj_type_u == 'PACKAGE BODY':
                return strip_body_suffix(tgt)
            return tgt
    return None


def generate_master_list(source_objects: SourceObjectMap, remap_rules: RemapRules) -> MasterCheckList:
    """
    生成“最终校验清单”并检测 "多对一" 映射。
    仅保留 PRIMARY_OBJECT_TYPES 中的主对象，用于主校验。
    """
    log.info("正在生成主校验清单 (应用 Remap 规则)...")
    master_list: MasterCheckList = []

    target_tracker: Dict[Tuple[str, str], str] = {}

    for src_name, obj_types in source_objects.items():
        src_name_u = src_name.upper()
        for obj_type in sorted(obj_types):
            obj_type_u = obj_type.upper()
            if obj_type_u not in PRIMARY_OBJECT_TYPES:
                continue

            tgt_name = resolve_remap_target(src_name_u, obj_type_u, remap_rules) or src_name_u
            tgt_name_u = tgt_name.upper()

            key = (tgt_name_u, obj_type_u)
            if key in target_tracker:
                existing_src = target_tracker[key]
                log.error(f"{'='*80}")
                log.error(f"                 !!! 致命配置错误 !!!")
                log.error(f"发现“多对一”映射。同一个目标对象 '{tgt_name_u}' (类型 {obj_type_u}) 被映射了多次：")
                log.error(f"  1. 源: '{existing_src}' -> 目标: '{tgt_name_u}'")
                log.error(f"  2. 源: '{src_name_u}' -> 目标: '{tgt_name_u}'")
                log.error("这会导致校验逻辑混乱。请检查您的 remap_rules.txt 文件，")
                log.error("确保每一个目标对象只被一个源对象所映射。")
                log.error(f"{'='*80}")
                sys.exit(1)

            target_tracker[key] = src_name_u
            master_list.append((src_name_u, tgt_name_u, obj_type_u))

    log.info(f"主校验清单生成完毕，共 {len(master_list)} 个待校验项。")
    return master_list


def build_full_object_mapping(source_objects: SourceObjectMap, remap_rules: RemapRules) -> FullObjectMapping:
    """
    为所有受管对象建立映射 (源 -> 目标)。
    返回 {'SRC.OBJ': {'TYPE': 'TGT.OBJ'}}
    """
    mapping: FullObjectMapping = {}
    for src_name, obj_types in source_objects.items():
        src_name_u = src_name.upper()
        for obj_type in obj_types:
            obj_type_u = obj_type.upper()
            tgt_name = resolve_remap_target(src_name_u, obj_type_u, remap_rules) or src_name_u
            mapping.setdefault(src_name_u, {})[obj_type_u] = tgt_name.upper()
    return mapping


def get_mapped_target(
    full_object_mapping: FullObjectMapping,
    src_full_name: str,
    obj_type: str
) -> Optional[str]:
    src_key = src_full_name.upper()
    obj_type_u = obj_type.upper()
    type_map = full_object_mapping.get(src_key)
    if not type_map:
        return None
    return type_map.get(obj_type_u)


def ensure_mapping_entry(
    full_object_mapping: FullObjectMapping,
    src_full_name: str,
    obj_type: str,
    tgt_full_name: str
) -> None:
    src_key = src_full_name.upper()
    obj_type_u = obj_type.upper()
    tgt_full = tgt_full_name.upper()
    full_object_mapping.setdefault(src_key, {})[obj_type_u] = tgt_full


def find_source_by_target(
    full_object_mapping: FullObjectMapping,
    tgt_full_name: str,
    obj_type: str
) -> Optional[str]:
    obj_type_u = obj_type.upper()
    tgt_u = tgt_full_name.upper()
    for src_name, type_map in full_object_mapping.items():
        target = type_map.get(obj_type_u)
        if target and target.upper() == tgt_u:
            return src_name
    return None


def compute_object_counts(
    source_objects: SourceObjectMap,
    ob_meta: ObMetadata,
    monitored_types: Tuple[str, ...] = OBJECT_COUNT_TYPES
) -> ObjectCountSummary:
    oracle_counts: Dict[str, int] = {t.upper(): 0 for t in monitored_types}
    for obj_types in source_objects.values():
        for obj_type in obj_types:
            obj_type_u = obj_type.upper()
            if obj_type_u in oracle_counts:
                oracle_counts[obj_type_u] += 1

    ob_counts: Dict[str, int] = {
        t: len(ob_meta.objects_by_type.get(t.upper(), set()))
        for t in oracle_counts.keys()
    }

    mismatches = [t for t in oracle_counts if oracle_counts[t] != ob_counts[t]]
    if mismatches:
        log.warning(
            "检查汇总: 以下对象类型数量在 Oracle 与 OceanBase 中不一致，将在报告中重点标注: %s",
            ", ".join(mismatches)
        )
    else:
        log.info("检查汇总: 所有关注对象类型数量在 Oracle 与 OceanBase 中一致。")

    return {
        "oracle": oracle_counts,
        "oceanbase": ob_counts
    }


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
    object_types_clause = ",".join(f"'{obj}'" for obj in ALL_TRACKED_OBJECT_TYPES)

    sql = f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OWNER IN ({owners_in})
          AND OBJECT_TYPE IN (
              {object_types_clause}
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

    # 补充 ALL_TYPES (部分 OB 环境中 TYPE/TYPE BODY 不出现在 ALL_OBJECTS)
    sql_types = f"""
        SELECT OWNER, TYPE_NAME, TYPECODE
        FROM ALL_TYPES
        WHERE OWNER IN ({owners_in})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql_types)
    if not ok:
        log.warning("读取 ALL_TYPES 失败，TYPE / TYPE BODY 检查可能不完整: %s", err)
    elif out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            owner, name, typecode = parts[0].strip().upper(), parts[1].strip().upper(), parts[2].strip().upper()
            full = f"{owner}.{name}"
            objects_by_type.setdefault('TYPE', set()).add(full)
            if typecode == 'OBJECT':
                objects_by_type.setdefault('TYPE BODY', set()).add(full)

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

    def _safe_upper(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return value.upper()
        except AttributeError:
            return str(value).upper()

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
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        col = _safe_upper(row[2])
                        if not owner or not table or not col:
                            continue
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
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        if not owner or not table:
                            continue
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        idx_name = _safe_upper(row[2])
                        if not idx_name:
                            continue
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
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        if not owner or not table:
                            continue
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        idx_name = _safe_upper(row[2])
                        col_name = _safe_upper(row[3])
                        if not idx_name or not col_name:
                            continue
                        indexes.setdefault(key, {}).setdefault(
                            idx_name, {"uniqueness": "UNKNOWN", "columns": []}
                        )["columns"].append(col_name)

                # 约束
                sql_cons = f"""
                    SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, R_OWNER, R_CONSTRAINT_NAME
                    FROM ALL_CONSTRAINTS
                    WHERE OWNER IN ({owners_clause})
                      AND CONSTRAINT_TYPE IN ('P','U','R')
                      AND STATUS = 'ENABLED'
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_cons, owners)
                    for row in cursor:
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        if not owner or not table:
                            continue
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        name = _safe_upper(row[2])
                        if not name:
                            continue
                        constraints.setdefault(key, {})[name] = {
                            "type": (row[3] or "").upper(),
                            "columns": [],
                            "r_owner": _safe_upper(row[4]) if row[4] else None,
                            "r_constraint": _safe_upper(row[5]) if row[5] else None,
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
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        if not owner or not table:
                            continue
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        cons_name = _safe_upper(row[2])
                        col_name = _safe_upper(row[3])
                        if not cons_name or not col_name:
                            continue
                        constraints.setdefault(key, {}).setdefault(
                            cons_name, {"type": "UNKNOWN", "columns": []}
                        )["columns"].append(col_name)

                # 为外键补齐被引用表信息 (基于约束引用)
                cons_table_lookup: Dict[Tuple[str, str], Tuple[str, str]] = {}
                for (owner, table), cons_map in constraints.items():
                    for cons_name, info in cons_map.items():
                        ctype = (info.get("type") or "").upper()
                        if ctype in ('P', 'U'):
                            cons_table_lookup[(owner, cons_name)] = (owner, table)
                for (owner, _), cons_map in constraints.items():
                    for cons_name, info in cons_map.items():
                        ctype = (info.get("type") or "").upper()
                        if ctype != 'R':
                            continue
                        r_owner = (info.get("r_owner") or "").upper()
                        r_cons = (info.get("r_constraint") or "").upper()
                        if not r_owner or not r_cons:
                            continue
                        ref_table = cons_table_lookup.get((r_owner, r_cons))
                        if ref_table:
                            info["ref_table_owner"], info["ref_table_name"] = ref_table

                # 触发器
                sql_trg = f"""
                    SELECT TABLE_OWNER, TABLE_NAME, TRIGGER_NAME, TRIGGERING_EVENT, STATUS
                    FROM ALL_TRIGGERS
                    WHERE TABLE_OWNER IN ({owners_clause})
                """
                with ora_conn.cursor() as cursor:
                    cursor.execute(sql_trg, owners)
                    for row in cursor:
                        owner = _safe_upper(row[0])
                        table = _safe_upper(row[1])
                        if not owner or not table:
                            continue
                        key = (owner, table)
                        if key not in table_pairs:
                            continue
                        trg_name = _safe_upper(row[2])
                        if not trg_name:
                            continue
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
                        owner = _safe_upper(row[0])
                        seq_name = _safe_upper(row[1])
                        if not owner or not seq_name:
                            continue
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


def load_oracle_dependencies(
    ora_cfg: OraConfig,
    schemas_list: List[str]
) -> List[DependencyRecord]:
    """
    从 Oracle 批量读取源 schema 内部的依赖关系。
    """
    if not schemas_list:
        return []

    owners_clause = ','.join([f":{i+1}" for i in range(len(schemas_list))])
    types_clause = ",".join(f"'{t}'" for t in ALL_TRACKED_OBJECT_TYPES)

    sql = f"""
        SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
        FROM ALL_DEPENDENCIES
        WHERE OWNER IN ({owners_clause})
          AND REFERENCED_OWNER IN ({owners_clause})
          AND TYPE IN ({types_clause})
          AND REFERENCED_TYPE IN ({types_clause})
    """

    records: List[DependencyRecord] = []
    try:
        with oracledb.connect(
            user=ora_cfg['user'],
            password=ora_cfg['password'],
            dsn=ora_cfg['dsn']
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, schemas_list)
                for row in cursor:
                    owner = (row[0] or '').strip().upper()
                    name = (row[1] or '').strip().upper()
                    obj_type = (row[2] or '').strip().upper()
                    ref_owner = (row[3] or '').strip().upper()
                    ref_name = (row[4] or '').strip().upper()
                    ref_type = (row[5] or '').strip().upper()
                    if not owner or not name or not ref_owner or not ref_name:
                        continue
                    records.append(DependencyRecord(
                        owner=owner,
                        name=name,
                        object_type=obj_type,
                        referenced_owner=ref_owner,
                        referenced_name=ref_name,
                        referenced_type=ref_type
                    ))
    except oracledb.Error as exc:
        log.error(f"严重错误: 加载 Oracle 依赖信息失败: {exc}")
        sys.exit(1)

    log.info("Oracle 依赖信息加载完成，共 %d 条记录。", len(records))
    return records


def load_ob_dependencies(ob_cfg: ObConfig, target_schemas: Set[str]) -> Set[Tuple[str, str, str, str]]:
    """
    通过 obclient 读取 OceanBase 侧的依赖信息。
    返回集合 { (OWNER.OBJ, TYPE, REF_OWNER.OBJ, REF_TYPE) }
    """
    if not target_schemas:
        return set()

    owners_in = ",".join(f"'{s}'" for s in sorted(target_schemas))
    types_clause = ",".join(f"'{t}'" for t in ALL_TRACKED_OBJECT_TYPES)

    sql = f"""
        SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
        FROM ALL_DEPENDENCIES
        WHERE OWNER IN ({owners_in})
          AND REFERENCED_OWNER IN ({owners_in})
          AND TYPE IN ({types_clause})
          AND REFERENCED_TYPE IN ({types_clause})
    """
    ok, out, err = obclient_run_sql(ob_cfg, sql)
    if not ok:
        log.error("无法从 OB 读取 ALL_DEPENDENCIES，程序退出。")
        sys.exit(1)

    result: Set[Tuple[str, str, str, str]] = set()
    if out:
        for line in out.splitlines():
            parts = line.split('\t')
            if len(parts) < 6:
                continue
            owner = parts[0].strip().upper()
            name = parts[1].strip().upper()
            obj_type = parts[2].strip().upper()
            ref_owner = parts[3].strip().upper()
            ref_name = parts[4].strip().upper()
            ref_type = parts[5].strip().upper()
            if not owner or not name or not ref_owner or not ref_name:
                continue
            result.add((
                f"{owner}.{name}",
                obj_type,
                f"{ref_owner}.{ref_name}",
                ref_type
            ))

    log.info("OceanBase 依赖信息加载完成，共 %d 条记录。", len(result))
    return result


def build_expected_dependency_pairs(
    dependencies: List[DependencyRecord],
    full_mapping: FullObjectMapping
) -> Tuple[Set[Tuple[str, str, str, str]], List[DependencyIssue]]:
    """
    将源端依赖映射到目标端 (schema/object 名已替换)。
    返回 (期望依赖集合, 被跳过的依赖列表)。
    """
    expected: Set[Tuple[str, str, str, str]] = set()
    skipped: List[DependencyIssue] = []

    for dep in dependencies:
        dep_key = f"{dep.owner}.{dep.name}".upper()
        ref_key = f"{dep.referenced_owner}.{dep.referenced_name}".upper()
        dep_target = get_mapped_target(full_mapping, dep_key, dep.object_type)
        ref_target = get_mapped_target(full_mapping, ref_key, dep.referenced_type)

        if dep_target is None:
            skipped.append(DependencyIssue(
                dependent=dep_key,
                dependent_type=dep.object_type.upper(),
                referenced=ref_key,
                referenced_type=dep.referenced_type.upper(),
                reason="源对象未纳入受管范围或缺少 remap 规则，无法建立依赖。"
            ))
            continue
        if ref_target is None:
            skipped.append(DependencyIssue(
                dependent=dep_key,
                dependent_type=dep.object_type.upper(),
                referenced=ref_key,
                referenced_type=dep.referenced_type.upper(),
                reason="被依赖对象未纳入受管范围或缺少 remap 规则，无法建立依赖。"
            ))
            continue

        expected.add((
            dep_target.upper(),
            dep.object_type.upper(),
            ref_target.upper(),
            dep.referenced_type.upper()
        ))

    return expected, skipped


def check_dependencies_against_ob(
    expected_pairs: Set[Tuple[str, str, str, str]],
    actual_pairs: Set[Tuple[str, str, str, str]],
    skipped: List[DependencyIssue],
    ob_meta: ObMetadata
) -> DependencyReport:
    """
    对比目标端依赖关系，返回缺失/多余/跳过的依赖项。
    """
    report: DependencyReport = {
        "missing": [],
        "unexpected": [],
        "skipped": skipped
    }

    def object_exists(full_name: str, obj_type: str) -> bool:
        return full_name in ob_meta.objects_by_type.get(obj_type.upper(), set())

    def build_missing_reason(dep_name: str, dep_type: str, ref_name: str, ref_type: str) -> str:
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        dep_schema = dep_name.split('.', 1)[0]
        ref_schema = ref_name.split('.', 1)[0]
        cross_schema_note = ""
        if dep_schema != ref_schema:
            cross_schema_note = " 跨 schema 依赖，请确认 remap 后的授权（SELECT/EXECUTE/REFERENCES）或同义词已就绪。"

        if dep_type in {"FUNCTION", "PROCEDURE"}:
            action = (
                f"依赖关系未建立：在 OceanBase 执行 ALTER {dep_type} {dep_name} COMPILE；"
                f"如仍失败，请检查 {dep_obj} 中对 {ref_obj} 的调用及授权/Remap。"
            )
        elif dep_type in {"PACKAGE", "PACKAGE BODY"}:
            action = (
                f"依赖关系未建立：执行 ALTER PACKAGE {dep_name} COMPILE 及 ALTER PACKAGE {dep_name} COMPILE BODY，"
                f"确认包定义能够访问 {ref_obj}。"
            )
        elif dep_type == "TRIGGER":
            action = (
                f"依赖关系未建立：执行 ALTER TRIGGER {dep_name} COMPILE，"
                f"确认触发器引用的对象 {ref_obj} 已存在且可访问。"
            )
        elif dep_type in {"VIEW", "MATERIALIZED VIEW"}:
            action = (
                f"依赖关系未建立：请 CREATE OR REPLACE {dep_type} {dep_name}，"
                f"确保所有底层对象（如 {ref_obj}）已存在，再执行 ALTER {dep_type} {dep_name} COMPILE。"
            )
        elif dep_type == "SYNONYM":
            action = (
                f"依赖关系未建立：请重新创建同义词（CREATE OR REPLACE SYNONYM {dep_name} FOR {ref_name}），"
                f"确认 remap 目标和授权正确。"
            )
        elif dep_type in {"TYPE", "TYPE BODY"}:
            compile_stmt = f"ALTER TYPE {dep_name} COMPILE{' BODY' if dep_type == 'TYPE BODY' else ''}"
            action = (
                f"依赖关系未建立：先创建/校验 TYPE 定义，再执行 {compile_stmt}，"
                f"确保 {ref_obj} 已存在且可访问。"
            )
        elif dep_type == "INDEX":
            action = (
                f"依赖关系未建立：请重建索引 {dep_obj}，"
                f"检查索引表达式或函数中对 {ref_obj} 的引用是否有效。"
            )
        elif dep_type == "SEQUENCE":
            action = (
                f"依赖关系未建立：请重新创建序列 {dep_obj}，"
                f"检查同义词或授权设置是否让 {ref_obj} 可见。"
            )
        else:
            action = (
                f"依赖关系未建立：请重新部署 {dep_obj}，"
                f"确认定义中对 {ref_obj} 的引用与 remap/授权保持一致。"
            )

        return action + cross_schema_note

    missing_pairs = expected_pairs - actual_pairs
    extra_pairs = actual_pairs - expected_pairs

    for dep_name, dep_type, ref_name, ref_type in sorted(missing_pairs):
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        if not object_exists(dep_name, dep_type):
            reason = f"依赖对象 {dep_obj} 在目标端缺失，请补齐该对象后再重新编译依赖。"
        elif not object_exists(ref_name, ref_type):
            reason = f"被依赖对象 {ref_obj} 在目标端缺失，请先创建/迁移该对象，再重新部署 {dep_obj}。"
        else:
            reason = build_missing_reason(dep_name, dep_type, ref_name, ref_type)
        report["missing"].append(DependencyIssue(
            dependent=dep_name,
            dependent_type=dep_type,
            referenced=ref_name,
            referenced_type=ref_type,
            reason=reason
        ))

    for dep_name, dep_type, ref_name, ref_type in sorted(extra_pairs):
        dep_obj = f"{dep_name} ({dep_type})"
        ref_obj = f"{ref_name} ({ref_type})"
        reason = (
            f"OceanBase 中存在额外依赖 {dep_obj} -> {ref_obj}，"
            f"请确认是否需要保留或清理。"
        )
        report["unexpected"].append(DependencyIssue(
            dependent=dep_name,
            dependent_type=dep_type,
            referenced=ref_name,
            referenced_type=ref_type,
            reason=reason
        ))

    return report


def compute_required_grants(
    expected_pairs: Set[Tuple[str, str, str, str]]
) -> Dict[str, Set[Tuple[str, str]]]:
    grants: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)

    for dep_full, dep_type, ref_full, ref_type in expected_pairs:
        dep_schema, _ = dep_full.split('.', 1)
        ref_schema, _ = ref_full.split('.', 1)
        if dep_schema == ref_schema:
            continue
        privilege = GRANT_PRIVILEGE_BY_TYPE.get(ref_type.upper())
        if not privilege:
            continue
        grants[dep_schema].add((privilege, ref_full))
        # 对外键依赖的表补充 REFERENCES 权限，便于创建 FK
        if ref_type.upper() == 'TABLE' and dep_type.upper() == 'TABLE':
            grants[dep_schema].add(('REFERENCES', ref_full))

    return grants


# ====================== TABLE / VIEW / 其他主对象校验 ======================

def check_primary_objects(
    master_list: MasterCheckList,
    extraneous_rules: List[str],
    ob_meta: ObMetadata,
    oracle_meta: OracleMetadata
) -> ReportResults:
    """
    核心主对象校验：
      - TABLE: 存在性 + 列名集合 (忽略 OMS_OBJECT_NUMBER/OMS_RELATIVE_FNO/OMS_BLOCK_NUMBER/OMS_ROW_NUMBER)
      - VIEW / PROCEDURE / FUNCTION / PACKAGE / PACKAGE BODY / SYNONYM: 只校验存在性
    """
    results: ReportResults = {
        "missing": [],
        "mismatched": [],
        "ok": [],
        "extraneous": extraneous_rules,
        "extra_targets": []
    }

    if not master_list:
        log.info("主校验清单为空，没有需要校验的对象。")
        return results

    log.info("--- 开始执行主对象批量验证 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM) ---")

    total = len(master_list)
    expected_targets: Dict[str, Set[str]] = defaultdict(set)
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
        expected_targets[obj_type_u].add(full_tgt)

        if obj_type_u == 'TABLE':
            # 1) OB 是否存在 TABLE
            ob_tables = ob_meta.objects_by_type.get('TABLE', set())
            if full_tgt not in ob_tables:
                results['missing'].append(('TABLE', full_tgt, src_name))
                continue

            # 2) 列级别详细对比 (VARCHAR/VARCHAR2 需 >= 源端长度 * 1.5 向上取整)
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

            ignored_oms = set(IGNORED_OMS_COLUMNS)
            src_col_names = {col for col in src_cols_details.keys() if col.upper() not in ignored_oms}
            tgt_col_names_raw = set(tgt_cols_details.keys())
            tgt_col_names = {col for col in tgt_col_names_raw if col.upper() not in ignored_oms}

            missing_in_tgt = src_col_names - tgt_col_names
            extra_in_tgt = tgt_col_names - src_col_names
            length_mismatches: List[ColumnLengthIssue] = []

            # 检查公共列的长度
            common_cols = src_col_names & tgt_col_names
            for col_name in common_cols:
                src_info = src_cols_details[col_name]
                tgt_info = tgt_cols_details[col_name]

                src_dtype = (src_info.get("data_type") or "").upper()

                if src_dtype in ('VARCHAR2', 'VARCHAR'):
                    src_len = src_info.get("char_length") or src_info.get("data_length")
                    tgt_len = tgt_info.get("char_length") or tgt_info.get("data_length")

                    try:
                        src_len_int = int(src_len)
                        tgt_len_int = int(tgt_len)
                    except (TypeError, ValueError):
                        continue

                    expected_min_len = int(math.ceil(src_len_int * VARCHAR_LEN_MIN_MULTIPLIER))
                    oversize_cap_len = int(math.ceil(src_len_int * VARCHAR_LEN_OVERSIZE_MULTIPLIER))
                    if tgt_len_int < expected_min_len:
                        length_mismatches.append(
                            ColumnLengthIssue(col_name, src_len_int, tgt_len_int, expected_min_len, 'short')
                        )
                    elif tgt_len_int > oversize_cap_len:
                        length_mismatches.append(
                            ColumnLengthIssue(col_name, src_len_int, tgt_len_int, oversize_cap_len, 'oversize')
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

        elif obj_type_u in PRIMARY_EXISTENCE_ONLY_TYPES:
            ob_set = ob_meta.objects_by_type.get(obj_type_u, set())
            if full_tgt in ob_set:
                results['ok'].append((obj_type_u, full_tgt))
            else:
                results['missing'].append((obj_type_u, full_tgt, src_name))

        else:
            # 不在主对比范围的类型直接忽略
            continue

    # 记录目标端多出的对象（任何受管类型）
    for obj_type in sorted(PRIMARY_OBJECT_TYPES):
        actual = ob_meta.objects_by_type.get(obj_type, set())
        expected = expected_targets.get(obj_type, set())
        extras = sorted(actual - expected)
        for tgt in extras:
            results['extra_targets'].append((obj_type, tgt))

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
    tgt_constraints = ob_meta.constraints.get(tgt_key, {})
    constraint_index_cols: Set[Tuple[str, ...]] = {
        normalize_column_sequence(cons.get("columns"))
        for cons in tgt_constraints.values()
        if (cons.get("type") or "").upper() in ("P", "U")
    }

    def build_index_map(entries: Dict[str, Dict]) -> Dict[Tuple[str, ...], Dict[str, Set[str]]]:
        result: Dict[Tuple[str, ...], Dict[str, Set[str]]] = {}
        for name, info in entries.items():
            cols = normalize_column_sequence(info.get("columns"))
            if not cols:
                continue
            uniq = (info.get("uniqueness") or "").upper()
            bucket = result.setdefault(cols, {"names": set(), "uniq": set()})
            bucket["names"].add(name)
            bucket["uniq"].add(uniq)
        return result

    src_map = build_index_map(src_idx)
    tgt_map = build_index_map(tgt_idx)

    def rep_name(entry_map: Dict[Tuple[str, ...], Dict[str, Set[str]]], cols: Tuple[str, ...]) -> str:
        names = entry_map.get(cols, {}).get("names") or []
        return next(iter(names), f"{cols}")

    missing_cols = set(src_map.keys()) - set(tgt_map.keys())
    extra_cols = set(tgt_map.keys()) - set(src_map.keys())

    detail_mismatch: List[str] = []

    for cols in set(src_map.keys()) & set(tgt_map.keys()):
        src_uniq = src_map[cols]["uniq"]
        tgt_uniq = tgt_map[cols]["uniq"]
        if src_uniq != tgt_uniq:
            detail_mismatch.append(
                f"索引列 {list(cols)} 唯一性不一致 (源 {sorted(src_uniq)}, 目标 {sorted(tgt_uniq)})。"
            )

    filtered_missing_cols: Set[Tuple[str, ...]] = set()
    for cols in missing_cols:
        # 如果已有 PK/UK 约束覆盖了同一列集，则视为已有唯一性支持，不再要求单独索引
        if cols in constraint_index_cols:
            continue
        filtered_missing_cols.add(cols)

    missing = {rep_name(src_map, cols) for cols in filtered_missing_cols}
    extra = {rep_name(tgt_map, cols) for cols in extra_cols}

    for cols in filtered_missing_cols:
        detail_mismatch.append(
            f"索引列 {list(cols)} 在目标端未找到。"
        )

    for cols in extra_cols:
        detail_mismatch.append(
            f"目标端存在额外索引列集 {list(cols)}。"
        )

    all_good = (not missing) and (not extra) and not detail_mismatch
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

    source_all_cols: Set[Tuple[str, ...]] = {
        normalize_column_sequence(cons.get("columns"))
        for cons in src_cons.values()
    }

    def bucket_constraints(cons_dict: Dict[str, Dict]) -> Dict[str, List[Tuple[Tuple[str, ...], str]]]:
        buckets: Dict[str, List[Tuple[Tuple[str, ...], str]]] = {'P': [], 'U': [], 'R': []}
        for name, cons in cons_dict.items():
            ctype = (cons.get("type") or "").upper()
            if ctype not in buckets:
                continue
            cols = normalize_column_sequence(cons.get("columns"))
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
                extra_cols = tgt_list[idx][0]
                if extra_cols in source_all_cols:
                    continue
                extra.add(extra_name)
                detail_mismatch.append(
                    f"{label}: 目标端存在额外约束 {extra_name} (列 {list(extra_cols)})。"
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
    tgt_table: str,
    full_object_mapping: FullObjectMapping
) -> Tuple[bool, Optional[TriggerMismatch]]:
    src_key = (src_schema.upper(), src_table.upper())
    src_trg = oracle_meta.triggers.get(src_key)
    if not src_trg:
        # 源端未记录任何触发器，视为无需校验，避免把“缺元数据”计入差异
        return False, None

    tgt_key = (tgt_schema.upper(), tgt_table.upper())
    tgt_trg = ob_meta.triggers.get(tgt_key, {})

    src_names_raw = set(src_trg.keys())
    tgt_names = set(tgt_trg.keys())

    src_names: Set[str] = set()
    for name in src_names_raw:
        full = f"{src_schema.upper()}.{name.upper()}"
        mapped = get_mapped_target(full_object_mapping, full, 'TRIGGER')
        if mapped and '.' in mapped:
            _, tgt_name = mapped.split('.', 1)
            src_names.add(tgt_name.upper())
        else:
            src_names.add(name.upper())
            ensure_mapping_entry(
                full_object_mapping,
                full,
                'TRIGGER',
                f"{tgt_schema.upper()}.{name.upper()}"
            )

    missing = src_names - tgt_names
    extra = tgt_names - src_names
    detail_mismatch: List[str] = []

    common = src_names & tgt_names
    for name in common:
        mapped_source = find_source_by_target(
            full_object_mapping,
            f"{tgt_schema.upper()}.{name}",
            'TRIGGER'
        )
        src_info_name = name
        if mapped_source and '.' in mapped_source:
            _, src_info_name = mapped_source.split('.', 1)
        s = src_trg.get(src_info_name) or {}
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
    oracle_meta: OracleMetadata,
    full_object_mapping: FullObjectMapping
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
            tgt_schema, tgt_table,
            full_object_mapping
        )
        if ok_trg:
            extra_results["trigger_ok"].append(tgt_name)
        elif trg_mis:
            extra_results["trigger_mismatched"].append(trg_mis)

    # 2) 序列校验（考虑 remap 后的目标 schema）
    sequence_groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for src_schema, seq_names in oracle_meta.sequences.items():
        src_schema_u = src_schema.upper()
        for seq_name in seq_names:
            seq_name_u = seq_name.upper()
            src_full = f"{src_schema_u}.{seq_name_u}"
            mapped = get_mapped_target(full_object_mapping, src_full, 'SEQUENCE')
            tgt_full = mapped or src_full
            if '.' not in tgt_full:
                tgt_schema_u = src_schema_u
                tgt_name_u = seq_name_u
            else:
                tgt_schema_u, tgt_name_u = tgt_full.split('.', 1)
                tgt_schema_u = tgt_schema_u.upper()
                tgt_name_u = tgt_name_u.upper()
            sequence_groups[(src_schema_u, tgt_schema_u)].append((seq_name_u, tgt_name_u))

    for (src_schema_u, tgt_schema_u), entries in sequence_groups.items():
        expected_tgt_names = {tgt_name for _, tgt_name in entries}
        actual_tgt_names = {name.upper() for name in ob_meta.sequences.get(tgt_schema_u, set())}

        missing_src = {
            src_name for src_name, tgt_name in entries
            if tgt_name not in actual_tgt_names
        }
        extra_tgt = actual_tgt_names - expected_tgt_names

        mapping_label = f"{src_schema_u}->{tgt_schema_u}"
        if not missing_src and not extra_tgt:
            extra_results["sequence_ok"].append(mapping_label)
        else:
            extra_results["sequence_mismatched"].append(SequenceMismatch(
                src_schema=src_schema_u,
                tgt_schema=tgt_schema_u,
                missing_sequences=missing_src,
                extra_sequences=extra_tgt,
                note=None
            ))

    return extra_results


# ====================== DDL 抽取 & ALTER 级别修补 ======================

def parse_oracle_dsn(dsn: str) -> Tuple[str, str, Optional[str]]:
    try:
        host_port, service = dsn.split('/', 1)
        host, port = host_port.split(':', 1)
        return host.strip(), port.strip(), service.strip()
    except ValueError:
        log.error("严重错误: 无法解析 Oracle DSN (host:port/service_name): %s", dsn)
        sys.exit(1)


def resolve_dbcat_cli(settings: Dict) -> Path:
    bin_path = settings.get('dbcat_bin', '').strip()
    if not bin_path:
        log.error("严重错误: 未配置 dbcat_bin，请在 db.ini 的 [SETTINGS] 中指定 dbcat 目录。")
        sys.exit(1)
    cli_path = Path(bin_path)
    if cli_path.is_dir():
        cli_path = cli_path / 'bin' / 'dbcat'
    if not cli_path.exists():
        log.error("严重错误: 找不到 dbcat 可执行文件: %s", cli_path)
        sys.exit(1)
    return cli_path


def locate_dbcat_schema_dir(base_dir: Path, schema: str) -> Optional[Path]:
    schema_upper = schema.upper()
    if base_dir.name.upper().startswith(f"{schema_upper}_"):
        return base_dir
    direct = base_dir / schema
    if direct.exists():
        return direct
    for child in base_dir.iterdir():
        if not child.is_dir():
            continue
        if child.name.upper().startswith(f"{schema_upper}_"):
            return child
        candidate = child / schema
        if candidate.exists():
            return candidate
    return None


def find_dbcat_object_file(schema_dir: Path, object_type: str, object_name: str) -> Optional[Path]:
    name_upper = object_name.upper()
    hints = DBCAT_OUTPUT_DIR_HINTS.get(object_type.upper(), ())
    for hint in hints:
        candidate = schema_dir / hint / f"{name_upper}-schema.sql"
        if candidate.exists():
            return candidate
    matches = list(schema_dir.rglob(f"{name_upper}-schema.sql"))
    if matches:
        hint_upper = tuple(h.upper() for h in hints if h)
        for candidate in matches:
            parent_names = {parent.name.upper() for parent in candidate.parents}
            if hint_upper:
                if any(h in parent_names for h in hint_upper):
                    return candidate
            else:
                return candidate
    return None


def load_cached_dbcat_results(
    base_output: Path,
    schema_requests: Dict[str, Dict[str, Set[str]]],
    accumulator: Dict[str, Dict[str, Dict[str, str]]]
) -> None:
    if not base_output.exists():
        return

    run_dirs = sorted(
        [d for d in base_output.iterdir() if d.is_dir()],
        key=lambda p: p.name,
        reverse=True
    )

    for run_dir in run_dirs:
        if not schema_requests:
            break
        for schema in list(schema_requests.keys()):
            schema_dir = locate_dbcat_schema_dir(run_dir, schema)
            if not schema_dir or not schema_dir.exists():
                continue
            type_map = schema_requests[schema]
            for obj_type in list(type_map.keys()):
                names = type_map[obj_type]
                satisfied: Set[str] = set()
                for name in list(names):
                    file_path = find_dbcat_object_file(schema_dir, obj_type, name)
                    if not file_path or not file_path.exists():
                        continue
                    try:
                        ddl_text = file_path.read_text('utf-8')
                    except OSError:
                        continue
                    accumulator.setdefault(schema, {}).setdefault(obj_type, {})[name] = ddl_text
                    satisfied.add(name)
                names -= satisfied
                if not names:
                    del type_map[obj_type]
            if not type_map:
                del schema_requests[schema]


def fetch_dbcat_schema_objects(
    ora_cfg: OraConfig,
    settings: Dict,
    schema_requests: Dict[str, Dict[str, Set[str]]]
) -> Dict[str, Dict[str, Dict[str, str]]]:
    results: Dict[str, Dict[str, Dict[str, str]]] = {}
    if not schema_requests:
        return results

    base_output = Path(settings.get('dbcat_output_dir', 'history/dbcat_output'))
    ensure_dir(base_output)
    load_cached_dbcat_results(base_output, schema_requests, results)

    if not schema_requests:
        return results

    host, port, service = parse_oracle_dsn(ora_cfg['dsn'])
    dbcat_cli = resolve_dbcat_cli(settings)
    java_home = settings.get('java_home') or os.environ.get('JAVA_HOME')
    if not java_home:
        log.error("严重错误: 需要 JAVA_HOME 才能运行 dbcat，请在环境或 db.ini 中配置。")
        sys.exit(1)

    for schema in list(schema_requests.keys()):
        type_map = schema_requests.get(schema)
        if not type_map:
            continue
        prepared: List[Tuple[str, str, str, List[str]]] = []
        for obj_type, names in type_map.items():
            option = DBCAT_OPTION_MAP.get(obj_type.upper())
            if not option:
                continue
            name_list = sorted(set(n.upper() for n in names if n))
            if not name_list:
                continue
            prepared.append((option, ','.join(name_list), obj_type.upper(), name_list))

        if not prepared:
            continue

        run_dir = base_output / f"{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        ensure_dir(run_dir)

        cmd = [
            str(dbcat_cli),
            'convert',
            '-H', host,
            '-P', port,
            '-u', ora_cfg['user'],
            '-p', ora_cfg['password'],
            '-D', schema,
            '--from', settings.get('dbcat_from', ''),
            '--to', settings.get('dbcat_to', ''),
            '--file-per-object',
            '-f', str(run_dir)
        ]
        if service:
            cmd.extend(['--service-name', service])

        for option, names_str, _, _ in prepared:
            cmd.extend([option, names_str])

        env = os.environ.copy()
        env['JAVA_HOME'] = java_home
        env.setdefault('JRE_HOME', java_home)

        log.info("[dbcat] 正在导出 schema=%s 对象 DDL...", schema)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=int(settings.get('cli_timeout', 600)),
            env=env
        )
        if proc.returncode != 0:
            log.error("[dbcat] 转换 schema=%s 失败: %s", schema, proc.stderr or proc.stdout)
            sys.exit(1)

        schema_dir = locate_dbcat_schema_dir(run_dir, schema)
        if not schema_dir:
            log.error("[dbcat] 未在输出目录 %s 下找到 schema=%s 的 DDL。", run_dir, schema)
            sys.exit(1)

        schema_result = results.setdefault(schema.upper(), {})
        for _, _, obj_type, name_list in prepared:
            current_map = schema_requests.get(schema.upper(), {})
            remaining_names = current_map.get(obj_type, set())
            type_result = schema_result.setdefault(obj_type, {})
            for obj_name in name_list:
                file_path = find_dbcat_object_file(schema_dir, obj_type, obj_name)
                if not file_path or not file_path.exists():
                    log.warning("[dbcat] 未找到对象 %s.%s (%s) 的 DDL 文件。", schema, obj_name, obj_type)
                    continue
                try:
                    ddl_text = file_path.read_text('utf-8')
                except OSError as exc:
                    log.warning("[dbcat] 读取 %s 失败: %s", file_path, exc)
                    continue
                type_result[obj_name] = ddl_text
                if remaining_names is not None:
                    remaining_names.discard(obj_name)
            if remaining_names is not None and not remaining_names:
                del current_map[obj_type]
        if schema.upper() in schema_requests and not schema_requests[schema.upper()]:
            del schema_requests[schema.upper()]

    return results


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
    'PACKAGE BODY': 'PACKAGE_BODY',
    'MATERIALIZED VIEW': 'MATERIALIZED_VIEW',
    'TYPE BODY': 'TYPE_BODY'
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


DELIMITER_LINE_PATTERN = re.compile(r'^\s*DELIMITER\b.*$', re.IGNORECASE)
BLOCK_END_PATTERN = re.compile(r'^\s*\$\$\s*;?\s*$', re.IGNORECASE)


def cleanup_dbcat_wrappers(ddl: str) -> str:
    """
    dbcat 在导出 PL/SQL 时可能使用 DELIMITER/$$ 包裹。
    这些标记在 OceanBase (Oracle 模式) 中无效，需要移除。
    """
    lines = []
    for line in ddl.splitlines():
        if DELIMITER_LINE_PATTERN.match(line):
            continue
        if BLOCK_END_PATTERN.match(line):
            lines.append('/')
            continue
        lines.append(line)
    return "\n".join(lines)


def prepend_set_schema(ddl: str, schema: str) -> str:
    """
    在 ddl 前加上 ALTER SESSION SET CURRENT_SCHEMA，避免对象落到错误的 schema。
    若已存在 set current schema 指令则不重复添加。
    """
    schema_u = schema.upper()
    lines = ddl.splitlines()
    head = "\n".join(lines[:3]).lower()
    if 'set current_schema' in head:
        return ddl
    prefix = f"ALTER SESSION SET CURRENT_SCHEMA = {schema_u};"
    return "\n".join([prefix, ddl])


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


def enforce_schema_for_ddl(ddl: str, schema: str, obj_type: str) -> str:
    obj_type_u = obj_type.upper()
    if obj_type_u not in DDL_OBJECT_TYPE_OVERRIDE:
        return ddl

    set_stmt = f"ALTER SESSION SET CURRENT_SCHEMA = {schema.upper()};"
    lines = ddl.splitlines()
    insert_idx = 0

    if lines and lines[0].strip().upper().startswith('DELIMITER'):
        insert_idx = 1
        while insert_idx < len(lines) and not lines[insert_idx].strip():
            insert_idx += 1

    lines.insert(insert_idx, set_stmt)
    return "\n".join(lines)


CONSTRAINT_ENABLE_VALIDATE_PATTERN = re.compile(
    r'\s+ENABLE\s+VALIDATE',
    re.IGNORECASE
)
CONSTRAINT_ENABLE_PATTERN = re.compile(
    r'\s+ENABLE(?=\s*;)',
    re.IGNORECASE
)

ENABLE_NOVALIDATE_PATTERN = re.compile(
    r'\s*\bENABLE\s+NOVALIDATE\b',
    re.IGNORECASE
)


def strip_constraint_enable(ddl: str) -> str:
    ddl = CONSTRAINT_ENABLE_VALIDATE_PATTERN.sub(' VALIDATE', ddl)
    ddl = CONSTRAINT_ENABLE_PATTERN.sub('', ddl)
    return ddl


def strip_enable_novalidate(ddl: str) -> str:
    """
    移除行内的 ENABLE NOVALIDATE 关键字组合，以适配 OB 的 CREATE TABLE。
    """
    cleaned_lines: List[str] = []
    for line in ddl.splitlines():
        cleaned = ENABLE_NOVALIDATE_PATTERN.sub('', line)
        cleaned_lines.append(cleaned.rstrip())
    return "\n".join(cleaned_lines)


def split_ddl_statements(ddl: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    for ch in ddl:
        current.append(ch)
        if ch == ';':
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    tail = ''.join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def extract_statements_for_names(
    ddl: str,
    names: Set[str],
    predicate: Callable[[str], bool]
) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {name.upper(): [] for name in names}
    if not ddl:
        return result

    statements = split_ddl_statements(ddl)
    for stmt in statements:
        stmt_upper = stmt.upper()
        if not predicate(stmt_upper):
            continue
        for name in names:
            name_u = name.upper()
            if (
                f'"{name_u}"' in stmt_upper
                or re.search(rf'\b{re.escape(name_u)}\b', stmt_upper)
            ):
                result.setdefault(name_u, []).append(stmt.strip())
    return result


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def write_fixup_file(base_dir: Path, subdir: str, filename: str, content: str, header_comment: str):
    target_dir = base_dir / subdir
    ensure_dir(target_dir)
    file_path = target_dir / filename
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(f"-- {header_comment}\n")
        f.write("-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。\n\n")
        body = content.strip()
        f.write(body)
        f.write('\n')
        tail = body.rstrip()
        if tail and not tail.endswith((';', '/')):
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
    length_mismatches: List[ColumnLengthIssue]
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
        lines.append("-- 列长度不匹配 (目标端长度需在 [ceil(src*1.5), ceil(src*2.5)] 区间)：")
        for issue in length_mismatches:
            col_name, src_len, tgt_len, limit_len, issue_type = issue
            info = col_details.get(col_name)
            if not info:
                continue

            if issue_type == 'short':
                # 在 OB 中，VARCHAR2 等同于 VARCHAR
                modified_type = format_oracle_column_type(info) \
                    .replace("VARCHAR2", "VARCHAR") \
                    .replace(f"({src_len})", f"({limit_len})")

                lines.append(
                    f"ALTER TABLE {tgt_schema_u}.{tgt_table_u} "
                    f"MODIFY ({col_name.upper()} {modified_type}); "
                    f"-- 源长度: {src_len}, 目标长度: {tgt_len}, 期望下限: {limit_len}"
                )
            else:
                lines.append(
                    f"-- WARNING: {col_name.upper()} 长度过大 (源={src_len}, 目标={tgt_len}, "
                    f"建议上限={limit_len})，请人工评估是否需要收敛。"
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
    full_object_mapping: FullObjectMapping,
    required_grants: Optional[Dict[str, Set[Tuple[str, str]]]] = None,
    dependency_report: Optional[DependencyReport] = None,
    ob_meta: Optional[ObMetadata] = None
):
    """
    基于校验结果生成 fix_up DDL 脚本，并按依赖顺序排列：
      1. SEQUENCE
      2. TABLE (CREATE)
      3. TABLE (ALTER - for column diffs)
      4. VIEW / MATERIALIZED VIEW 等代码对象
      5. INDEX
      6. CONSTRAINT
      7. TRIGGER
      8. 依赖重编译 (ALTER ... COMPILE)
      9. 依赖授权 (跨 schema)
    """
    base_dir = Path(settings.get('fixup_dir', 'fix_up'))

    if not master_list:
        log.info("[FIXUP] master_list 为空，未生成修补脚本。")
        return

    ensure_dir(base_dir)
    for child in base_dir.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
    log.info(f"[FIXUP] 修补脚本将生成到目录: {base_dir.resolve()}")

    table_map: Dict[str, str] = {
        tgt_name: src_name
        for (src_name, tgt_name, obj_type) in master_list
        if obj_type.upper() == 'TABLE'
    }

    object_replacements: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    replacement_set: Set[Tuple[str, str, str, str]] = set()
    for src_name, type_map in full_object_mapping.items():
        for tgt_name in type_map.values():
            try:
                src_schema, src_object = src_name.split('.')
                tgt_schema, tgt_object = tgt_name.split('.')
            except ValueError:
                continue
            key = (src_schema.upper(), src_object.upper(), tgt_schema.upper(), tgt_object.upper())
            if key in replacement_set:
                continue
            object_replacements.append(((key[0], key[1]), (key[2], key[3])))
            replacement_set.add(key)

    all_replacements = list(object_replacements)

    obj_type_to_dir = {
        'TABLE': 'table',
        'VIEW': 'view',
        'MATERIALIZED VIEW': 'materialized_view',
        'PROCEDURE': 'procedure',
        'FUNCTION': 'function',
        'PACKAGE': 'package',
        'PACKAGE BODY': 'package_body',
        'SYNONYM': 'synonym',
        'JOB': 'job',
        'SCHEDULE': 'schedule',
        'TYPE': 'type',
        'TYPE BODY': 'type_body',
        'SEQUENCE': 'sequence',
        'TRIGGER': 'trigger'
    }

    grants_map: Dict[str, Set[Tuple[str, str]]] = required_grants if required_grants is not None else {}

    schema_requests: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    unsupported_types: Set[str] = set()

    def queue_request(schema: str, obj_type: str, obj_name: str) -> None:
        obj_type_u = obj_type.upper()
        if obj_type_u not in DBCAT_OPTION_MAP:
            unsupported_types.add(obj_type_u)
            return
        schema_requests[schema.upper()][obj_type_u].add(obj_name.upper())

    missing_tables: List[Tuple[str, str, str, str]] = []
    other_missing_objects: List[Tuple[str, str, str, str, str]] = []

    for (obj_type, tgt_name, src_name) in tv_results.get('missing', []):
        obj_type_u = obj_type.upper()
        if '.' not in src_name or '.' not in tgt_name:
            continue
        src_schema, src_obj = src_name.split('.')
        tgt_schema, tgt_obj = tgt_name.split('.')
        queue_request(src_schema, obj_type_u, src_obj)
        if obj_type_u == 'TABLE':
            missing_tables.append((src_schema, src_obj, tgt_schema, tgt_obj))
        else:
            other_missing_objects.append((obj_type_u, src_schema, src_obj, tgt_schema, tgt_obj))

    sequence_tasks: List[Tuple[str, str, str, str]] = []
    for seq_mis in extra_results.get('sequence_mismatched', []):
        src_schema = seq_mis.src_schema.upper()
        for seq_name in sorted(seq_mis.missing_sequences):
            seq_name_u = seq_name.upper()
            queue_request(src_schema, 'SEQUENCE', seq_name_u)
            src_full = f"{src_schema}.{seq_name_u}"
            mapped = get_mapped_target(full_object_mapping, src_full, 'SEQUENCE')
            if mapped and '.' in mapped:
                tgt_schema, tgt_name = mapped.split('.')
            else:
                tgt_schema = seq_mis.tgt_schema.upper()
                tgt_name = seq_name_u
            sequence_tasks.append((src_schema, seq_name_u, tgt_schema, tgt_name))

    index_tasks: List[Tuple[IndexMismatch, str, str, str, str]] = []
    for item in extra_results.get('index_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = table_str.split('.')
        queue_request(src_schema, 'TABLE', src_table)
        index_tasks.append((item, src_schema, src_table, tgt_schema.upper(), tgt_table.upper()))

    constraint_tasks: List[Tuple[ConstraintMismatch, str, str, str, str]] = []
    for item in extra_results.get('constraint_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = table_str.split('.')
        queue_request(src_schema, 'TABLE', src_table)
        constraint_tasks.append((item, src_schema, src_table, tgt_schema.upper(), tgt_table.upper()))

    trigger_tasks: List[Tuple[str, str, str, str]] = []
    for item in extra_results.get('trigger_mismatched', []):
        table_str = item.table.split()[0]
        if '.' not in table_str:
            continue
        src_name = table_map.get(table_str)
        if not src_name or '.' not in src_name:
            continue
        src_schema, _ = src_name.split('.')
        tgt_schema, _ = table_str.split('.')
        for trg_name in sorted(item.missing_triggers):
            trg_name_u = trg_name.upper()
            queue_request(src_schema, 'TRIGGER', trg_name_u)
            src_full = f"{src_schema.upper()}.{trg_name_u}"
            mapped = get_mapped_target(full_object_mapping, src_full, 'TRIGGER')
            if mapped and '.' in mapped:
                tgt_schema_final, tgt_obj = mapped.split('.')
            else:
                tgt_schema_final = tgt_schema.upper()
                tgt_obj = trg_name_u
            trigger_tasks.append((src_schema, trg_name_u, tgt_schema_final, tgt_obj))

    dbcat_data = fetch_dbcat_schema_objects(ora_cfg, settings, schema_requests)

    def get_dbcat_ddl(schema: str, obj_type: str, obj_name: str) -> Optional[str]:
        return (
            dbcat_data
            .get(schema.upper(), {})
            .get(obj_type.upper(), {})
            .get(obj_name.upper())
        )

    oracle_conn = None

    def get_fallback_ddl(schema: str, obj_type: str, obj_name: str) -> Optional[str]:
        """当 dbcat 缺失 DDL 时尝试使用 DBMS_METADATA 兜底 (仅针对 TYPE/TYPE BODY)。"""
        nonlocal oracle_conn
        if obj_type.upper() not in ('TYPE', 'TYPE BODY'):
            return None
        try:
            if oracle_conn is None:
                oracle_conn = oracledb.connect(
                    user=ora_cfg['user'],
                    password=ora_cfg['password'],
                    dsn=ora_cfg['dsn']
                )
                setup_metadata_session(oracle_conn)
            return oracle_get_ddl(oracle_conn, obj_type, schema, obj_name)
        except Exception as exc:
            log.warning("[DDL] DBMS_METADATA 获取 %s.%s (%s) 失败: %s", schema, obj_name, obj_type, exc)
            return None

    table_ddl_cache: Dict[Tuple[str, str], str] = {}
    for schema, type_map in dbcat_data.items():
        for table_name, ddl in type_map.get('TABLE', {}).items():
            table_ddl_cache[(schema, table_name)] = ddl

    log.info("[FIXUP] (1/9) 正在生成 SEQUENCE 脚本...")
    for src_schema, seq_name, tgt_schema, tgt_name in sequence_tasks:
        ddl = get_dbcat_ddl(src_schema, 'SEQUENCE', seq_name)
        if not ddl:
            log.warning("[FIXUP] 未找到 SEQUENCE %s.%s 的 dbcat DDL。", src_schema, seq_name)
            continue
        ddl_adj = adjust_ddl_for_object(
            ddl,
            src_schema,
            seq_name,
            tgt_schema,
            tgt_name,
            extra_identifiers=all_replacements
        )
        ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
        ddl_adj = prepend_set_schema(ddl_adj, tgt_schema)
        ddl_adj = normalize_ddl_for_ob(ddl_adj)
        ddl_adj = strip_constraint_enable(ddl_adj)
        filename = f"{tgt_schema}.{tgt_name}.sql"
        header = f"修补缺失的 SEQUENCE {tgt_schema}.{tgt_name} (源: {src_schema}.{seq_name})"
        write_fixup_file(base_dir, 'sequence', filename, ddl_adj, header)

    log.info("[FIXUP] (2/9) 正在生成缺失的 TABLE CREATE 脚本...")
    for src_schema, src_table, tgt_schema, tgt_table in missing_tables:
        ddl = get_dbcat_ddl(src_schema, 'TABLE', src_table)
        if not ddl:
            log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL。", src_schema, src_table)
            continue
        ddl_adj = adjust_ddl_for_object(
            ddl,
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            extra_identifiers=all_replacements
        )
        ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
        ddl_adj = prepend_set_schema(ddl_adj, tgt_schema)
        ddl_adj = normalize_ddl_for_ob(ddl_adj)
        ddl_adj = strip_constraint_enable(ddl_adj)
        ddl_adj = strip_enable_novalidate(ddl_adj)
        filename = f"{tgt_schema}.{tgt_table}.sql"
        header = f"修补缺失的 TABLE {tgt_schema}.{tgt_table} (源: {src_schema}.{src_table})"
        write_fixup_file(base_dir, 'table', filename, ddl_adj, header)

    log.info("[FIXUP] (3/9) 正在生成 TABLE ALTER 脚本...")
    for (obj_type, tgt_name, missing_cols, extra_cols, length_mismatches) in tv_results.get('mismatched', []):
        if obj_type.upper() != 'TABLE' or "获取失败" in tgt_name:
            continue
        src_name = table_map.get(tgt_name)
        if not src_name:
            continue
        src_schema, src_table = src_name.split('.')
        tgt_schema, tgt_table = tgt_name.split('.')
        alter_sql = generate_alter_for_table_columns(
            oracle_meta,
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            missing_cols,
            extra_cols,
            length_mismatches
        )
        if alter_sql:
            alter_sql = prepend_set_schema(alter_sql, tgt_schema)
            filename = f"{tgt_schema}.{tgt_table}.alter_columns.sql"
            header = f"基于列差异的 ALTER TABLE 修补脚本: {tgt_schema}.{tgt_table} (源: {src_schema}.{src_table})"
            write_fixup_file(base_dir, 'table_alter', filename, alter_sql, header)

    log.info("[FIXUP] (4/9) 正在生成 VIEW / MATERIALIZED VIEW / 其他对象脚本...")
    for (obj_type, src_schema, src_obj, tgt_schema, tgt_obj) in other_missing_objects:
        ddl = get_dbcat_ddl(src_schema, obj_type, src_obj)
        if not ddl:
            ddl = get_fallback_ddl(src_schema, obj_type, src_obj)
            if ddl:
                log.info("[DDL] 使用 DBMS_METADATA 兜底导出 %s %s.%s。", obj_type, src_schema, src_obj)
        if not ddl:
            log.warning("[FIXUP] 未找到 %s %s.%s 的 dbcat DDL。", obj_type, src_schema, src_obj)
            continue
        ddl_adj = adjust_ddl_for_object(
            ddl,
            src_schema,
            src_obj,
            tgt_schema,
            tgt_obj,
            extra_identifiers=all_replacements
        )
        ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
        ddl_adj = prepend_set_schema(ddl_adj, tgt_schema)
        ddl_adj = normalize_ddl_for_ob(ddl_adj)
        ddl_adj = strip_constraint_enable(ddl_adj)
        ddl_adj = enforce_schema_for_ddl(ddl_adj, tgt_schema, obj_type)
        subdir = obj_type_to_dir.get(obj_type, obj_type.lower())
        filename = f"{tgt_schema}.{tgt_obj}.sql"
        header = f"修补缺失的 {obj_type} {tgt_schema}.{tgt_obj} (源: {src_schema}.{src_obj})"
        write_fixup_file(base_dir, subdir, filename, ddl_adj, header)

    log.info("[FIXUP] (5/9) 正在生成 INDEX 脚本...")
    for item, src_schema, src_table, tgt_schema, tgt_table in index_tasks:
        table_ddl = table_ddl_cache.get((src_schema.upper(), src_table.upper()))
        if not table_ddl:
            log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL，无法生成索引。", src_schema, src_table)
            continue

        def index_predicate(stmt_upper: str) -> bool:
            return 'CREATE' in stmt_upper and ' INDEX ' in stmt_upper

        extracted = extract_statements_for_names(table_ddl, item.missing_indexes, index_predicate)
        for idx_name in sorted(item.missing_indexes):
            idx_name_u = idx_name.upper()
            statements = extracted.get(idx_name_u) or []
            if not statements:
                log.warning("[FIXUP] 未在 TABLE %s.%s 的 DDL 中找到索引 %s。", src_schema, src_table, idx_name_u)
                continue
            ddl_lines: List[str] = []
            for stmt in statements:
                ddl_adj = adjust_ddl_for_object(
                    stmt,
                    src_schema,
                    idx_name_u,
                    tgt_schema,
                    idx_name_u,
                    extra_identifiers=all_replacements
                )
                ddl_adj = normalize_ddl_for_ob(ddl_adj)
                ddl_lines.append(ddl_adj if ddl_adj.endswith(';') else ddl_adj + ';')
            content = prepend_set_schema("\n".join(ddl_lines), tgt_schema)
            filename = f"{tgt_schema}.{idx_name_u}.sql"
            header = f"修补缺失的 INDEX {idx_name_u} (表: {tgt_schema}.{tgt_table})"
            write_fixup_file(base_dir, 'index', filename, content, header)

    log.info("[FIXUP] (6/9) 正在生成 CONSTRAINT 脚本...")
    for item, src_schema, src_table, tgt_schema, tgt_table in constraint_tasks:
        table_ddl = table_ddl_cache.get((src_schema.upper(), src_table.upper()))
        if not table_ddl:
            log.warning("[FIXUP] 未找到 TABLE %s.%s 的 dbcat DDL，无法生成约束。", src_schema, src_table)
            continue

        def constraint_predicate(stmt_upper: str) -> bool:
            return 'ALTER TABLE' in stmt_upper and 'CONSTRAINT' in stmt_upper

        extracted = extract_statements_for_names(table_ddl, item.missing_constraints, constraint_predicate)
        for cons_name in sorted(item.missing_constraints):
            cons_name_u = cons_name.upper()
            statements = extracted.get(cons_name_u) or []
            cons_meta = oracle_meta.constraints.get((src_schema.upper(), src_table.upper()), {}).get(cons_name_u)
            ctype = (cons_meta or {}).get("type", "").upper()
            cols = cons_meta.get("columns") if cons_meta else []
            # 针对跨 schema 的外键，准备 REFERENCES 授权
            if cons_meta and ctype == 'R':
                ref_owner = cons_meta.get("ref_table_owner") or cons_meta.get("r_owner")
                ref_table = cons_meta.get("ref_table_name")
                if ref_owner and ref_table and ref_owner.upper() != tgt_schema.upper():
                    ref_src_full = f"{ref_owner}.{ref_table}"
                    ref_tgt_full = get_mapped_target(full_object_mapping, ref_src_full, 'TABLE') or ref_src_full
                    grants_map.setdefault(tgt_schema.upper(), set()).add(('REFERENCES', ref_tgt_full.upper()))
            # Fallback: PK/UK 可能内联在 CREATE TABLE 中，尝试用元数据重建
            if not statements:
                cols_join = ", ".join(c for c in cols if c)
                if cols_join and ctype in ('P', 'U'):
                    add_clause = "PRIMARY KEY" if ctype == 'P' else "UNIQUE"
                    stmt = (
                        f"ALTER TABLE {tgt_schema}.{tgt_table} "
                        f"ADD CONSTRAINT {cons_name_u} {add_clause} ({cols_join})"
                    )
                    statements = [stmt]
                elif cons_meta:
                    log.warning(
                        "[FIXUP] 约束 %s 类型为 %s，无内联 DDL 可用，无法自动重建。",
                        cons_name_u, ctype or "UNKNOWN"
                    )
            if not statements:
                log.warning("[FIXUP] 未在 TABLE %s.%s 的 DDL 中找到约束 %s。", src_schema, src_table, cons_name_u)
                continue
            ddl_lines: List[str] = []
            for stmt in statements:
                ddl_adj = adjust_ddl_for_object(
                    stmt,
                    src_schema,
                    cons_name_u,
                    tgt_schema,
                    cons_name_u,
                    extra_identifiers=all_replacements
                )
                ddl_adj = normalize_ddl_for_ob(ddl_adj)
                ddl_adj = strip_constraint_enable(ddl_adj)
                ddl_adj = strip_enable_novalidate(ddl_adj)
                ddl_lines.append(ddl_adj if ddl_adj.endswith(';') else ddl_adj + ';')
            content = prepend_set_schema("\n".join(ddl_lines), tgt_schema)
            filename = f"{tgt_schema}.{cons_name_u}.sql"
            header = f"修补缺失的约束 {cons_name_u} (表: {tgt_schema}.{tgt_table})"
            write_fixup_file(base_dir, 'constraint', filename, content, header)

    log.info("[FIXUP] (7/9) 正在生成 TRIGGER 脚本...")
    for src_schema, trg_name, tgt_schema, tgt_obj in trigger_tasks:
        ddl = get_dbcat_ddl(src_schema, 'TRIGGER', trg_name)
        if not ddl:
            log.warning("[FIXUP] 未找到 TRIGGER %s.%s 的 dbcat DDL。", src_schema, trg_name)
            continue
        ddl_adj = adjust_ddl_for_object(
            ddl,
            src_schema,
            trg_name,
            tgt_schema,
            tgt_obj,
            extra_identifiers=all_replacements
        )
        ddl_adj = cleanup_dbcat_wrappers(ddl_adj)
        ddl_adj = prepend_set_schema(ddl_adj, tgt_schema)
        ddl_adj = strip_constraint_enable(ddl_adj)
        ddl_adj = enforce_schema_for_ddl(ddl_adj, tgt_schema, 'TRIGGER')
        filename = f"{tgt_schema}.{tgt_obj}.sql"
        header = f"修补缺失的触发器 {tgt_obj} (源: {src_schema}.{trg_name})"
        write_fixup_file(base_dir, 'trigger', filename, ddl_adj, header)

    dep_report = dependency_report or {}
    compile_tasks: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)

    def _ob_object_exists(full_name: str, obj_type: str) -> bool:
        if ob_meta is None:
            return True
        return full_name.upper() in ob_meta.objects_by_type.get(obj_type.upper(), set())

    def _compile_statements(obj_type: str, obj_name: str) -> List[str]:
        obj_type_u = obj_type.upper()
        obj_name_u = obj_name.upper()
        if obj_type_u in ("FUNCTION", "PROCEDURE"):
            return [f"ALTER {obj_type_u} {obj_name_u} COMPILE;"]
        if obj_type_u in ("PACKAGE", "PACKAGE BODY"):
            return [
                f"ALTER PACKAGE {obj_name_u} COMPILE;",
                f"ALTER PACKAGE {obj_name_u} COMPILE BODY;"
            ]
        if obj_type_u == "TRIGGER":
            return [f"ALTER TRIGGER {obj_name_u} COMPILE;"]
        if obj_type_u in ("VIEW", "MATERIALIZED VIEW"):
            return [f"ALTER {obj_type_u} {obj_name_u} COMPILE;"]
        if obj_type_u == "TYPE":
            return [f"ALTER TYPE {obj_name_u} COMPILE;"]
        if obj_type_u == "TYPE BODY":
            return [f"ALTER TYPE {obj_name_u} COMPILE BODY;"]
        return []

    log.info("[FIXUP] (8/9) 正在生成依赖重编译脚本...")
    for issue in dep_report.get("missing", []):
        dep_name = (issue.dependent or "").upper()
        dep_type = (issue.dependent_type or "").upper()
        if not dep_name or not dep_type:
            continue
        if not _ob_object_exists(dep_name, dep_type):
            continue
        parts = dep_name.split('.', 1)
        if len(parts) != 2:
            continue
        schema_u, obj_u = parts[0], parts[1]
        stmts = _compile_statements(dep_type, obj_u)
        if not stmts:
            continue
        compile_tasks[(schema_u, obj_u, dep_type)].update(stmts)

    if compile_tasks:
        for (schema_u, obj_u, dep_type), stmts in sorted(compile_tasks.items()):
            content = "\n".join(sorted(stmts))
            content = prepend_set_schema(content, schema_u)
            filename = f"{schema_u}.{obj_u}.compile.sql"
            header = f"依赖重编译 {dep_type} {schema_u}.{obj_u}"
            write_fixup_file(base_dir, 'compile', filename, content, header)
    else:
        log.info("[FIXUP] (8/9) 无需生成依赖重编译脚本。")

    if grants_map:
        log.info("[FIXUP] (9/9) 生成依赖授权脚本...")
        for grantee, entries in sorted(grants_map.items()):
            if not entries:
                continue
            statements = sorted({
                f"GRANT {priv} ON {obj} TO {grantee};"
                for priv, obj in entries
            })
            if not statements:
                continue
            content = "\n".join(statements)
            has_ref = any(s.startswith("GRANT REFERENCES") for s in statements)
            ref_note = "；包含 REFERENCES 用于跨 schema 外键" if has_ref else ""
            header = f"授予 {grantee} 访问 remap 依赖目标的权限{ref_note}"
            write_fixup_file(
                base_dir,
                'grants',
                f"{grantee}_grants.sql",
                content,
                header
            )
    else:
        log.info("[FIXUP] (9/9) 无需生成依赖授权脚本。")

    if oracle_conn:
        try:
            oracle_conn.close()
        except Exception:
            pass

    if unsupported_types:
        log.warning(
            "[dbcat] 以下对象类型当前未集成自动导出，需人工处理: %s",
            ", ".join(sorted(unsupported_types))
        )


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
    extra_results: Optional[ExtraCheckResults] = None,
    dependency_report: Optional[DependencyReport] = None,
    required_grants: Optional[Dict[str, Set[Tuple[str, str]]]] = None,
    report_file: Optional[Path] = None,
    object_counts_summary: Optional[ObjectCountSummary] = None
):
    custom_theme = Theme({
        "ok": "green",
        "missing": "red",
        "mismatch": "yellow",
        "info": "cyan",
        "header": "bold magenta",
        "title": "bold white on blue"
    })
    console = Console(theme=custom_theme, record=report_file is not None)

    if extra_results is None:
        extra_results = {
            "index_ok": [], "index_mismatched": [], "constraint_ok": [],
            "constraint_mismatched": [], "sequence_ok": [], "sequence_mismatched": [],
            "trigger_ok": [], "trigger_mismatched": [],
        }
    if dependency_report is None:
        dependency_report = {
            "missing": [],
            "unexpected": [],
            "skipped": []
        }
    if required_grants is None:
        required_grants = {}

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
    extra_target_cnt = len(tv_results.get('extra_targets', []))
    dep_missing_cnt = len(dependency_report.get("missing", []))
    dep_unexpected_cnt = len(dependency_report.get("unexpected", []))
    dep_skipped_cnt = len(dependency_report.get("skipped", []))
    grant_stmt_cnt = sum(len(entries) for entries in required_grants.values())

    console.print(Panel.fit("[bold]数据库对象迁移校验报告 (V0.5 - Rich)[/bold]", style="title"))

    section_width = 140
    count_table_kwargs: Dict[str, object] = {"width": section_width, "expand": False}
    TYPE_COL_WIDTH = 16
    OBJECT_COL_WIDTH = 42
    DETAIL_COL_WIDTH = 90

    # --- 综合概要 ---
    summary_table = Table(
        title="[header]综合概要",
        show_header=False,
        box=None,
        width=section_width,
        pad_edge=False,
        padding=(0, 1)
    )
    summary_table.add_column("Category", justify="left", width=24, no_wrap=True)
    summary_table.add_column("Details", justify="left", width=section_width - 28)

    primary_text = Text()
    primary_text.append(f"总计校验对象 (来自源库): {total_checked}\n")
    primary_text.append("一致: ", style="ok")
    primary_text.append(f"{ok_count}\n")
    primary_text.append("缺失: ", style="missing")
    primary_text.append(f"{missing_count}\n")
    primary_text.append("不匹配 (表列/长度): ", style="mismatch")
    primary_text.append(f"{mismatched_count}\n")
    primary_text.append("多余: ", style="mismatch")
    primary_text.append(f"{extra_target_cnt}\n")
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

    dep_text = Text()
    dep_text.append("缺失依赖: ", style="missing")
    dep_text.append(f"{dep_missing_cnt}  ")
    dep_text.append("额外依赖: ", style="mismatch")
    dep_text.append(f"{dep_unexpected_cnt}  ")
    dep_text.append("跳过: ", style="info")
    dep_text.append(f"{dep_skipped_cnt}")
    summary_table.add_row("[bold]依赖关系[/bold]", dep_text)

    grant_text = Text()
    grant_text.append("GRANT 语句数: ", style="info")
    grant_text.append(str(grant_stmt_cnt))
    summary_table.add_row("[bold]授权建议[/bold]", grant_text)
    console.print(summary_table)
    console.print("")
    console.print("")

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
        return Panel.fit(text, title="[info]执行摘要", border_style="info", width=section_width)

    console.print(summarize_actions())

    if object_counts_summary:
        count_table = Table(title="[header]0. 检查汇总", **count_table_kwargs)
        count_table.add_column("对象类型", style="info", width=TYPE_COL_WIDTH)
        count_table.add_column("Oracle 数量", justify="right", width=12)
        count_table.add_column("OceanBase 数量", justify="right", width=14)
        count_table.add_column("差异", justify="right", width=8)
        oracle_counts = object_counts_summary.get("oracle", {})
        ob_counts = object_counts_summary.get("oceanbase", {})
        for obj_type in OBJECT_COUNT_TYPES:
            ora_val = oracle_counts.get(obj_type, 0)
            ob_val = ob_counts.get(obj_type, 0)
            diff = ob_val - ora_val
            diff_text = "0" if diff == 0 else f"{diff:+d}"
            diff_style = "ok" if diff == 0 else "missing"
            count_table.add_row(
                obj_type,
                str(ora_val),
                str(ob_val),
                f"[{diff_style}]{diff_text}[/{diff_style}]"
            )
        console.print(count_table)

    # --- 1. 缺失的主对象 ---
    if tv_results['missing']:
        table = Table(title=f"[header]1. 缺失的主对象 (共 {missing_count} 个)", width=section_width)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("源对象 = 目标对象(应存在)", style="info")
        for obj_type, tgt_name, src_name in tv_results['missing']:
            table.add_row(
                f"[{obj_type}]",
                f"{src_name} = {tgt_name}"
            )
        console.print(table)

    if tv_results.get('extra_targets'):
        extra_target_count = len(tv_results['extra_targets'])
        table = Table(title=f"[header]1.b 目标端多出的对象 (共 {extra_target_count} 个)", width=section_width)
        table.add_column("类型", style="info", width=TYPE_COL_WIDTH)
        table.add_column("目标对象(多余)", style="info")
        for obj_type, tgt_name in tv_results['extra_targets']:
            table.add_row(f"[{obj_type}]", tgt_name)
        console.print(table)

    # --- 2. 列不匹配的表 ---
    if tv_results['mismatched']:
        table = Table(title=f"[header]2. 不匹配的表 (共 {mismatched_count} 个)", width=section_width)
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
                    for issue in length_mismatches:
                        col, src_len, tgt_len, limit_len, issue_type = issue
                        if issue_type == 'short':
                            details.append(
                                f"    - {col}: 源={src_len}, 目标={tgt_len}, 期望下限={limit_len}\n"
                            )
                        else:
                            details.append(
                                f"    - {col}: 源={src_len}, 目标={tgt_len}, 上限允许={limit_len}\n"
                            )
            table.add_row(tgt_name, details)
        console.print(table)

    # --- 3. 扩展对象差异 ---
    def print_ext_mismatch_table(title, items, headers, render_func):
        if not items:
            return
        table = Table(title=f"[header]{title} (共 {len(items)} 项差异)", width=section_width)
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

    dep_total = dep_missing_cnt + dep_unexpected_cnt + dep_skipped_cnt
    if dep_total:
        dep_table = Table(title=f"[header]9. 依赖关系校验 (共 {dep_total} 项)", width=section_width)
        dep_table.add_column("类别", style="info", width=12)
        dep_table.add_column("依赖对象", style="info", width=OBJECT_COL_WIDTH)
        dep_table.add_column("依赖类型", style="info", width=TYPE_COL_WIDTH)
        dep_table.add_column("被依赖对象", style="info", width=OBJECT_COL_WIDTH)
        dep_table.add_column("被依赖类型", style="info", width=TYPE_COL_WIDTH)
        dep_table.add_column("修复建议", width=DETAIL_COL_WIDTH)

        def render_dep_rows(label: str, entries: List[DependencyIssue], style: str) -> None:
            for issue in entries:
                dep_table.add_row(
                    f"[{style}]{label}[/{style}]",
                    issue.dependent,
                    issue.dependent_type,
                    issue.referenced,
                    issue.referenced_type,
                    issue.reason
                )

        render_dep_rows("缺失", dependency_report.get("missing", []), "missing")
        render_dep_rows("额外", dependency_report.get("unexpected", []), "mismatch")
        render_dep_rows("跳过", dependency_report.get("skipped", []), "info")
        console.print(dep_table)

    if required_grants:
        grant_table = Table(title=f"[header]10. 授权建议 (共 {grant_stmt_cnt} 条)", width=section_width)
        grant_table.add_column("授权对象", style="info", width=OBJECT_COL_WIDTH)
        grant_table.add_column("语句", width=DETAIL_COL_WIDTH)
        for grantee, entries in sorted(required_grants.items()):
            lines = [
                f"GRANT {priv} ON {obj} TO {grantee};"
                for priv, obj in sorted(entries)
            ]
            grant_table.add_row(grantee, "\n".join(lines))
        console.print(grant_table)

    # --- 4. 无效 Remap 规则 ---
    if tv_results['extraneous']:
        table = Table(title=f"[header]4. 无效的 Remap 规则 (共 {extraneous_count} 个)", width=section_width)
        table.add_column("在 remap_rules.txt 中定义, 但在源端 Oracle 中未找到的对象", style="info", width=section_width - 6)
        for item in tv_results['extraneous']:
            table.add_row(item, style="mismatch")
        console.print(table)

    # --- 提示 ---
    fixup_panel = Panel.fit(
        "[bold]Fixup 脚本生成目录[/bold]\n\n"
        "fix_up/table         : 缺失 TABLE 的 CREATE 脚本\n"
        "fix_up/view          : 缺失 VIEW 的 CREATE 脚本\n"
        "fix_up/materialized_view : 缺失 MATERIALIZED VIEW 的 CREATE 脚本\n"
        "fix_up/procedure     : 缺失 PROCEDURE 的 CREATE 脚本\n"
        "fix_up/function      : 缺失 FUNCTION 的 CREATE 脚本\n"
        "fix_up/package       : 缺失 PACKAGE 的 CREATE 脚本\n"
        "fix_up/package_body  : 缺失 PACKAGE BODY 的 CREATE 脚本\n"
        "fix_up/synonym       : 缺失 SYNONYM 的 CREATE 脚本\n"
        "fix_up/job           : 缺失 JOB 的 CREATE 脚本\n"
        "fix_up/schedule      : 缺失 SCHEDULE 的 CREATE 脚本\n"
        "fix_up/type          : 缺失 TYPE 的 CREATE 脚本\n"
        "fix_up/type_body     : 缺失 TYPE BODY 的 CREATE 脚本\n"
        "fix_up/index         : 缺失 INDEX 的 CREATE 脚本\n"
        "fix_up/constraint    : 缺失约束的 CREATE 脚本\n"
        "fix_up/sequence      : 缺失 SEQUENCE 的 CREATE 脚本\n"
        "fix_up/trigger       : 缺失 TRIGGER 的 CREATE 脚本\n"
        "fix_up/compile       : 依赖重编译脚本 (ALTER ... COMPILE)\n"
        "fix_up/grants        : 依赖对象所需的授权脚本\n"
        "fix_up/table_alter   : 列不匹配 TABLE 的 ALTER 修补脚本\n\n"
        "[bold]请在 OceanBase 执行前逐一人工审核上述脚本。[/bold]",
        title="[info]提示",
        border_style="info"
    )
    console.print(fixup_panel)
    console.print(Panel.fit("[bold]报告结束[/bold]", style="title"))

    if report_file:
        report_path = Path(report_file)
        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_text = console.export_text(clear=False)
            report_path.write_text(report_text, encoding='utf-8')
            console.print(f"[info]报告已保存: {report_path}")
        except OSError as exc:
            console.print(f"[missing]报告写入失败: {exc}")


# ====================== 主函数 ======================

def main():
    """主执行函数"""
    CONFIG_FILE = 'db.ini'

    # 1) 加载配置
    ora_cfg, ob_cfg, settings = load_config(CONFIG_FILE)

    # 初始化 Oracle Instant Client (Thick Mode)
    init_oracle_client_from_settings(settings)

    # 2) 加载 Remap 规则
    remap_rules = load_remap_rules(settings['remap_file'])

    # 3) 加载源端主对象 (TABLE/VIEW/PROC/FUNC/PACKAGE/PACKAGE BODY/SYNONYM)
    source_objects = get_source_objects(ora_cfg, settings['source_schemas_list'])

    # 4) 验证 Remap 规则
    extraneous_rules = validate_remap_rules(remap_rules, source_objects)

    # 5) 生成主校验清单
    master_list = generate_master_list(source_objects, remap_rules)
    full_object_mapping = build_full_object_mapping(source_objects, remap_rules)
    oracle_dependencies = load_oracle_dependencies(ora_cfg, settings['source_schemas_list'])
    expected_dependency_pairs, skipped_dependency_pairs = build_expected_dependency_pairs(
        oracle_dependencies,
        full_object_mapping
    )
    target_schemas: Set[str] = set()
    for type_map in full_object_mapping.values():
        for tgt_name in type_map.values():
            try:
                schema, _ = tgt_name.split('.')
                target_schemas.add(schema.upper())
            except ValueError:
                continue

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir_setting = settings.get('report_dir', 'reports').strip() or 'reports'
    report_dir = Path(report_dir_setting)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"report_{timestamp}.txt"
    log.info(f"本次报告将输出到: {report_path}")

    dependency_report: DependencyReport = {
        "missing": [],
        "unexpected": [],
        "skipped": skipped_dependency_pairs
    }
    required_grants: Dict[str, Set[Tuple[str, str]]] = {}
    object_counts_summary: Optional[ObjectCountSummary] = None

    if not master_list:
        log.info("主校验清单为空，程序结束。")
        tv_results: ReportResults = {
            "missing": [],
            "mismatched": [],
            "ok": [],
            "extraneous": extraneous_rules,
            "extra_targets": []
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
        print_final_report(
            tv_results,
            0,
            extra_results,
            dependency_report,
            required_grants,
            report_path,
            object_counts_summary
        )
        return

    # 6) 计算目标端 schema 集合并一次性 dump OB 元数据
    ob_meta = dump_ob_metadata(ob_cfg, target_schemas)
    object_counts_summary = compute_object_counts(source_objects, ob_meta, OBJECT_COUNT_TYPES)

    ob_dependencies = load_ob_dependencies(ob_cfg, target_schemas)

    # 7) 主对象校验
    oracle_meta = dump_oracle_metadata(ora_cfg, master_list, settings)

    tv_results = check_primary_objects(
        master_list,
        extraneous_rules,
        ob_meta,
        oracle_meta
    )

    # 8) 扩展对象校验 (索引/约束/序列/触发器)
    extra_results = check_extra_objects(settings, master_list, ob_meta, oracle_meta, full_object_mapping)

    dependency_report = check_dependencies_against_ob(
        expected_dependency_pairs,
        ob_dependencies,
        skipped_dependency_pairs,
        ob_meta
    )
    required_grants = compute_required_grants(expected_dependency_pairs)

    # 9) 生成修补脚本
    if settings.get('generate_fixup', 'true').strip().lower() in ('true', '1', 'yes'):
        log.info('已开启修补脚本生成，开始写入 fix_up 目录...')
        generate_fixup_scripts(
            ora_cfg,
            settings,
            tv_results,
            extra_results,
            master_list,
            oracle_meta,
            full_object_mapping,
            required_grants,
            dependency_report,
            ob_meta
        )
    else:
        log.info('已根据配置跳过修补脚本生成，仅打印对比报告。')

    # 10) 输出最终报告
    print_final_report(
        tv_results,
        len(master_list),
        extra_results,
        dependency_report,
        required_grants,
        report_path,
        object_counts_summary
    )


if __name__ == "__main__":
    main()
