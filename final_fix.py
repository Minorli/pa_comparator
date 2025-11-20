#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apply all fix-up SQL scripts under fix_up/* to OceanBase by invoking obclient.

Usage:
    python3 final_fix.py [optional path to db.ini]

Behavior:
    * Reads OceanBase connection info from db.ini (same format used by the comparator).
    * Discovers every *.sql file under the configured fix_up directory (recursively through first-level subfolders).
    * Executes each file sequentially via obclient. On failure, prints the error and continues.
    * Prints a final summary showing total scripts, successes, failures, and failed file names.
"""

import configparser
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

CONFIG_DEFAULT_PATH = "db.ini"
DEFAULT_FIXUP_DIR = "fix_up"
DONE_DIR_NAME = "done"


class ConfigError(Exception):
    """Custom exception for configuration issues."""


def load_ob_config(config_path: Path) -> Tuple[Dict[str, str], Path, Path]:
    """
    Load OceanBase connection info and fix_up directory from db.ini.

    Returns:
        (ob_cfg, fixup_dir, repo_root)
    """
    parser = configparser.ConfigParser()
    if not config_path.exists():
        raise ConfigError(f"配置文件不存在: {config_path}")

    parser.read(config_path, encoding="utf-8")

    if "OCEANBASE_TARGET" not in parser:
        raise ConfigError("db.ini 缺少 [OCEANBASE_TARGET] 配置段。")

    ob_section = parser["OCEANBASE_TARGET"]
    required_keys = ["executable", "host", "port", "user_string", "password"]
    missing = [key for key in required_keys if key not in ob_section or not ob_section[key].strip()]
    if missing:
        raise ConfigError(f"[OCEANBASE_TARGET] 缺少必填项: {', '.join(missing)}")

    ob_cfg = {key: ob_section[key].strip() for key in required_keys}
    ob_cfg["port"] = str(int(ob_cfg["port"]))  # 规范化端口

    repo_root = config_path.parent.resolve()
    fixup_dir = parser.get("SETTINGS", "fixup_dir", fallback=DEFAULT_FIXUP_DIR).strip()
    fixup_path = (repo_root / fixup_dir).resolve()

    if not fixup_path.exists():
        raise ConfigError(f"修补脚本目录不存在: {fixup_path}")

    return ob_cfg, fixup_path, repo_root


def build_obclient_command(ob_cfg: Dict[str, str]) -> List[str]:
    """Assemble the obclient command line."""
    return [
        ob_cfg["executable"],
        "-h",
        ob_cfg["host"],
        "-P",
        ob_cfg["port"],
        "-u",
        ob_cfg["user_string"],
        f"-p{ob_cfg['password']}",
        "--prompt",
        "fixup>",
        "--silent",
    ]


def collect_sql_files(fixup_dir: Path, done_dir_name: str = DONE_DIR_NAME) -> List[Path]:
    """
    Collect *.sql files under fix_up with dependency-aware ordering:
      1) sequence → table → table_alter → constraint → index
      2) view / materialized_view
      3) remaining code objects (synonym/procedure/function/package/type/trigger/etc.)
    """
    priority = [
        "sequence",
        "table",
        "table_alter",
        "constraint",
        "index",
        "view",
        "materialized_view",
        "synonym",
        "procedure",
        "function",
        "package",
        "package_body",
        "type",
        "type_body",
        "trigger",
        "job",
        "schedule",
        "grants",
    ]
    subdirs = {p.name: p for p in fixup_dir.iterdir() if p.is_dir() and p.name != done_dir_name}

    ordered_groups: List[Path] = []
    seen = set()
    for name in priority:
        if name in subdirs:
            ordered_groups.append(subdirs[name])
            seen.add(name)
    # Append any remaining subfolders in alpha order to avoid missing custom categories
    for name in sorted(subdirs.keys()):
        if name not in seen:
            ordered_groups.append(subdirs[name])

    sql_files: List[Path] = []
    for group in ordered_groups:
        for sql_file in sorted(group.glob("*.sql")):
            if sql_file.is_file():
                sql_files.append(sql_file)
    return sql_files


def run_sql(obclient_cmd: List[str], sql_text: str) -> subprocess.CompletedProcess:
    """Execute SQL text by piping it to obclient."""
    return subprocess.run(
        obclient_cmd,
        input=sql_text,
        capture_output=True,
        text=True,
        check=False,
    )


@dataclass
class ScriptResult:
    path: Path
    status: str
    message: str = ""


def main() -> None:
    config_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(CONFIG_DEFAULT_PATH)

    try:
        ob_cfg, fixup_dir, repo_root = load_ob_config(config_arg.resolve())
    except ConfigError as exc:
        print(f"[配置错误] {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # unexpected IO errors, permission issues, etc.
        print(f"[致命错误] 无法读取配置: {exc}", file=sys.stderr)
        sys.exit(1)

    done_dir = fixup_dir / DONE_DIR_NAME
    done_dir.mkdir(exist_ok=True)

    sql_files = collect_sql_files(fixup_dir)
    if not sql_files:
        print(f"[提示] 目录 {fixup_dir} 中未找到任何 *.sql 文件。")
        return

    obclient_cmd = build_obclient_command(ob_cfg)
    total_scripts = len(sql_files)
    width = len(str(total_scripts)) or 1
    results: List[ScriptResult] = []

    header = "=" * 58
    print(header)
    print("开始执行修补脚本")
    print(f"目录: {fixup_dir}")
    print(f"共发现 SQL 文件: {total_scripts}")
    print(header)

    for idx, sql_path in enumerate(sql_files, start=1):
        relative_path = sql_path.relative_to(repo_root)
        label = f"[{idx:0{width}}/{total_scripts}]"
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
        except Exception as exc:
            msg = f"读取文件失败: {exc}"
            results.append(ScriptResult(relative_path, "ERROR", msg))
            print(f"{label} {relative_path} -> 错误")
            print(f"    {msg}")
            continue

        if not sql_text.strip():
            results.append(ScriptResult(relative_path, "SKIPPED", "文件为空"))
            print(f"{label} {relative_path} -> 跳过 (文件为空)")
            continue

        result = run_sql(obclient_cmd, sql_text)
        if result.returncode == 0:
            move_note = ""
            try:
                target_dir = done_dir / sql_path.parent.name
                target_dir.mkdir(parents=True, exist_ok=True)
                target_path = target_dir / sql_path.name
                shutil.move(str(sql_path), target_path)
                move_note = f"(已移至 {target_path.relative_to(repo_root)})"
            except Exception as exc:
                move_note = f"(移动到 done 目录失败: {exc})"
            results.append(ScriptResult(relative_path, "SUCCESS", move_note.strip()))
            print(f"{label} {relative_path} -> 成功 {move_note}")
        else:
            stderr = (result.stderr or "").strip()
            results.append(ScriptResult(relative_path, "FAILED", stderr))
            print(f"{label} {relative_path} -> 失败")
            if stderr:
                print(f"    {stderr}")

    executed = sum(1 for r in results if r.status != "SKIPPED")
    success = sum(1 for r in results if r.status == "SUCCESS")
    failed = sum(1 for r in results if r.status in ("FAILED", "ERROR"))
    skipped = sum(1 for r in results if r.status == "SKIPPED")

    print("\n================== 执行结果汇总 ==================")
    print(f"扫描脚本数 : {total_scripts}")
    print(f"实际执行数 : {executed}")
    print(f"成功       : {success}")
    print(f"失败       : {failed}")
    print(f"跳过       : {skipped}")

    table_rows: List[Tuple[str, str]] = []
    for item in results:
        display_path = str(item.path)
        if item.status == "SUCCESS":
            message = item.message or "成功"
        elif item.status == "SKIPPED":
            message = item.message or "跳过"
        elif item.status == "ERROR":
            message = item.message or "读取文件失败"
        else:  # FAILED
            message = item.message or "执行失败"
        message = message.splitlines()[0]
        table_rows.append((display_path, message))

    if table_rows:
        col1_width = max(len("脚本"), max(len(row[0]) for row in table_rows))
        col2_width = max(len("信息"), max(len(row[1]) for row in table_rows))
        border = f"+{'-' * (col1_width + 2)}+{'-' * (col2_width + 2)}+"
        header_row = f"| {'脚本'.ljust(col1_width)} | {'信息'.ljust(col2_width)} |"

        print("\n明细表：")
        print(border)
        print(header_row)
        print(border)
        for script, msg in table_rows:
            print(f"| {script.ljust(col1_width)} | {msg.ljust(col2_width)} |")
        print(border)

    print("=================================================")


if __name__ == "__main__":
    main()
