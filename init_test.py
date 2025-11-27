import argparse
import configparser
import subprocess
import sys
from pathlib import Path
from typing import Optional

import oracledb


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.ini"
ORACLE_SQL = ROOT / "test_scenarios" / "gorgon_knot_case" / "oracle_setup.sql"
OCEANBASE_SQL = ROOT / "test_scenarios" / "gorgon_knot_case" / "oceanbase_setup.sql"


def load_config():
    parser = configparser.ConfigParser(inline_comment_prefixes=("#", ";"))
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"config.ini not found at {CONFIG_PATH}")
    parser.read(CONFIG_PATH)
    return parser


def parse_sql_script(path: Path):
    """
    Minimal SQL*Plus-style splitter:
    - Treats a standalone "/" line as a block delimiter.
    - For PL/SQL/package-like statements, waits for "/" delimiter.
    - For plain DDL/DML, executes on a trailing ";" when not in a PL/SQL block.
    """
    statements = []
    buffer = []
    in_plsql = False
    buffer_is_plsql = False

    def flush(is_plsql: bool):
        stmt = "\n".join(buffer).strip()
        if not is_plsql and stmt.endswith(";"):
            stmt = stmt[:-1]
        if stmt:
            statements.append(stmt.strip())
        buffer.clear()
        return False, False  # reset flags

    def is_plsql_start(upper_line: str) -> bool:
        starters = (
            "DECLARE",
            "BEGIN",
            "CREATE OR REPLACE PACKAGE BODY",
            "CREATE OR REPLACE PACKAGE",
            "CREATE PACKAGE BODY",
            "CREATE PACKAGE",
            "CREATE OR REPLACE PROCEDURE",
            "CREATE PROCEDURE",
            "CREATE OR REPLACE FUNCTION",
            "CREATE FUNCTION",
            "CREATE OR REPLACE TRIGGER",
            "CREATE TRIGGER",
            "CREATE OR REPLACE TYPE BODY",
            "CREATE TYPE BODY",
            "CREATE OR REPLACE TYPE",
            "CREATE TYPE",
        )
        return upper_line.startswith(starters)

    with path.open(encoding="utf-8") as f:
        for raw_line in f:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("--"):
                continue

            if stripped == "/":
                buffer_is_plsql, in_plsql = flush(buffer_is_plsql)
                continue

            upper = stripped.upper()
            if not in_plsql and is_plsql_start(upper):
                if buffer:  # flush any pending non-PLSQL before entering PL/SQL
                    buffer_is_plsql, in_plsql = flush(False)
                in_plsql = True
                buffer_is_plsql = True

            buffer.append(raw_line.rstrip("\n"))

            if not in_plsql and stripped.endswith(";"):
                buffer_is_plsql, in_plsql = flush(False)

    if buffer:
        flush(buffer_is_plsql)

    return statements


def preview(stmt: str, width: int = 90) -> str:
    single_line = " ".join(stmt.split())
    return single_line if len(single_line) <= width else single_line[: width - 3] + "..."


def init_oracle(cfg):
    user = cfg.get("ORACLE_SOURCE", "user", fallback=None)
    password = cfg.get("ORACLE_SOURCE", "password", fallback=None)
    dsn = cfg.get("ORACLE_SOURCE", "dsn", fallback=None)
    lib_dir = cfg.get("SETTINGS", "oracle_client_lib_dir", fallback=None)

    if not all([user, password, dsn]):
        raise ValueError("Missing ORACLE_SOURCE credentials in config.ini")

    if lib_dir:
        oracledb.init_oracle_client(lib_dir=lib_dir)

    statements = parse_sql_script(ORACLE_SQL)
    print(f"[INFO] Parsed {len(statements)} statements from {ORACLE_SQL.name}")

    with oracledb.connect(user=user, password=password, dsn=dsn, mode=oracledb.DEFAULT_AUTH) as conn:
        cur = conn.cursor()
        for idx, stmt in enumerate(statements, 1):
            print(f"[ORACLE] Step {idx}/{len(statements)}: {preview(stmt)}")
            try:
                cur.execute(stmt)
            except Exception as exc:  # pragma: no cover - runtime diagnostic path
                print(f"[ERROR] Oracle step {idx} failed: {exc}")
                raise
        conn.commit()
    print("[INFO] Oracle setup completed.")


def init_oceanbase(cfg):
    exe = cfg.get("OCEANBASE_TARGET", "executable", fallback=None)
    host = cfg.get("OCEANBASE_TARGET", "host", fallback=None)
    port = cfg.get("OCEANBASE_TARGET", "port", fallback=None)
    user_string = cfg.get("OCEANBASE_TARGET", "user_string", fallback=None)
    password = cfg.get("OCEANBASE_TARGET", "password", fallback=None)

    if not all([exe, host, port, user_string, password]):
        raise ValueError("Missing OCEANBASE_TARGET config entries in config.ini")

    statements = parse_sql_script(OCEANBASE_SQL)
    print(f"[INFO] Parsed {len(statements)} statements from {OCEANBASE_SQL.name}")

    base_cmd = [
        exe,
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user_string,
        f"-p{password}",
    ]

    for idx, stmt in enumerate(statements, 1):
        print(f"[OB] Step {idx}/{len(statements)}: {preview(stmt)}")
        cmd = base_cmd + ["-e", stmt]
        result = subprocess.run(cmd, text=True, capture_output=True)
        if result.returncode != 0:
            combined = (result.stdout or "") + (result.stderr or "")
            print(combined)
            if "ORA-01031" in combined.upper():
                print(f"[WARN] Step {idx} failed due to insufficient privileges; continuing.")
                continue
            raise RuntimeError(f"OceanBase step {idx} failed with code {result.returncode}")
    print("[INFO] OceanBase setup completed.")


def choose_target(cli_choice: Optional[str]) -> str:
    mapping = {"1": "oracle", "2": "oceanbase"}
    if cli_choice:
        choice = mapping.get(cli_choice.lower()) or cli_choice.lower()
        if choice not in mapping.values():
            raise ValueError("Invalid target selection. Use 1/oracle or 2/oceanbase.")
        return choice

    print("Select target to initialize:")
    print("  1) Oracle (oracle_setup.sql)")
    print("  2) OceanBase (oceanbase_setup.sql)")
    while True:
        user_input = input("Enter choice (1/2): ").strip()
        choice = mapping.get(user_input.lower()) or user_input.lower()
        if choice in mapping.values():
            return choice
        print("Invalid choice, please enter 1 or 2.")


def main():
    parser = argparse.ArgumentParser(description="Initialize test scenarios for Oracle or OceanBase.")
    parser.add_argument(
        "--target",
        "-t",
        help="Target to initialize: 1/oracle or 2/oceanbase. If omitted, you will be prompted.",
    )
    args = parser.parse_args()

    cfg = load_config()
    target = choose_target(args.target)

    print(f"[INFO] Starting initialization for: {target}")
    if target == "oracle":
        init_oracle(cfg)
    else:
        init_oceanbase(cfg)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # pragma: no cover - runtime diagnostic path
        print(f"[FATAL] {e}")
        sys.exit(1)
