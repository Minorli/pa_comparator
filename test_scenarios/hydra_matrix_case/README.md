# Hydra Matrix Scenario (Complex Remap Coverage)

This edition of Hydra Matrix builds **eleven** fully functional Oracle schemas and remaps their objects across **fourteen** OceanBase schemas to exercise every feature in `db_comparator_fixup_release.py`.  The Oracle script creates every referenced table, sequence, trigger, PL/SQL unit, object type, materialized view, synonym, and view, so it runs cleanly on a brand-new database.  The OceanBase script intentionally recreates only part of the landscape—dropping columns, shortening VARCHAR lengths, omitting sequences/triggers/packages/types, and sprinkling extra artifacts—so each comparator report bucket (missing/mismatched/extraneous/dependency/grant) gets coverage.

## Layout

```
test_scenarios/hydra_matrix_case/
├── README.md
├── oracle_setup.sql        <- Oracle source-side DDL for 11 schemas
├── oceanbase_setup.sql     <- OceanBase target-side DDL with intentional gaps
└── remap_rules_hydra.txt   <- Canonical remap definitions
```

## Schema Matrix

| Oracle schema | Objects created | Remap target(s) | Coverage purpose |
| --- | --- | --- | --- |
| `ORA_REF` | REGION/CHANNEL/PRODUCT dimensions, 3 sequences, refresh proc | `OB_DIM`, `OB_STAGE`, `OB_SHARE` | Column drift, missing constraints, missing sequence, missing proc |
| `ORA_TXN` | SALES_ORDER / SALES_ORDER_LINE, 2 sequences, trigger, procedure, function, package spec/body, analytic view | `OB_APP`, `OB_ANALYTICS` | Missing trigger/sequence/package body/function/view, extra table, column length mismatch |
| `ORA_DIGITAL` | STREAM_EVENT, sequence, materialized view, custom object type + body, features proc | `OB_STREAM`, `OB_ANALYTICS` | Missing MV/type body/sequence |
| `ORA_BILL` | INVOICE tables, sequence, trigger, SP_POST_INVOICE, billing package spec/body, object type, overdue view | `OB_LEDGER`, `OB_FIN` | Missing trigger/proc/package/type, column gaps, extra sequence |
| `ORA_SEC` | USER_MATRIX + ROLE_POLICY, sequence, grant procedure, function, active-user view | `OB_SEC` | Missing table/program units, grant generation |
| `ORA_UTIL` | Synonyms into other schemas, canary function, gateway procedure | `OB_SHARE` | Missing synonyms/procedure, synonym remap handling |
| `ORA_AUDIT` | AUDIT_LOG, JOB_STATUS, sequence, trigger, archive proc, summary function/view | `OB_AUDIT` | Missing trigger/sequence/function/view, unexpected OB objects |
| `ORA_PLAN` | FORECAST_PLAN table, sequence, publish proc, planning package spec/body referencing REF/TXN | `OB_PLAN` | Missing package body/procedure/table columns |
| `ORA_MDM` | ATTRIBUTE_DEF table, SEQ_ATTR, custom type + body, refresh proc | `OB_MDM` | Missing type body/procedure/sequence |
| `ORA_RPT` | Reporting view + materialized view + refresh proc spanning all schemas | `OB_RPT` | Missing MV/view/proc ensures dependency coverage |
| `ORA_ML` | MODEL_VERSION/RUN tables, sequence, register proc, scoring function | `OB_ML` | Missing function/procedure columns/sequence |

## How to Execute

1. **Oracle source**
   ```sql
   sqlplus sys/<pwd>@<oracle_dsn> as sysdba
   @test_scenarios/hydra_matrix_case/oracle_setup.sql
   ```
   This provisions eleven schemas with the objects outlined above plus all required cross-schema grants.

2. **OceanBase target**
   ```sql
   obclient -h <host> -P <port> -u <tenant user> -p
   source test_scenarios/hydra_matrix_case/oceanbase_setup.sql
   ```
   The script creates fourteen schemas (`OB_DIM`, `OB_STAGE`, `OB_APP`, `OB_STREAM`, `OB_ANALYTICS`, `OB_LEDGER`, `OB_FIN`, `OB_SEC`, `OB_SHARE`, `OB_AUDIT`, `OB_PLAN`, `OB_MDM`, `OB_RPT`, `OB_ML`) but intentionally:
   - Removes key columns or constraints (e.g., no VIP/Loyalty columns in `OB_APP.CUSTOMER_DIM`, no REGION_ID in `OB_STAGE.PRODUCT_CATEGORY`).
   - Leaves VARCHAR lengths shorter than the `ceil(1.5 × source)` rule.
   - Omits or renames sequences, triggers, materialized views, object types, procedures, package bodies, and synonyms.
   - Seeds stray objects such as `OB_APP.EXTRA_GHOST_ORDER`, `OB_LEDGER.EXTRA_SEQ_AUDIT`, and `OB_PLAN.EXTRA_PLAN_LOG`.

3. **Run the comparator**
   ```
   [SETTINGS]
   source_schemas = ORA_REF,ORA_TXN,ORA_DIGITAL,ORA_BILL,ORA_SEC,ORA_UTIL,ORA_AUDIT,ORA_PLAN,ORA_MDM,ORA_RPT,ORA_ML
   remap_file = test_scenarios/hydra_matrix_case/remap_rules_hydra.txt
   ```
   Execute `python3 db_comparator_fixup_release.py` and inspect the console output, report file, and generated `fix_up/` scripts.

## Coverage Highlights

- **Primary objects**: Tables, views, materialized views, PL/SQL units, object types, synonyms, and object-type bodies all appear in the remap file.  The OB side omits many of them, ensuring missing-object reports across every category.
- **Column/length drift**: Several target tables drop columns or use shorter VARCHAR lengths so the comparator emits column-mismatch and length-adjustment scripts.
- **OMS 列过滤示例**: `OB_STAGE.CHANNEL_DIM` 注入 6 个以 `OMS_` 开头的列，其中 4 个为内置(`OMS_OBJECT_NUMBER/OMS_RELATIVE_FNO/OMS_BLOCK_NUMBER/OMS_ROW_NUMBER`)会被忽略，另外 2 个 (`OMS_EXTRA_FLAG/OMS_AUDIT_NOTE`) 会被报告为目标端多余列。
- **Extra objects**: Noise tables and sequences on the OB side validate “unexpected object” reporting.
- **Dependencies and grants**: Cross-schema FKs and program-unit calls span almost every schema.  The comparator must map dependencies through the remap rules and emit `GRANT SELECT/EXECUTE` scripts for dozens of cases.
- **Fix-up generation**: Missing sequences, triggers, package bodies, types, and synonyms produce fix-up scripts that leverage dbcat DDL, while column mismatches route to `fix_up/table_alter`.

Use this Hydra Matrix case whenever you need a deterministic regression suite that exercises complex remap networks, dependency analysis, grant generation, and fix-up output in one run.
