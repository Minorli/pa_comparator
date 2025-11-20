# Hydra Matrix 10→15 Schema Stress Scenario

This scenario fabricates ten busy Oracle schemas (core, orders, logistics, reference data, analytics, audit, billing, security, utilities, and machine learning) and remaps them across **fifteen** OceanBase schemas (`OB_MASTER`, `OB_APP`, `OB_DW`, `OB_ANALYTICS`, `OB_SUPPLY`, `OB_STREAM`, `OB_REF`, `OB_STAGE`, `OB_SHARE`, `OB_AUDIT`, `OB_BILL`, `OB_FIN`, `OB_SEC`, `OB_ML`, `OB_ODS`).  Every schema boundary is crossed at least once: tables depend on dimensions maintained in a different schema, PL/SQL units hop across three destinations, and synonyms jump twice.  The OceanBase script intentionally leaves holes (missing columns, packages, sequences, triggers, and synonyms) and sprinkles extra tables/sequences to ensure the comparator exhausts every report path.

## Contents

```
test_scenarios/hydra_matrix_case/
├── README.md                        <- This document
├── oracle_setup.sql                 <- Oracle-side DDL (10 schemas, 60+ objects)
├── oceanbase_setup.sql              <- OceanBase-side DDL with deliberate gaps
└── remap_rules_hydra.txt            <- Cross-schema remap directives (10 -> 15)
```

## Schema / Object Matrix (Oracle → OceanBase)

| Source schema | Key objects (non-exhaustive) | Remap target(s) | Notes |
| ------------- | --------------------------- | --------------- | ----- |
| `ORA_REF`     | REGION/CHANNEL/SERVICE/SHIP lookup tables, 4 sequences, SP_REFRESH_LOOKUP | `OB_REF`, `OB_STAGE`, `OB_SHARE` | Lookup data split between two OB schemas; stored procedure moves to OB_SHARE even though no table lives there. |
| `ORA_CORE`    | CUSTOMER_DIM, ADDRESS_DIM, SEQ_CUSTOMER, PKG_CUSTOMER_API, SP_PROFILE_REFRESH | `OB_MASTER`, `OB_ODS`, `OB_APP` | Customer tables jump to two targets; PL/SQL units land in OB_APP, driving cross-tenant grants. |
| `ORA_ORD`     | ORDER_FACT/LINE, SEQ_ORDER, trigger, package spec/body, procedure, function, VW_OPEN_ORDERS | `OB_DW`, `OB_APP`, `OB_ANALYTICS` | Fact tables shift to DW, PL/SQL to APP, reporting view to ANALYTICS; trigger/sequence land on different targets. |
| `ORA_FULFILL` | SHIPMENT_FACT/EVENT, sequence, trigger, procedure, VW_DUE_SHIPMENTS | `OB_SUPPLY`, `OB_STREAM`, `OB_ANALYTICS` | Fact + events split, while the analytics view lives elsewhere; all supporting objects remap individually. |
| `ORA_ANALYTICS` | Three summary views + SP_SNAPSHOT_FACTS | `OB_ANALYTICS` | Serves as the convergence point for facts from four other schemas plus ML outputs. |
| `ORA_AUDIT`   | AUDIT_LOG, JOB_RUN, two sequences, trigger, SP_PURGE_AUDIT, FN_LAST_JOB_RUN | `OB_AUDIT` | Most objects missing on target, ensuring comparator flags tables/sequences/triggers/PLSQL gaps. |
| `ORA_BILL`    | INVOICE header/line, sequence, trigger, SP_POST_INVOICE, PKG_BILLING_ENGINE, VW_OVERDUE_INVOICE | `OB_BILL`, `OB_FIN` | Tables stay together but procedure/package/view move into OB_FIN’s financial services schema. |
| `ORA_SEC`     | USER_MATRIX, ROLE_RIGHTS, SEQ_USER, SP_GRANT_ROLE, FN_IS_AUTHORIZED, VW_ACTIVE_USERS | `OB_SEC` | Entire security domain migrates to OB_SEC; comparator must report missing view/function/table pieces. |
| `ORA_UTIL`    | Four synonyms, FN_CANARY, PKG_DATA_ROUTER (spec/body) | `OB_SHARE` | Only one synonym survives on the OB side; package and stored procedure vanish to exercise missing synonym/package detection. |
| `ORA_ML`      | MODEL_VERSION/FEATURE, SEQ_MODEL, SP_REGISTER_MODEL, FN_SCORE_MODEL, VW_FEATURE_COVERAGE | `OB_ML`, `OB_ANALYTICS` (view) | Tables/sequences live in OB_ML while the analytics view remaps into OB_ANALYTICS but stays missing for detection. |

## Execution Steps

1. **Provision Oracle schemas and objects.**  Connect as a privileged Oracle user and execute `@test_scenarios/hydra_matrix_case/oracle_setup.sql`.  The script creates ten schemas, complete referential integrity, sequences, trigger, PL/SQL packages, views, and synonyms.
2. **Deploy OceanBase targets.**  Connect to the Oracle-compatible OceanBase tenant and run `@test_scenarios/hydra_matrix_case/oceanbase_setup.sql`.  Fifteen target schemas are created, but key columns, sequences, packages, and synonyms are intentionally omitted while extra artifacts are added for noise.
3. **Wire remap rules.**  Point `db.ini` → `[SETTINGS].remap_file = test_scenarios/hydra_matrix_case/remap_rules_hydra.txt` and set `source_schemas = ORA_CORE,ORA_ORD,ORA_FULFILL,ORA_REF,ORA_ANALYTICS,ORA_AUDIT,ORA_BILL,ORA_SEC,ORA_UTIL,ORA_ML`.
4. **Run the comparator.**  Execute `python3 db_comparator_fixup_release.py` with credentials for both sides.  Review console output plus `fix_up/` artifacts.

## Expected Comparator Findings (Highlights)

- **Tables & Column Sets**
  - `OB_MASTER.CUSTOMER_DIM` drops `VIP_FLAG`, `PREFERRED_CHANNEL_ID`, `LOYALTY_SCORE`, and keeps VARCHAR lengths at the Oracle size (no 1.5× expansion).  FK constraints disappear as well.
  - `OB_ODS.ADDRESS_DIM`, `OB_SUPPLY.SHIPMENT_FACT`, `OB_STREAM.SHIPMENT_EVENT`, `OB_BILL.INVOICE_HEADER`, and `OB_ML.MODEL_*` lose columns referenced by downstream objects, so the comparator reports missing columns plus FK/count mismatches.
  - Lookup tables in `OB_STAGE` omit `ONLINE_FLAG`, `DESCRIPTION`, and all FK/unique constraints, generating column gaps and extra-column warnings (`MIGRATION_TAG`).

- **Sequences, Triggers, Indexes**
  - `SEQ_REGION`, `SEQ_SERVICE`, `SEQ_SHIP_METHOD`, `SEQ_SHIPMENT`, and `SEQ_AUDIT` are not created on OceanBase even though the remap file expects them.
  - `TRG_F_ORDER_BI`, `TRG_SHIPMENT_FACT_BI`, and `TRG_INVOICE_HEADER_BI` are missing entirely, while `OB_DW.EXTRA_LOAD_SEQ` and `OB_DW.EXTRA_FACT_GHOST` appear only on OB, showing the “extra object” path.

- **Procedures, Functions, Packages**
  - `OB_APP` omits `SP_PROFILE_REFRESH` and `FN_ORDER_MARGIN`, publishes package specs without bodies (`PKG_CUSTOMER_API`, `PKG_ORDER_WORKFLOW`), and therefore triggers both “missing program unit” and “missing package body” findings.
  - `OB_AUDIT` has neither `SP_PURGE_AUDIT` nor `FN_LAST_JOB_RUN`; `OB_SUPPLY.SP_CLOSE_SHIPMENT`, `OB_FIN.SP_POST_INVOICE`, `OB_ANALYTICS.SP_SNAPSHOT_FACTS`, and `OB_ML.SP_REGISTER_MODEL` are absent as well.

- **Views & Synonyms**
  - Only two analytics views exist; `VW_OPEN_ORDERS`, `VW_DELIVERY_DELAY`, `VW_DUE_SHIPMENTS`, and `VW_FEATURE_COVERAGE` never show up on the OB side.
  - `OB_SHARE` defines just one synonym and a canary function, so `SYN_CUSTOMER`, `SYN_ORDER`, `SYN_INVOICE`, `PKG_DATA_ROUTER`, and `SP_REFRESH_LOOKUP` are all reported missing.  `OB_STAGE.EXTRA_LOOKUP_SHADOW` and `OB_AUDIT.EXTRA_EVENT_STUB` count as extra tables.

- **Security & Utility Objects**
  - `OB_SEC.USER_MATRIX` loses the `CUSTOMER_ID` column and unique constraint; the entire `ROLE_RIGHTS` table, function, and view are missing, guaranteeing FK/constraint deltas.
  - `OB_REF.REGION_DIM` lacks `ACTIVE_FLAG`, the REGION_CODE unique constraint, and the remapped sequence.

Combined, the scenario produces:

* Missing tables (SHIP_METHOD, INVOICE_LINE, ROLE_RIGHTS, JOB_RUN, etc.).
* Missing sequences/triggers/functions/procedures/packages/views/synonyms from every Oracle schema.
* Extra sequences/tables on the OB side for “unexpected object” validation.
* Column-set drift (missing/excess columns and 1.5× length violations) across OB_MASTER, OB_STAGE, OB_ODS, OB_SUPPLY, OB_ML, and OB_BILL.

Use this scenario when you need a heavy-weight regression suite that stresses remap parsing, column-set comparison, package body detection, and extra-object reporting across fifteen target schemas.
