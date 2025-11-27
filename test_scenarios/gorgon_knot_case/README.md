# Gorgon's Knot Case (Ultimate Complexity)

This case now carries roughly 5× the object volume of the original draft. It pushes the comparator through dense, interdependent remaps: many-to-one merges, one-to-many splits, colliding names, cross-schema packages, and intentionally broken synonym mappings—all while ensuring both setup scripts compile cleanly in a single run.

## Layout

```
test_scenarios/gorgon_knot_case/
├── README.md
├── oracle_setup.sql
├── oceanbase_setup.sql
└── remap_rules_gorgon.txt
```

## Schema Mapping Strategy

| Oracle Schema | Remap Target(s) | Coverage Purpose |
| --- | --- | --- |
| `HERO_A` | `OLYMPIAN_A` | Many-to-one merge (missions, relics, titles, cross-schema packages). |
| `HERO_B` | `OLYMPIAN_A` | Many-to-one merge with colliding names, quests, scores, and archive package. |
| `MONSTER_A` | `TITAN_A`, `TITAN_B` | One-to-many split (data tables to `TITAN_A`, logic/views/packages to `TITAN_B`). |
| `MONSTER_B` | (none) | Unmapped source schema; all objects reported as missing in target. |
| `GOD_A` | `PRIMORDIAL` | One-to-one remap with heavy renaming and missing bodies/sequences. |
| (none) | `OLYMPIAN_B` | Extraneous target schema; all objects reported as extra. |

## Key Test Scenarios

- **Collision Factory:** `HERO_A` and `HERO_B` each define overlapping treasures, missions/quests, sequences, triggers, and views. Remaps rename them into a single `OLYMPIAN_A` namespace while preserving cross-schema joins.
- **Split Brain Data:** `MONSTER_A` data (lairs, dungeons, raids, supply lines, war chests) map to `TITAN_A`, while traps/curse logic, views, and packages map to `TITAN_B`. Multiple sequences/triggers are intentionally unmapped or missing on the target side.
- **Dependency Chains:** `HERO_A.PKG_HERO_NETWORK` relies on `HERO_B.PKG_LEGEND_ARCHIVE`; `GOD_A.PKG_PROPHECY` calls back into `HERO_A.FN_HERO_NET_WORTH`; `GOD_A.SP_BLESS_HERO` uses a procedure that is missing in `OLYMPIAN_A`, invoked via a dynamic call in the target to keep compilation clean.
- **Broken Remaps:** Synonyms `MONSTER_A.SYN_SECRET` and `MONSTER_A.SYN_FUNDS` remap across schemas, pointing to split tables. Package body mappings for `PKG_MONSTER_OPS`, `PKG_DIVINITY/PKG_COSMOS`, and `PKG_PROPHECY` deliberately mismatch or are absent in the target.
- **Standard Mismatches at Scale:** Numerous missing/extra tables, views, sequences, triggers, packages (spec/body), and type bodies. Columns diverge in name, type, length, and constraints. Noise objects remain in every target schema.

## How to Execute

1. **Oracle Source:** Run `oracle_setup.sql` to create the five source schemas. Objects are ordered with grants so the script completes without recompilation loops.
2. **OceanBase Target:** Run `oceanbase_setup.sql` to create the five target schemas with their intentional gaps and stubs (all statements compile in one pass).
3. **Run Comparator:** Configure `config.ini` with `source_schemas = HERO_A,HERO_B,MONSTER_A,MONSTER_B,GOD_A` and `remap_file = test_scenarios/gorgon_knot_case/remap_rules_gorgon.txt`, then execute the main script.
