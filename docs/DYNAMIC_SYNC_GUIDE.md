# Dynamic Sync Implementation Guide

## Overview

This document describes the dynamic CSV-based mapping system for synchronizing data from IQVIA OneKey to Microsoft Dataverse. The implementation provides:

1. **Dynamic Field Mapping** - CSV-driven configuration instead of hardcoded transforms
2. **Pre-Sync Validation** - Validates schema, fields, relationships before sync
3. **Comprehensive Logging** - Detailed logs for every operation and error
4. **Relationship Resolution** - Automatic parent hierarchy and HCP-HCO linking

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Configuration Layer                             │
├─────────────────────────────────────────────────────────────────────────┤
│  config/entity_mapping.csv      - Field-level mapping rules             │
│  config/relationship_mapping.csv - Entity relationship definitions       │
│  config/entity_definition.csv   - Entity metadata for validation        │
│  config/value_mapping.csv       - Picklist/code value transformations   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Integration Layer                                │
├─────────────────────────────────────────────────────────────────────────┤
│  dynamic_mapping.py  - Load and index CSV configurations                │
│  validation.py       - Validate mappings, schema, source data           │
│  transform.py        - Apply transform rules to convert records         │
│  dynamic_sync.py     - Orchestrate full/delta sync with logging         │
│  sync_logger.py      - Comprehensive structured logging                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          API Clients                                     │
├─────────────────────────────────────────────────────────────────────────┤
│  onekey/client.py    - OneKey API client (search, population, CDC)      │
│  dataverse/client.py - Dataverse Web API client (CRUD, upsert)          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## CSV Mapping Configuration

### 1. Entity Mapping (`config/entity_mapping.csv`)

Defines field-level mapping from OneKey to Dataverse.

| Column | Description | Example |
|--------|-------------|---------|
| `mapping_id` | Unique identifier | HCO-001, HCP-001 |
| `source_platform` | Source system | OneKey |
| `source_entity` | Source entity type | Workplace, Individual |
| `source_field` | Source field name | workplaceEid, firstName |
| `source_field_path` | Full JSONPath (for reference) | workplace.workplaceEid |
| `target_platform` | Target system | Dataverse |
| `target_entity` | Target table | account, contact |
| `target_field` | Target column | cree7_externalid, firstname |
| `target_field_type` | Data type | string, picklist, lookup |
| `max_length` | Max string length | 50, 160 |
| `is_required` | Mandatory field | true/false |
| `is_key` | Upsert key field | true/false |
| `is_lookup` | Foreign key lookup | true/false |
| `lookup_entity` | Target lookup entity | account |
| `lookup_key_field` | Lookup match field | cree7_externalid |
| `transform_rule` | Transformation logic | direct, map:A=1, template:{value} |
| `default_value` | Fallback value | Unknown |
| `notes` | Documentation | |

#### Transform Rules

| Rule | Syntax | Description |
|------|--------|-------------|
| `direct` | `direct` | Pass value through unchanged |
| `coalesce` | `coalesce:field2` | Use value, or fallback to field2 |
| `map` | `map:A=1\|B=2` | Map source value to target value |
| `template` | `template:Type: {value}` | Format string |
| `decode` | `decode:TYP` | Decode using value_mapping.csv |
| `append` | `append: - {value}` | Append to another field |
| `or` | `or:field2` | Boolean OR with another field |

### 2. Relationship Mapping (`config/relationship_mapping.csv`)

Defines entity relationships for proper load order.

| Column | Description |
|--------|-------------|
| `relationship_id` | Unique identifier |
| `relationship_name` | Human-readable name |
| `source_entity` | Parent entity |
| `source_field` | Key field on parent |
| `target_entity` | Child entity |
| `target_field` | Lookup field on child |
| `relationship_type` | self_referential, many_to_one, etc. |
| `load_order` | Sequence for loading |

### 3. Entity Definition (`config/entity_definition.csv`)

Metadata for validation.

| Column | Description |
|--------|-------------|
| `entity_id` | Unique identifier |
| `platform` | Dataverse or OneKey |
| `entity_name` | Logical name |
| `entity_set` | API endpoint name (accounts, contacts) |
| `key_field` | Primary key |
| `alternate_key` | Upsert key |
| `is_required` | Must exist for sync |
| `load_order` | Sequence for loading |

### 4. Value Mapping (`config/value_mapping.csv`)

Code/picklist value transformations.

| Column | Description |
|--------|-------------|
| `mapping_id` | Unique identifier |
| `mapping_type` | Category (gendercode, statecode, typecode) |
| `source_value` | OneKey value |
| `target_value` | Dataverse value |
| `description` | Documentation |

---

## Usage

### Basic Usage

```python
from integration import create_sync_engine, setup_logging
from onekey import OneKeyClient, OneKeySession, OneKeyConfig
from dataverse import DataverseClient, DataverseConfig

# Setup logging
setup_logging(level=logging.INFO)

# Connect clients
onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
dataverse = DataverseClient(DataverseConfig.from_env())

# Create sync engine (loads CSV config automatically)
engine = create_sync_engine(onekey, dataverse)

# Run full sync
result = engine.sync_full(
    codebases=["WUK", "WDE"],
    include_address=False,
    max_records=100,  # For testing
    dry_run=False,
)

print(result)
```

### Delta/CDC Sync

```python
# Delta sync for changed records
result = engine.sync_delta(
    codebases=["WUK"],
    date_from="2026-09-07T00:00:00Z",
    date_to="2026-09-11T23:59:59Z",
    include_address=False,
)
```

### Validation Only

```python
from integration import load_mapping_config, MappingValidator

config = load_mapping_config()
validator = MappingValidator(config)

# Validate mapping configuration
result = validator.validate_mapping_config()
result.log_all()

if not result.is_valid:
    print("Validation failed!")
```

### Command Line

```bash
# Dry run with validation
python scripts/run_dynamic_sync.py --dry-run --codbase WUK

# Full sync
python scripts/run_dynamic_sync.py --codbase WUK,WDE --max-records 100

# Delta sync
python scripts/run_dynamic_sync.py --delta --date-from 2026-09-07 --date-to 2026-09-11

# Validate configuration only
python scripts/run_dynamic_sync.py --validate-only
```

---

## Logging

### Log Files

Logs are written to `logs/` directory:

- `integration_YYYYMMDD.log` - Daily integration log
- `sync_SESSIONID.log` - Session-specific log
- `sync_SESSIONID_summary.json` - JSON summary

### Log Levels

| Level | Content |
|-------|---------|
| ERROR | Failed operations, validation errors |
| WARNING | Missing relationships, data truncation |
| INFO | Progress, phase transitions, summary |
| DEBUG | Individual record operations |

### Example Log Output

```
10:30:15 | INFO    | ============================================================
10:30:15 | INFO    | SYNC STARTED
10:30:15 | INFO    |   Session:    20260914_103015
10:30:15 | INFO    |   Type:       full
10:30:15 | INFO    |   Codebases:  WUK, WDE
10:30:15 | INFO    | ============================================================
10:30:15 | INFO    | ----------------------------------------
10:30:15 | INFO    | [PHASE] PRE-SYNC VALIDATION
10:30:15 | INFO    | ----------------------------------------
10:30:16 | INFO    | [VALIDATION] Mapping Config: VALID: 0 errors, 2 warnings, 3 info
10:30:17 | INFO    | [VALIDATION] Dataverse Schema: VALID: 0 errors, 0 warnings, 2 info
10:30:17 | INFO    | Pre-sync validation: VALID: 0 errors, 2 warnings, 5 info
10:30:17 | INFO    | ----------------------------------------
10:30:17 | INFO    | [PHASE] SYNC CODEBASE: WUK
10:30:17 | INFO    | ----------------------------------------
10:30:17 | INFO    | ----------------------------------------
10:30:17 | INFO    | [PHASE] HCO SYNC: WUK
10:30:17 | INFO    | ----------------------------------------
10:30:18 | INFO    | Extracting HCOs from OneKey WUK...
10:30:25 | INFO    | Extracted 300 HCOs from WUK
10:30:25 | INFO    | HCO hierarchy: max depth 3
10:30:25 | INFO    | [PROGRESS] accounts: 50/300 (16.7%)
10:30:26 | INFO    | [CREATE] account WUKE00001260 -> a1b2c3d4 (Betsi Cadwaladr University HB)
10:30:26 | INFO    | [RELATIONSHIP] account WUKE00001260 -> account a1b2c3d4
...
10:31:45 | INFO    | ============================================================
10:31:45 | INFO    | SYNC COMPLETED
10:31:45 | INFO    |   Duration:         88.23 seconds
10:31:45 | INFO    |   Accounts:         300 processed (150 created, 145 updated, 5 failed)
10:31:45 | INFO    |   Contacts:         1200 processed (800 created, 385 updated, 15 failed)
10:31:45 | INFO    |   Orphan contacts:  12
10:31:45 | INFO    |   Validation:       3 errors, 25 warnings
10:31:45 | INFO    |   Relationships:    1188 resolved, 12 failed
10:31:45 | INFO    | ============================================================
```

---

## Validation

### Pre-Sync Validation

The engine validates before any data is written:

1. **Mapping Configuration**
   - Required entities are defined
   - Key fields exist for each entity
   - Lookup references point to valid entities
   - No invalid transform rules

2. **Dataverse Schema**
   - Required entity sets exist (accounts, contacts)
   - Connection is authenticated

3. **Source Data** (per record)
   - Required fields are present
   - Values don't exceed max length
   - Lookup keys can be resolved

### Validation Result

```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[ValidationError]    # Blocks sync
    warnings: list[ValidationError]  # Logged, sync continues
    info: list[ValidationError]      # Informational
```

---

## Migration Flow

### Full Sync Order

```
0. Upsert picklists from value_mapping.csv (load_picklist_values / Phase 0)
1. Extract HCOs (Workplaces) → upsert Accounts (no parent bind yet)
2. Upsert customeraddress rows ({workplaceEid} - {addressEid})
3. Link parentaccountid from managerWorkplaceEid
4. Extract HCPs → upsert Contacts + parentcustomerid
```

Unmapped picklist codes are omitted and written to ``logs/picklist_gaps_*.csv``.

### Delta Sync Order

Same phases for CDC window records: workplaces first, then addresses and
hierarchy for those HCOs, then activities → contacts. ``run_dynamic_sync.py``
stores watermarks under ``logs/sync_watermark_{CODEBASE}.json``.

The scheduled jobs (``function_app.py`` timers and ``scripts/run_scheduled_sync.py``)
use ``ts_integrationjob`` instead: the daily delta runs from the ``ts_startedon``
of the last Full or Delta job that covered its window (less 15 minutes of
overlap) up to now. A full sync runs at 00:00 on the 1st of each month and the
delta runs at 00:00 on every other day.

### CLI

```bash
python scripts/run_dynamic_sync.py --codbase WUK --max-records 100
python scripts/run_dynamic_sync.py --delta --date-from 2026-09-07 --date-to 2026-09-11
python scripts/run_dynamic_sync.py --skip-address --skip-hierarchy --skip-picklists
python scripts/load_hco_addresses.py --full --codbase WUK --max-records 50
```

---

## Customization

### Adding New Fields

1. Add row to `config/entity_mapping.csv`
2. If picklist, add values to `config/value_mapping.csv`
3. Run validation: `python scripts/run_dynamic_sync.py --validate-only`

### Adding New Relationships

1. Add row to `config/relationship_mapping.csv`
2. Update `load_order` values
3. If custom entity, add to `config/entity_definition.csv`

### Custom Transform Rules

Extend `transform.py`:

```python
def _apply_transform(self, value, mapping, source):
    rule = mapping.transform_rule
    
    # Add custom rule
    if rule.startswith("custom_rule:"):
        param = rule.split(":", 1)[1]
        return self._custom_transform(value, param)
    
    # ... existing rules
```

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `missing_required` | Required source field empty | Add default_value in CSV |
| `lookup_not_found` | Parent account doesn't exist | Ensure parent loaded first |
| `exceeds_max_length` | Value too long | Truncated automatically with warning |
| `entity_not_found` | Dataverse entity missing | Create entity or remove from is_required |

### Recovery

Failed records are logged with source IDs. After fixing issues:

```bash
# Re-run for specific records
python scripts/run_dynamic_sync.py --codbase WUK --max-records 10
```

---

## Best Practices

1. **Always run validation first**
   ```bash
   python scripts/run_dynamic_sync.py --validate-only
   ```

2. **Test with dry run**
   ```bash
   python scripts/run_dynamic_sync.py --dry-run --max-records 10
   ```

3. **Start with small batches**
   ```bash
   python scripts/run_dynamic_sync.py --max-records 100
   ```

4. **Review logs after sync**
   - Check `logs/sync_*_summary.json` for counts
   - Search logs for `[FAILED]` entries

5. **Schedule delta sync nightly**
   - Run after OneKey DB closure (~18:00 CET for WUK)
   - Track last successful sync date

---

## Files Reference

```
onekey-paion-integration/
├── config/
│   ├── entity_mapping.csv        # Field mapping rules
│   ├── relationship_mapping.csv  # Entity relationships
│   ├── entity_definition.csv     # Entity metadata
│   └── value_mapping.csv         # Picklist transformations
├── integration/
│   ├── __init__.py               # Module exports
│   ├── dynamic_mapping.py        # CSV loader and indexer
│   ├── validation.py             # Pre-sync validation
│   ├── transform.py              # Record transformation
│   ├── dynamic_sync.py           # Sync orchestration
│   ├── sync_logger.py            # Structured logging
│   ├── mapping.py                # Original hardcoded mapping
│   └── sync.py                   # Original sync (backward compat)
├── scripts/
│   ├── run_dynamic_sync.py       # CLI for dynamic sync
│   └── run_full_sync.py          # Original sync script
├── logs/
│   ├── integration_YYYYMMDD.log  # Daily log
│   ├── sync_SESSIONID.log        # Session log
│   └── sync_SESSIONID_summary.json
└── docs/
    └── DYNAMIC_SYNC_GUIDE.md     # This document
```
