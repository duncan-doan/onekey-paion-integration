# OneKey ↔ Dataverse Mapping Quick Reference

> **Quick lookup for mapping between OneKey and Dataverse**

---

## Entity Mapping Summary

| OneKey Entity | Dataverse Entity | Key Field Mapping | Load Order |
|---------------|------------------|-------------------|------------|
| **Workplace** (HCO) | `account` | `workplaceEid` → `cree7_externalid` | 1 |
| **Individual** (HCP) | `contact` | `individualEid` → `externaluseridentifier` | 2 |
| Activity | (embedded in contact) | `activityEid` | - |
| Address | `customeraddress` | `{workplaceEid} - {addressEid}` → `cree7_externalid` | 3 |

> **Address duplication:** OneKey Address↔HCO is N-N. Dataverse keeps Account→CustomerAddress as **1-n**, so a shared physical address is written once per HCO with a distinct composite key (same street/city, different `cree7_externalid`).

---

## Field Mapping Quick Reference

### HCO (Workplace → Account)

| OneKey Field | → | Dataverse Field | Transform |
|--------------|---|-----------------|-----------|
| `workplaceEid` | → | `cree7_externalid` | direct (KEY) |
| `officialName` | → | `name` | coalesce:usualName |
| `typeCode` | → | `sic` | direct |
| `statusCode` | → | `statecode` | map: 3→0, 9→1 |
| `managerWorkplaceEid` | → | `parentaccountid` | lookup |
| main-activity Contact placement chain | → | `ts_hierarchylevel` | full sync: included in Account upsert; department=3, hospital=2, hospital group=1 |
| `address_street` | → | `address1_line1` | direct |
| `address_city` | → | `address1_city` | direct |
| `address_postalCode` | → | `address1_postalcode` | direct |
| `address_country` | → | `address1_country` | direct |

### HCP (Individual → Contact)

| OneKey Field | → | Dataverse Field | Transform |
|--------------|---|-----------------|-----------|
| `individualEid` | → | `externaluseridentifier` | direct (KEY) |
| `firstName` | → | `firstname` | direct |
| `lastName` | → | `lastname` | coalesce:individualEid |
| `genderCode` | → | `gendercode` | map: M→1, F→2 |
| `title` | → | `salutation` | decode:title |
| `typeLabel` | → | `jobtitle` | direct |
| `statusCode` | → | `statecode` | map: 3→0, 9→1 |
| `workplaceEid` | → | `ts_department` | lookup |
| `hospitalWorkplaceEid` | → | `ts_hospital` | lookup |
| hospital's parent | → | `ts_hospitalgroup` | lookup |
| `usualName` | → | `department` | direct |

### Address (Workplace address → CustomerAddress)

| OneKey Field | → | Dataverse Field | Transform |
|--------------|---|-----------------|-----------|
| `workplaceEid` + `addressEid` | → | `cree7_externalid` | composite KEY: `{workplaceEid} - {addressEid}` |
| `address_street` | → | `line1` | direct |
| `address_city` | → | `city` | direct |
| `address_postalCode` | → | `postalcode` | direct |
| `address_country` | → | `country` | direct |
| `address_addressType` | → | `addresstypecode` | decode:addresstype |
| `workplaceEid` | → | `parentid` | lookup Account by `cree7_externalid` |

---

## Transform Rules Cheat Sheet

| Rule | Syntax | Example | Result |
|------|--------|---------|--------|
| **direct** | `direct` | `firstName` | Copy as-is |
| **map** | `map:A=1\|B=2` | `genderCode=M` | `1` |
| **coalesce** | `coalesce:field2` | If empty, use field2 | Fallback value |
| **template** | `template:Prefix: {value}` | `typeLabel=Hospital` | `"HCO type: Hospital"` |
| **decode** | `decode:mapType` | `titleCode=03` | `"Dr"` |
| **constant** | `constant:3` | Always | `3` |
| **append** | `append: - {value}` | `jobtitle + specialty` | `"Doctor - Cardiology"` |
| **lookup_by_key** | `lookup_by_key` | `workplaceEid` | Account GUID |

---

## Common Value Mappings

### Gender Code
```
M → 1 (Male)
F → 2 (Female)
```

### Status Code
```
3 → 0 (Valid → Active)
9 → 1 (Invalid → Inactive)
1 → 1 (Created → Inactive)
```

### Title Code
```
01 → Mr     04 → Prof
02 → Mrs    05 → Ms
03 → Dr     06 → Miss
```

### HCO Type Code
```
HAU → Health Board/Agency
ACT → Acute Trust
MHT → Mental Health Trust
PRA → Practice
```

### Activity Location
```
H → Hospital (11)
S → Surgery (12)
N → NHS Est. (13)
O → Office (14)
```

---

## Max Length Reference

| Entity | Field | Max Length |
|--------|-------|------------|
| account | cree7_externalid | 100 |
| account | name | 160 |
| account | description | 2000 |
| account | address1_line1 | 250 |
| account | address1_city | 80 |
| contact | externaluseridentifier | 50 |
| contact | firstname | 50 |
| contact | lastname | 50 |
| contact | jobtitle | 100 |
| contact | department | 100 |
| contact | description | 2000 |

---

## Config File Locations

```
config/
├── entity_definition.csv    # What entities exist
├── entity_mapping.csv       # Field mappings
├── relationship_mapping.csv # Entity relationships
├── value_mapping.csv        # Code translations
└── field_constraints.csv    # Validation rules
```

---

## Mapping ID Prefixes

| Prefix | Entity | Example |
|--------|--------|---------|
| `HCO-` | Workplace → account | `HCO-001` |
| `HCP-` | Individual → contact | `HCP-003` |
| `AFF-` | Activity → affiliation | `AFF-001` |
| `VAL-` | Value mapping | `VAL-GEN-001` |
| `CON-` | Constraint | `CON-CON-003` |
| `REL-` | Relationship | `REL-002` |
| `ENT-` | Entity definition | `ENT-DV-001` |

---

## Quick Add Templates

### Add New Field Mapping
```csv
{ID},{Platform},Workplace|Individual,{sourceField},{sourcePath},Dataverse,account|contact,{targetField},{type},{maxLen},{required},{isKey},{isLookup},{lookupEntity},{lookupKey},{transform},{default},{notes}
```

**Example:**
```csv
HCP-022,OneKey,Individual,mobilePhone,individual.mobilePhone,Dataverse,contact,mobilephone,string,50,false,false,false,,,direct,,Mobile phone
```

### Add New Value Mapping
```csv
{ID},{type},{sourceCode},{sourceLabel},{targetValue},{targetLabel},{notes}
```

**Example:**
```csv
VAL-GEN-003,gendercode,U,Unknown,,Unknown,Unknown gender
```

### Add New Constraint
```csv
{ID},Dataverse,{entity},{field},{type},{value},{action},{notes}
```

**Example:**
```csv
CON-CON-025,Dataverse,contact,mobilephone,max_length,50,truncate,Mobile phone limit
```

---

## Verification Commands

```python
from integration import load_mapping_config
from pathlib import Path

config = load_mapping_config(Path("config"))

# Count mappings
print(f"Fields: {len(config.field_mappings)}")
print(f"Values: {len(config.value_mappings)}")

# Get entity mappings
account_fields = config.get_field_mappings("account")
contact_fields = config.get_field_mappings("contact")

# Check specific mapping
print(config.get_max_length("contact", "lastname"))
print(config.is_field_required("contact", "lastname"))

# Check value mapping
print(config._value_map.get("gendercode", {}).get("M"))
```

---

*Quick Reference v1.0 | September 2026*
