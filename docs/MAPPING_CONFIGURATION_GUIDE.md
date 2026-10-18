# OneKey to Dataverse Mapping Configuration Guide

This guide explains how to configure mappings between **IQVIA OneKey** data and **Microsoft Dataverse** tables. The mapping system uses CSV configuration files, allowing non-developers to modify mappings without changing code.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Configuration Files](#2-configuration-files)
3. [Entity (Object) Mapping](#3-entity-object-mapping)
4. [Field Mapping](#4-field-mapping)
5. [Relationship Mapping](#5-relationship-mapping)
6. [Value Transformations](#6-value-transformations)
7. [Field Constraints](#7-field-constraints)
8. [Step-by-Step Examples](#8-step-by-step-examples)
9. [Best Practices](#9-best-practices)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Overview

### 1.1 What is This System?

This mapping system synchronizes healthcare data from **IQVIA OneKey** to **Microsoft Dataverse (D365)**:

| OneKey Concept | Dataverse Entity | Description |
|----------------|------------------|-------------|
| **Workplace** (HCO) | `account` | Healthcare Organizations (hospitals, clinics) |
| **Individual** (HCP) | `contact` | Healthcare Professionals (doctors, nurses) |
| **Activity** | Embedded in `contact` | Links HCP to their workplace(s) |
| **Address** | `address1_*` fields | Physical location data |

### 1.2 Configuration File Location

All mapping configurations are stored in CSV files:

```
onekey-paion-integration/
└── config/
    ├── entity_definition.csv    # What objects exist
    ├── entity_mapping.csv       # How fields map between systems
    ├── relationship_mapping.csv # How objects relate to each other
    ├── value_mapping.csv        # Code translations (M→Male, etc.)
    └── field_constraints.csv    # Validation rules (max length, required)
```

### 1.3 Why CSV Files?

- **Easy to edit**: Open in Excel, Google Sheets, or any text editor
- **Version control**: Track changes in Git
- **No code changes**: Modify mappings without touching Python
- **Team collaboration**: Business analysts can update mappings

---

## 2. Configuration Files

### 2.1 File Overview

| File | Purpose | When to Edit |
|------|---------|--------------|
| `entity_definition.csv` | Define what objects/entities exist | Adding new entities |
| `entity_mapping.csv` | Map fields between OneKey → Dataverse | Adding/changing field mappings |
| `relationship_mapping.csv` | Define parent-child relationships | Adding entity relationships |
| `value_mapping.csv` | Translate codes to values | Adding code lookups |
| `field_constraints.csv` | Set validation rules | Changing length limits, required fields |

### 2.2 CSV Format Rules

1. **First row is header** - Column names (don't change these)
2. **Comments** - Lines starting with `#` are ignored
3. **Empty values** - Leave blank (don't use NULL or N/A)
4. **Boolean values** - Use `true` or `false` (lowercase)
5. **Avoid commas in values** - Or the CSV parsing will break

---

## 3. Entity (Object) Mapping

### 3.1 Understanding `entity_definition.csv`

This file defines **what objects exist** in both OneKey and Dataverse.

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `entity_id` | Unique identifier | `ENT-DV-001` |
| `platform` | Source system | `OneKey` or `Dataverse` |
| `entity_name` | Object/table name | `account`, `Workplace` |
| `entity_set` | API collection name (Dataverse only) | `accounts`, `contacts` |
| `primary_key` | Main ID field | `accountid`, `workplaceEid` |
| `alternate_key` | Has alternate key for upsert? | `Yes` or `No` |
| `alternate_key_field` | Field used for upsert matching | `cree7_externalid` |
| `is_required` | Must this entity be synced? | `true` or `false` |
| `load_order` | Sync sequence (1=first) | `1`, `2`, `3` |
| `parent_entity` | Depends on which entity | `account` |
| `description` | Human-readable notes | Free text |

#### Current Entity Mappings

```
┌─────────────────────────────────────────────────────────────────┐
│                 OneKey → Dataverse Entity Map                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   OneKey                              Dataverse                 │
│   ────────                            ─────────                 │
│                                                                 │
│   ┌───────────────┐                   ┌───────────────┐        │
│   │  Workplace    │  ═══════════════► │   account     │        │
│   │  (HCO)        │     load_order=1  │               │        │
│   └───────────────┘                   └───────────────┘        │
│          │                                   │                  │
│          │ parent                            │ parent           │
│          ▼                                   ▼                  │
│   ┌───────────────┐                   ┌───────────────┐        │
│   │  Individual   │  ═══════════════► │   contact     │        │
│   │  + Activity   │     load_order=2  │               │        │
│   │  (HCP)        │                   │               │        │
│   └───────────────┘                   └───────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Example Entry

```csv
ENT-DV-001,Dataverse,account,accounts,accountid,Yes,cree7_externalid,true,1,,"Healthcare Organization (HCO)"
```

This means:
- **Platform**: Dataverse
- **Entity**: `account` table
- **API endpoint**: `/accounts`
- **Primary key**: `accountid` (GUID)
- **Alternate key**: `cree7_externalid` / `cree7_onekeyid` (for upsert by OneKey workplaceEid)
- **Load order**: 1 (load first, before contacts)

### 3.2 Adding a New Entity

To add a new entity (e.g., Product):

**Step 1**: Add Dataverse entity definition
```csv
ENT-DV-006,Dataverse,product,products,productid,Yes,productnumber,true,3,,"Product catalog"
```

**Step 2**: Add OneKey entity definition (if applicable)
```csv
ENT-OK-005,OneKey,Product,N/A,productEid,N/A,productEid,true,N/A,,"OneKey Product entity"
```

---

## 4. Field Mapping

### 4.1 Understanding `entity_mapping.csv`

This is the **main mapping file** that defines how each OneKey field maps to a Dataverse field.

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `mapping_id` | Unique ID (prefix indicates entity) | `HCO-001`, `HCP-003` |
| `source_platform` | Always `OneKey` | `OneKey` |
| `source_entity` | OneKey object name | `Workplace`, `Individual` |
| `source_field` | Field name from flatten_hcp/hco | `workplaceEid`, `lastName` |
| `source_field_path` | Full path in raw JSON | `workplace.workplaceEid` |
| `target_platform` | Always `Dataverse` | `Dataverse` |
| `target_entity` | Dataverse table | `account`, `contact` |
| `target_field` | Dataverse column | `cree7_externalid`, `lastname` |
| `target_field_type` | Data type | `string`, `picklist`, `lookup` |
| `max_length` | Max characters (for strings) | `50`, `160` |
| `is_required` | Must have value? | `true` / `false` |
| `is_key` | Is this the upsert key? | `true` / `false` |
| `is_lookup` | Is this a relationship? | `true` / `false` |
| `lookup_entity` | Related entity (if lookup) | `account` |
| `lookup_key_field` | Field to match (if lookup) | `cree7_externalid` |
| `transform_rule` | How to transform value | `direct`, `map:M=1\|F=2` |
| `default_value` | Fallback if empty | `Unknown` |
| `notes` | Documentation | Free text |

### 4.2 Mapping ID Naming Convention

Use prefixes to identify which entity a mapping belongs to:

| Prefix | Entity | Description |
|--------|--------|-------------|
| `HCO-*` | Workplace → account | Healthcare Organization |
| `HCP-*` | Individual → contact | Healthcare Professional |
| `AFF-*` | Activity → affiliation | Affiliation (future) |
| `ADR-*` | Address → address | Address (future) |
| `PRD-*` | Product → product | Product (future) |

### 4.3 Transform Rules

Transform rules define **how to convert** OneKey values to Dataverse format:

| Rule | Syntax | Description | Example |
|------|--------|-------------|---------|
| `direct` | `direct` | Copy value as-is | `firstName` → `firstname` |
| `map` | `map:A=1\|B=2` | Map code to value | `M` → `1` (Male) |
| `coalesce` | `coalesce:field2` | Use field2 if empty | lastName or individualEid |
| `template` | `template:Text: {value}` | Format into string | `"HCO type: Hospital"` |
| `decode` | `decode:mapType` | Lookup in value_mapping.csv | titleCode → "Dr" |
| `constant` | `constant:3` | Always use fixed value | addresstypecode = 3 |
| `append` | `append: - {value}` | Add to existing value | jobtitle = "Doctor - Cardiology" |
| `lookup_by_key` | `lookup_by_key` | Resolve to Dataverse GUID | workplaceEid → account GUID |

### 4.4 Field Mapping Examples

#### Example 1: Simple Direct Copy
```csv
HCP-002,OneKey,Individual,firstName,individual.firstName,Dataverse,contact,firstname,string,50,false,false,false,,,direct,,First name
```
- **Source**: `firstName` from OneKey Individual
- **Target**: `firstname` in Dataverse contact
- **Transform**: Direct copy (no change)

#### Example 2: Code Mapping
```csv
HCP-004,OneKey,Individual,genderCode,individual.genderCode,Dataverse,contact,gendercode,picklist,,false,false,false,,,map:M=1|F=2,,Gender mapping
```
- **Source**: `genderCode` (M or F)
- **Target**: `gendercode` (1 or 2)
- **Transform**: `M` → `1`, `F` → `2`

#### Example 3: Lookup Relationship
```csv
HCP-010,OneKey,Activity,workplaceEid,activity.workplaceEid,Dataverse,contact,parentcustomerid,lookup,,false,false,true,account,cree7_externalid,lookup_by_key,,Link to workplace
```
- **Source**: `workplaceEid` from Activity
- **Target**: `parentcustomerid` lookup field
- **Transform**: Find account where `cree7_externalid` = workplaceEid, bind to GUID

#### Example 4: Fallback/Coalesce
```csv
HCP-003,OneKey,Individual,lastName,individual.lastName,Dataverse,contact,lastname,string,50,true,false,false,,,coalesce:individualEid,Unknown,Last name with fallback
```
- **Source**: `lastName`
- **Fallback**: If empty, use `individualEid`
- **Default**: If both empty, use "Unknown"

### 4.5 Visual Field Mapping Reference

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     WORKPLACE (HCO) → ACCOUNT FIELD MAP                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  OneKey Workplace                        Dataverse account                  │
│  ─────────────────                       ─────────────────                  │
│                                                                             │
│  workplaceEid ──────── direct ────────► cree7_externalid (KEY)              │
│  officialName ──────── coalesce ──────► name                               │
│  usualName    ─────────┘                                                   │
│  typeCode ───────────── direct ────────► sic                               │
│  typeLabel ──────────── template ──────► description (part of)             │
│  activityLocationCode ─ lookup ────────► ts_category_ref                   │
│  managerWorkplaceEid ── lookup ────────► parentaccountid                   │
│  statusCode ─────────── map ───────────► statecode                         │
│  address_street ─────── direct ────────► address1_line1                    │
│  address_city ───────── direct ────────► address1_city                     │
│  address_postalCode ─── direct ────────► address1_postalcode               │
│  address_country ────── direct ────────► address1_country                  │
│  isPrivacyLaw ───────── direct ────────► msdyn_gdproptout                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    INDIVIDUAL (HCP) → CONTACT FIELD MAP                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  OneKey Individual/Activity              Dataverse contact                  │
│  ──────────────────────────              ─────────────────                  │
│                                                                             │
│  individualEid ─────── direct ─────────► externaluseridentifier (KEY)      │
│  firstName ──────────── direct ─────────► firstname                         │
│  lastName ───────────── coalesce ───────► lastname                          │
│  genderCode ─────────── map ────────────► gendercode                        │
│  title ──────────────── decode ─────────► salutation                        │
│  typeLabel ──────────── direct ─────────► jobtitle                          │
│  specialtyLabel ─────── append ─────────► jobtitle (appended)               │
│  statusCode ─────────── map ────────────► statecode                         │
│  workplaceEid (Act) ─── lookup ─────────► parentcustomerid                  │
│  usualName (Wkp) ────── direct ─────────► department                        │
│  address_street ─────── direct ─────────► address1_line1                    │
│  address_city ───────── direct ─────────► address1_city                     │
│  isPrivacyLaw ───────── direct ─────────► msdyn_gdproptout                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Relationship Mapping

### 5.1 Understanding `relationship_mapping.csv`

This file defines **how entities relate to each other** and the order they must be loaded.

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `relationship_id` | Unique identifier | `REL-001` |
| `relationship_name` | Human-readable name | `HCO Parent Hierarchy` |
| `source_entity` | Child entity | `contact` |
| `source_field` | Child's key field | `externaluseridentifier` |
| `target_entity` | Parent entity | `account` |
| `target_field` | Lookup field name | `parentcustomerid` |
| `relationship_type` | Type of relationship | `many_to_one`, `self_referential` |
| `lookup_key_field` | Parent's match field | `cree7_externalid` |
| `is_required` | Must parent exist? | `true` / `false` |
| `load_order` | Processing sequence | `1`, `2` |
| `notes` | Technical notes | Free text |
| `description` | Business description | Free text |

### 5.2 Relationship Types

| Type | Description | Example |
|------|-------------|---------|
| `self_referential` | Entity relates to itself | Account → parent Account |
| `many_to_one` | Many children to one parent | Contact → Account |
| `one_to_many` | One parent to many children | Account → Contacts |
| `many_to_many` | Requires junction table | HCP → multiple HCOs |

### 5.3 Current Relationships

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          RELATIONSHIP DIAGRAM                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                        ┌─────────────────┐                                  │
│                        │     account     │                                  │
│                        │ (cree7_externalid) │                                │
│                        └────────┬────────┘                                  │
│                                 │                                           │
│              ┌──────────────────┼──────────────────┐                       │
│              │                  │                  │                        │
│              ▼                  ▼                  ▼                        │
│    ┌─────────────────┐  ┌─────────────┐  ┌───────────────────┐            │
│    │ account (child) │  │   contact   │  │ account (partner) │            │
│    │ parentaccountid │  │ parent      │  │ msa_managing      │            │
│    │                 │  │ customerid  │  │ partnerid         │            │
│    └─────────────────┘  └─────────────┘  └───────────────────┘            │
│                                                                             │
│    REL-001: HCO Hierarchy    REL-002: HCP     REL-003: Managing            │
│    (self_referential)        to Workplace     Partner (optional)           │
│                              (many_to_one)    (many_to_one)                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.4 Load Order Importance

**Why order matters**: You cannot create a Contact linked to an Account that doesn't exist yet.

| Load Order | Entity | Reason |
|------------|--------|--------|
| 1 | `account` (root) | No dependencies |
| 1 | `account` (child) | Depends on parent account |
| 2 | `contact` | Depends on account existing |
| 3 | `fc_affiliation` | Depends on both account and contact |

---

## 6. Value Transformations

Country-specific OneKey lists (specialty, HCO type, title, role, and the other large lists) are stored in the code catalog, not in `value_mapping.csv`. Build steps, languages, and the three tables are in [PICKLIST_GUIDE.md](PICKLIST_GUIDE.md). This section remains the reference for short choices such as gender and status.

### 6.1 Understanding `value_mapping.csv`

This file defines **how to translate codes** from OneKey to Dataverse picklist values for those short choices.

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `mapping_id` | Unique identifier | `VAL-GEN-001` |
| `mapping_type` | Category of mapping | `gendercode`, `statecode` |
| `source_code` | OneKey code | `M`, `3` |
| `source_value` | OneKey label (for reference) | `Male`, `Valid` |
| `target_value` | Dataverse value | `1`, `0` |
| `target_label` | Dataverse label (for reference) | `Male`, `Active` |
| `notes` | Additional context | Free text |

### 6.2 Current Value Mappings

#### Gender Code
| OneKey | Dataverse | Label |
|--------|-----------|-------|
| `M` | `1` | Male |
| `F` | `2` | Female |

#### Status Code (statecode)
| OneKey | Dataverse | Meaning |
|--------|-----------|---------|
| `3` | `0` | Valid → Active |
| `9` | `1` | Invalid → Inactive |
| `1` | `1` | Created → Inactive |
| `6` | `1` | Deceased → Inactive |

#### Title Code
| OneKey | Dataverse | Title |
|--------|-----------|-------|
| `01` | `Mr` | Mr |
| `02` | `Mrs` | Mrs |
| `03` | `Dr` | Doctor |
| `04` | `Prof` | Professor |
| `05` | `Ms` | Ms |

#### HCO Activity Location
| OneKey | Dataverse | Type |
|--------|-----------|------|
| `H` | `11` | Hospital |
| `S` | `12` | Surgery/Practice |
| `N` | `13` | NHS Establishment |
| `O` | `14` | Office |
| `P` | `15` | Pharmacy |

### 6.3 Adding New Value Mappings

To add a new code translation:

```csv
VAL-NEW-001,newcodetype,SOURCE,Source Label,TARGET,Target Label,Description
```

Example - Adding a new HCP type:
```csv
VAL-TYP-008,typecode,T,Technician,Technician,Lab Technician,New HCP type for lab techs
```

---

## 7. Field Constraints

### 7.1 Understanding `field_constraints.csv`

This file defines **validation rules** for Dataverse fields.

#### Column Reference

| Column | Description | Example |
|--------|-------------|---------|
| `constraint_id` | Unique identifier | `CON-CON-003` |
| `platform` | Always `Dataverse` | `Dataverse` |
| `entity` | Table name | `contact`, `account` |
| `field` | Column name | `lastname`, `name` |
| `constraint_type` | Type of constraint | `max_length`, `required` |
| `constraint_value` | Constraint value | `50`, `true` |
| `error_action` | What to do on violation | `truncate`, `error`, `warn` |
| `notes` | Additional context | Free text |

### 7.2 Constraint Types

| Type | Description | Action Options |
|------|-------------|----------------|
| `max_length` | Maximum string length | `truncate`, `error` |
| `required` | Must have value | `error` |
| `pattern` | Regex pattern (future) | `error`, `warn` |

### 7.3 Current Key Constraints

| Entity | Field | Constraint | Value | Action |
|--------|-------|------------|-------|--------|
| account | cree7_externalid | max_length | 100 | truncate |
| account | name | max_length | 160 | truncate |
| account | name | required | true | error |
| contact | externaluseridentifier | max_length | 50 | truncate |
| contact | lastname | max_length | 50 | truncate |
| contact | lastname | required | true | error |
| contact | firstname | max_length | 50 | truncate |

---

## 8. Step-by-Step Examples

### 8.1 Example: Add a New Field to Contact

**Scenario**: Map OneKey `mobilePhone` to Dataverse `mobilephone`

**Step 1**: Add to `entity_mapping.csv`
```csv
HCP-022,OneKey,Individual,mobilePhone,individual.telephones.3_1.callNumber,Dataverse,contact,mobilephone,string,50,false,false,false,,,direct,,Mobile phone number
```

**Step 2**: Add constraint to `field_constraints.csv`
```csv
CON-CON-025,Dataverse,contact,mobilephone,max_length,50,truncate,Mobile phone
```

**Step 3**: Verify
```python
config = load_mapping_config(Path("config"))
mobile_mapping = [m for m in config.get_field_mappings("contact") if m.target_field == "mobilephone"]
print(mobile_mapping)
```

### 8.2 Example: Add a New Picklist Mapping

**Scenario**: Map OneKey specialty codes to Dataverse

**Step 1**: Add value mappings to `value_mapping.csv`
```csv
VAL-SPC-001,specialtycode,46,Radiology,100000001,Radiology,Radiology specialty
VAL-SPC-002,specialtycode,01,General Practice,100000002,General Practice,GP specialty
VAL-SPC-003,specialtycode,12,Cardiology,100000003,Cardiology,Cardiology specialty
```

**Step 2**: Update field mapping to use decode
```csv
HCP-023,OneKey,Individual,specialtyCode,individual.ada.SP_1.code,Dataverse,contact,fc_specialtycode,picklist,,false,false,false,,,decode:specialtycode,,Primary specialty code
```

### 8.3 Example: Add a New Entity (Affiliation)

**Scenario**: Create separate Affiliation records for multi-workplace HCPs

**Step 1**: Add entity to `entity_definition.csv`
```csv
ENT-DV-006,Dataverse,fc_affiliation,fc_affiliations,fc_affiliationid,Yes,fc_affiliationkey,true,3,contact,"HCP-HCO Affiliation"
```

**Step 2**: Add relationships to `relationship_mapping.csv`
```csv
REL-006,Affiliation to HCP,fc_affiliation,fc_affiliationkey,contact,fc_hcpid,many_to_one,externaluseridentifier,true,3,"Affiliation belongs to HCP",Links affiliation to healthcare professional
REL-007,Affiliation to HCO,fc_affiliation,fc_affiliationkey,account,fc_hcoid,many_to_one,cree7_externalid,true,3,"Affiliation belongs to HCO",Links affiliation to healthcare organization
```

**Step 3**: Add field mappings to `entity_mapping.csv`
```csv
AFF-001,OneKey,Activity,activityEid,activity.activityEid,Dataverse,fc_affiliation,fc_affiliationkey,string,50,true,true,false,,,direct,,Primary key
AFF-002,OneKey,Activity,individualEid,activity.individualEid,Dataverse,fc_affiliation,fc_hcpid,lookup,,true,false,true,contact,externaluseridentifier,lookup_by_key,,Link to HCP
AFF-003,OneKey,Activity,workplaceEid,activity.workplaceEid,Dataverse,fc_affiliation,fc_hcoid,lookup,,true,false,true,account,cree7_externalid,lookup_by_key,,Link to HCO
AFF-004,OneKey,Activity,roleLabel,activity.roleLabel,Dataverse,fc_affiliation,fc_role,string,100,false,false,false,,,direct,,Role at workplace
AFF-005,OneKey,Activity,isMainActivity,activity.isMainActivity,Dataverse,fc_affiliation,fc_ismain,boolean,,false,false,false,,,direct,,Is primary affiliation
AFF-006,OneKey,Activity,statusCode,activity.statusCode,Dataverse,fc_affiliation,statecode,state,,false,false,false,,,map:3=0|9=1,,Status
```

**Step 4**: Add constraints to `field_constraints.csv`
```csv
CON-AFF-001,Dataverse,fc_affiliation,fc_affiliationkey,max_length,50,truncate,Affiliation key
CON-AFF-002,Dataverse,fc_affiliation,fc_role,max_length,100,truncate,Role name
```

---

## 9. Best Practices

### 9.1 Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Entity ID | `ENT-{platform}-{number}` | `ENT-DV-001` |
| Mapping ID (HCO) | `HCO-{number}` | `HCO-001` |
| Mapping ID (HCP) | `HCP-{number}` | `HCP-003` |
| Value Mapping ID | `VAL-{type}-{number}` | `VAL-GEN-001` |
| Constraint ID | `CON-{entity}-{number}` | `CON-CON-003` |
| Relationship ID | `REL-{number}` | `REL-002` |

### 9.2 Documentation

- Always fill the `notes` or `description` column
- Include the OneKey VR field reference when known
- Document any business rules or exceptions

### 9.3 Testing Changes

After modifying CSV files:

```python
from integration import load_mapping_config
from pathlib import Path

# Load and verify
config = load_mapping_config(Path("config"))

# Check counts
print(f"Field mappings: {len(config.field_mappings)}")
print(f"Value mappings: {len(config.value_mappings)}")
print(f"Constraints: {len(config.field_constraints)}")

# Validate specific entity
account_fields = config.get_field_mappings("account")
print(f"Account fields: {len(account_fields)}")
```

### 9.4 Version Control

- Commit CSV changes with descriptive messages
- Include ticket/issue number if applicable
- Review changes before merging

---

## 10. Troubleshooting

### 10.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Mapping not loading | Comma in value breaking CSV | Use underscore instead of comma in field paths |
| Value not translating | Wrong mapping_type | Check `decode:type` matches `mapping_type` in value_mapping.csv |
| Lookup failing | Parent not loaded first | Check `load_order` in entity_definition.csv |
| Truncation | Field exceeds max_length | Add constraint with `error_action: truncate` |

### 10.2 Debugging

**Check if mapping is loaded:**
```python
config = load_mapping_config(Path("config"))

# Find specific mapping
my_mapping = [m for m in config.field_mappings if m.mapping_id == "HCP-003"]
print(my_mapping)
```

**Check value mapping:**
```python
gender_map = config._value_map.get("gendercode", {})
print(f"M maps to: {gender_map.get('M')}")
```

**Check constraint:**
```python
max_len = config.get_max_length("contact", "lastname")
print(f"Max length: {max_len}")
```

### 10.3 Getting Help

If you encounter issues:
1. Check the column headers match expected format
2. Verify no extra commas or special characters
3. Run the validation script
4. Check the sync logs for detailed errors

---

## Appendix A: OneKey Field Reference

### A.1 Flattened HCO (Workplace) Fields

These field names come from `flatten_hco()` in `onekey/parse.py`:

| Field | Source Path | Description |
|-------|-------------|-------------|
| `workplaceEid` | workplace.workplaceEid | Unique workplace ID |
| `officialName` | workplace.officialName | Legal/official name |
| `usualName` | workplace.usualName | Common name |
| `typeCode` | workplace.typeCode | HCO type (HAU, ACT, etc.) |
| `typeLabel` | workplace.typeCorporateLabel | Type description |
| `activityLocationCode` | workplace.activityLocationCode | Location type (H, S, N) |
| `statusCode` | workplace.statusCode | Status (3=Valid, 9=Invalid) |
| `parentWorkplaceEid` | workplace.managerWorkplaceEid | Parent HCO ID |
| `address_street` | workplaceAddresses.P_1.address.addressLongLabel | Street address |
| `address_city` | workplaceAddresses.P_1.address.villageLabel | City |
| `address_postalCode` | workplaceAddresses.P_1.address.longPostalCode | Postal code |
| `address_country` | workplaceAddresses.P_1.country | Country |

### A.2 Flattened HCP (Individual) Fields

These field names come from `flatten_hcp()` in `onekey/parse.py`:

| Field | Source Path | Description |
|-------|-------------|-------------|
| `individualEid` | individual.individualEid | Unique individual ID |
| `activityEid` | activity.activityEid | Activity ID |
| `firstName` | individual.firstName | First name |
| `lastName` | individual.lastName | Last name |
| `genderCode` | individual.genderCode | Gender (M/F) |
| `title` | individual.titleCode | Title code |
| `typeCode` | individual.typeCode | HCP type (M, P, D, etc.) |
| `typeLabel` | individual.typeCorporateLabel | Type description |
| `specialtyCode` | individual.ada.SP_1.code | Primary specialty code |
| `specialtyLabel` | individual.ada.SP_1.codeCorporateLabel | Specialty description |
| `statusCode` | individual.statusCode | Status (3=Valid) |
| `workplaceEid` | activity.workplaceEid | Linked workplace ID |
| `workplaceName` | workplace.officialName | Workplace name |

---

## Appendix B: Dataverse Field Reference

### B.1 Account (HCO) Key Fields

| Field | Type | Max Length | Required | Notes |
|-------|------|------------|----------|-------|
| `cree7_externalid` | String | 100 | Yes (key) | Alternate key cree7_onekeyid for upsert |
| `name` | String | 160 | Yes | Organization name |
| `statecode` | State | - | No | 0=Active, 1=Inactive |
| `parentaccountid` | Lookup | - | No | Parent account |
| `address1_line1` | String | 250 | No | Street address |
| `address1_city` | String | 80 | No | City |

### B.2 Contact (HCP) Key Fields

| Field | Type | Max Length | Required | Notes |
|-------|------|------------|----------|-------|
| `externaluseridentifier` | String | 50 | Yes (key) | Alternate key for upsert |
| `firstname` | String | 50 | No | First name |
| `lastname` | String | 50 | Yes | Last name |
| `gendercode` | Picklist | - | No | 1=Male, 2=Female |
| `parentcustomerid` | Lookup | - | No | Company/Account |
| `statecode` | State | - | No | 0=Active, 1=Inactive |

---

*Document Version: 1.0*  
*Last Updated: September 2026*
