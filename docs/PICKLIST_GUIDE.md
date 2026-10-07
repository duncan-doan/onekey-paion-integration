# OneKey Picklists in Dataverse

This guide is the build reference for OneKey code lists in Dataverse. It covers every list, including specialty and HCO type, for these countries:

| Country | OneKey `codBase` | Language | Dataverse LCID |
|---------|------------------|----------|----------------|
| United Kingdom | `WUK` | English | 1033 |
| Germany | `WDE` | German | 1031 |
| Netherlands | `WNL` | Dutch | 1043 |
| Denmark | `WDK` | Danish | 1030 |
| Norway | `WNO` | Norwegian Bokmål | 1044 |
| Sweden | `WSE` | Swedish | 1053 |
| Finland | `WFI` | Finnish | 1035 |

Counts in this guide come from the German exports (`WDE_*.json` and `output/codelists/DE_picklists.csv`). The other six country files are loaded with the same rules when they are available.

The current sync (`integration/picklist_sync.py`, `config/value_mapping.csv`, `config/picklist_targets.csv`) publishes one English label (LCID 1033) and keys the row by local code only. That is enough for gender and status. It is not enough for specialty, HCO type, title, or any list whose code changes meaning by country.

---

## 1. What a OneKey code contains

Each code on a list has three identifiers. They are not interchangeable.

| Field | Example (German specialty `01`) | Use |
|-------|----------------------------------|-----|
| `externalId` | `01` | Local code stored on the workplace or person. Meaningful only inside that country. |
| `miCode` | `000J` | Stable id of that value **inside this list**. Same `miCode` on the same list in two countries is the same value. |
| `corporateEid` | `1SP.0100` | Global parent, when OneKey has aligned the local code to an international list. Empty when there is no global parent. |

`miCode` is unique inside one list. It is reused on other lists, so always read it with the list name. `ENTITIES_LINK` `DOU` is also `miCode` `000J`, and that value means "Is duplicate of", not General Medicine.

`corporateEid` is the "same as global" link. German `LEX` `H` points at `1LX.0020`. German `LEX` `C` has `corporateEid` null.

---

## 2. Three cases, on every list

Group codes by **list + `miCode`**. Country and language are attached to that row.

### 2.1 Same local code, different meaning

The local code is only a key inside one country. German `SP` `01` is `miCode` `000J`. Another country's `01` joins that row only when its `miCode` is also `000J`.

UK `LEX` `C` is `miCode` `00CH` (Retail Chemist). German `LEX` `C` is `miCode` `0018` (Cabinet / Praxis). Those are two values.

Decode with **country + list + local code**. A map keyed only by `01` cannot be shared across the seven countries. `config/value_mapping.csv` currently has about 200 specialty rows in that shape.

### 2.2 Same value as the global list

`corporateEid` is stored as the parent. It does not replace the local value.

German specialty `01` (Allgemeinmedizin, `miCode` `000J`) and `L0` (Praktischer Arzt, `miCode` `03DT`) both point at `1SP.0100`. They stay two rows with the same parent.

German HCO types `ABT` (Abteilung) and `AKH` (Allgemeines Krankenhaus) both point at `1TE.0020A`. They stay two rows. Collapsing them would replace those labels with "Hospital (General)".

Countries that share a `miCode` add a language and a local code to the existing row. They do not create another selectable value.

### 2.3 Code missing in other countries

German `SP` `46` (Radiologie) and `TET` `CAB` (Praxis) have no global parent. Title (`TIT`), professional type (`TYP`), and legal category (`CAJ`) have no `corporateEid` in the German extract. A country row is created only where the file contains that `miCode`. The other countries leave the field empty.

---

## 3. Choice or lookup

Microsoft Dataverse can hold thousands of options on one choice. The [metadata guidance](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/create-edit-metadata) says not to use a choice as the dropdown for lists of that size. Global specialty `1SP` is already 594 values. Germany alone has 245 local specialties.

A multi-select choice can have at most **150 options selected on one record**.

| OneKey list | German codes | Global parent | Dataverse storage | Record field |
|-------------|--------------|---------------|-------------------|--------------|
| `SP` | 245 | 227 link to 155 of 594 `1SP` values; 18 have none | Lookup | `ts_specialty1`–`ts_specialty5` on account and contact |
| `TET` | 104 | 93 link to 35 of 36 `1TE` values; 11 have none | Lookup | `account.ts_type` |
| `TIH` | 86 | all 86 link to 35 `1TH` values | Lookup | affiliation role |
| `TSR` | 10 | all 10 link to 7 `1TS` values | Lookup | `account.ts_structuretype` |
| `LEX` | 14 | 8 link to `1LX`; 6 have none | Lookup | activity location |
| `TIT` | 146 | none | Lookup | `contact.ts_title` |
| `TYP` | 22 | none | Lookup | `contact.ts_type` |
| `ENTITIES_LINK` | 38 in Germany, 17 in the UK | mixed | Lookup | `ts_affiliation.ts_linktype` |
| `CAJ` | 34 | none | Lookup | legal category |
| Language, gender, status | a handful, same meaning in every country | n/a | Global choice | existing choice columns |

Gender and status stay on the current `value_mapping.csv` choice path. Every other OneKey list uses the catalog below.

---

## 4. Catalog tables

Create these in a solution. Prefix `ts_`.

### 4.1 `ts_okcode`

One row per list + `miCode`.

| Column | Type | Required | Example |
|--------|------|----------|---------|
| `ts_name` | Primary name | yes | `General Medicine` |
| `ts_list` | Text (100) | yes | `SP` |
| `ts_micode` | Text (20) | yes | `000J` |
| `ts_corporateeid` | Text (50) | no | `1SP.0100` |

Alternate key: `ts_list` + `ts_micode`.

`ts_name` is the English base label (LCID 1033). The lookup shows this name. Translated text lives in `ts_oklabel`.

### 4.2 `ts_oklabel`

One row per code and language.

| Column | Type | Required | Example |
|--------|------|----------|---------|
| `ts_name` | Primary name | yes | `SP 000J 1031` |
| `ts_code` | Lookup → `ts_okcode` | yes | the `000J` row |
| `ts_lcid` | Whole number | yes | `1031` |
| `ts_label` | Text (200) | yes | `Allgemeinmedizin` |

Alternate key: `ts_code` + `ts_lcid`.

Trim OneKey labels before writing. Some exports pad `longLabel` with spaces.

### 4.3 `ts_okcountry`

One row per country and local code. Several countries can point at one `ts_okcode`.

| Column | Type | Required | Example |
|--------|------|----------|---------|
| `ts_name` | Primary name | yes | `WDE SP 01` |
| `ts_code` | Lookup → `ts_okcode` | yes | the `000J` row |
| `ts_codbase` | Text (10) | yes | `WDE` |
| `ts_list` | Text (100) | yes | `SP` |
| `ts_externalid` | Text (50) | yes | `01` |

Alternate key: `ts_codbase` + `ts_list` + `ts_externalid`.

---

## 5. Build steps

### Step 1. Provision languages

In the Power Platform admin center, add German, Dutch, Danish, Norwegian Bokmål, Swedish, and Finnish. English 1033 stays the base language. A label for a language that is not provisioned is never shown.

### Step 2. Create the three tables

Create `ts_okcode`, `ts_oklabel`, and `ts_okcountry` with the columns and alternate keys in section 4. Publish the solution.

### Step 3. Load code lists

For each `codBase` in `WUK`, `WDE`, `WNL`, `WDK`, `WNO`, `WSE`, `WFI`:

1. Fetch each list in that country's `isoLanguage` (`en`, `de`, `nl`, `da`, `nb`, `sv`, `fi`) so `longLocalizedLabel` is the local text.
2. For each code, upsert `ts_okcode` on list + `miCode`. Set `ts_corporateeid` when present.
3. Upsert `ts_oklabel` for that country's LCID from `longLocalizedLabel`. When loading `WUK`, also write LCID 1033 from `longLabel`, and copy that English text into `ts_okcode.ts_name`.
4. Upsert `ts_okcountry` with `codBase`, list, and `externalId`.

Run this before any HCO or HCP sync. A record cannot reference a code that is not loaded.

Worked result for German specialty `01`:

| Table | Row |
|-------|-----|
| `ts_okcode` | list `SP`, miCode `000J`, parent `1SP.0100`, name `General Medicine` |
| `ts_oklabel` | `1033` = General Medicine, `1031` = Allgemeinmedizin |
| `ts_okcountry` | `WDE` + `SP` + `01` → that code |

`SP` `L0` is a second `ts_okcode` (`miCode` `03DT`, parent still `1SP.0100`). Dutch, Danish, Norwegian, Swedish, and Finnish labels are added on the same `000J` row only when that country's file has `miCode` `000J`.

### Step 4. Add lookups on the business tables

Replace the large choice columns with a lookup to `ts_okcode`.

| OneKey source | Lookup |
|---------------|--------|
| `workplace.typeCode` (`TET`) | `account.ts_type` |
| `workplace.structureTypeCode` (`TSR`) | `account.ts_structuretype` |
| `workplace` / `individual` specialty `SP_1`–`SP_5` | `ts_specialty1`–`ts_specialty5` |
| `individual.titleCode` (`TIT`) | `contact.ts_title` |
| `individual.typeCode` (`TYP`) | `contact.ts_type` |
| `activity` role `TIH_1`–`TIH_10` | affiliation role lookup |
| `workplace.relations[*].linkCode` | `ts_affiliation.ts_linktype` |

Each lookup has a view filtered to its list, for example `ts_list = SP`. On forms where a person edits the field, further restrict the view to `ts_okcountry` rows for the record's `codBase`. Integration-written fields can stay read-only: the sync already picks the row for that country.

### Step 5. Show the user language

The lookup displays `ts_name` (English). Add a read-only text column beside each lookup, for example `ts_specialty1label`. On the form, set it from `ts_oklabel.ts_label` where `ts_lcid` is the current user's language. A German user then sees Allgemeinmedizin. Until a language file is loaded, leave the label empty and the form shows `ts_name`.

Views that must be read in a local language use that label column.

### Step 6. Keep short lists as choices

Language, gender, and status stay global choices. Publish every language you have:

```json
{
  "OptionSetName": "ts_preferredlanguage",
  "Value": 126870001,
  "MergeLabels": true,
  "Label": {
    "LocalizedLabels": [
      { "LanguageCode": 1033, "Label": "German" },
      { "LanguageCode": 1031, "Label": "Deutsch" }
    ]
  }
}
```

`MergeLabels: true` updates only the languages in the request. `integration/picklist_sync.py` currently sends LCID 1033 only (`LANG = 1033`). Extend that payload before these choices match the catalog.

Two options in one choice cannot share the same text in the same language. Suffix only the colliding label, and only in that language.

### Step 7. Resolve codes during record sync

Lookup key: `ts_codbase` + `ts_list` + `ts_externalid`.

A German workplace with specialty `01` stores the `ts_okcode` row for `SP` / `000J`. The contact does not store the word Allgemeinmedizin. A country with no `ts_okcountry` row leaves the lookup empty and the code is written to the picklist gap log, same as an unmapped choice today.

`entity_mapping.csv` transform for these fields changes from `decode:specialty` to a lookup on `ts_okcountry`. Gender and status keep `decode:` / `map:`.

---

## 6. What the sync must not do

- Key a specialty or HCO type by local code alone. `01` is not one value across seven countries.
- Replace `ts_oklabel` for 1031 when loading Sweden. Each language is its own row.
- Copy `corporateLongLabel` over the local label. The parent is for reporting. The selectable value stays Abteilung, not "Hospital (General)".
- Insert a code into a country that did not send it.
- Publish specialty or HCO type as a choice of thousands of options.

---

## 7. Reporting

Group by `ts_okcode.ts_corporateeid` when a cross-country total is required. `1SP.0100` then includes both Allgemeinmedizin and Praktischer Arzt, in whatever language the viewer uses for each row. Codes with an empty parent, such as Radiologie `46`, appear only in the countries that have them.

---

## 8. Related files

| File | Role today | After this guide |
|------|------------|------------------|
| `config/value_mapping.csv` | Local code → choice integer, one label | Gender, status, and other short choices only |
| `config/picklist_targets.csv` | Choice to update in Phase 0 | Short choices only. Large lists are catalog lookups |
| `integration/picklist_sync.py` | Inserts option labels for LCID 1033 | Same path, with every provisioned LCID, for short choices |
| `docs/MAPPING_CONFIGURATION_GUIDE.md` | Field and value mapping | Still the reference for `decode:` and `map:` |
| `docs/DYNAMIC_SYNC_GUIDE.md` | Phase 0 runs before HCO/HCP sync | Catalog load is the new Phase 0 for large lists |
