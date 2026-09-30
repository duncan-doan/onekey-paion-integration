# Dataverse trace tables

Setup guide for the two custom tables that store OneKey sync job summaries and failed source-record IDs.

These tables are operational. They are not part of the HCO/HCP data model (`account`, `contact`, `customeraddress`, `ts_affiliation`). The integration creates one **Integration Job** row when a job starts, updates it as phases complete, and writes one **Integration Failure** row per failed OneKey record.

Use the publisher that already owns `ts_affiliation`. Every schema name below already includes the `ts_` prefix. Create the columns with these exact schema names and choice values. The sync job will send the integer values, not the labels.

## Table settings (both tables)

Apply these settings to both tables.

| Setting | Value |
| --- | --- |
| Ownership | Organization |
| Record ownership | No owner column |
| Duplicate detection | Off |
| Activities | Off |
| Notes | Off |
| Auditing | On |
| Change tracking | Off |
| Queues, connections, feedback, sharepoint | Off |

Organization ownership is required so the integration application user can create rows without an Assign privilege.

Do not use the system `statuscode` for job outcome. A failed job must stay Active (`statecode` = Active) so it remains visible. Business outcome is stored in `ts_status` on Integration Job and `ts_retrystatus` on Integration Failure.

Date and time columns: behavior **Time zone independent**, format **Date and time**.

## 1. Integration Job

One row per job execution.

| Property | Value |
| --- | --- |
| Display name | Integration Job |
| Plural name | Integration Jobs |
| Schema name | `ts_integrationjob` |
| Logical name | `ts_integrationjob` |
| Entity set | `ts_integrationjobs` |
| Primary column display name | Name |
| Primary column schema name | `ts_name` |
| Primary column max length | 200 |

### Columns

`ts_name` is the primary name column created with the table. Increase its length to 200. Add the remaining columns.

| Schema name | Display name | Type | Length / format | Required | Default |
| --- | --- | --- | --- | --- | --- |
| `ts_name` | Name | Text | 200 | Required | |
| `ts_jobid` | Job ID | Text | 36 | Required | |
| `ts_jobtype` | Job type | Choice (local) | | Required | |
| `ts_codebase` | Codebase | Text | 10 | Required | |
| `ts_status` | Status | Choice (local) | | Required | Running (126870001) |
| `ts_startedon` | Started on | Date and time | Time zone independent | Required | |
| `ts_completedon` | Completed on | Date and time | Time zone independent | Optional | |
| `ts_durationseconds` | Duration (seconds) | Whole number | Min 0 | Optional | |
| `ts_watermarkfrom` | Watermark from | Date and time | Time zone independent | Optional | |
| `ts_watermarkto` | Watermark to | Date and time | Time zone independent | Optional | |
| `ts_dryrun` | Dry run | Yes/No | | Required | No |
| `ts_processed` | Processed | Whole number | Min 0 | Required | 0 |
| `ts_createdcount` | Created | Whole number | Min 0 | Required | 0 |
| `ts_updatedcount` | Updated | Whole number | Min 0 | Required | 0 |
| `ts_skipped` | Skipped | Whole number | Min 0 | Required | 0 |
| `ts_failedcount` | Failed | Whole number | Min 0 | Required | 0 |
| `ts_phasecounts` | Phase counts | Multiline text | Plain text, 10,000 | Optional | |
| `ts_lastheartbeat` | Last heartbeat | Date and time | Time zone independent | Optional | |
| `ts_errorsummary` | Error summary | Multiline text | Plain text, 2,000 | Optional | |
| `ts_trigger` | Trigger | Choice (local) | | Optional | |
| `ts_retryofjob` | Retry of job | Lookup | See relationship below | Optional | |

`ts_phasecounts` holds a JSON object of per-phase counts. `ts_errorsummary` is a short message for the job. Do not store the failed-record list in either column.

### Choice: Job type (`ts_jobtype`)

| Value | Label |
| --- | --- |
| 126870001 | Full |
| 126870002 | Delta |
| 126870003 | Affiliations Only |
| 126870004 | HCO Only |
| 126870005 | HCP Only |
| 126870006 | Addresses Only |
| 126870007 | Hierarchy Only |

### Choice: Status (`ts_status`)

| Value | Label |
| --- | --- |
| 126870001 | Running |
| 126870002 | Succeeded |
| 126870003 | Partially Succeeded |
| 126870004 | Failed |
| 126870005 | Abandoned |

Default: **Running** (126870001).

### Choice: Trigger (`ts_trigger`)

| Value | Label |
| --- | --- |
| 126870001 | Manual |
| 126870002 | Timer |
| 126870003 | Daemon |

### Alternate key

| Key name | Column | Purpose |
| --- | --- | --- |
| `ts_jobid_key` | `ts_jobid` | One row per job. The sync upserts by this value. |

Wait until the key status is **Active** before the integration writes to the table.

### Relationship

Create this after **Integration Failure** exists, or create the lookup from that table. The self-lookup below belongs on this table.

| Property | Value |
| --- | --- |
| Schema name | `ts_integrationjob_retryofjob` |
| Type | Self-referential, many-to-one |
| Lookup column | `ts_retryofjob` |
| Web API navigation property | `ts_Retryofjob` |
| Related table | `ts_integrationjob` |
| Behavior | Referential, Restrict Delete |
| Display name | Retry of job |

Restrict Delete stops someone from deleting a job that later jobs point at.

## 2. Integration Failure

One row per failed source record, linked to the job that produced it.

| Property | Value |
| --- | --- |
| Display name | Integration Failure |
| Plural name | Integration Failures |
| Schema name | `ts_integrationfailure` |
| Logical name | `ts_integrationfailure` |
| Entity set | `ts_integrationfailures` |
| Primary column display name | Name |
| Primary column schema name | `ts_name` |
| Primary column max length | 250 |

### Columns

| Schema name | Display name | Type | Length / format | Required | Default |
| --- | --- | --- | --- | --- | --- |
| `ts_name` | Name | Text | 250 | Required | |
| `ts_failurekey` | Failure key | Text | 300 | Required | |
| `ts_integrationjob` | Integration Job | Lookup | See relationship below | Required | |
| `ts_sourceid` | Source ID | Text | 100 | Required | |
| `ts_externalid` | External ID | Text | 200 | Optional | |
| `ts_entitytype` | Entity type | Choice (local) | | Required | |
| `ts_phase` | Phase | Choice (local) | | Required | |
| `ts_operation` | Operation | Choice (local) | | Required | |
| `ts_errorcategory` | Error category | Choice (local) | | Required | |
| `ts_httpstatus` | HTTP status | Whole number | Min 0, max 599 | Optional | |
| `ts_errorcode` | Error code | Text | 100 | Optional | |
| `ts_errormessage` | Error message | Multiline text | Plain text, 2,000 | Required | |
| `ts_isretryable` | Retryable | Yes/No | | Required | No |
| `ts_retrystatus` | Retry status | Choice (local) | | Required | Pending (126870001) |
| `ts_attemptcount` | Attempt count | Whole number | Min 1 | Required | 1 |
| `ts_occurredon` | Occurred on | Date and time | Time zone independent | Required | |

`ts_sourceid` is the OneKey identifier (workplace EID, individual EID, or activity EID). `ts_externalid` is the Dataverse alternate key that was attempted when it is different from the source ID, for example a composite address or affiliation key.

`ts_failurekey` is `{jobId}|{phase}|{sourceId}|{operation}`. The sync uses it to upsert the same failure if a phase is retried inside one job. Do not put a payload or stack trace in `ts_errormessage`.

### Choice: Entity type (`ts_entitytype`)

| Value | Label |
| --- | --- |
| 126870001 | Account |
| 126870002 | Contact |
| 126870003 | Customer Address |
| 126870004 | Affiliation |

### Choice: Phase (`ts_phase`)

| Value | Label |
| --- | --- |
| 126870001 | Picklists |
| 126870002 | HCO |
| 126870003 | Addresses |
| 126870004 | Hierarchy |
| 126870005 | HCP |
| 126870006 | HCP Activity |

HCO–HCO relation failures use phase **HCO**. There is no separate HCO Relations choice on the table.

### Choice: Operation (`ts_operation`)

| Value | Label |
| --- | --- |
| 126870001 | Validate |
| 126870002 | Create |
| 126870003 | Update |
| 126870004 | Upsert |
| 126870005 | Relate |

### Choice: Error category (`ts_errorcategory`)

| Value | Label |
| --- | --- |
| 126870001 | Validation |
| 126870002 | Mapping |
| 126870003 | Lookup |
| 126870004 | Timeout |
| 126870005 | Unknown |

Dataverse HTTP errors use **Unknown** and store the status on `ts_httpstatus`. Timeouts use **Timeout**.

### Choice: Retry status (`ts_retrystatus`)

| Value | Label |
| --- | --- |
| 126870001 | Pending |
| 126870002 | Retried |
| 126870003 | Resolved |
| 126870004 | Ignored |

Default: **Pending** (126870001).

### Alternate key

| Key name | Column | Purpose |
| --- | --- | --- |
| `ts_failurekey_key` | `ts_failurekey` | Upsert one row per failed record inside a job. |

Wait until the key status is **Active**.

### Relationship to Integration Job

Create the lookup from Integration Failure.

| Property | Value |
| --- | --- |
| Schema name | `ts_integrationfailure_job` |
| Type | Many-to-one |
| Lookup column on Integration Failure | `ts_integrationjob` |
| Web API navigation property | `ts_IntegrationJob` |
| Related table | `ts_integrationjob` |
| Behavior | Parental |
| Delete | Cascade All |

Deleting an Integration Job deletes its Integration Failure rows.

## Security

Grant these privileges to the integration application user. No Assign privilege is required.

| Table | Privileges |
| --- | --- |
| Integration Job | Create, Read, Write, Append, Append To |
| Integration Failure | Create, Read, Write, Append |

Delete is only needed for a cleanup job. Support users who review failures need Read on both tables.

## Views

Create these public views.

**Integration Jobs**

| View | Filter | Columns |
| --- | --- | --- |
| Running | `ts_status` = Running | Name, Codebase, Job type, Started on, Last heartbeat, Processed, Failed |
| Needs review | `ts_status` = Partially Succeeded or Failed | Name, Codebase, Job type, Started on, Completed on, Failed, Error summary |

Sort both by Started on, newest first.

**Integration Failures**

| View | Filter | Columns |
| --- | --- | --- |
| Pending retry | `ts_retrystatus` = Pending and `ts_isretryable` = Yes | Name, Integration Job, Source ID, Entity type, Phase, Error category, Error message, Occurred on |

Sort by Occurred on, newest first. On the Integration Job main form, add a subgrid of Integration Failures.

## Setup checklist

1. Create `ts_integrationjob` with the columns, local choices, and integer values in this document.
2. Create the `ts_jobid_key` alternate key and confirm it is Active.
3. Create the `ts_retryofjob` self-lookup (Referential, Restrict Delete).
4. Create `ts_integrationfailure` with the columns, local choices, and integer values in this document.
5. Create the `ts_integrationfailure_job` parental lookup with Cascade All on delete.
6. Create the `ts_failurekey_key` alternate key and confirm it is Active.
7. Add the application user privileges listed above.
8. Add the three views and the failures subgrid on the Integration Job form.
9. Confirm system status stays Active. Job outcome is `ts_status` only.
