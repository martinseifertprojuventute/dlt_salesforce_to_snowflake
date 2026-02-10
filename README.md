# Salesforce to Snowflake dlt Pipeline

Loads Salesforce CRM data into Snowflake using the Bulk API v2 and [dlt](https://dlthub.com/).

## Setup

### Credentials

Configure `.dlt/secrets.toml`:

```toml
[sources.salesforce]
client_id = "your_connected_app_client_id"
client_secret = "your_connected_app_client_secret"
username = "your_salesforce_username"
password = "your_salesforce_password"
security_token = "your_security_token"

[destination.snowflake.credentials]
# standard dlt snowflake credentials
```

### Install

```bash
pip install -r requirements.txt
```

## Usage

```
python load_salesforce.py [objects ...] [--full] [--task DB.SCHEMA.TASK]
```

| Command | Description |
|---|---|
| `python load_salesforce.py` | All 56 objects, incremental merge |
| `python load_salesforce.py Account` | Only Account, incremental merge |
| `python load_salesforce.py Account Campaign Contact` | Multiple objects, incremental merge |
| `python load_salesforce.py --full` | All objects, full replace |
| `python load_salesforce.py --full Account` | Only Account, full replace |
| `python load_salesforce.py --task RAW.SALESFORCE.TA_REFRESH_DOWNSTREAM` | All objects, then execute downstream task |

Object names are case-insensitive. Unknown names raise an error.

### Incremental vs Full Load

**Incremental (default):** Uses `merge` write disposition. Tracks a watermark per object via `dlt.sources.incremental` on the configured date field (usually `SystemModstamp`). On first run, loads everything. On subsequent runs, only fetches records modified since the last watermark. Uses `queryAll` operation so deleted records (`IsDeleted = True`) are also captured.

**Full (`--full`):** Uses `refresh="drop_resources"` which drops and recreates the target table, then loads all records from Salesforce using merge. No WHERE clause in the SOQL query. Resets the incremental watermark -- the next incremental run will re-establish it.

### Downstream Task Execution

Use `--task` to execute a Snowflake task after the load and history sync complete. The task name must be fully qualified (`DB.SCHEMA.TASK`). The pipeline checks whether the task exists before executing it -- if it doesn't exist, a warning is printed and execution continues normally.

```bash
python load_salesforce.py --task RAW.SALESFORCE.TA_REFRESH_DOWNSTREAM
```

The Snowflake role used by the pipeline needs `EXECUTE TASK` privileges on the target task.

## Architecture

```
load_salesforce.py
  |
  salesforce_source()                  @dlt.source
    |
    SalesforceClient                   OAuth2 auth + Bulk API v2
    |  _authenticate()                 POST login.salesforce.com/services/oauth2/token
    |  describe_object()               GET  /sobjects/{obj}/describe/
    |  bulk_query()                    POST /jobs/query -> poll -> GET results (CSV)
    |
    _make_resource(client, config)     One dlt.resource per Salesforce object
       primary_key="Id"
       max_table_nesting=0
       parallelized=True
```

### Key behaviors

- **Parallel extraction:** 6 workers for extract, normalize, and load phases (configured in `.dlt/config.toml`), each object gets its own Bulk API job running concurrently.
- **Token refresh:** Automatic re-authentication on HTTP 401 (Salesforce tokens expire after 2 hours).
- **Schema evolution:** New Salesforce fields are auto-discovered via the describe API on each run. dlt adds new columns to Snowflake automatically.
- **Locator-based pagination:** Bulk API v2 returns CSV results in pages. Each page is yielded as a batch of dicts.
- **Field/type exclusions:** `EXCLUDED_TYPES` (address, location, base64) and `EXCLUDED_FIELDS` (per-object) filter out fields that cannot be exported via Bulk API CSV.

## Configuration

### Adding objects

Add entries to `SALESFORCE_OBJECTS` in `load_salesforce.py`:

```python
{"name": "MyCustomObject__c", "date_field": "SystemModstamp"},
```

The `date_field` must be a queryable date/datetime field on the Salesforce object. Most standard objects use `SystemModstamp`. History objects use `CreatedDate`.

### Excluding fields

Add entries to `EXCLUDED_FIELDS`:

```python
EXCLUDED_FIELDS = {
    "Profile": {"PermissionsAccessServiceEinstein", "PermissionsAccessSfDrive", ...},
    "User": {"HasUserVerifiedEmail", "HasUserVerifiedPhone", ...},
}
```

Currently excluded: 24 fields from `Profile`, 7 fields from `User` (fields not supported by Bulk API).

### Destination

Data lands in Snowflake database `RAW`, schema `SALESFORCE`. Table names are case-insensitive Salesforce object API names (e.g., `ACCOUNT`, `CAMPAIGNMEMBER`, `RAISENOW__TRANSACTION__C`).

Note: With `sql_ci_v1` naming, table names are case-insensitive -- `CampaignMember` becomes `CAMPAIGNMEMBER` (queryable as `campaignmember`, `CampaignMember`, etc.).

## History Sync

After each pipeline run, changed rows are automatically copied to a separate `HISTORY` database. This preserves every version of a record, enabling auditing and change tracking.

### How it works

1. **Incremental loads:** A Snowflake stream on each RAW table captures only changed rows. The stream is consumed after `pipeline.run()` and inserted into `HISTORY.<schema>.<table>` with dedup on `(Id, date_field)`.
2. **Full loads:** `refresh="drop_resources"` drops and recreates the RAW table, invalidating any existing stream. Instead, a direct `INSERT ... SELECT` from the RAW table into HISTORY is used, with the same `(Id, date_field)` dedup. The stream is then recreated for the next incremental run.
3. **Dedup logic:** A row is only inserted into history if no row with the same `(Id, date_field)` combination already exists (`NOT EXISTS` with `EQUAL_NULL` to handle NULLs).

### Schema evolution

History tables are never dropped or recreated. On each run:
- If the history table doesn't exist yet, it is created with the same schema as the RAW table (minus dlt internal columns).
- If the history table exists and the RAW table has new columns, they are added via `ALTER TABLE ADD COLUMN`.
- Columns are never removed from history tables.
- The `INSERT` uses only the columns common to both RAW and HISTORY, so new columns in RAW won't break the insert for existing history tables.

### History location

The history schema mirrors the RAW schema name: `HISTORY.SALESFORCE` (derived from `pipeline.dataset_name`).

### Prerequisites

The Snowflake role used by the pipeline needs grants on the `HISTORY` database:

```sql
GRANT USAGE ON DATABASE HISTORY TO ROLE <your_role>;
GRANT CREATE SCHEMA ON DATABASE HISTORY TO ROLE <your_role>;
-- After first run creates the schema:
GRANT CREATE TABLE ON SCHEMA HISTORY.SALESFORCE TO ROLE <your_role>;
-- Stream creation on the RAW schema:
GRANT CREATE STREAM ON SCHEMA RAW.SALESFORCE TO ROLE <your_role>;
```

## Deployment

The pipeline runs as a Snowflake container service. See `SPCS job specs/` for container specs (requests: 0.5 vCPU, 1 GB RAM; limits: 6 vCPU, 58 GB RAM for HIGHMEM_S node).

For scheduled runs, the container is invoked without arguments for incremental loading. Use `--full` only for manual re-loads when needed.
