from typing import Any, Iterator
import argparse
import csv
import time

import dlt
from dlt.pipeline.progress import alive_progress
import requests
import pendulum
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# Workaround for pendulum bug: pytz.FixedOffset(N).zone is None, which crashes
# pendulum._safe_timezone when timestamps have non-UTC offsets (e.g. from local runs).
_original_instance = pendulum.instance
def _patched_instance(dt, tz=None):
    if tz is None and dt.tzinfo is not None and not hasattr(dt.tzinfo, 'key'):
        import pytz
        if isinstance(dt.tzinfo, pytz._FixedOffset):
            tz = pendulum.UTC
            dt = dt.astimezone(tz)
    return _original_instance(dt, tz=tz)
pendulum.instance = _patched_instance


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://<salesforce org>.my.salesforce.com"
API_VERSION = "v63.0"

SALESFORCE_OBJECTS: list[dict[str, str]] = [
    {"name": "Account",                                "date_field": "SystemModstamp"},
    {"name": "AccountHistory",                         "date_field": "CreatedDate"},
    {"name": "Campaign",                               "date_field": "SystemModstamp"},
    {"name": "CampaignMember",                         "date_field": "SystemModstamp"},
    {"name": "Contact",                                "date_field": "SystemModstamp"},
    {"name": "ContactHistory",                         "date_field": "CreatedDate"},
    {"name": "ContentDocument",                        "date_field": "SystemModstamp"},
    {"name": "ContentDocumentLink",                    "date_field": "SystemModstamp"},
    {"name": "ContentNote",                            "date_field": "LastModifiedDate"},
    {"name": "dupcheck__dc3Duplicate__c",              "date_field": "SystemModstamp"},
    {"name": "dupcheck__dcAudit__c",                   "date_field": "SystemModstamp"},
    {"name": "dupcheck__dcDiscard__c",                 "date_field": "SystemModstamp"},
    {"name": "dupcheck__dcGroup__c",                   "date_field": "SystemModstamp"},
    {"name": "dupcheck__dcJob__c",                     "date_field": "SystemModstamp"},
    {"name": "DuplicateRecordItem",                    "date_field": "SystemModstamp"},
    {"name": "DuplicateRecordSet",                     "date_field": "SystemModstamp"},
    {"name": "Event",                                  "date_field": "SystemModstamp"},
    {"name": "Lead",                                   "date_field": "SystemModstamp"},
    {"name": "LoginHistory",                           "date_field": "LoginTime"},
    {"name": "npe01__OppPayment__c",                   "date_field": "SystemModstamp"},
    {"name": "npe03__Recurring_Donation__c",           "date_field": "SystemModstamp"},
    {"name": "npe03__Recurring_Donation__History",     "date_field": "CreatedDate"},
    {"name": "npsp__Address__c",                       "date_field": "SystemModstamp"},
    {"name": "npsp__Allocation__c",                    "date_field": "SystemModstamp"},
    {"name": "npsp__Error__c",                         "date_field": "SystemModstamp"},
    {"name": "npsp__General_Accounting_Unit__c",       "date_field": "SystemModstamp"},
    {"name": "nxErrorLog__c",                          "date_field": "SystemModstamp"},
    {"name": "Opportunity",                            "date_field": "SystemModstamp"},
    {"name": "OpportunityFieldHistory",                "date_field": "CreatedDate"},
    {"name": "OpportunityHistory",                     "date_field": "CreatedDate"},
    {"name": "OpportunityLineItem",                    "date_field": "SystemModstamp"},
    {"name": "OpportunityStage",                       "date_field": "SystemModstamp"},
    {"name": "PJ_Adressfinanzierung__c",               "date_field": "SystemModstamp"},
    {"name": "PJ_Gemeinde__c",                         "date_field": "SystemModstamp"},
    {"name": "PJ_Post_Retour__c",                      "date_field": "SystemModstamp"},
    {"name": "PJ_Postleitzahl__c",                     "date_field": "SystemModstamp"},
    {"name": "PJ_Produktfinanzierung__c",              "date_field": "SystemModstamp"},
    {"name": "PJ_Setposition__c",                      "date_field": "SystemModstamp"},
    {"name": "PJ_Telefonmarketing__c",                 "date_field": "SystemModstamp"},
    {"name": "PJ_Textbaustein__c",                     "date_field": "SystemModstamp"},
    {"name": "Pricebook2",                             "date_field": "SystemModstamp"},
    {"name": "PricebookEntry",                         "date_field": "SystemModstamp"},
    {"name": "Product2",                               "date_field": "SystemModstamp"},
    {"name": "Profile",                                "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Bank_Account__c",              "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Business_Process_Reference__c","date_field": "SystemModstamp"},
    {"name": "RaiseNow__Customer__c",                  "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Direct_Debit_Mandate__c",      "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Payment_Source__c",            "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Reconciliation_Report__c",     "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Subscription__c",              "date_field": "SystemModstamp"},
    {"name": "RaiseNow__Transaction__c",               "date_field": "SystemModstamp"},
    {"name": "RecordType",                             "date_field": "SystemModstamp"},
    {"name": "Report",                                 "date_field": "SystemModstamp"},
    {"name": "Task",                                   "date_field": "SystemModstamp"},
    {"name": "User",                                   "date_field": "SystemModstamp"},
]

# Salesforce field types that cannot be exported via Bulk API CSV
EXCLUDED_TYPES: set[str] = {"address", "location", "base64"}

# Per-object field exclusions (from meta.load.t_salesforce_fields_exclude)
EXCLUDED_FIELDS: dict[str, set[str]] = {
    "Profile": {
        "PermissionsAccessServiceEinstein", "PermissionsAccessSfDrive",
        "PermissionsAgentforceServiceAgentUser", "PermissionsAllowObjectDetection",
        "PermissionsAllowObjectDetectionTraining", "PermissionsAppFrameworkManageApp",
        "PermissionsAppFrameworkManageTemplate", "PermissionsAppFrameworkViewApp",
        "PermissionsApprovalAdmin", "PermissionsApprovalDesigner",
        "PermissionsAttributionModelUser", "PermissionsCanDoActAsUser",
        "PermissionsCanSendInitialSMSToIndividual", "PermissionsCanViewDataPrepRecipe",
        "PermissionsDigitalLendingEditReadOnly", "PermissionsDigitalLendingWorkbench",
        "PermissionsEngagementConfigUser", "PermissionsEnhancedSalesMobileExp",
        "PermissionsFreezeUsers", "PermissionsHeadlessPublishNudges",
        "PermissionsManageAccessPolicies", "PermissionsManageAgentforceServiceAgent",
        "PermissionsManageCdpMlModels", "PermissionsManageCertificatesExpiration",
        "PermissionsManageCustomDomains", "PermissionsManageDevSandboxes",
        "PermissionsManageFilesAndAttachments", "PermissionsManageIntegrationConnections",
        "PermissionsManagePersonalOrg", "PermissionsMetadataStudioUser",
        "PermissionsMobileMessagingAgent", "PermissionsModifyAccessAllowPolicies",
        "PermissionsModifyAccessDenyPolicies", "PermissionsModifyAllPolicyCenterPolicies",
        "PermissionsMonitorLoginHistory", "PermissionsPersonalizationDecisioningUser",
        "PermissionsPersonalizationIntelUser", "PermissionsPrismPlaygroundUser",
        "PermissionsPrmExtIntPrtnrAdminUser", "PermissionsQueryNonVetoedFiles",
        "PermissionsSalesActionPlansUserAccess", "PermissionsSchedulerAIAgentUserAccess",
        "PermissionsTerritoryOperations", "PermissionsUseServicePartReturn",
        "PermissionsViewAccessPolicies", "PermissionsViewAllCalls",
        "PermissionsViewAllFieldsGlobal", "PermissionsViewAllPolicyCenterPolicies",
        "PermissionsViewConsumption", "PermissionsViewOrchestrationsInAutomApp",
        "PermissionsViewPersonalOrg", "PermissionsViewRecommendations",
        "PermissionsYourAccountCDAPublishEvents",
    },
    "User": {
        "HasUserVerifiedEmail", "HasUserVerifiedPhone",
        "UserPreferencesActionLauncherEinsteinGptConsent",
        "UserPreferencesAssistiveActionsEnabledInActionLauncher",
        "UserPreferencesHideInvoicesRedirectConfirmation",
        "UserPreferencesHideOnlineSalesAppTabVisibilityRequirementsModal",
        "UserPreferencesHideStatementsRedirectConfirmation",
    },
}


# ---------------------------------------------------------------------------
# Salesforce Bulk API v2 Client
# ---------------------------------------------------------------------------

class _EmptyResultError(Exception):
    """Raised when a Bulk API job completes with 0 records."""
    pass


class SalesforceClient:
    """Handles OAuth2 authentication and Bulk API v2 query operations."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        security_token: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.security_token = security_token
        self.session = requests.Session()
        self._authenticate()

    def _authenticate(self) -> None:
        """Obtain an access token via OAuth2 password grant."""
        response = requests.post(
            "https://login.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
                "password": self.password + self.security_token,
            },
            timeout=120,
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Execute an HTTP request with retry on transient errors and 401."""
        kwargs.setdefault("timeout", 120)
        last_exc = None
        for attempt in range(4):
            try:
                response = self.session.request(method, url, **kwargs)
                if response.status_code == 401:
                    self._authenticate()
                    response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.ConnectionError as e:
                last_exc = e
                wait = 2 ** attempt  # 1, 2, 4 seconds
                print(f"  Connection error (attempt {attempt + 1}/4), retrying in {wait}s: {e}")
                time.sleep(wait)
        raise last_exc

    # ---- Describe --------------------------------------------------------

    def describe_object(self, object_name: str) -> list[tuple[str, str]]:
        """Return (name, sf_type) for queryable fields of a Salesforce object."""
        url = f"{BASE_URL}/services/data/{API_VERSION}/sobjects/{object_name}/describe/"
        response = self._request("GET", url)
        fields = response.json()["fields"]

        excluded_for_object = EXCLUDED_FIELDS.get(object_name, set())
        return [
            (f["name"], f["type"])
            for f in fields
            if not f["deprecatedAndHidden"]
            and f["type"].lower() not in EXCLUDED_TYPES
            and f["name"] not in excluded_for_object
        ]

    # ---- Bulk API v2 -----------------------------------------------------

    def bulk_query(
        self,
        object_name: str,
        columns: list[tuple[str, str]],
        date_field: str,
        last_value: str | None = None,
    ) -> Iterator[list[dict]]:
        """Create a Bulk API v2 queryAll job and yield batches of record dicts."""
        col_names = [name for name, _ in columns]
        type_map = {name: sf_type for name, sf_type in columns}
        cols = ",".join(col_names)
        soql = f"SELECT {cols} FROM {object_name}"

        if last_value is not None:
            where_parts = [f"{date_field} > {last_value}"]
            if "IsDeleted" in col_names:
                where_parts.append("IsDeleted = True")
            soql += " WHERE " + " OR ".join(where_parts)

        print(f"  {object_name}: SOQL length={len(soql)}")

        # 1) Create query job
        job_url = f"{BASE_URL}/services/data/{API_VERSION}/jobs/query"
        job_resp = self._request("POST", job_url, json={
            "operation": "queryAll",
            "query": soql,
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        })
        job_id = job_resp.json()["id"]
        print(f"  {object_name}: Bulk API job {job_id} created")

        # 2) Poll until complete
        status_url = f"{job_url}/{job_id}"
        try:
            self._poll_job(status_url, object_name)
        except _EmptyResultError:
            return

        # 3) Retrieve CSV results with locator-based pagination
        results_url = f"{status_url}/results"
        yield from self._retrieve_results(results_url, object_name, type_map)

    def _poll_job(self, status_url: str, object_name: str) -> None:
        """Poll a Bulk API v2 job until it completes."""
        poll_count = 0
        while True:
            response = self._request("GET", status_url)
            state = response.json()["state"]

            if state == "JobComplete":
                n = response.json().get("numberRecordsProcessed", 0)
                print(f"  {object_name}: job complete, {n} records")
                if n == 0:
                    raise _EmptyResultError()
                return
            if state in ("Failed", "Aborted"):
                raise Exception(
                    f"Bulk API job {state} for {object_name}: "
                    f"{response.json()}"
                )

            poll_count += 1
            if poll_count == 1:
                time.sleep(2)
            elif poll_count == 2:
                time.sleep(5)
            elif poll_count <= 60:
                time.sleep(10)
            else:
                time.sleep(20)

    def _retrieve_results(
        self, results_url: str, object_name: str, type_map: dict[str, str]
    ) -> Iterator[list[dict]]:
        """Retrieve CSV results from a completed Bulk API v2 job."""
        locator = None
        page = 0
        while True:
            params: dict[str, str] = {}
            if locator and locator != "null":
                params["locator"] = locator

            response = self._request("GET", results_url, params=params)
            response.encoding = "utf-8"

            reader = csv.DictReader(response.text.splitlines())
            batch = [_cast_row(row, type_map) for row in reader]

            page += 1
            if batch:
                print(f"  {object_name}: page {page}, {len(batch)} records")
                yield batch

            new_locator = response.headers.get("Sforce-Locator")
            if not new_locator or new_locator == "null":
                break
            locator = new_locator


# ---------------------------------------------------------------------------
# CSV type casting
# ---------------------------------------------------------------------------

_SF_BOOL_TYPES = {"boolean"}
_SF_INT_TYPES = {"int"}
_SF_FLOAT_TYPES = {"double", "currency", "percent"}


def _cast_row(row: dict[str, str], type_map: dict[str, str]) -> dict:
    """Cast CSV string values to Python types based on Salesforce field types."""
    out = {}
    for key, val in row.items():
        if val == "" or val is None:
            out[key] = None
            continue
        sf_type = type_map.get(key)
        if sf_type in _SF_BOOL_TYPES:
            out[key] = val.lower() == "true"
        elif sf_type in _SF_INT_TYPES:
            out[key] = int(val)
        elif sf_type in _SF_FLOAT_TYPES:
            out[key] = float(val)
        else:
            out[key] = val
    return out


# ---------------------------------------------------------------------------
# dlt Resource Factory
# ---------------------------------------------------------------------------

def _make_resource(client: SalesforceClient, obj_config: dict, full_load: bool = False):
    """Create a dlt resource for a single Salesforce object."""
    object_name: str = obj_config["name"]
    date_field: str = obj_config["date_field"]

    def _generator(
        last_modified=dlt.sources.incremental(
            date_field, initial_value=None
        ),
    ) -> Iterator[list[dict]]:
        columns = client.describe_object(object_name)
        last_value = None if full_load else last_modified.last_value
        print(f"  {object_name}: {len(columns)} columns"
              f"{' (full load)' if full_load else ''}")

        for batch in client.bulk_query(
            object_name=object_name,
            columns=columns,
            date_field=date_field,
            last_value=last_value,
        ):
            yield batch

    return dlt.resource(
        _generator,
        name=object_name.lower(),
        primary_key="Id",
        write_disposition="merge",
        max_table_nesting=0,
        parallelized=True,
    )


# ---------------------------------------------------------------------------
# dlt Source
# ---------------------------------------------------------------------------

@dlt.source(name="salesforce")
def salesforce_source(objects: list[str] | None = None, full_load: bool = False) -> Any:
    client = SalesforceClient(
        client_id=dlt.secrets["sources.salesforce.client_id"],
        client_secret=dlt.secrets["sources.salesforce.client_secret"],
        username=dlt.secrets["sources.salesforce.username"],
        password=dlt.secrets["sources.salesforce.password"],
        security_token=dlt.secrets["sources.salesforce.security_token"],
    )

    if objects:
        selected = {name.lower() for name in objects}
        obj_list = [o for o in SALESFORCE_OBJECTS if o["name"].lower() in selected]
        unknown = selected - {o["name"].lower() for o in obj_list}
        if unknown:
            raise ValueError(f"Unknown Salesforce objects: {', '.join(sorted(unknown))}")
    else:
        obj_list = SALESFORCE_OBJECTS

    print(f"Loading {len(obj_list)} object(s), mode={'full' if full_load else 'incremental'}")

    for obj_config in obj_list:
        yield _make_resource(client, obj_config, full_load=full_load)


# ---------------------------------------------------------------------------
# History Sync (RAW -> HISTORY database)
# ---------------------------------------------------------------------------


def _to_snowflake_column(name: str) -> str:
    """Convert Salesforce field name to Snowflake column name (sql_ci_v1, no snake_case).

    Examples: SystemModstamp -> SYSTEMMODSTAMP, CreatedDate -> CREATEDDATE, Id -> ID
    """
    return name.upper()


def _get_snowflake_connection():
    """Connect to Snowflake using dlt's configured credentials."""
    creds = dlt.secrets["destination.snowflake.credentials"]
    pem_key = serialization.load_pem_private_key(
        creds["private_key"].encode(),
        password=creds["private_key_passphrase"].encode(),
    )
    pkb = pem_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=creds["host"],
        user=creds["username"],
        private_key=pkb,
        database=creds["database"],
        warehouse=creds["warehouse"],
        role=creds["role"],
        session_parameters={"TIMEZONE": "UTC"},
    )


def _get_columns(cur, database: str, schema: str, table: str) -> list[tuple[str, str]]:
    """Get (column_name, data_type) from a table, excluding dlt internal columns."""
    cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
          AND COLUMN_NAME NOT IN ('_DLT_ID', '_DLT_LOAD_ID')
        ORDER BY ORDINAL_POSITION
    """)
    return [(row[0], row[1]) for row in cur.fetchall()]


def _prepare_history_sync(cur, table: str, date_field: str, full_load: bool,
                          raw_db: str, schema: str) -> tuple[str | None, bool]:
    """Prepare history table schema and return (INSERT SQL, needs_stream_recreate).

    Returns (None, False) if there's nothing to insert.
    """
    raw_fqn = f"{raw_db}.{schema}.{table}"
    hist_fqn = f"HISTORY.{schema}.{table}"
    stream_fqn = f"{raw_db}.{schema}.STREAM__{table}"

    # 1. Get RAW columns and check if history table exists
    raw_cols = _get_columns(cur, raw_db, schema, table)

    cur.execute(f"""
        SELECT COUNT(*) FROM HISTORY.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """)
    hist_exists = cur.fetchone()[0] > 0

    if not hist_exists:
        cols_csv = ", ".join(name for name, _ in raw_cols)
        cur.execute(f"CREATE TABLE {hist_fqn} AS SELECT {cols_csv} FROM {raw_fqn} WHERE 1=0")
        print(f"  {table}: created history table")
        common_cols = [name for name, _ in raw_cols]
    else:
        hist_cols = _get_columns(cur, "HISTORY", schema, table)
        hist_col_names = {name for name, _ in hist_cols}

        # Add new columns from RAW that are missing in HISTORY
        for col_name, col_type in raw_cols:
            if col_name not in hist_col_names:
                cur.execute(f"ALTER TABLE {hist_fqn} ADD COLUMN {col_name} {col_type}")
                hist_col_names.add(col_name)
                print(f"  {table}: added column {col_name}")

        # Use only columns present in both (in RAW order)
        common_cols = [name for name, _ in raw_cols if name in hist_col_names]

    if not common_cols:
        print(f"  {table}: no common columns, skipping")
        return None, False

    cols_csv = ", ".join(common_cols)
    dedup = (f"NOT EXISTS (SELECT 1 FROM {hist_fqn} h "
             f"WHERE h.ID = s.ID AND EQUAL_NULL(h.{date_field}, s.{date_field}))")

    # 2. Build INSERT SQL
    if full_load:
        sql = f"""
            INSERT INTO {hist_fqn} ({cols_csv})
            SELECT {cols_csv} FROM {raw_fqn} s WHERE {dedup}
        """
        return sql, True
    else:
        # Incremental load — use stream to narrow scan to changed rows only
        cur.execute(f"""
            CREATE STREAM IF NOT EXISTS {stream_fqn}
            ON TABLE {raw_fqn} SHOW_INITIAL_ROWS = TRUE
        """)
        cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_fqn}')")
        has_data = cur.fetchone()[0]

        if has_data in (True, "true", "True"):
            sql = f"""
                INSERT INTO {hist_fqn} ({cols_csv})
                SELECT {cols_csv} FROM {stream_fqn} s
                WHERE s.METADATA$ACTION = 'INSERT' AND {dedup}
            """
            return sql, False
        else:
            print(f"  {table}: no changes")
            return None, False


def _sync_history(pipeline, objects: list[dict[str, str]], full_load: bool, raw_schema: str) -> None:
    """After pipeline.run(), sync changed rows to the HISTORY database."""
    # Map dlt-normalized table names to their date fields
    sf_lookup = {
        obj["name"].lower(): _to_snowflake_column(obj["date_field"])
        for obj in objects
    }
    table_map = {}
    for dlt_name in pipeline.default_schema.tables:
        if not dlt_name.startswith("_dlt_") and dlt_name in sf_lookup:
            table_map[dlt_name.upper()] = sf_lookup[dlt_name]

    if not table_map:
        print("History sync: no tables to sync")
        return

    schema = raw_schema.upper()
    raw_db = dlt.secrets["destination.snowflake.credentials"]["database"]

    conn = _get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS HISTORY.{schema}")

        # Phase 1: Prepare all tables (fast metadata ops)
        pending: list[tuple[str, str, bool]] = []  # (table, sql, needs_stream_recreate)
        for table, date_field in table_map.items():
            try:
                sql, recreate_stream = _prepare_history_sync(
                    cur, table, date_field, full_load, raw_db, schema
                )
                if sql:
                    pending.append((table, sql, recreate_stream))
            except Exception as e:
                print(f"  History sync ERROR for {table}: {e}")

        if not pending:
            return

        # Phase 2: Submit all INSERTs asynchronously
        query_ids: dict[str, str] = {}
        for table, sql, _ in pending:
            cur.execute_async(sql)
            query_ids[table] = cur.sfqid
        print(f"  Submitted {len(pending)} async history inserts")

        # Phase 3: Wait for all to complete
        for table, _, recreate_stream in pending:
            qid = query_ids[table]
            while conn.is_still_running(conn.get_query_status(qid)):
                time.sleep(2)
            conn.get_query_status_throw_if_error(qid)
            cur.get_results_from_sfqid(qid)
            rows = cur.rowcount if cur.rowcount else 0
            mode = "full load" if full_load else "incremental"
            print(f"  {table}: {rows} rows -> history ({mode})")

            # Recreate stream after full load INSERT completes
            if recreate_stream:
                raw_fqn = f"{raw_db}.{schema}.{table}"
                stream_fqn = f"{raw_db}.{schema}.STREAM__{table}"
                cur.execute(f"CREATE OR REPLACE STREAM {stream_fqn} ON TABLE {raw_fqn}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Downstream Task Execution
# ---------------------------------------------------------------------------


def _execute_downstream_task(task_name: str) -> None:
    """Execute a Snowflake task by fully qualified name, if it exists."""
    parts = task_name.split(".")
    if len(parts) != 3:
        print(f"Task name must be fully qualified (DB.SCHEMA.TASK): {task_name}")
        return

    db, schema, tname = parts
    conn = _get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TASKS LIKE '{tname}' IN SCHEMA {db}.{schema}")
        if not cur.fetchall():
            print(f"Task {task_name} not found, skipping")
            return
        cur.execute(f"EXECUTE TASK {task_name}")
        print(f"Executed task {task_name}")
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def load_salesforce(objects: list[str] | None = None, full_load: bool = False,
                    downstream_task: str | None = None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="salesforce",
        destination="snowflake",
        dataset_name="salesforce",
        # progress=alive_progress(enrich_print=False), # when running locally
    )

    # Determine which objects will be loaded (needed for truncate + history sync)
    if objects:
        selected = {name.lower() for name in objects}
        obj_list = [o for o in SALESFORCE_OBJECTS if o["name"].lower() in selected]
    else:
        obj_list = SALESFORCE_OBJECTS

    if not full_load:
        # Incremental: explicit sync for logging (pipeline.run also syncs internally)
        print("Syncing pipeline state from destination...")
        try:
            pipeline.sync_destination()
        except Exception:
            pass  # First run - no state in destination yet
        print("Pipeline state synced")

    # For full loads, refresh="drop_resources" drops tables + resets incremental
    # state at the right point in the lifecycle (after pipeline.run's internal
    # sync_destination, before extraction). No manual truncate/pop needed.
    run_kwargs: dict = {"loader_file_format": "parquet"}
    if full_load:
        run_kwargs["refresh"] = "drop_resources"

    print("Starting pipeline.run()...")
    load_info = pipeline.run(
        salesforce_source(objects=objects, full_load=full_load),
        **run_kwargs,
    )
    print(load_info)

    # Sync changed rows to HISTORY database
    try:
        print("Syncing history...")
        _sync_history(pipeline, obj_list, full_load, pipeline.dataset_name)
        print("History sync complete")
    except Exception as e:
        print(f"History sync failed: {e}")

    # Execute downstream Snowflake task if requested
    if downstream_task:
        try:
            print(f"Executing downstream task {downstream_task}...")
            _execute_downstream_task(downstream_task)
        except Exception as e:
            print(f"Downstream task execution failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Salesforce data to Snowflake via dlt")
    parser.add_argument("objects", nargs="*", help="Salesforce object names to sync (default: all)")
    parser.add_argument("--full", action="store_true", help="Full load (replace) instead of incremental merge")
    parser.add_argument("--task", help="Fully qualified Snowflake task to execute after load (e.g. RAW.SALESFORCE.TA_REFRESH_DOWNSTREAM)")
    args = parser.parse_args()

    load_salesforce(objects=args.objects or None, full_load=args.full, downstream_task=args.task)

