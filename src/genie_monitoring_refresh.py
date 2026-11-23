# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Genie Monitoring: daily refresh
# MAGIC
# MAGIC Run this notebook as a daily job (the Asset Bundle in this repo sets that up for you). Each run:
# MAGIC
# MAGIC 1. **Tables:** creates the schema and any missing tables, and adds new workspaces to the
# MAGIC    `workspace_summary` mapping table.
# MAGIC 2. **Genie Agents:** refreshes `dim_genie_space` (agent names, SQL warehouses, `application` tag).
# MAGIC 3. **Budgets (optional):** snapshots Unity AI Gateway budgets from the account Budget API.
# MAGIC    Skipped, without failing the run, until the secret scope holds an account-admin service
# MAGIC    principal's OAuth credentials (see DEPLOY.md, "Turn on budgets").
# MAGIC 4. **Dashboard:** creates and publishes the dashboard if it doesn't exist yet.
# MAGIC
# MAGIC Only `system.*` tables and the tables in the target schema are read. Nothing leaves the account.

# COMMAND ----------

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace default)")
dbutils.widgets.text("schema", "genie_monitoring", "Schema")
dbutils.widgets.text("secret_scope", "genie_monitoring", "Secret scope (optional credentials)")
dbutils.widgets.text("warehouse_id", "", "Dashboard SQL warehouse (blank = pick one)")
dbutils.widgets.text("dashboard_name", "Advanced Genie Monitoring", "Dashboard name")
dbutils.widgets.text("dashboard_folder", "", "Dashboard folder (blank = this repo's folder)")
dbutils.widgets.dropdown("overwrite_dashboard", "false", ["false", "true"], "Overwrite existing dashboard")
dbutils.widgets.text("bu_label", "Business Unit", "Label for workspace_summary.bu")
dbutils.widgets.text("environment_label", "Environment", "Label for workspace_summary.environment")

CATALOG = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = dbutils.widgets.get("schema").strip()
SCOPE = dbutils.widgets.get("secret_scope").strip()
FQ = f"`{CATALOG.replace('`', '``')}`.`{SCHEMA.replace('`', '``')}`"
NOW = datetime.now(timezone.utc)
w = WorkspaceClient()
failures = []   # steps that failed; the run is failed at the end, after the dashboard step
print(f"Target schema: {CATALOG}.{SCHEMA}")


def secret(key):
    try:
        return dbutils.secrets.get(SCOPE, key) or None
    except Exception:
        return None


# Optional account-admin service principal (OAuth M2M). Used for budgets and for listing Genie
# Agents in other workspaces. Without it both steps fall back gracefully.
SP_ID, SP_SECRET = secret("client_id"), secret("client_secret")

# COMMAND ----------

# MAGIC %md ## 1. Tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'Advanced Genie Monitoring'")

DDL = {
    "workspace_summary": """workspace_id BIGINT, workspace_name STRING, bu STRING, environment STRING,
        status STRING, workspace_url STRING""",
    "dim_genie_space": """genie_space_id STRING, genie_space_name STRING, sql_warehouse_id STRING,
        sql_warehouse_name STRING, sql_warehouse_size STRING, sql_warehouse_type STRING,
        sql_warehouse_tags MAP<STRING, STRING>, warehouse_application STRING, description STRING,
        source_workspace_id STRING, last_refreshed TIMESTAMP""",
    "budget_job_status": "run_time TIMESTAMP, status STRING, snapshot_time TIMESTAMP, detail STRING",
    "budget_snapshot": """budget_id STRING, display_name STRING, resource_type STRING, account_id STRING,
        create_time STRING, update_time STRING, spend_amount DOUBLE, spend_status_json STRING,
        snapshot_time TIMESTAMP""",
    "budget_workspace": "budget_id STRING, workspace_id STRING, snapshot_time TIMESTAMP",
    "budget_tag": "budget_id STRING, tag_key STRING, tag_operator STRING, tag_value STRING, snapshot_time TIMESTAMP",
    "budget_limit": """budget_id STRING, scope_type STRING, default_threshold DOUBLE, time_period STRING,
        trigger_type STRING, action_types STRING, snapshot_time TIMESTAMP""",
    "budget_override": "budget_id STRING, principal_id STRING, override_threshold DOUBLE, snapshot_time TIMESTAMP",
    "budget_principal": """principal_id STRING, principal_type STRING, display_name STRING, user_name STRING,
        application_id STRING, run_as STRING, snapshot_time TIMESTAMP""",
    "budget_group_member": """group_id STRING, member_principal_id STRING, member_type STRING,
        membership STRING, snapshot_time TIMESTAMP""",
}
COMMENTS = {
    "workspace_summary": "Workspace mapping for the dashboard filters. Fill in bu and environment; new workspaces are added daily.",
    "dim_genie_space": "Genie Agent names and SQL warehouses, rebuilt daily.",
    "budget_job_status": "One row per run of the budget step: SUCCEEDED, NOT_CONFIGURED or FAILED.",
}
for name, cols in DDL.items():
    comment = COMMENTS.get(name, "Unity AI Gateway budget snapshot (appended daily, keyed by snapshot_time).")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.{name} ({cols}) COMMENT '{comment}'")

# Add workspaces the mapping doesn't have yet. Names, status and URLs come from system tables;
# bu and environment are left for you to fill in, and edits you make are never overwritten.
spark.sql(f"""
MERGE INTO {FQ}.workspace_summary t
USING (SELECT CAST(workspace_id AS BIGINT) AS workspace_id, workspace_name, status, workspace_url
       FROM system.access.workspaces_latest) s
ON t.workspace_id = s.workspace_id
WHEN MATCHED THEN UPDATE SET
  t.workspace_name = COALESCE(t.workspace_name, s.workspace_name),
  t.status = s.status,
  t.workspace_url = s.workspace_url
WHEN NOT MATCHED THEN INSERT (workspace_id, workspace_name, status, workspace_url)
  VALUES (s.workspace_id, s.workspace_name, s.status, s.workspace_url)
""")
print(f"workspace_summary: {spark.table(f'{FQ}.workspace_summary').count()} workspaces")

# COMMAND ----------

# MAGIC %md ## 2. Genie Agents
# MAGIC
# MAGIC The Genie API is per workspace and only returns agents the caller can see. This workspace is
# MAGIC listed as the job's own identity. If the service principal credentials are present, every
# MAGIC other workspace with Genie Agents activity in the last 90 days is listed as the service
# MAGIC principal too (it must be a member of those workspaces; the ones it can't reach are skipped).

# COMMAND ----------

def list_spaces(client, workspace_id):
    rows, token = [], None
    while True:
        q = {"page_size": 100}
        if token:
            q["page_token"] = token
        resp = client.api_client.do("GET", "/api/2.0/genie/spaces", query=q)
        for s in resp.get("spaces") or []:
            rows.append(Row(genie_space_id=s.get("space_id"), genie_space_name=s.get("title"),
                            sql_warehouse_id=s.get("warehouse_id"), description=s.get("description"),
                            source_workspace_id=workspace_id))
        token = resp.get("next_page_token")
        if not token:
            return rows


def host_of(url):
    url = url if "://" in (url or "") else f"https://{url}"
    return urlparse(url).hostname


spaces = []
try:
    spaces = list_spaces(w, None)
    print(f"this workspace: {len(spaces)} Genie Agents")
except Exception as e:
    failures.append(f"Genie Agents (this workspace): {e}")

if SP_ID and SP_SECRET:
    others = [r for r in spark.sql("""
        SELECT DISTINCT CAST(w.workspace_id AS STRING) AS workspace_id, w.workspace_url
        FROM system.access.audit a
        JOIN system.access.workspaces_latest w ON CAST(a.workspace_id AS BIGINT) = CAST(w.workspace_id AS BIGINT)
        WHERE a.service_name = 'aibiGenie' AND a.event_date >= date_sub(current_date(), 90)
          AND w.status = 'RUNNING' AND w.workspace_url IS NOT NULL""").collect()
        if host_of(r.workspace_url) != host_of(w.config.host)]

    def list_other(r):
        try:
            wc = WorkspaceClient(host=f"https://{host_of(r.workspace_url)}", client_id=SP_ID,
                                 client_secret=SP_SECRET, auth_type="oauth-m2m")
            return list_spaces(wc, r.workspace_id), None
        except Exception as e:
            return [], f"{r.workspace_id}: {str(e)[:120]}"

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(list_other, others))
    skipped = [err for _, err in results if err]
    for rows, _ in results:
        spaces.extend(rows)
    print(f"other workspaces: {len(others) - len(skipped)} listed, {len(skipped)} skipped "
          f"(the service principal isn't a member, or the workspace is unreachable)")

seen, deduped = set(), []
for r in spaces:
    if r.genie_space_id and r.genie_space_id not in seen:
        seen.add(r.genie_space_id)
        deduped.append(r)

if deduped or not failures:
    schema = StructType([StructField(c, StringType()) for c in
                         ("genie_space_id", "genie_space_name", "sql_warehouse_id", "description", "source_workspace_id")])
    spark.createDataFrame(deduped, schema=schema).createOrReplaceTempView("_api_spaces")
    # Warehouse name, size and tags from the latest row per warehouse in system.compute.warehouses.
    # warehouse_application is the warehouse's 'application' tag (any case), the hook for grouping
    # agents by application.
    spark.sql(f"""
    INSERT OVERWRITE {FQ}.dim_genie_space
    WITH wh AS (
      SELECT warehouse_id, warehouse_name, warehouse_size, warehouse_type, tags,
             ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
      FROM system.compute.warehouses
      WHERE delete_time IS NULL
    )
    SELECT s.genie_space_id, s.genie_space_name, s.sql_warehouse_id,
           wh.warehouse_name, wh.warehouse_size, wh.warehouse_type, wh.tags,
           try_element_at(map_values(map_filter(wh.tags, (k, v) -> lower(k) = 'application')), 1),
           s.description, s.source_workspace_id, TIMESTAMP'{NOW:%Y-%m-%d %H:%M:%S}'
    FROM _api_spaces s
    LEFT JOIN wh ON wh.warehouse_id = s.sql_warehouse_id AND wh.rn = 1
    """)
    print(f"dim_genie_space: {len(deduped)} Genie Agents")

# COMMAND ----------

# MAGIC %md ## 3. Budgets (optional)
# MAGIC
# MAGIC Unity AI Gateway budgets aren't in system tables, so they come from the account Budget API
# MAGIC (`GET /api/2.1/accounts/{account_id}/budgets?include_spend_status=true`), which needs an
# MAGIC account admin. Budget exceptions name people by account SCIM ID while billing records users by
# MAGIC email and service principals by application ID, so the account SCIM API resolves each one
# MAGIC (`budget_principal`) and expands group exceptions to their members, nested groups included
# MAGIC (`budget_group_member`). All seven tables are appended with one `snapshot_time` per run.
# MAGIC
# MAGIC Every run records its outcome in `budget_job_status`. The dashboard reads budgets at the
# MAGIC latest run's snapshot and shows `N/A: ...` in the budget columns if that run didn't succeed.

# COMMAND ----------

def account_host():
    override = secret("account_host")
    if override:
        return override
    host = host_of(w.config.host)
    if host.endswith(".azuredatabricks.net"):
        return "https://accounts.azuredatabricks.net"
    if host.endswith(".gcp.databricks.com"):
        return "https://accounts.gcp.databricks.com"
    return "https://accounts.cloud.databricks.com"


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def snapshot_budgets():
    account_id = secret("account_id") or spark.sql(
        "SELECT FIRST(account_id) FROM system.access.workspaces_latest").first()[0]
    a = AccountClient(host=account_host(), account_id=account_id, client_id=SP_ID,
                      client_secret=SP_SECRET, auth_type="oauth-m2m")

    budgets, token = [], None
    while True:
        q = {"include_spend_status": "true"}
        if token:
            q["page_token"] = token
        resp = a.api_client.do("GET", f"/api/2.1/accounts/{account_id}/budgets", query=q)
        budgets.extend(resp.get("budgets") or resp.get("budget_configurations") or [])
        token = resp.get("next_page_token")
        if not token:
            break
    budgets = [b for b in budgets if b.get("resource_type") == "BUDGET_RESOURCE_TYPE_UNITY_AI_GATEWAY"]

    snap, wsb, tagb, lim, ovr = [], [], [], [], []
    for b in budgets:
        bid = b.get("budget_configuration_id")
        # The spend-status block is newer and its shape may change: keep it raw as well as parsed.
        ss = b.get("spend_status") or b.get("current_spend") or b.get("status") or {}
        spend = None
        if isinstance(ss, dict):
            spend = next((_f(ss[k]) for k in ("current_spend", "spend", "consumed", "amount", "quantity")
                          if ss.get(k) is not None), None)
        snap.append(Row(budget_id=bid, display_name=b.get("display_name"), resource_type=b.get("resource_type"),
                        account_id=b.get("account_id"), create_time=str(b.get("create_time")),
                        update_time=str(b.get("update_time")), spend_amount=spend,
                        spend_status_json=json.dumps(ss) if ss else None, snapshot_time=NOW))
        f = b.get("filter") or {}
        for wid in (f.get("workspace_id") or {}).get("values") or []:
            wsb.append(Row(budget_id=bid, workspace_id=str(wid), snapshot_time=NOW))
        for t in f.get("tags") or []:
            # {"key": k, "value": {"operator": "IN", "values": [...]}}; one row per value.
            val = t.get("value")
            op, vals = (val.get("operator"), val.get("values") or [None]) if isinstance(val, dict) else (None, [val])
            for v in vals:
                tagb.append(Row(budget_id=bid, tag_key=t.get("key"), tag_operator=op,
                                tag_value=None if v is None else str(v), snapshot_time=NOW))
        for ac in b.get("alert_configurations") or []:
            actions = sorted({x.get("action_type") for x in ac.get("action_configurations") or [] if x.get("action_type")})
            lim.append(Row(budget_id=bid, scope_type=ac.get("scope_type"),
                           default_threshold=_f(ac.get("quantity_threshold")), time_period=ac.get("time_period"),
                           trigger_type=ac.get("trigger_type"), action_types=",".join(actions) or None,
                           snapshot_time=NOW))
            for po in ac.get("principal_overrides") or []:
                ovr.append(Row(budget_id=bid, principal_id=str(po.get("principal_id")),
                               override_threshold=_f(po.get("override_threshold")), snapshot_time=NOW))

    # Resolve exception principals to the billing join key (run_as) and expand groups.
    scim = f"/api/2.0/accounts/{account_id}/scim/v2"
    kind = {"Users": "USER", "ServicePrincipals": "SERVICE_PRINCIPAL", "Groups": "GROUP"}

    def scim_list(resource, attributes):
        out, start = [], 1
        while True:
            resp = a.api_client.do("GET", f"{scim}/{resource}",
                                   query={"attributes": attributes, "startIndex": start, "count": 100})
            page = resp.get("Resources") or []
            out.extend(page)
            start += len(page)
            if not page or start > int(resp.get("totalResults") or 0):
                return out

    groups = {}

    def get_group(gid):
        if gid not in groups:
            try:
                groups[gid] = a.api_client.do("GET", f"{scim}/Groups/{gid}")
            except NotFound:
                groups[gid] = None
        return groups[gid]

    override_ids = sorted({r.principal_id for r in ovr})
    users, sps = {}, {}
    if override_ids:
        users = {u["id"]: u for u in scim_list("Users", "id,userName,displayName")}
        sps = {s["id"]: s for s in scim_list("ServicePrincipals", "id,applicationId,displayName")}

    # group_id is the group named in the exception; members reached through a nested group are
    # 'indirect'. A member reachable both ways is kept as 'direct'.
    members = {}
    for pid in override_ids:
        if pid in users or pid in sps or not get_group(pid):
            continue
        seen_groups, queue = {pid}, [(pid, "direct")]
        while queue:
            gid, membership = queue.pop(0)
            for m in (get_group(gid) or {}).get("members") or []:
                mid = str(m.get("value"))
                mkind = kind.get((m.get("$ref") or "").split("/")[0], "UNKNOWN")
                if members.get((pid, mid), (None, None))[1] != "direct":
                    members[(pid, mid)] = (mkind, membership)
                if mkind == "GROUP" and mid not in seen_groups:
                    seen_groups.add(mid)
                    queue.append((mid, "indirect"))
    gm = [Row(group_id=g, member_principal_id=mid, member_type=k, membership=ms, snapshot_time=NOW)
          for (g, mid), (k, ms) in members.items()]

    pr = []
    for pid in sorted(set(override_ids) | {r.member_principal_id for r in gm}):
        u, s, g = users.get(pid), sps.get(pid), groups.get(pid)
        if u:
            vals = ("USER", u.get("displayName"), u.get("userName"), None, u.get("userName"))
        elif s:
            vals = ("SERVICE_PRINCIPAL", s.get("displayName"), None, s.get("applicationId"), s.get("applicationId"))
        elif g:
            vals = ("GROUP", g.get("displayName"), None, None, None)
        else:   # deleted since the exception was set, or not visible to these credentials
            vals = ("UNKNOWN", None, None, None, None)
        pr.append(Row(principal_id=pid, principal_type=vals[0], display_name=vals[1], user_name=vals[2],
                      application_id=vals[3], run_as=vals[4], snapshot_time=NOW))

    data = {"budget_snapshot": snap, "budget_workspace": wsb, "budget_tag": tagb, "budget_limit": lim,
            "budget_override": ovr, "budget_principal": pr, "budget_group_member": gm}
    for name, rows in data.items():
        # Explicit schemas: a column can be entirely NULL, which breaks type inference.
        schema = spark.table(f"{FQ}.{name}").schema
        spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
    return f"{len(budgets)} AI Gateway budgets, {len(ovr)} exceptions, {len(pr)} principals, {len(gm)} group memberships"


if not (SP_ID and SP_SECRET):
    status, snapshot, detail = "NOT_CONFIGURED", None, f"No client_id / client_secret in secret scope '{SCOPE}'"
else:
    try:
        status, snapshot, detail = "SUCCEEDED", NOW, snapshot_budgets()
    except Exception as e:
        status, snapshot, detail = "FAILED", None, str(e)[:2000]
        failures.append(f"Budgets: {str(e)[:500]}")
spark.createDataFrame([Row(run_time=NOW, status=status, snapshot_time=snapshot, detail=detail)],
                      schema=spark.table(f"{FQ}.budget_job_status").schema) \
     .write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.budget_job_status")
print(f"budgets: {status}: {detail}")

# COMMAND ----------

# MAGIC %md ## 4. Dashboard
# MAGIC
# MAGIC Created from `dashboard_template.json` (next to this notebook) with this run's catalog, schema
# MAGIC and labels filled in, then published so queries run as the viewer. An existing dashboard is
# MAGIC left alone, so your edits survive daily runs. To install a newer template, run once with
# MAGIC `overwrite_dashboard = true`.

# COMMAND ----------

try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    src_dir = os.path.dirname(nb_path if nb_path.startswith("/Workspace") else f"/Workspace{nb_path}")
except Exception:
    src_dir = os.getcwd()
folder = dbutils.widgets.get("dashboard_folder").strip() or os.path.dirname(src_dir)
folder = folder[len("/Workspace"):] if folder.startswith("/Workspace/") else folder
name = dbutils.widgets.get("dashboard_name").strip()


def filled_template():
    text = open(os.path.join(src_dir, "dashboard_template.json")).read()
    subs = {
        "__CATALOG__": CATALOG.replace("`", "``"),
        "__SCHEMA__": SCHEMA.replace("`", "``"),
        "__BU_LABEL__": dbutils.widgets.get("bu_label").strip() or "Business Unit",
        "__ENV_LABEL__": dbutils.widgets.get("environment_label").strip() or "Environment",
    }
    for k, v in subs.items():
        text = text.replace(k, json.dumps(v)[1:-1])
    json.loads(text)   # still valid JSON
    return text


def pick_warehouse():
    wid = dbutils.widgets.get("warehouse_id").strip()
    if wid:
        return wid
    whs = w.api_client.do("GET", "/api/2.0/sql/warehouses").get("warehouses") or []
    whs.sort(key=lambda x: (not x.get("enable_serverless_compute"), x.get("state") != "RUNNING"))
    return whs[0]["id"] if whs else None


try:
    existing = None
    try:
        st = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": f"{folder}/{name}.lvdash.json"})
        existing = st.get("resource_id")
    except NotFound:
        pass
    overwrite = dbutils.widgets.get("overwrite_dashboard") == "true"
    if existing and not overwrite:
        dash_id = existing
        print("dashboard already exists, left unchanged")
    else:
        body = {"display_name": name, "serialized_dashboard": filled_template()}
        wh = pick_warehouse()
        if wh:
            body["warehouse_id"] = wh
        if existing:
            dash_id = w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{existing}", body=body)["dashboard_id"]
        else:
            w.api_client.do("POST", "/api/2.0/workspace/mkdirs", body={"path": folder})
            body["parent_path"] = folder
            dash_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)["dashboard_id"]
        w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                        body={"embed_credentials": False, **({"warehouse_id": wh} if wh else {})})
        print(f"dashboard {'updated' if existing else 'created'} and published")
    print(f"{w.config.host.rstrip('/')}/dashboardsv3/{dash_id}/published")
except Exception as e:
    failures.append(f"Dashboard: {e}")

# COMMAND ----------

if failures:
    raise RuntimeError("Some steps failed (the others completed):\n- " + "\n- ".join(failures))
print("All steps completed.")
