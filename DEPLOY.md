# Deploying Advanced Genie Monitoring

Two ways to install it, with the same result: one daily job and one dashboard. Pick whichever
suits you.

- **[Option A: Asset Bundle (CLI)](#option-a-asset-bundle-cli)**: two commands.
- **[Option B: Workspace UI](#option-b-workspace-ui)**: no CLI needed.

Neither needs any settings. By default the job writes to a `genie_monitoring` schema in the
workspace's default catalog and creates the dashboard on its first run. Budgets and Genie Agent
names from other workspaces are optional extras you can turn on later ([below](#turn-on-budgets-optional)).

## Prerequisites

1. **A Unity Catalog workspace** with serverless jobs and a SQL warehouse (serverless recommended).
2. **System tables enabled** for the `access`, `billing` and `compute` schemas. A metastore admin
   can check and enable them with `databricks system-schemas list <metastore-id>` and
   `databricks system-schemas enable <metastore-id> <schema>`.
3. **The identity that runs the job** (you, unless you change "Run as") needs:
   - `SELECT` on `system.access.audit`, `system.access.assistant_events`,
     `system.access.workspaces_latest`, `system.billing.usage`, `system.billing.list_prices` and
     `system.compute.warehouses`;
   - `USE CATALOG` and `CREATE SCHEMA` on the target catalog (or `CREATE TABLE` on an existing
     schema).

## Option A: Asset Bundle (CLI)

Needs a recent [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (tested
with v1.21), signed in to the workspace (`databricks auth login --host <workspace-url>`).

```bash
git clone https://github.com/ghajaanan-arunachalam_data/advanced_genie_monitoring.git
cd advanced_genie_monitoring
databricks bundle deploy --profile <profile>
databricks bundle run genie_monitoring_refresh --profile <profile>
```

This creates:
- the job **Advanced Genie Monitoring - daily refresh** (serverless, daily at 04:00 UTC);
- an empty secret scope `genie_monitoring`, used only if you turn on budgets;
- after the first run, the published dashboard in
  `/Workspace/Users/<you>/advanced_genie_monitoring/`. The run's output prints its link.

To change a default, add `--var` to `bundle deploy`:

| Variable | Default | What it does |
|---|---|---|
| `catalog` | workspace default catalog | Catalog for the job's tables. |
| `schema` | `genie_monitoring` | Schema for the job's tables (created if missing). |
| `warehouse_id` | a serverless warehouse | SQL warehouse the dashboard uses. |
| `bu_label` | `Business Unit` | What the dashboard calls the `bu` column ([see below](#fill-in-and-rename-your-groupings)). |
| `environment_label` | `Environment` | What the dashboard calls the `environment` column. |

For example: `databricks bundle deploy --var="catalog=analytics" --var="bu_label=Team"`.

## Option B: Workspace UI

### 1. Bring the files into the workspace
Either:
- **Git folder:** in **Workspace**, open your home folder, then **Create** > **Git folder**. Paste
  the repository URL and click **Create Git folder**. (A private repository needs a
  [Git credential](https://docs.databricks.com/repos/get-access-tokens-from-git-provider.html) first.)
- **Upload:** download the repository as a ZIP from GitHub, then in **Workspace** open a folder's
  menu (⋮) > **Import**, and import the ZIP.

You should now have a folder containing `src/genie_monitoring_refresh` (a notebook) and
`src/dashboard_template.json`.

### 2. Create the job
1. Go to **Jobs & Pipelines**, then **Create** > **Job**.
2. For the first task: **Type** Notebook, **Source** Workspace, **Path**
   `<your folder>/src/genie_monitoring_refresh`, **Compute** Serverless. Click **Create task**.
3. Under **Schedules & Triggers**, click **Add trigger**, choose **Scheduled**, set it to run every
   day at 04:00 in the UTC time zone, and **Save**.
4. Optional: rename the job (for example "Advanced Genie Monitoring - daily refresh"). To change a
   default, add a job parameter under **Job parameters** with the variable name from the
   [Option A table](#option-a-asset-bundle-cli) (`catalog`, `schema`, `warehouse_id`, `bu_label`,
   `environment_label`).

### 3. Run it
Click **Run now**. The first run takes a few minutes. It creates the tables, then the published
dashboard, named **Advanced Genie Monitoring**, in the folder from step 1. Its link is at the end of
the run's output.

## After the first run

### Share the dashboard
The job publishes the dashboard with **individual data permissions**: queries run as the person
viewing it, so each viewer only sees what they're allowed to read. Viewers need:
- `CAN VIEW` on the dashboard (**Share** in the dashboard) and `CAN USE` on its SQL warehouse;
- `SELECT` on the system tables listed in [Prerequisites](#prerequisites), granted by a metastore
  admin;
- `USE CATALOG`, `USE SCHEMA` and `SELECT` on the job's schema, for example:
  ```sql
  GRANT USE CATALOG ON CATALOG <catalog> TO `genie-monitoring-viewers`;
  GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.genie_monitoring TO `genie-monitoring-viewers`;
  ```

If you'd rather not grant system tables to viewers, republish with **Share data permissions**
(**Publish** in the dashboard). Queries then run as the publisher, so every viewer sees all
usage, including other people's, and the **My Genie Agents** tab shows the publisher's usage, not
the viewer's.

### Fill in (and rename) your groupings
`workspace_summary` has one row per workspace in the account, added by the job. Two columns are
yours to fill in, and they drive the dashboard's filters:

| Column | Meant for | Could equally be |
|---|---|---|
| `bu` | business unit | team, application, department, cost centre, product |
| `environment` | environment | development / test / production, region, data classification |

```sql
UPDATE <catalog>.genie_monitoring.workspace_summary
SET bu = 'Finance', environment = 'Production'
WHERE workspace_id = 1234567890123456;
```
Blank values show as `(unmapped)` in the dashboard. The job never overwrites what you enter, and
adds new workspaces on each run. If you already keep this mapping elsewhere, you can
`MERGE` it in, or schedule your own job to keep it in sync.

**To rename the labels** (for example "Business Unit" to "Team"), set `bu_label` and
`environment_label` (Asset Bundle variables, or job parameters in the UI), then run the job once
with `overwrite_dashboard` set to `true`:
- Asset Bundle: `databricks bundle deploy --var="bu_label=Team" --var="environment_label=Stage"`, then
  `databricks bundle run genie_monitoring_refresh --params overwrite_dashboard=true`.
- UI: in the job, under **Job parameters**, add `bu_label` and `environment_label` with your labels
  and `overwrite_dashboard` = `true`, click **Run now**, then set `overwrite_dashboard` back to
  `false` so later runs keep your dashboard edits.

The labels change in the filters, chart titles, column headers and descriptions. The column names
in `workspace_summary` stay `bu` and `environment`. Overwriting resets any edits you've made to
the dashboard, so rename before customising it.

## Turn on budgets (optional)

Unity AI Gateway budgets aren't in system tables. The job reads them from the account Budget API,
which only account admins can call, so it needs an account-admin service principal. Until you
add one, the dashboard's Budget columns say `N/A: budgets not set up` and the job still succeeds.

1. In the **account console**, go to **User management** > **Service principals** > **Add service
   principal**, and give it a name (for example `genie-monitoring`).
2. On its **Roles** tab, turn on **Account admin**.
3. On its **Secrets** tab, click **Generate secret**. Copy the **Client ID** and **Secret**.
4. Store both in the `genie_monitoring` secret scope of the workspace that runs the job:
   ```bash
   databricks secrets put-secret genie_monitoring client_id --profile <profile>      # paste the client ID
   databricks secrets put-secret genie_monitoring client_secret --profile <profile>  # paste the secret
   ```
   With Option B the scope doesn't exist yet: create it first at
   `https://<workspace-url>#secrets/createScope` (Scope name `genie_monitoring`), or with
   `databricks secrets create-scope genie_monitoring`.
5. Run the job (or wait for tomorrow's run). The Budget columns fill in.

The account ID and accounts host are detected automatically on AWS, Azure and GCP. To override them,
add `account_id` and `account_host` secrets to the same scope.

**Genie Agent names from other workspaces.** The Genie API only lists agents in the workspace you
call it from. If the service principal above is also a member of your other workspaces, the job
lists their agents too, so the dashboard can show their names. Without it, agents outside the job's
workspace show by ID. Workspaces the service principal can't reach are skipped.

**If a run fails**, the budget columns say `N/A: budget job failed` and the job run is marked
failed. Its output (and `budget_job_status.detail`) says why. The most common causes are a
service principal without the Account admin role, or a wrong or expired secret.

## Upgrading

Pull the latest version (in a Git folder, **Pull**; on the command line, `git pull`), then:
- Asset Bundle: `databricks bundle deploy`, then
  `databricks bundle run genie_monitoring_refresh --params overwrite_dashboard=true`.
- UI: add the job parameter `overwrite_dashboard` = `true`, click **Run now**, then set it back to
  `false`.

Overwriting replaces the dashboard's layout and queries with the new version (your sharing
settings stay). Without it, the job leaves an existing dashboard unchanged.

## Uninstalling

- Asset Bundle: `databricks bundle destroy` removes the job and the secret scope.
  UI: delete the job (and the secret scope, if you created one).
- Delete the dashboard and the repository folder.
- `DROP SCHEMA <catalog>.genie_monitoring CASCADE;` removes the tables.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Job fails with `TABLE_OR_VIEW_NOT_FOUND` on a `system.*` table | The system schema isn't enabled, or the job's identity can't read it. See [Prerequisites](#prerequisites). |
| Job fails creating the schema | The job's identity can't create schemas in the default catalog. Set `catalog` (and `schema`) to somewhere it can write. |
| Dashboard visuals error with "table not found" | Run the job once; it creates the tables. Viewers also need `SELECT` on the schema. |
| Budget column says `N/A: budget job failed` | See the job run output. Check the service principal has **Account admin** and the secret is current. |
| Genie Agents show as IDs, not names | The agent is in another workspace. See "Genie Agent names from other workspaces" above. |
