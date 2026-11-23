# Advanced Genie Monitoring

An AI/BI dashboard for monitoring the adoption, quality and cost of Databricks Genie (Genie
Agents, Genie Code and Genie One) across every workspace in your account. A daily job keeps the
few lookup tables it needs up to date. Everything runs on your own system tables, in your own
workspace.

**To install it, see [DEPLOY.md](DEPLOY.md).** It takes about 15 minutes with either the Databricks
CLI (Asset Bundle) or the workspace UI, and needs no decisions up front.

## What you get from it

Genie activity is otherwise spread across audit logs, billing records and the account console,
and none of them break it down by team. This dashboard brings them together so you can see:

- **Who is using Genie and how much:** active users week by week and month by month, per surface,
  by workspace and by your own groupings (business unit and environment by default).
- **Which Genie Agents work well:** the most-used agents, and the ones that are slow, error often,
  get poor feedback, or are barely used.
- **What Genie costs:** paid cost and free usage, per user, per agent, per workspace and per group.
  This shows what spend will look like when the free period for Genie One and Genie Agents ends on
  31 January 2027.
- **Budgets against usage:** each person's Unity AI Gateway budget limit next to their usage, and
  where the limit comes from (their own exception, their group's, or the budget default).
- **Who to engage:** Genie Code champions to amplify, and new, lapsed and one-time users to follow
  up with.

So you can aim enablement where adoption is low, fix or retire agents that aren't serving users,
set budgets and charge back using real usage, and catch unexpected spend (for example from
service principals) early.

## The dashboard

| Tab | What it shows |
|---|---|
| **Overview** | Cost, free usage and unique users for each surface; daily usage and daily users by surface. |
| **Genie Agents** | Active users, agents, messages, peak queries per minute, successes and errors, feedback, WAU and MAU, top users and agents, latency, errors by type, productivity and quality trends. |
| **My Genie Agents** | The viewer's own usage and recent errors. |
| **Genie Agents Usage** | Genie Agents cost and free usage by day, by user and by agent. |
| **Genie One Usage** | Genie One cost and free usage by day and by user. |
| **Genie Code** | Genie Code adoption this month, when it's used, a scorecard per group (new and lapsed users, engagement depth), new vs returning users, and a list of champions and at-risk users. |
| **Genie Code Usage** | Genie Code cost and free usage by day, per-user percentiles, top users, by group and by workspace. |

Every visual has a short description of what it shows.

**Filters** (left-hand panel): **Date range** (default: last 90 days), **Workspace**, **Business
Unit**, **Environment** and **User type**. Some tabs add their own (Genie Agent, SQL warehouse,
application). Not every filter reaches every visual:
- **My Genie Agents** ignores them all: it always shows your own usage.
- **User type** applies to the Genie Code, Genie Code Usage, Genie Agents Usage and Genie One Usage
  tabs. The Genie Agents tab is built from audit logs, which don't record it.
- **Workspace / Business Unit / Environment** don't reach the WAU and MAU chart (it counts each
  person once across all workspaces), the 90-day Genie Code metrics and action list, the Genie
  Code per-user cost visuals, the Overview's Genie Agents and Genie One counters, its unique-user
  counters, or its Daily Unique Users chart.
- **Date range** doesn't change Genie Code metrics that have a fixed window (monthly, 90-day).

**Budget columns.** Five tables show each person's or service principal's AI Gateway budget: Top
Users (Genie Agents), the user-level breakdowns on the three Usage tabs, and Users: champions &
at-risk (Genie Code).
- **Budget:** the per-user budget that covers them. If several do, the one with the lowest limit
  (the one that would block them first).
- **Monthly limit ($):** the highest of their own exception and any of their groups' exceptions. If
  they have none, the budget's default per-user limit. (An exception replaces the default even
  when it's lower.)
- **Limit from:** where that limit came from: Own override, Group override or Budget default.

A budget covers someone when their Genie usage in the last 90 days falls in that budget's
workspaces (none listed means all) and carries its tags. Budgets with only a shared limit (one
cap for everyone together) aren't shown. Until budgets are turned on, the Budget column says why:

| Budget column shows | Meaning |
|---|---|
| `N/A: budgets not set up` | The job has no service principal credentials yet. See DEPLOY.md, "Turn on budgets". |
| `N/A: budget job failed` | The latest budget snapshot failed. The job run's output says why. |
| `N/A: budget job has not run` | The job hasn't run yet. |
| `No per-user budget` | Budgets loaded, and no per-user budget covers this identity. |

## How it works

```
system.access.audit ────────────┐
system.access.assistant_events ─┤
system.billing.usage ───────────┼──────────────────────────────▶ dashboard
system.billing.list_prices ─────┤                                    ▲
system.access.workspaces_latest ┘                                    │
                                                                     │
daily job (src/genie_monitoring_refresh.py) ──▶ <catalog>.<schema>.* ┘
  1. workspace_summary   workspace → name, bu, environment (you fill in bu / environment)
  2. dim_genie_space     Genie Agent → name, SQL warehouse, application tag (Genie API)
  3. budget_*            Unity AI Gateway budgets (account Budget API + SCIM; optional)
  4. creates and publishes the dashboard on the first run
```

| Table | Filled by | Purpose |
|---|---|---|
| `workspace_summary` | job adds new workspaces; **you fill in `bu` and `environment`** | Drives the Workspace, Business Unit and Environment filters. |
| `dim_genie_space` | job, daily | Genie Agent ID to name, SQL warehouse and the warehouse's `application` tag. |
| `budget_snapshot`, `budget_limit`, `budget_workspace`, `budget_tag`, `budget_override` | job, daily (optional) | What each budget is, its monthly limits, the workspaces and tags it covers, and per-person exceptions. |
| `budget_principal`, `budget_group_member` | job, daily (optional) | Turns the IDs in budget exceptions into the emails and application IDs that billing uses, and lists each exception group's members. |
| `budget_job_status` | job, daily | One row per run of the budget step: `SUCCEEDED`, `NOT_CONFIGURED` or `FAILED`. |

The budget tables are appended to each day, so you keep a history of budget settings.

## Definitions and data behaviours worth knowing

- **Monthly vs 30 days.** "Monthly" figures are month to date: they reset on the 1st and grow
  through the month. "30d" figures are a rolling 30 days.
- **WAU / MAU.** Distinct active users over a rolling 7 days (WAU) and 30 days (MAU), for each
  day. The date filter sets which days are shown, not the definition.
- **Latency** is estimated from how long the app polls for an answer (system tables don't record
  Genie response time directly). Messages over 600 seconds are left out of the percentiles and
  shown separately as the long-running rate.
- **User type** is "Human" if the identity is an email address, otherwise "Service".
- **Costs are estimated, at list price** (`system.billing.list_prices`). Your contract may have
  discounts that system tables don't show.
- **Free usage** is only recorded in system tables from 20 July 2026.
- **$0 cost is normal for now.** Genie One and Genie Agents are free for users through 31 January
  2027, and Genie Code includes a free monthly allowance per user. Service principals are billed.
- **Budgets can overlap.** One person can fall under several budgets, so budget figures are for
  monitoring and shouldn't be added together.
- **Deleted people in budget exceptions** appear in `budget_principal` with type `UNKNOWN`.
- **System tables lag** by up to a few hours.

## Known limitations

- **Own vs group exceptions.** Within a budget the most generous exception applies, as Databricks
  documents for per-user thresholds and overrides. The docs don't separately address a person's
  own exception against a group's, so the dashboard applies the same rule to both. When several
  budgets cover someone, the dashboard shows the tightest, since any one of them can block usage.
- **Tag filters** in budgets are matched with `IN` only.
- **The Genie Agents tab** loads more slowly than the others, because its main dataset also looks
  up budgets.
- **Top Users / Top Genie Agents** rank by message volume.
- **Genie Agent names** come from the Genie API, which only returns agents the job's identity can
  see. Agents in other workspaces are named only if the optional service principal is a member of
  those workspaces (DEPLOY.md); otherwise they show by ID.

## Credits

The usage tabs build on the Databricks Genie team's
[Genie Usage Dashboard](https://github.com/databricks/tmm/tree/main/System-Tables-Demo/Genie-Observability).

## Disclaimer

This is a field-built asset, not an official Databricks product. It is provided as is, without
support commitments; test it in your environment before relying on it.
