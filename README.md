# BI Autopilot — Slack Bot with Claude Agent

An automated BI reporting bot that responds to data requests in Slack using an AI agent (Claude Haiku) connected to a live Postgres database.

Built as an internal tool at **Bites** (a micro-learning SaaS platform) to eliminate manual, repetitive BI report requests from Customer Success.

---

## What it does

When a team member tags the bot in `#bi-sync-test` with a data request (e.g. *"how many active users did Electra have last month?"*), the bot:

1. Classifies the request into one of 3 tracks
2. Runs the appropriate SQL query against a Postgres read replica
3. Posts the result back to Slack — either automatically or after Adi's approval

---

## Decision Engine — 3 Tracks

| Track | Trigger | Action |
|-------|---------|--------|
| **A — Auto Answer** | Summary request, R2/R16 templates | Run SQL → post answer directly to thread → FYI DM to Adi |
| **B — Pending Approval** | Per-user data, CSV output, >30 rows | Run SQL → post summary to thread → DM Adi full data + "APPROVE/SKIP" |
| **C — Route to Adi** | Unknown template, missing params, ambiguous | Post "Adi will get back to you ⏳" → DM Adi with details |

---

## Report Templates

| ID | Name | Track |
|----|------|-------|
| R2 | Active Users | A |
| R16 | Overall KPIs | A |
| R17 | User Analysis (per-user engagement) | B |
| R1 | Playlist Completion | B |
| R20 | Assigned Watch Status / Roster | B |
| R20b | Playlist Engagement with Attributes (role/branch) | B |
| R3 | Quiz Results Per User | B |
| R5 | Dropoff Funnel | B |

---

## Architecture

```
Slack message (@bot)
        │
        ▼
Vercel Serverless Function (Node.js)
        │
        ├─ Verifies Slack signature (HMAC-SHA256)
        ├─ Acknowledges Slack within 3s
        │
        ▼
Claude Haiku (Anthropic API) — Agentic loop (up to 10 rounds)
        │
        ├─ Tool: run_sql     → AWS RDS Postgres read replica
        ├─ Tool: post_to_thread → Slack channel thread
        └─ Tool: send_dm_to_adi → Adi's Slack DM
```

---

## Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Node.js (ES Modules) |
| Hosting | Vercel Serverless Functions |
| AI Agent | Anthropic Claude Haiku (`claude-haiku-4-5`) |
| Database | PostgreSQL on AWS RDS (read replica) |
| Slack | `@slack/web-api` — Bot Token + Event Subscriptions |
| Auth | Slack signature verification (HMAC-SHA256, replay attack guard) |

---

## Security

- Slack signature verified on every request (timing-safe comparison)
- Replay attack protection (5-minute timestamp window)
- All SQL is read-only — no writes to production DB
- Secrets in environment variables, never in code

---

## Key Implementation Details

- **`waitUntil`** (Vercel) — keeps the serverless function alive after the HTTP response, so the Claude agentic loop can complete without Slack timing out
- **Agentic loop** — Claude calls tools iteratively (max 10 rounds) until `stop_reason === 'end_turn'`
- **Message splitting** — Slack's 40k char limit handled automatically; long DMs split into parts
- **Materialized view fallback** — R17 checks if the MV has data for the org, falls back to raw tables if not

---

## Environment Variables

```
SLACK_BOT_TOKEN
SLACK_SIGNING_SECRET
ANTHROPIC_API_KEY
PG_CONNECTION
ADI_SLACK_USER_ID
BOT_USER_ID
```
