import Anthropic from '@anthropic-ai/sdk';
import { WebClient } from '@slack/web-api';
import { Client as PgClient } from 'pg';
import crypto from 'crypto';
import { waitUntil } from '@vercel/functions';

// ── clients ──────────────────────────────────────────────────────────────────
const slack = new WebClient(process.env.SLACK_BOT_TOKEN);
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── Slack signature verification ──────────────────────────────────────────────
function verifySlackSignature(req, rawBody) {
  const timestamp = req.headers['x-slack-request-timestamp'];
  const slackSig  = req.headers['x-slack-signature'];
  if (!timestamp || !slackSig) return false;
  if (Math.abs(Date.now() / 1000 - Number(timestamp)) > 300) return false;
  const baseString = `v0:${timestamp}:${rawBody}`;
  const hmac = crypto.createHmac('sha256', process.env.SLACK_SIGNING_SECRET);
  hmac.update(baseString);
  const computed = `v0=${hmac.digest('hex')}`;
  return crypto.timingSafeEqual(Buffer.from(computed), Buffer.from(slackSig));
}

// ── Postgres ──────────────────────────────────────────────────────────────────
async function runSql(query) {
  const pg = new PgClient({
    connectionString: process.env.PG_CONNECTION,
    ssl: { rejectUnauthorized: false }
  });
  await pg.connect();
  try {
    const result = await pg.query(query);
    return result.rows;
  } finally {
    await pg.end();
  }
}

// ── SQL Templates (stored in code — not in SYSTEM_PROMPT) ────────────────────
const SQL_TEMPLATES = {
  R2: ({ org_id, start_date, end_date }) => `
WITH total_org_users AS (
  SELECT COUNT(DISTINCT uo.user_id) AS total_users
  FROM users_userorganization uo JOIN users_user u ON uo.user_id = u.id
  WHERE uo.organization_id = ${org_id} AND uo.active = TRUE
    AND u.is_staff = FALSE AND u.full_name NOT IN ('Bites User','Bites Admin')
),
active_in_range AS (
  SELECT COUNT(DISTINCT bsu.user_id) AS active_users
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  JOIN bites_bite b ON bs.bite_id = b.id
  WHERE b.organization_id = ${org_id}
    AND bsu.created_at >= '${start_date}' AND bsu.created_at < '${end_date}'
    AND bsu.user_id != 1
)
SELECT t.total_users, a.active_users,
  ROUND(a.active_users * 100.0 / NULLIF(t.total_users,0), 1) AS pct_active
FROM total_org_users t, active_in_range a;`,

  R16: ({ org_id }) => `
WITH users_count AS (
  SELECT COUNT(DISTINCT uo.user_id) AS total_users
  FROM users_userorganization uo JOIN users_user u ON uo.user_id = u.id
  WHERE uo.organization_id = ${org_id} AND uo.active = TRUE
    AND u.is_staff = FALSE AND u.full_name NOT IN ('Bites User','Bites Admin')
),
bites_count AS (
  SELECT COUNT(*) AS total_bites_created FROM bites_bite
  WHERE organization_id = ${org_id} AND NOT deleted
),
playlists_count AS (
  SELECT COUNT(*) AS total_playlists,
    COUNT(*) FILTER (WHERE is_quiz = TRUE) AS total_quizzes
  FROM bites_playlist WHERE organization_id = ${org_id} AND NOT deleted
),
views_count AS (
  SELECT COUNT(DISTINCT (bsu.user_id, bs.bite_id)) AS total_bite_views
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  JOIN bites_bite bb ON bs.bite_id = bb.id
  JOIN users_userorganization uo ON bsu.user_id = uo.user_id AND uo.organization_id = ${org_id}
  JOIN users_user u ON bsu.user_id = u.id
  WHERE bb.organization_id = ${org_id} AND bsu.user_id != 1
    AND uo.active = TRUE AND u.is_staff = FALSE
    AND u.full_name NOT IN ('Bites User','Bites Admin')
)
SELECT u.total_users, b.total_bites_created, p.total_playlists, p.total_quizzes, v.total_bite_views
FROM users_count u, bites_count b, playlists_count p, views_count v;`,

  R17_CHECK: ({ org_id }) => `
SELECT COUNT(*) AS row_count FROM user_feed_playlists_mv WHERE organization_id = ${org_id};`,

  R17_MV_PLAYLISTS: ({ org_id, start_date, end_date }) => `
SELECT fp.user_id, fp.playlist_id, fp.subject AS content_name,
  fp.playlist_is_quiz AS is_quiz, fp.is_completed, fp.progress AS progress_pct,
  fp.feed_created_at AS first_seen_at, fp.materialized_at AS data_freshness
FROM user_feed_playlists_mv fp
JOIN users_userorganization uo ON uo.user_id = fp.user_id AND uo.organization_id = fp.organization_id
WHERE fp.organization_id = ${org_id}
  AND fp.playlist_deleted = false AND uo.user_id != 1 AND uo.active = true
  ${start_date ? `AND fp.feed_created_at >= '${start_date}'` : ''}
  ${end_date ? `AND fp.feed_created_at <= '${end_date}'` : ''}
ORDER BY fp.user_id ASC, fp.feed_created_at DESC LIMIT 500;`,

  R17_MV_BITES: ({ org_id, start_date, end_date }) => `
SELECT fb.user_id, fb.bite_id AS content_id, fb.subject AS content_name,
  fb.is_completed, fb.bite_progress, fb.video_progress AS video_progress_pct,
  fb.feed_created_at AS first_seen_at
FROM user_feed_bites_mv fb
JOIN users_userorganization uo ON uo.user_id = fb.user_id AND uo.organization_id = fb.organization_id
WHERE fb.organization_id = ${org_id}
  AND fb.bite_deleted = false AND uo.user_id != 1 AND uo.active = true
  AND fb.biteshareuser_id IS NOT NULL
  ${start_date ? `AND fb.feed_created_at >= '${start_date}'` : ''}
  ${end_date ? `AND fb.feed_created_at <= '${end_date}'` : ''}
ORDER BY fb.user_id ASC, fb.feed_created_at DESC LIMIT 500;`,

  R17_FALLBACK: ({ org_id, start_date, end_date }) => `
WITH user_engagement AS (
  SELECT bsu.user_id, u.full_name,
    COUNT(DISTINCT bs.bite_id) AS total_bites_viewed,
    COUNT(DISTINCT bs.playlist_id) AS total_playlists_touched,
    MIN(bsu.created_at::date) AS first_view,
    MAX(bsu.created_at::date) AS last_view
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  JOIN bites_bite b ON bs.bite_id = b.id
  JOIN users_user u ON bsu.user_id = u.id
  WHERE b.organization_id = ${org_id}
    AND bsu.user_id != 1 AND u.is_staff = FALSE
    AND u.full_name NOT IN ('Bites User', 'Bites Admin')
    ${start_date ? `AND bsu.created_at >= '${start_date}'` : ''}
    ${end_date ? `AND bsu.created_at < '${end_date}'` : ''}
  GROUP BY bsu.user_id, u.full_name
)
SELECT full_name, total_bites_viewed, total_playlists_touched, first_view, last_view
FROM user_engagement ORDER BY total_bites_viewed DESC LIMIT 50;`,

  R1: ({ org_id, playlist_id }) => `
SELECT fp.user_id, fp.playlist_id, fp.subject AS playlist_name,
  fp.playlist_is_quiz AS is_quiz, fp.is_completed, fp.progress AS progress_pct,
  fp.is_assigned, fp.assignment_due_date AS due_date,
  fp.feed_created_at AS first_seen_at,
  CASE
    WHEN fp.is_completed = true THEN 'Completed'
    WHEN fp.progress > 0 THEN 'In Progress'
    ELSE 'Not Started'
  END AS status
FROM user_feed_playlists_mv fp
WHERE fp.organization_id = ${org_id}
  AND fp.playlist_id = ${playlist_id}
  AND fp.playlist_deleted = false
ORDER BY fp.is_completed DESC, fp.progress DESC LIMIT 500;`,

  R20: ({ org_id, playlist_id, start_date, end_date }) => `
WITH
playlist_info AS (
  SELECT COUNT(DISTINCT bs.bite_id) AS total_bites
  FROM bites_biteshare bs WHERE bs.playlist_id = ${playlist_id}
),
assigned_users AS (
  SELECT DISTINCT f.user_id
  FROM bites_feed f JOIN users_user u ON f.user_id = u.id
  WHERE f.organization_id = ${org_id} AND f.playlist_id = ${playlist_id}
    AND f.user_id != 1 AND u.is_staff = FALSE
    AND u.full_name NOT IN ('Bites User', 'Bites Admin')
),
user_views AS (
  SELECT bsu.user_id,
    COUNT(DISTINCT bs.bite_id) AS viewed_bites,
    MIN(bsu.created_at::date) AS first_view,
    MAX(bsu.created_at::date) AS last_view
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  WHERE bs.playlist_id = ${playlist_id} AND bsu.user_id != 1
    ${start_date ? `AND bsu.created_at >= '${start_date}'` : ''}
    ${end_date ? `AND bsu.created_at < '${end_date}'` : ''}
  GROUP BY bsu.user_id
)
SELECT a.user_id AS "User ID",
  COALESCE(v.viewed_bites, 0) || '/' || pi.total_bites AS "Views",
  CASE
    WHEN COALESCE(v.viewed_bites, 0) = 0 THEN 'Not Started'
    WHEN v.viewed_bites >= pi.total_bites THEN 'Completed'
    ELSE 'In Progress'
  END AS "Status",
  ROUND(COALESCE(v.viewed_bites, 0) * 1.0 / NULLIF(pi.total_bites, 0), 3) AS "Completion",
  v.first_view AS "First View", v.last_view AS "Last View"
FROM assigned_users a CROSS JOIN playlist_info pi
LEFT JOIN user_views v ON a.user_id = v.user_id
ORDER BY
  CASE WHEN COALESCE(v.viewed_bites,0)=0 THEN 3 WHEN v.viewed_bites>=pi.total_bites THEN 1 ELSE 2 END,
  a.user_id;`,

  R20b: ({ org_id, playlist_id, start_date, end_date }) => `
WITH user_playlist AS (
  SELECT
    bsu.user_id, u.full_name, bs.playlist_id, p.subject AS playlist_subject,
    (SELECT COUNT(DISTINCT bs2.bite_id) FROM bites_biteshare bs2 WHERE bs2.playlist_id = bs.playlist_id) AS total_bites_in_playlist,
    MAX(CASE WHEN at2.attribute_index = 1 THEN av.name END) AS data1_value,
    MAX(CASE WHEN at2.attribute_index = 2 THEN av.name END) AS data2_value,
    COUNT(DISTINCT CASE WHEN bsu.created_at >= '${start_date}' THEN bs.bite_id END) AS viewed_bites_in_period,
    COUNT(DISTINCT bs.bite_id) AS viewed_bites_all_time,
    MAX(CASE WHEN bsu.created_at >= '${start_date}' THEN bsu.created_at::date END) AS last_view_in_period,
    TO_CHAR(MAX(CASE WHEN bsu.created_at >= '${start_date}' THEN bsu.created_at END), 'YYYY-MM') AS last_view_month
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  JOIN bites_playlist p ON bs.playlist_id = p.id
  JOIN users_user u ON bsu.user_id = u.id
  LEFT JOIN users_userorganization uo ON uo.user_id = bsu.user_id AND uo.organization_id = ${org_id}
  LEFT JOIN organizations_userorganizationattributevalue uoav ON uoav.user_organization_id = uo.id
  LEFT JOIN organizations_attributevalue av ON av.id = uoav.attribute_value_id
  LEFT JOIN organizations_attributetitle at2 ON at2.id = uoav.attribute_title_id AND at2.organization_id = ${org_id}
  WHERE p.organization_id = ${org_id} AND bs.playlist_id = ${playlist_id}
    AND bsu.user_id != 1 AND u.is_staff = FALSE
    AND u.full_name NOT IN ('Bites User', 'Bites Admin')
  GROUP BY bsu.user_id, u.full_name, bs.playlist_id, p.subject
)
SELECT
  full_name AS "Name", data1_value AS "Role", data2_value AS "Branch",
  CASE WHEN viewed_bites_all_time >= total_bites_in_playlist THEN 'Yes' ELSE 'No' END AS "Completed",
  CONCAT(viewed_bites_in_period::text, '/', total_bites_in_playlist::text) AS "Started",
  last_view_in_period AS "Last View Date", last_view_month AS "Last View Month"
FROM user_playlist
WHERE viewed_bites_in_period > 0
ORDER BY playlist_subject, full_name;`,

  R3: ({ org_id, playlist_id }) => `
SELECT bsu.user_id, bs.playlist_id, p.subject AS playlist_subject,
  COUNT(DISTINCT uc.question_id) AS total_questions,
  COUNT(DISTINCT CASE WHEN c.is_correct = TRUE THEN uc.question_id END) AS correct_answers,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.is_correct = TRUE THEN uc.question_id END) * 100.0
    / NULLIF(COUNT(DISTINCT uc.question_id), 0), 1
  ) AS score_pct
FROM bites_biteshareuser bsu
JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
JOIN bites_playlist p ON bs.playlist_id = p.id
JOIN users_user u ON bsu.user_id = u.id
JOIN bites_userchoice uc ON uc.bite_share_user_id = bsu.id
LEFT JOIN bites_choice c ON c.id = uc.choice_id
WHERE p.organization_id = ${org_id} AND bs.playlist_id = ${playlist_id}
  AND p.is_quiz = TRUE AND bsu.user_id != 1 AND u.is_staff = FALSE
  AND u.full_name NOT IN ('Bites User', 'Bites Admin')
GROUP BY bsu.user_id, bs.playlist_id, p.subject
ORDER BY score_pct DESC;`,

  R5: ({ org_id, playlist_id, from_date }) => `
WITH
playlist_bites AS (
  SELECT COUNT(DISTINCT bite_id) AS total_bites
  FROM bites_biteshare WHERE playlist_id = ${playlist_id}
),
user_views AS (
  SELECT bsu.user_id,
    COUNT(DISTINCT bs.bite_id) AS viewed_bites,
    MAX(bs."order") AS last_bite_order
  FROM bites_biteshareuser bsu
  JOIN bites_biteshare bs ON bsu.bite_share_id = bs.id
  JOIN bites_bite b ON bs.bite_id = b.id
  WHERE bs.playlist_id = ${playlist_id} AND b.organization_id = ${org_id}
    AND bsu.created_at >= '${from_date}' AND bsu.user_id != 1
  GROUP BY bsu.user_id
),
assigned_users AS (
  SELECT COUNT(DISTINCT user_id) AS assigned_count
  FROM bites_feed WHERE organization_id = ${org_id} AND playlist_id = ${playlist_id} AND user_id != 1
),
summary AS (
  SELECT a.assigned_count,
    COUNT(DISTINCT uv.user_id) AS started_count,
    COUNT(DISTINCT CASE WHEN uv.viewed_bites >= pb.total_bites THEN uv.user_id END) AS completed_count
  FROM assigned_users a CROSS JOIN playlist_bites pb LEFT JOIN user_views uv ON TRUE
  GROUP BY a.assigned_count
),
dropoff AS (
  SELECT uv.last_bite_order, COUNT(*) AS users_stopped_here
  FROM user_views uv CROSS JOIN playlist_bites pb
  WHERE uv.viewed_bites < pb.total_bites
  GROUP BY uv.last_bite_order
)
SELECT 'summary' AS result_type, s.assigned_count, s.started_count, s.completed_count,
  ROUND(s.started_count * 100.0 / NULLIF(s.assigned_count,0),1) AS start_rate_pct,
  ROUND(s.completed_count * 100.0 / NULLIF(s.started_count,0),1) AS completion_rate_pct,
  NULL AS last_bite_order, NULL AS users_stopped_here
FROM summary s
UNION ALL
SELECT 'dropoff', NULL, NULL, NULL, NULL, NULL, d.last_bite_order, d.users_stopped_here
FROM dropoff d ORDER BY result_type, last_bite_order;`
};

// ── Run a named template ──────────────────────────────────────────────────────
async function runTemplate(templateId, params) {
  if (templateId === 'R17') {
    // Step 1: check MV
    const checkRows = await runSql(SQL_TEMPLATES.R17_CHECK(params));
    const hasMV = parseInt(checkRows[0]?.row_count || '0') > 0;
    const sql1 = SQL_TEMPLATES.R17_CHECK(params);
    if (hasMV) {
      const sql2 = SQL_TEMPLATES.R17_MV_PLAYLISTS(params);
      const sql3 = SQL_TEMPLATES.R17_MV_BITES(params);
      const [playlists, bites] = await Promise.all([runSql(sql2), runSql(sql3)]);
      return { rows: [...playlists, ...bites], row_count: playlists.length + bites.length, sql: `${sql1}\n---\n${sql2}\n---\n${sql3}` };
    } else {
      const sql2 = SQL_TEMPLATES.R17_FALLBACK(params);
      const rows = await runSql(sql2);
      return { rows, row_count: rows.length, sql: `${sql1}\n---\n${sql2}` };
    }
  }
  const sqlFn = SQL_TEMPLATES[templateId];
  if (!sqlFn) return { error: `Unknown template: ${templateId}` };
  const sql = sqlFn(params);
  const rows = await runSql(sql);
  return { rows, row_count: rows.length, sql };
}

// ── Tools ─────────────────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: 'use_template',
    description: 'Run a predefined BI report template. Use for R1, R2, R3, R5, R16, R17, R20, R20b.',
    input_schema: {
      type: 'object',
      properties: {
        template_id: { type: 'string', description: 'R1 | R2 | R3 | R5 | R16 | R17 | R20 | R20b' },
        params: {
          type: 'object',
          description: 'Parameters: org_id (required), playlist_id, start_date, end_date, from_date'
        }
      },
      required: ['template_id', 'params']
    }
  },
  {
    name: 'run_sql',
    description: 'Run a custom read-only SQL query. Use ONLY for Track D (free queries not covered by templates).',
    input_schema: {
      type: 'object',
      properties: { query: { type: 'string' } },
      required: ['query']
    }
  },
  {
    name: 'post_to_thread',
    description: 'Post a message as a thread reply in the Slack channel.',
    input_schema: {
      type: 'object',
      properties: {
        channel_id: { type: 'string' },
        thread_ts:  { type: 'string' },
        message:    { type: 'string' }
      },
      required: ['channel_id', 'thread_ts', 'message']
    }
  },
  {
    name: 'send_dm_to_adi',
    description: 'Send a private DM to Adi — for approval requests, FYI, or alerts.',
    input_schema: {
      type: 'object',
      properties: { message: { type: 'string' } },
      required: ['message']
    }
  }
];

// ── Tool executor ─────────────────────────────────────────────────────────────
async function executeTool(name, input) {
  if (name === 'use_template') {
    try {
      const result = await runTemplate(input.template_id, input.params);
      return result;
    } catch (err) {
      return { error: err.message };
    }
  }

  if (name === 'run_sql') {
    try {
      const rows = await runSql(input.query);
      return { rows, row_count: rows.length, sql: input.query };
    } catch (err) {
      return { error: err.message };
    }
  }

  if (name === 'post_to_thread') {
    if (!input.message || input.message.trim() === '') {
      return { error: 'message cannot be empty' };
    }
    const text = input.message.length > 39000
      ? input.message.slice(0, 39000) + '\n\n_[truncated]_'
      : input.message;
    await slack.chat.postMessage({ channel: input.channel_id, thread_ts: input.thread_ts, text });
    return { ok: true };
  }

  if (name === 'send_dm_to_adi') {
    if (!input.message || input.message.trim() === '') {
      return { error: 'message cannot be empty' };
    }
    const MAX = 39000;
    if (input.message.length > MAX) {
      await slack.chat.postMessage({ channel: process.env.ADI_SLACK_USER_ID, text: input.message.slice(0, MAX) });
      await slack.chat.postMessage({ channel: process.env.ADI_SLACK_USER_ID, text: '_(continued)_\n' + input.message.slice(MAX) });
    } else {
      await slack.chat.postMessage({ channel: process.env.ADI_SLACK_USER_ID, text: input.message });
    }
    return { ok: true };
  }

  return { error: `Unknown tool: ${name}` };
}

// ── System prompt (slim — no SQL) ─────────────────────────────────────────────
const SYSTEM_PROMPT = `
You are BI Autopilot, an automated BI reporting agent for Bites — a mobile-first micro-learning platform for frontline workers.

## YOUR TOOLS
- use_template: run a predefined report template (R1/R2/R3/R5/R16/R17/R20/R20b)
- run_sql: run a custom SQL query (Track D only)
- post_to_thread: post a reply in the Slack thread
- send_dm_to_adi: send Adi a private DM

## DECISION ENGINE

### Track A — AUTO_ANSWER
When: R2 or R16, all params present
Action: use_template → post answer to thread → send FYI DM to Adi with SQL + result

### Track B — PENDING_APPROVAL
When: R17, R1, R3, R5, R20, R20b — OR result > 30 rows — OR output = CSV/Excel/Google Sheet
Action: use_template → post SHORT summary to thread → DM Adi with full tab-separated data + SQL + "APPROVE/SKIP"

### Track D — FREE_QUERY
When: valid BI question, org_id known, does NOT match any template
Action: run_sql (write safe read-only SQL) → post SHORT summary to thread (mark ⚠️ AI-generated) → DM Adi with full data + SQL + "APPROVE/SKIP"

### Track C — ROUTE_TO_ADI
When: ambiguous, missing org_id, too complex, output = Power BI dashboard
Action: post "Adi will get back to you shortly ⏳" → DM Adi with details

## REPORT TEMPLATES

R2 — Active Users | Keywords: active users, engagement rate, % active | Required: org_id, start_date, end_date | Track A
R16 — Overall KPIs | Keywords: KPIs, overview, general stats, total users, bites created | Required: org_id | Track A
R17 — User Analysis | Keywords: user analysis, per-user engagement, who viewed what, user activity | Required: org_id | Optional: start_date, end_date | Track B
R1 — Playlist Completion | Keywords: who completed, completion rate, playlist progress, not started | Required: org_id, playlist_id | Track B
R20 — Assigned Watch Status | Keywords: assigned users, watch status, roster, who was assigned | Required: org_id, playlist_id | Optional: start_date, end_date | Track B
R20b — Playlist Engagement with Attributes | Keywords: who watched, role, branch, תפקיד, סניף, per-role breakdown | Required: org_id, playlist_id, start_date | Track B — use when requester wants names or role/branch breakdown
R3 — Quiz Results | Keywords: quiz results, quiz scores, who passed, quiz performance | Required: org_id, playlist_id | Track B
R5 — Dropoff Funnel | Keywords: dropoff, funnel, where did they stop, abandonment | Required: org_id, playlist_id, from_date | Track B

## ORG SHORTCUTS
- "Electra" → orgs [25268,25191,24099,19516,17655,12911,5108,259,8179]
- "Afikim" → 8179 | "Castro" → 3695 | "Unilever" → 21109 | "Urbanica" → 4839
- Unknown name → run: SELECT id, readable_name FROM organizations_organization WHERE readable_name ILIKE '%{name}%' LIMIT 5

## CRITICAL SQL RULES (Track D only)
- ALWAYS filter user_id != 1, is_staff = FALSE, full_name NOT IN ('Bites User','Bites Admin')
- ALWAYS filter by organization_id
- NEVER use last_visit — use bites_biteshareuser.created_at
- Quote "order" column in bites_biteshare

## BITES CONCEPTS
- Bite: single micro-learning video. Viewed = biteshareuser_id IS NOT NULL. Completed = is_completed = true in MV.
- Playlist: ordered collection of bites. Completed = all bites done. Started = progress > 0.
- Quiz: playlist with is_quiz = TRUE. Success % = correct answers / distinct multiple-choice questions × 100.
- Status priority: Completed > Overdue (due_date < NOW()) > In Progress > Not Started
- Data1–Data8: org-specific employee attributes — always query organizations_attributetitle first.
- MVs refresh every ~5 min. Always use user_feed_bites_mv / user_feed_playlists_mv (never shadow versions).

## RESPONSE FORMAT

### Track A — post to thread:
<@USER_ID> here's your *[Report Title]* — org [org_id]:
📊 *Summary:*
[key metrics as bullet points]
_Template: R[N] — [name]_

### Track A — FYI DM to Adi:
🤖 *BI Autopilot — Auto-answered (Track A)*
*Request:* [summary] (org [org_id])
*Requester:* <@USER_ID>
🔍 *SQL used:*
[exact SQL]
📊 *Result:* [key numbers]

### Track B — post to thread:
[DRAFT - pending approval] ⏳
<@USER_ID> here's your *[Report Title]* — org [org_id]:
📊 [X] Completed · [Y] In Progress · [Z] Not Started (out of [N] total)
_Full data sent to Adi for review._
_Template: R[N] — [name]_

### Track B — DM to Adi:
🤖 *BI Autopilot — Needs your approval*
*Request:* [summary] (org [org_id])
*Template:* R[N] — [name]
*Requester:* <@USER_ID>
🔍 *SQL:*
[exact SQL]
📊 *Results ([N] rows) — copy-paste to Google Sheets:*
[full tab-separated table with headers]
Reply *APPROVE* to publish · Reply *SKIP* to discard

### Track D — post to thread:
[DRAFT - pending approval] ⏳
<@USER_ID> here's a summary — org [org_id]:
📊 [key metrics only]
⚠️ _AI-generated query — pending Adi review._

### Track D — DM to Adi:
⚠️ *AI-generated query — please verify SQL before approving*
[same format as Track B DM]

### Track C:
<@USER_ID> Thanks for your request! ⏳ Adi will get back to you shortly.

IMPORTANT: Always tag the requester as <@USER_ID>. Never write raw user IDs as plain text.
`;

// ── Main handler ──────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).end();

  const rawBody = await new Promise((resolve) => {
    let data = '';
    req.on('data', chunk => data += chunk);
    req.on('end', () => resolve(data));
  });

  if (!verifySlackSignature(req, rawBody)) {
    return res.status(401).json({ error: 'Invalid signature' });
  }

  const body = JSON.parse(rawBody);

  if (body.type === 'url_verification') {
    return res.status(200).json({ challenge: body.challenge });
  }

  const BOT_USER_ID = process.env.BOT_USER_ID || 'U0ALK1C9686';
  const eventType = body.event?.type;
  const isAppMention = eventType === 'app_mention';
  const isMessageWithTag = eventType === 'message'
    && body.event?.user !== BOT_USER_ID
    && body.event?.text?.includes(`<@${BOT_USER_ID}>`);

  if (!isAppMention && !isMessageWithTag) {
    return res.status(200).end();
  }

  res.status(200).end();

  const event = body.event;

  waitUntil((async () => {
    try {
      const userMessage = `
New BI request from Slack:
- Channel: ${event.channel}
- Thread ts (for replies): ${event.thread_ts || event.ts}
- Message ts: ${event.ts}
- From user: ${event.user}
- Text: ${event.text}
`;

      const messages = [{ role: 'user', content: userMessage }];

      for (let i = 0; i < 10; i++) {
        const response = await anthropic.messages.create({
          model: 'claude-haiku-4-5',
          max_tokens: 4096,
          system: SYSTEM_PROMPT,
          tools: TOOLS,
          messages
        });

        messages.push({ role: 'assistant', content: response.content });

        if (response.stop_reason === 'end_turn') break;

        const toolResults = [];
        for (const block of response.content) {
          if (block.type === 'tool_use') {
            const result = await executeTool(block.name, block.input);
            toolResults.push({
              type: 'tool_result',
              tool_use_id: block.id,
              content: JSON.stringify(result)
            });
          }
        }

        if (toolResults.length === 0) break;
        messages.push({ role: 'user', content: toolResults });
      }
    } catch (err) {
      console.error('BI Autopilot error:', err);
      try {
        await slack.chat.postMessage({
          channel: process.env.ADI_SLACK_USER_ID,
          text: `🚨 *BI Autopilot — Unhandled Error*\n*Message:* ${err.message}\n*Request:* ${event.text}\n*User:* <@${event.user}>\n*Channel:* ${event.channel}`
        });
        await slack.chat.postMessage({
          channel: event.channel,
          thread_ts: event.thread_ts || event.ts,
          text: `<@${event.user}> Sorry, something went wrong. Adi has been notified 🔧`
        });
      } catch (notifyErr) {
        console.error('Failed to send error notification:', notifyErr);
      }
    }
  })());
}
