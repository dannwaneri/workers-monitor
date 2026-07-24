/**
 * workers-monitor — proactive Cloudflare Workers fleet monitor.
 *
 * Hourly pipeline:
 *   cron → GraphQL Analytics (two hour windows, one request)
 *        → deterministic threshold gate (no LLM call on a normal hour)
 *        → Claude Haiku 4.5 judgement (structured output) on candidates only
 *        → Telegram alert (KV-deduped) / daily heartbeat
 *
 * Fail-loud: if the Anthropic call fails after the gate tripped, a raw
 * threshold alert is sent instead of staying silent.
 */

import Anthropic from "@anthropic-ai/sdk";
import * as Sentry from "@sentry/cloudflare";

export interface Env {
  STATE: KVNamespace;
  CF_ACCOUNT_ID: string;
  TELEGRAM_CHAT_ID: string;
  HEARTBEAT_HOUR_UTC: string;
  CF_API_TOKEN: string;
  ANTHROPIC_API_KEY: string;
  TELEGRAM_BOT_TOKEN: string;
  MONITOR_CONTROL_TOKEN: string;
  SENTRY_DSN: string;
}

// ---------------------------------------------------------------------------
// Tuning — thresholds for the deterministic gate.
// ---------------------------------------------------------------------------

/** This worker's own script name — excluded so it never alerts on itself. */
const SELF_NAME = "workers-monitor";

/** Error rate above which a worker becomes a candidate (2%). */
const ERROR_RATE_THRESHOLD = 0.02;
/** ...but only if there are at least this many absolute errors in the hour. */
const MIN_ERRORS = 10;
/** Traffic collapse: current requests dropped by at least this fraction (80%). */
const TRAFFIC_DROP_RATIO = 0.8;
/** ...measured only against workers with a meaningful baseline. */
const MIN_BASELINE_REQUESTS = 50;

/** Re-alert on the same ongoing incident after this many seconds (6h). */
const DEDUP_TTL_SECONDS = 6 * 3600;

const ANTHROPIC_MODEL = "claude-haiku-4-5";
const TELEGRAM_MAX_CHARS = 4000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface WorkerStats {
  requests: number;
  errors: number;
}

interface WorkerRow {
  scriptName: string;
  current: WorkerStats;
  previous: WorkerStats;
}

interface Candidate {
  scriptName: string;
  reason: string;
}

interface Verdict {
  alert: boolean;
  severity: "warning" | "critical";
  workers_affected: string[];
  summary: string;
  evidence: string;
}

// ---------------------------------------------------------------------------
// Cloudflare GraphQL Analytics
// ---------------------------------------------------------------------------

const FLEET_QUERY = `
query FleetHealth($accountTag: string!, $curStart: Time!, $curEnd: Time!, $prevStart: Time!, $prevEnd: Time!) {
  viewer {
    accounts(filter: { accountTag: $accountTag }) {
      current: workersInvocationsAdaptive(
        limit: 1000
        filter: { datetime_geq: $curStart, datetime_lt: $curEnd }
      ) {
        sum { requests errors }
        dimensions { scriptName }
      }
      previous: workersInvocationsAdaptive(
        limit: 1000
        filter: { datetime_geq: $prevStart, datetime_lt: $prevEnd }
      ) {
        sum { requests errors }
        dimensions { scriptName }
      }
    }
  }
}`;

interface GqlGroup {
  sum: { requests: number; errors: number };
  dimensions: { scriptName: string };
}

interface GqlResponse {
  data?: {
    viewer: {
      accounts: Array<{ current: GqlGroup[]; previous: GqlGroup[] }>;
    };
  };
  errors?: Array<{ message: string }>;
}

async function fetchFleetMetrics(
  env: Env,
  curStart: Date,
  curEnd: Date,
  prevStart: Date,
): Promise<WorkerRow[]> {
  const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.CF_API_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: FLEET_QUERY,
      variables: {
        accountTag: env.CF_ACCOUNT_ID,
        curStart: curStart.toISOString(),
        curEnd: curEnd.toISOString(),
        prevStart: prevStart.toISOString(),
        prevEnd: curStart.toISOString(),
      },
    }),
  });

  if (!res.ok) {
    throw new Error(`GraphQL HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
  }
  const json = (await res.json()) as GqlResponse;
  if (json.errors?.length) {
    throw new Error(`GraphQL error: ${json.errors.map((e) => e.message).join("; ")}`);
  }
  const account = json.data?.viewer.accounts[0];
  if (!account) {
    throw new Error("GraphQL returned no account — check CF_ACCOUNT_ID and token scope");
  }

  const toMap = (groups: GqlGroup[]): Map<string, WorkerStats> => {
    const m = new Map<string, WorkerStats>();
    for (const g of groups) {
      const prev = m.get(g.dimensions.scriptName) ?? { requests: 0, errors: 0 };
      m.set(g.dimensions.scriptName, {
        requests: prev.requests + g.sum.requests,
        errors: prev.errors + g.sum.errors,
      });
    }
    return m;
  };

  const current = toMap(account.current);
  const previous = toMap(account.previous);
  const names = new Set([...current.keys(), ...previous.keys()]);
  names.delete(SELF_NAME);

  return [...names].sort().map((scriptName) => ({
    scriptName,
    current: current.get(scriptName) ?? { requests: 0, errors: 0 },
    previous: previous.get(scriptName) ?? { requests: 0, errors: 0 },
  }));
}

// ---------------------------------------------------------------------------
// Deterministic threshold gate — no candidates means no LLM call and no alert.
// ---------------------------------------------------------------------------

function thresholdGate(rows: WorkerRow[]): Candidate[] {
  const candidates: Candidate[] = [];
  for (const row of rows) {
    const { scriptName, current, previous } = row;

    const errorRate = current.requests > 0 ? current.errors / current.requests : 0;
    if (current.errors >= MIN_ERRORS && errorRate > ERROR_RATE_THRESHOLD) {
      candidates.push({
        scriptName,
        reason:
          `error rate ${(errorRate * 100).toFixed(1)}% ` +
          `(${current.errors}/${current.requests} this hour; ` +
          `previous hour: ${previous.errors}/${previous.requests})`,
      });
      continue;
    }

    if (
      previous.requests >= MIN_BASELINE_REQUESTS &&
      current.requests <= previous.requests * (1 - TRAFFIC_DROP_RATIO)
    ) {
      candidates.push({
        scriptName,
        reason:
          `traffic collapse: ${current.requests} requests this hour ` +
          `vs ${previous.requests} the previous hour`,
      });
    }
  }
  return candidates;
}

// ---------------------------------------------------------------------------
// Judgement layer — one bounded Haiku call, structured output.
// ---------------------------------------------------------------------------

const JUDGEMENT_SYSTEM = `You are a monitoring triage filter for a Cloudflare Workers fleet. You receive per-worker request/error metrics for the last hour and the previous hour, plus a list of candidate anomalies that tripped deterministic thresholds. Your only job is to decide whether a human should be interrupted with a Telegram alert.

Alert (signal) means: sustained elevated error rate on a worker with meaningful traffic; a worker whose invocations collapsed versus its baseline; an error pattern absent from the previous window; correlated failures across multiple workers.

Do NOT alert (noise) means: a handful of errors on low-traffic workers; brief error blips already recovering relative to the previous window; traffic fluctuations plausible for time of day; anything explainable by the previous window's data.

Rules:
- Every claim in your summary must cite a specific number from the input.
- If the evidence is ambiguous, set alert to false — a missed marginal alert costs less than a false alarm, and real problems persist into the next hourly check.
- Never infer problems from data you were not given.
- severity "critical" only for total outage or error rate above 25% on a high-traffic worker; otherwise "warning".
- Keep summary to at most 2 plain-language sentences.`;

const VERDICT_SCHEMA = {
  type: "object",
  properties: {
    alert: { type: "boolean" },
    severity: { type: "string", enum: ["warning", "critical"] },
    workers_affected: { type: "array", items: { type: "string" } },
    summary: { type: "string" },
    evidence: { type: "string" },
  },
  required: ["alert", "severity", "workers_affected", "summary", "evidence"],
  additionalProperties: false,
} as const;

async function judge(env: Env, rows: WorkerRow[], candidates: Candidate[]): Promise<Verdict> {
  const client = new Anthropic({
    apiKey: env.ANTHROPIC_API_KEY,
    timeout: 60_000, // ms — keep the cron run snappy; SDK retries twice by default
  });

  const payload = {
    candidates,
    fleet: rows.map((r) => ({
      worker: r.scriptName,
      current_hour: r.current,
      previous_hour: r.previous,
    })),
  };

  const response = await client.messages.create({
    model: ANTHROPIC_MODEL,
    max_tokens: 1024,
    system: JUDGEMENT_SYSTEM,
    output_config: { format: { type: "json_schema", schema: VERDICT_SCHEMA } },
    messages: [
      {
        role: "user",
        content: `Candidate anomalies and full fleet metrics:\n${JSON.stringify(payload, null, 2)}`,
      },
    ],
  });

  if (response.stop_reason === "refusal") {
    throw new Error("judgement call refused");
  }
  const text = response.content.find((b) => b.type === "text")?.text;
  if (!text) {
    throw new Error(`judgement returned no text (stop_reason: ${response.stop_reason})`);
  }
  return JSON.parse(text) as Verdict;
}

// ---------------------------------------------------------------------------
// Telegram
// ---------------------------------------------------------------------------

async function sendTelegram(env: Env, text: string): Promise<void> {
  const res = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: env.TELEGRAM_CHAT_ID,
      // Plain text on purpose — MarkdownV2 escaping errors silently 400.
      text: text.slice(0, TELEGRAM_MAX_CHARS),
      disable_web_page_preview: true,
    }),
  });
  if (!res.ok) {
    throw new Error(`Telegram HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
  }
}

// ---------------------------------------------------------------------------
// KV dedup — returns true when this fingerprint was already sent within TTL.
// ---------------------------------------------------------------------------

async function isDuplicate(env: Env, key: string, fingerprint: string): Promise<boolean> {
  return (await env.STATE.get(key)) === fingerprint;
}

async function recordSent(env: Env, key: string, fingerprint: string): Promise<void> {
  await env.STATE.put(key, fingerprint, { expirationTtl: DEDUP_TTL_SECONDS });
}

// ---------------------------------------------------------------------------
// Maintenance window — operator-declared alert suppression.
// The gate and judgement still run and log during a window; only Telegram
// sends (alerts, fail-loud fallbacks, heartbeat) are suppressed.
// ---------------------------------------------------------------------------

const MAINTENANCE_KEY = "maintenance-window";

interface MaintenanceWindow {
  start: string; // ISO 8601
  end: string; // ISO 8601
  setAt: string; // ISO 8601, audit/display only
  reason?: string; // operator note, unused by logic
}

/**
 * Parse-safe read of the maintenance window. Malformed data fails OPEN
 * (returns null → no suppression) — a broken window read must never
 * accidentally silence a real incident.
 */
async function readMaintenanceWindow(env: Env): Promise<MaintenanceWindow | null> {
  let raw: string | null;
  try {
    raw = await env.STATE.get(MAINTENANCE_KEY);
  } catch (err) {
    console.error(
      JSON.stringify({
        event: "maintenance_window_read_error",
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    return null;
  }
  if (!raw) return null;
  try {
    const w = JSON.parse(raw) as MaintenanceWindow;
    if (Number.isNaN(Date.parse(w.start)) || Number.isNaN(Date.parse(w.end))) {
      throw new Error("unparseable start/end timestamps");
    }
    return w;
  } catch (err) {
    console.error(
      JSON.stringify({
        event: "maintenance_window_parse_error",
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    return null;
  }
}

/** Active for timestamps in [start, end) — inclusive of start, exclusive of end. */
function windowActiveAt(w: MaintenanceWindow, epochMs: number): boolean {
  return epochMs >= Date.parse(w.start) && epochMs < Date.parse(w.end);
}

// ---------------------------------------------------------------------------
// Control endpoints — POST/GET/DELETE /maintenance, bearer-token protected.
// ---------------------------------------------------------------------------

function isAuthorized(request: Request, env: Env): boolean {
  return (
    typeof env.MONITOR_CONTROL_TOKEN === "string" &&
    env.MONITOR_CONTROL_TOKEN.length > 0 &&
    request.headers.get("Authorization") === `Bearer ${env.MONITOR_CONTROL_TOKEN}`
  );
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function handleMaintenance(request: Request, env: Env): Promise<Response> {
  if (!env.MONITOR_CONTROL_TOKEN) {
    return jsonResponse({ error: "MONITOR_CONTROL_TOKEN secret is not configured" }, 503);
  }
  if (!isAuthorized(request, env)) {
    return jsonResponse({ error: "unauthorized" }, 401);
  }

  if (request.method === "GET") {
    const window = await readMaintenanceWindow(env);
    return jsonResponse({
      active: window !== null && windowActiveAt(window, Date.now()),
      window,
    });
  }

  if (request.method === "POST") {
    let body: { start?: string; end?: string; reason?: string };
    try {
      body = (await request.json()) as typeof body;
    } catch {
      return jsonResponse({ error: "body must be JSON: { start, end, reason? }" }, 400);
    }
    const startMs = Date.parse(body.start ?? "");
    const endMs = Date.parse(body.end ?? "");
    if (Number.isNaN(startMs) || Number.isNaN(endMs)) {
      return jsonResponse({ error: "start and end must be ISO 8601 timestamps" }, 400);
    }
    if (endMs <= startMs) {
      return jsonResponse({ error: "end must be after start" }, 400);
    }
    if (endMs <= Date.now()) {
      return jsonResponse({ error: "end must be in the future" }, 400);
    }
    // A new window replaces any existing one — single active window by design.
    const window: MaintenanceWindow = {
      start: new Date(startMs).toISOString(),
      end: new Date(endMs).toISOString(),
      setAt: new Date().toISOString(),
      ...(body.reason ? { reason: body.reason } : {}),
    };
    await env.STATE.put(MAINTENANCE_KEY, JSON.stringify(window));
    console.log(JSON.stringify({ event: "maintenance_window_set", ...window }));
    return jsonResponse({ ok: true, window });
  }

  if (request.method === "DELETE") {
    const existing = await env.STATE.get(MAINTENANCE_KEY);
    await env.STATE.delete(MAINTENANCE_KEY);
    console.log(
      JSON.stringify({ event: "maintenance_window_cleared", cleared: existing !== null }),
    );
    return jsonResponse({ ok: true, cleared: existing !== null });
  }

  return jsonResponse({ error: "method not allowed" }, 405);
}

// ---------------------------------------------------------------------------
// Main run
// ---------------------------------------------------------------------------

async function run(event: ScheduledController, env: Env): Promise<void> {
  // Missing secrets produce confusing downstream errors (e.g. Telegram 404 on
  // "botundefined") — fail fast with a clear log line instead. Locally these
  // come from .dev.vars; in production from `wrangler secret put`.
  const missing = (["CF_API_TOKEN", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN"] as const).filter(
    (k) => !env[k],
  );
  if (missing.length > 0) {
    console.error(
      JSON.stringify({ event: "config_error", missing_secrets: missing }),
    );
    return;
  }

  // Maintenance window: suppress all Telegram sends while active, but keep
  // evaluating and logging so the hour is still visible in `wrangler tail`.
  const maintenance = await readMaintenanceWindow(env);
  const suppress = maintenance !== null && windowActiveAt(maintenance, event.scheduledTime);
  if (suppress && maintenance) {
    console.log(
      JSON.stringify({
        event: "maintenance_window_active",
        window_end: maintenance.end,
        reason: maintenance.reason ?? null,
      }),
    );
  }

  // Align to full hours: cron fires at minute 0, so read the two hours that
  // just completed — stable data instead of a partial trailing window.
  const end = new Date(Math.floor(event.scheduledTime / 3_600_000) * 3_600_000);
  const curStart = new Date(end.getTime() - 3_600_000);
  const prevStart = new Date(end.getTime() - 7_200_000);

  // --- fetch ---------------------------------------------------------------
  let rows: WorkerRow[];
  try {
    rows = await fetchFleetMetrics(env, curStart, end, prevStart);
  } catch (err) {
    // Fail loud, but dedup so a multi-hour CF API outage sends one message.
    const msg = err instanceof Error ? err.message : String(err);
    const fingerprint = "gql-failure";
    if (suppress) {
      console.log(JSON.stringify({ event: "alert_suppressed", kind: "gql_failure" }));
    } else if (!(await isDuplicate(env, "last-monitor-error", fingerprint))) {
      await sendTelegram(env, `🛠️ workers-monitor cannot read fleet metrics:\n${msg}`);
      await recordSent(env, "last-monitor-error", fingerprint);
    }
    console.error(JSON.stringify({ event: "gql_failure", error: msg }));
    return;
  }

  // --- gate ----------------------------------------------------------------
  const candidates = thresholdGate(rows);
  console.log(
    JSON.stringify({
      event: "gate",
      workers: rows.length,
      candidates: candidates.map((c) => c.scriptName),
    }),
  );

  let alertSent = false;

  // --- judge + alert -------------------------------------------------------
  if (candidates.length > 0) {
    let verdict: Verdict | null = null;
    try {
      verdict = await judge(env, rows, candidates);
      console.log(JSON.stringify({ event: "verdict", ...verdict }));
    } catch (err) {
      // Judgement unavailable after the gate tripped → raw threshold alert.
      const msg = err instanceof Error ? err.message : String(err);
      console.error(JSON.stringify({ event: "judge_failure", error: msg }));
      const fingerprint = `raw:${candidates.map((c) => c.scriptName).sort().join(",")}`;
      if (suppress) {
        console.log(JSON.stringify({ event: "alert_suppressed", kind: "raw_threshold" }));
      } else if (!(await isDuplicate(env, "last-alert", fingerprint))) {
        const lines = candidates.map((c) => `• ${c.scriptName} — ${c.reason}`);
        await sendTelegram(
          env,
          `⚠️ WARNING — thresholds tripped, judgement unavailable (${msg})\n${lines.join("\n")}`,
        );
        await recordSent(env, "last-alert", fingerprint);
        alertSent = true;
      }
    }

    if (verdict?.alert) {
      const fingerprint = `${verdict.severity}:${[...verdict.workers_affected].sort().join(",")}`;
      // Critical severity bypasses the maintenance window — a confirmed severe
      // incident should page even during a planned deploy; only warning-level
      // noise is assumed to be an artifact of the deploy itself. Dedup still
      // applies either way, so an ongoing critical incident pages once, not
      // every hour.
      const windowBypassed = suppress && verdict.severity === "critical";
      if (suppress && !windowBypassed) {
        // No recordSent either — if the incident persists past the window,
        // the first post-window run alerts normally.
        console.log(JSON.stringify({ event: "alert_suppressed", kind: "alert", fingerprint }));
      } else if (await isDuplicate(env, "last-alert", fingerprint)) {
        console.log(JSON.stringify({ event: "alert_deduped", fingerprint }));
      } else {
        const icon = verdict.severity === "critical" ? "🚨" : "⚠️";
        const prefix = windowBypassed ? "🚨 (during maintenance window) " : icon + " ";
        await sendTelegram(
          env,
          `${prefix}${verdict.severity.toUpperCase()} — Workers fleet alert\n` +
            `${verdict.summary}\n` +
            `Workers: ${verdict.workers_affected.join(", ") || "(none listed)"}\n` +
            `Evidence: ${verdict.evidence}`,
        );
        await recordSent(env, "last-alert", fingerprint);
        alertSent = true;
      }
    }
  }

  // --- daily heartbeat (proof of life; skipped when an alert already went) --
  const heartbeatHour = Number.parseInt(env.HEARTBEAT_HOUR_UTC, 10);
  if (!alertSent && end.getUTCHours() === heartbeatHour) {
    if (suppress) {
      // A "fleet healthy" heartbeat mid-deploy would be a false signal.
      console.log(JSON.stringify({ event: "alert_suppressed", kind: "heartbeat" }));
    } else {
      const totalReq = rows.reduce((n, r) => n + r.current.requests, 0);
      const totalErr = rows.reduce((n, r) => n + r.current.errors, 0);
      await sendTelegram(
        env,
        `✅ Fleet healthy — ${rows.length} workers, ${totalReq} requests, ` +
          `${totalErr} errors in the last hour. (daily heartbeat)`,
      );
    }
  }
}

export default Sentry.withSentry(
  (env: Env) => ({
    dsn: env.SENTRY_DSN,
    tracesSampleRate: 1.0,
  }),
  {
    // HTTP surface: only /maintenance (bearer-token protected) does anything.
    // Everything else gets the plain status line so browsers/bots hitting the
    // public workers.dev URL don't see a Cloudflare 1101 error page.
    async fetch(request, env) {
      const url = new URL(request.url);
      if (url.pathname === "/maintenance") {
        return handleMaintenance(request, env);
      }
      return new Response(
        "workers-monitor: cron-only worker (runs hourly; operator API at /maintenance)",
        { status: 200, headers: { "Content-Type": "text/plain" } },
      );
    },

    async scheduled(event, env, ctx) {
      // Top-level catch: an unexpected throw (e.g. Telegram itself failing)
      // should land in the logs as one structured line, not an uncaught
      // error — and now also reported to Sentry, since catching it here
      // ourselves means withSentry's automatic uncaught-exception capture
      // never sees it.
      ctx.waitUntil(
        Sentry.withMonitor("workers-monitor-hourly-poll", () => run(event, env)).catch(
          (err) => {
            console.error(
              JSON.stringify({
                event: "unhandled_error",
                error: err instanceof Error ? err.message : String(err),
              }),
            );
            Sentry.captureException(err);
          },
        ),
      );
    },
  } satisfies ExportedHandler<Env>,
);
