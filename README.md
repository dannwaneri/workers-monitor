# workers-monitor

Proactive Cloudflare Workers fleet monitor. Runs hourly on a cron trigger,
reads fleet metrics from the Cloudflare GraphQL Analytics API, applies a
deterministic threshold gate, asks Claude Haiku 4.5 to separate signal from
noise (only when thresholds trip), and sends a Telegram message only when
something actually matters.

```
cron (hourly)
  → GraphQL Analytics: per-worker requests/errors, last hour + previous hour
  → threshold gate (code, not LLM — a normal hour never calls Anthropic)
  → Claude Haiku 4.5 judgement (structured JSON verdict)
  → Telegram alert (KV-deduped, re-alerts after 6h on ongoing incidents)
  → daily "fleet healthy" heartbeat at HEARTBEAT_HOUR_UTC (proof of life)
```

Fail-loud design: if the Anthropic call fails after the gate tripped, you get
a raw threshold alert instead of silence. If the GraphQL fetch fails, you get
one (deduped) "monitor cannot read metrics" message.

## Setup

### 1. Install

```sh
npm install
```

### 2. KV namespace (dedup state — two keys, not a database)

```sh
npx wrangler kv namespace create STATE
```

Paste the returned `id` into `kv_namespaces` in `wrangler.jsonc`.

### 3. Vars in wrangler.jsonc

- `CF_ACCOUNT_ID` — dashboard → Workers & Pages → right sidebar, or `npx wrangler whoami`
- `TELEGRAM_CHAT_ID` — see step 5
- `HEARTBEAT_HOUR_UTC` — hour (UTC) for the daily healthy heartbeat

### 4. Secrets

```sh
npx wrangler secret put CF_API_TOKEN          # Cloudflare token, permission: Account → Account Analytics → Read
npx wrangler secret put ANTHROPIC_API_KEY
npx wrangler secret put TELEGRAM_BOT_TOKEN    # from @BotFather
npx wrangler secret put MONITOR_CONTROL_TOKEN # any long random string — protects /maintenance
```

Create the Cloudflare token at dash.cloudflare.com → My Profile → API Tokens →
Create Token → Custom, with **Account Analytics: Read** on your account. Nothing else.

### 5. Telegram

1. Message [@BotFather](https://t.me/BotFather) → `/newbot` → copy the token.
2. Open your new bot in Telegram and press **Start** (the bot cannot message you first).
3. Get your chat id:
   ```sh
   curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```
   → `result[0].message.chat.id`. Put it in `TELEGRAM_CHAT_ID`.

### 6. Test locally, then deploy

```sh
npm run dev
# in another terminal — fires the scheduled handler once:
curl "http://localhost:8787/__scheduled?cron=0+*+*+*+*"

npm run deploy
npm run tail   # watch the first live runs
```

Note: local dev uses local KV and needs the secrets in a `.dev.vars` file
(KEY=value lines, gitignored) if you want a full end-to-end local run.

## Tuning

Thresholds live at the top of `src/index.ts`:

| Const | Default | Meaning |
|---|---|---|
| `ERROR_RATE_THRESHOLD` | `0.02` | error rate that makes a worker a candidate |
| `MIN_ERRORS` | `10` | minimum absolute errors (kills low-traffic noise) |
| `TRAFFIC_DROP_RATIO` | `0.8` | ≥80% invocation drop vs previous hour |
| `MIN_BASELINE_REQUESTS` | `50` | traffic-drop rule only applies above this baseline |
| `DEDUP_TTL_SECONDS` | `21600` | ongoing incident re-alerts after 6h |
| `SELF_NAME` | `workers-monitor` | excluded from monitoring (keep in sync with `name` in wrangler.jsonc) |

## Maintenance windows

Silence alerts during a planned deploy without stopping the monitor. The gate
and judgement still run and log every hour (visible in `wrangler tail`); only
Telegram sends are suppressed — **including the daily heartbeat**, so don't be
surprised when it skips a window that spans your heartbeat hour.

All three endpoints require the `MONITOR_CONTROL_TOKEN` secret as a bearer
token. Timestamps are ISO 8601; the window is active for `[start, end)`.

```sh
# Set a window (replaces any existing one; end must be after start and in the future)
curl -X POST https://workers-monitor.<your-subdomain>.workers.dev/maintenance \
  -H "Authorization: Bearer $MONITOR_CONTROL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"start":"2026-07-09T14:00:00Z","end":"2026-07-09T16:00:00Z","reason":"deploying v2"}'

# Check status
curl https://workers-monitor.<your-subdomain>.workers.dev/maintenance \
  -H "Authorization: Bearer $MONITOR_CONTROL_TOKEN"

# Cancel early
curl -X DELETE https://workers-monitor.<your-subdomain>.workers.dev/maintenance \
  -H "Authorization: Bearer $MONITOR_CONTROL_TOKEN"
```

Semantics:

- One window at a time — POSTing a new one replaces the old one entirely.
- Expiry is automatic: alerting resumes on the first run after `end`. No
  manual re-enable step.
- A future-dated `start` is accepted and activates on its own when reached.
- Suppression fails open: if the stored window is malformed or unreadable,
  the monitor alerts normally rather than staying silent.
- Suppressed alerts are not recorded in dedup state — if an incident
  persists past the window, the first post-window run alerts immediately.
- Severity does not bypass the window: critical alerts are suppressed too.
  You're assumed to be watching during your own deploy.

## Cost

Haiku 4.5 is $1/$5 per MTok. The gate means a normal hour makes **zero**
Anthropic calls; even alerting every hour is a few thousand tokens/day —
well under $1/month. GraphQL Analytics and Telegram are free.

## Design notes

- **No agentic loop.** Metrics are fetched deterministically; Haiku makes one
  bounded classification call with all data in the prompt. Nothing loops
  until success.
- **Zero false positives on a normal hour** is enforced by the gate in code,
  not by prompt engineering — if no threshold trips, the LLM never runs and
  no message can be sent (heartbeat aside).
- **Structured output** (`output_config.format` with a JSON schema) means the
  verdict is guaranteed-parseable JSON — no regex on prose.
