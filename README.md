# mcp-search-proxy

> Search-first proxy for tool-heavy MCP aggregators — stop injecting hundreds of schemas into every turn.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-Streamable_HTTP-blue)](https://modelcontextprotocol.io)
[![Version](https://img.shields.io/badge/version-1.0.0-blue)](#)
[![Author](https://img.shields.io/badge/author-p1s4-lightgrey)](#)

Search-first MCP proxy for tool-heavy MCP aggregators. Exposes **4 fixed tools** (~2–3k tokens) instead of forwarding hundreds of full input schemas (~180k tokens) into every model turn.

```
Client (e.g. OpenWebUI, Streamable HTTP) → :8092/mcp → N upstream MCP servers
  mcp_search(query, limit=5)   → BM25 over a cached manifest (names + descriptions + param names)
  mcp_describe(names[≤10])     → full inputSchema only for the named tools
  mcp_call(name, arguments)    → tools/call routed to the upstream that owns the tool
  mcp_refresh()                → re-list all upstreams + rebuild the index (purely reactive)
```

Retrieval pattern inspired by NousResearch's Hermes agent `tool_search` (MIT) — original code, same idea: progressive disclosure (search → describe → call) with local BM25, no embeddings, no LLM in the proxy.

## Why

Some MCP setups aggregate dozens of servers into one flat `tools/list`. Every turn the client injects **all** schemas into the context. Real numbers from my deployment:

| setup | tools | chars | ~tokens (chars/4) |
|---|---|---|---|
| 1 upstream (aggregator) | 532 | 718,172 | ~179,500 |
| 2 upstreams (aggregator + browser/a11y) | 560 | 745,017 | ~186,250 |
| through this proxy | **4** | ~1,800 | **~2–3k** |

Same capability, ~60x less context per turn. The cost moves to 1–2 extra tool round-trips on cold tools only (search, then describe) — warm tools go straight through `mcp_call`.

## Where it fits

- You sit behind an **MCP aggregator** (e.g. LiteLLM `/mcp`) that returns all tools flat on every `tools/list`.
- Tool count is in the **hundreds** and most turns need **zero or one** of them.
- Your client uses the **chat completions** path with native tools (server-side semantic filtering, where it exists, typically only covers the `/responses` API — not this path).
- Upstreams are reachable over the **internal network** (DNS or IP); the proxy holds one sticky `Mcp-Session-Id` per upstream.
- You want **zero ML dependencies**: BM25 + a tiny manual stemmer, stdlib + `fastmcp` only.

## Where it does NOT fit

- **< 30 tools**: just connect the server directly, the proxy adds round-trips for nothing.
- You need **full schemas every turn** (e.g. a planner that scores all tools in one pass) — this proxy hides schemas by design.
- You need **semantic/vector search**: retrieval here is purely **lexical** (BM25 + keyword boosts). Mixed-language catalogs (e.g. English queries over half-Italian descriptions) work for specific queries; single-word `mail` now resolves to `gmail-*` via a provider-suffix boost (`mail` is a suffix of `gmail`, not a prefix of `mailchimp`). Specific queries (`send gmail email`) rank correctly.
- You need **push updates**: there is no `listChanged` handling, no SSE subscription, no polling. Refresh is fetch-once + explicit `mcp_refresh` + one-shot re-list on unknown-tool + long TTL. Right for toolsets that change a few times a month; wrong for registries that churn every minute.
- You need **media rendering**: `mcp_call` returns the raw upstream result (`content`/`structuredContent`), no image/audio post-processing.
- Stateful servers behind a **non-sticky** aggregator stay broken: this proxy fixes that only when clients go **through the proxy** (it keeps sticky sessions itself).

## Security: token layer, not a security boundary

This proxy reduces what the model **sees**, not what it **can call**. `mcp_call` executes any tool in the catalog by exact name — a hallucinated plausible name or a prompt injection naming one still gets through. Retrieval here is purely a cost optimisation.

Security decisions belong **upstream**, before the proxy:

- Expose MCP servers through an aggregator with per-tool allowlists (e.g. LiteLLM), not directly to the client.
- Opt in explicitly: new tools added by an upstream server should not enter the served set automatically — someone reviews and enables them (a read vs write/destructive split helps).
- Anything not enabled is never exposed and not callable, even by exact name.
- If you plug MCP servers straight into the client with no allowlist thinking, everything gets exposed — the proxy won't save you.

## How it works

1. **Startup**: `tools/list` fan-out over all `UPSTREAM_URLS` → merge → in-memory BM25 index + on-disk manifest (`MANIFEST_PATH`, survives restarts). Collision policy is fail-closed: first upstream wins, duplicates logged and dropped.
2. **Search** (`mcp_search`): BM25 (`k1=1.5`, `b=0.75`) over `name + source label + description + top-level param names`. Admission via rarest-token gate (OOV-robust: only in-vocabulary tokens pick the gate), exact-name match short-circuits. A `NAME_BONUS` (1.0 × idf per query-token found in the tool name) compensates lexical body mismatch; a guarded substring match (`query → name`, length ≥ 4) covers cases like `mail` in `gmail` without a stemmer dictionary. A manual English stemmer (`ing/ed/ies/s…`, zero dependencies) runs on both docs and queries.
3. **Describe** (`mcp_describe`, ≤10 names/call): full `inputSchema` for named tools only. If you already see the exact name (tier-1 listing, previous search), skip search and come here directly.
4. **Call** (`mcp_call`): validates `required` params locally (fail-open on `$ref`/malformed schemas), then routes `tools/call` to the upstream that served the tool (`_TOOL_UPSTREAM`). Unknown-tool triggers a one-shot re-list + retry before failing.
5. **Tier-1 listing**: after each refresh the proxy rewrites its MCP `instructions` with one line per tool source (`name: first-sentence (N tools)`, sorted, byte-stable). My catalog: 21 lines / ~1.5k chars / ~370 tokens. Clients read it at handshake — already-connected clients see the old text until reconnect.

Multi-upstream specifics:

- **Sticky sessions**: one `Mcp-Session-Id` per upstream URL, reused strictly. Reset only on 400/404/session-expired, once, then retry. This is what keeps stateful servers (tabs/pages bound to a session id) usable — a non-sticky aggregator in front of them loses the tab between calls.
- **Per-upstream auth**: `UPSTREAM_TOKENS` is positional against `UPSTREAM_URLS` (empty entry = no auth). Fallback: `LITELLM_MASTER_KEY` applies only to URLs containing `litellm`, nothing elsewhere.
- **Per-upstream timeout**: `UPSTREAM_TIMEOUT_A11Y` (default 280s) applies to URLs containing `a11y`; everything else uses the call default. Raise it for browser/crawl servers behind slow reverse proxies.
- **Fail-soft fetch, fail-closed merge**: one upstream down → skipped with a log line, the rest still serve. All down → `mcp_refresh` returns an error and the cached catalog stays.

Names are `mcp_*` (not `tool_*`) because `tool_search` is a server-side reserved name on xAI (HTTP 400) — new names avoid the problem at the root.

## Quickstart

Requirements: Docker + Docker Compose.

```bash
cp .env.example .env
# edit .env: UPSTREAM_URLS + tokens
docker compose up -d --build
docker logs mcp-search-proxy --tail 30   # expect: catalog refresh: N tool {...}, fp=...
```

Register in your client as a **Streamable HTTP** MCP server with the bare root URL (no `/openapi.json` suffix):

```
http://<host>:8092/mcp
```

No auth client → proxy (internal network). Upstream Bearer tokens stay inside the proxy process.

Minimal `.env`:

```dotenv
UPSTREAM_URLS=http://upstream1:4000/mcp,http://upstream2:8101/mcp
UPSTREAM_TOKENS=sk-your-token-here,
LITELLM_MASTER_KEY=
CATALOG_TTL_SEC=1800
```

`UPSTREAM_TOKENS` has one comma-separated entry per URL in `UPSTREAM_URLS`; leave an entry empty for no-auth upstreams (note the trailing comma above: token for URL #1, none for URL #2). If `UPSTREAM_TOKENS` is unset, `LITELLM_MASTER_KEY` is sent only to URLs containing `litellm`.

## Configuration

| var | default | meaning |
|---|---|---|
| `UPSTREAM_URLS` | `http://litellm:4000/mcp` | comma-separated upstream MCP URLs (Streamable HTTP). First wins on name collisions. |
| `UPSTREAM_TOKENS` | (unset) | comma-separated, positional Bearer tokens matching `UPSTREAM_URLS`. Empty = no auth for that slot. |
| `LITELLM_MASTER_KEY` | (unset) | legacy single-token fallback, sent only to URLs containing `litellm`. |
| `UPSTREAM_TIMEOUT_A11Y` | `280` | timeout (s) for URLs containing `a11y`. |
| `CATALOG_TTL_SEC` | `1800` | safety-net TTL for background refresh. `0` = explicit/unknown-tool only. |
| `MANIFEST_PATH` | `/app/data/manifest.json` | persisted catalog (fingerprint + routing + tools). |
| `PORT` | `8092` | listen port (`/mcp`). |
| `LOG_LEVEL` | `INFO` | FastMCP log level. |

No proxy env vars are set in `docker-compose.yml` by design. If your environment forces `HTTP(S)_PROXY`, add the upstreams to `NO_PROXY` yourself — the proxy must reach upstreams directly.

## My live test results

Setup: Docker on ARM64, Python 3.12, FastMCP 4.0.3, 2 upstreams (MCP aggregator with 21 servers + Playwright accessibility scanner with 28 tools), client over Streamable HTTP. Server file md5 `279de46a6bf71279fba1cec2a43ba9f4`, 823 lines.

**Catalog**: `560 tool {aggregator:532, a11y:28}, 745017ch (~186255tok), fp=560:7f75ff700affc79f`. `tools/list` on the proxy itself returns exactly the 4 `mcp_*` bridges (~1.8k chars).

**Retrieval** (live, 560-tool catalog):

```
'send gmail email'               -> gmail-message_send FIRST
'list gmail threads unread'      -> 5x gmail-thread_* (stem threads->thread)
'create calendar event tomorrow' -> calendar-create_event FIRST
'github issue' / 'notebooklm query' / 'wger workout today' -> correct first hit
'navigate browser page screenshot' -> browser_* tools top-5 (cross-upstream search works)
'xyzzy plugh qqq'                -> [] + available_sources + hint (never a fake miss)
'mail'                           -> 5x gmail-* (provider-suffix boost; two-word queries unchanged)
```

**Stateful E2E** (the reason for per-upstream sticky sessions): `browser_navigate https://www.ilbisonte.com/` → lands on `/en` → `scan_page {wcag2a,wcag2aa,wcag21aa,wcag22aa}` on the **same** page → `Violations: 0, Incomplete: 2, Passes: 28` → `browser_snapshot` returns the **same** URL/title, no `No open pages`. Navigate → scan → snapshot share one session through the proxy.

**Refresh**: `mcp_refresh` returns same fingerprint when upstreams are unchanged; `mcp_describe` cross-upstream returns `not_found=[]`; unknown-tool triggers one re-list + retry.

## Limitations (honest)

- Lexical only: synonyms across languages (`mail`/`email`/`posta`) have no alias table. Single-word `mail` resolves to `gmail-*` via provider-suffix boost; other ambiguous one-word queries can still misrank. Specific multi-word queries are fine.
- Manual stemmer, English only, zero dependencies by choice. Known over-stemming (`created`→`creat` vs `create`) is rare in real queries and mitigated by gate + limit. Swap in Snowball/nltk if you need full recall.
- `instructions` tier-1 listing updates server-side on refresh, but already-connected clients keep the old text until their next handshake.
- `CATALOG_TTL_SEC` re-fetches fan out to every upstream (each aggregator `tools/list` fans out again downstream). Keep it long (default 30 min) or `0`.

## Attribution

Retrieval pattern inspired by NousResearch's Hermes agent `tool_search` (MIT). No Hermes code is vendored here — same idea (BM25 catalog, rarest-token admission, search/describe/call bridges), original implementation.

## Support

Published as-is, no assistance. I run this for my own stack and don't have time for setup help — issues and PRs are welcome but answers are not guaranteed.

## License

MIT — see [LICENSE](LICENSE).
