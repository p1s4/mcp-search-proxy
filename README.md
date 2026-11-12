# mcp-search-proxy

> Search-first proxy for tool-heavy MCP aggregators — stop injecting hundreds of schemas into every turn.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-Streamable_HTTP-blue)](https://modelcontextprotocol.io)
[![Version](https://img.shields.io/badge/version-1.1.1-blue)](#)
[![Author](https://img.shields.io/badge/author-p1s4-lightgrey)](#)

Search-first MCP proxy for tool-heavy MCP aggregators. Exposes **4 fixed tools** (~2–3k tokens) instead of forwarding hundreds of full input schemas (~400k tokens) into every model turn.

```
Client (e.g. OpenWebUI, Streamable HTTP) → :8092/mcp → N upstream MCP servers
  mcp_search(query, limit=5, group?)  → BM25 over a cached manifest (names + descriptions + params)
  mcp_describe(names[≤10], group?)    → full inputSchema only for the named tools
  mcp_call(name, arguments)           → tools/call routed to the upstream that owns the tool
  mcp_refresh()                       → re-list all upstreams + rebuild the index (purely reactive)
```

Retrieval pattern inspired by NousResearch's Hermes agent `tool_search` (MIT) — original code, same idea: progressive disclosure (search → describe → call) with local BM25, no embeddings, no LLM in the retrieval path.

Since 1.1.x the proxy also publishes **two layers of server guidance** in its MCP `instructions`: a semantic purpose per server (optionally LLM-written, hash-cached) and a deterministic, verbatim obligations layer extracted from what each upstream server itself declares. See [Server notes, groups and obligations](#server-notes-groups-and-obligations-11x).

## Why

Some MCP setups aggregate dozens of servers into one flat `tools/list`. Every turn the client injects **all** schemas into the context. Real numbers from my deployment:

| setup | tools | chars | ~tokens (chars/4) |
|---|---|---|---|
| 1 upstream (aggregator) | 532 | 718,172 | ~179,500 |
| 2 upstreams (aggregator + browser/a11y) | 560 | 745,017 | ~186,250 |
| 2 upstreams, grown set | 807 | 1,630,214 | ~407,500 |
| through this proxy | **4** | ~4,500 | **~2–3k** + ~2.1k instructions |

Same capability, ~60x less context per turn. The cost moves to 1–2 extra tool round-trips on cold tools only (search, then describe) — warm tools go straight through `mcp_call`.

## Where it fits

- You sit behind an **MCP aggregator** (e.g. LiteLLM `/mcp`) that returns all tools flat on every `tools/list`.
- Tool count is in the **hundreds** and most turns need **zero or one** of them.
- Your client uses the **chat completions** path with native tools (server-side semantic filtering, where it exists, typically only covers the `/responses` API — not this path).
- Upstreams are reachable over the **internal network** (DNS or IP); the proxy holds one sticky `Mcp-Session-Id` per upstream.
- You want **zero ML dependencies**: BM25 + a tiny manual stemmer, stdlib + `fastmcp` only. The optional semantic notes are the one exception — plain HTTP to any OpenAI-compatible endpoint, off by default.

## Where it does NOT fit

- **< 30 tools**: just connect the server directly, the proxy adds round-trips for nothing.
- You need **full schemas every turn** (e.g. a planner that scores all tools in one pass) — this proxy hides schemas by design.
- You need **semantic/vector search**: retrieval here is purely **lexical** (BM25 + keyword boosts). Mixed-language catalogs (e.g. English queries over half-Italian descriptions) work for specific queries; single-word `mail` resolves to `gmail-*` via a provider-suffix boost (`mail` is a suffix of `gmail`, not a prefix of `mailchimp`). Specific queries (`send gmail email`) rank correctly.
- You need **push updates**: there is no `listChanged` handling, no SSE subscription, no polling. Refresh is fetch-once + explicit `mcp_refresh` + one-shot re-list on unknown-tool + long TTL. Right for toolsets that change a few times a month; wrong for registries that churn every minute.
- You need **media rendering**: `mcp_call` returns the raw upstream result (`content`/`structuredContent`), no image/audio post-processing.
- Stateful servers behind a **non-sticky** aggregator stay broken: this proxy fixes that only when clients go **through the proxy** (it keeps sticky sessions itself).
- You need **guaranteed extraction of every** upstream rule: the obligations layer is marker-driven and deterministic. Rules phrased outside the recognised markers are simply not extracted (see [Limitations](#limitations-honest)).

## Security: token layer, not a security boundary

This proxy reduces what the model **sees**, not what it **can call**. `mcp_call` executes any tool in the catalog by exact name — a hallucinated plausible name or a prompt injection naming one still gets through. Retrieval here is purely a cost optimisation.

Security decisions belong **upstream**, before the proxy:

- Expose MCP servers through an aggregator with per-tool allowlists (e.g. LiteLLM), not directly to the client.
- Opt in explicitly: new tools added by an upstream server should not enter the served set automatically — someone reviews and enables them (a read vs write/destructive split helps).
- Anything not enabled is never exposed and not callable, even by exact name.
- If you plug MCP servers straight into the client with no allowlist thinking, everything gets exposed — the proxy won't save you.

## How it works

1. **Startup**: `tools/list` fan-out over all `UPSTREAM_URLS` → merge → in-memory BM25 index + on-disk manifest (`MANIFEST_PATH`, survives restarts). Collision policy is fail-closed: first upstream wins, duplicates logged and dropped. Refresh is fail-closed too: a refresh that would shrink a healthy cached catalog while any configured upstream is missing/empty is refused (previous index + manifest kept, `RuntimeError`); a genuine shrink applies only when ALL upstreams answer.
2. **Search** (`mcp_search`): BM25 (`k1=1.5`, `b=0.75`) over `name + source label + description + top-level param names`. Admission via rarest-token gate (OOV-robust: only in-vocabulary tokens pick the gate), exact-name match short-circuits. A `NAME_BONUS` (1.0 × idf per query-token found in the tool name) compensates lexical body mismatch; a guarded substring match (`query → name`, length ≥ 4) covers cases like `mail` in `gmail` without a stemmer dictionary. A manual English stemmer (`ing/ed/ies/s…`, zero dependencies) runs on both docs and queries.
3. **Describe** (`mcp_describe`, ≤10 names/call): full `inputSchema` for named tools only. If you already see the exact name (tier-1 listing, previous search), skip search and come here directly.
4. **Call** (`mcp_call`): validates `required` params locally (fail-open on `$ref`/malformed schemas), then routes `tools/call` to the upstream that served the tool (`_TOOL_UPSTREAM`). Unknown-tool triggers a one-shot re-list + retry before failing.
5. **Tier-1 listing**: after each refresh the proxy rewrites its MCP `instructions` with one line per tool source (`name: first-sentence (N tools)`, sorted, byte-stable). My catalog: 28 lines. Clients read it at handshake — already-connected clients see the old text until reconnect.

Multi-upstream specifics:

- **Sticky sessions**: one `Mcp-Session-Id` per upstream URL, reused strictly. Reset only on 400/404/session-expired, once, then retry. This is what keeps stateful servers (tabs/pages bound to a session id) usable — a non-sticky aggregator in front of them loses the tab between calls.
- **Stateless upstreams**: if `initialize` returns 200 without a `Mcp-Session-Id` header, the upstream is marked stateless and served with `session=None` (no `Mcp-Session-Id` header on list/call). No session reset/retry on 400 for these — a 400 is a normal error. Stateful behaviour is unchanged where the server returns a session id.
- **Per-upstream auth**: `UPSTREAM_TOKENS` is positional against `UPSTREAM_URLS` (empty entry = no auth). Fallback: `LITELLM_MASTER_KEY` applies only to URLs containing `litellm`, nothing elsewhere.
- **Per-upstream timeout**: `UPSTREAM_TIMEOUT_A11Y` (default 280s) applies to URLs containing `a11y`; everything else uses the call default. Raise it for browser/crawl servers behind slow reverse proxies.
- **Fail-soft fetch, fail-closed refresh/merge**: one upstream down → skipped with a log line, but if the survivors are fewer than the cached catalog the refresh is refused (`RuntimeError`, cached index + manifest kept). All down → `mcp_refresh` returns an error and the cached catalog stays. A shrink applies only when ALL configured upstreams answer.

Names are `mcp_*` (not `tool_*`) because `tool_search` is a server-side reserved name on xAI (HTTP 400) — new names avoid the problem at the root.

## Server notes, groups and obligations (1.1.x)

### Why the one-line-per-server summary existed at all

The tier-1 listing used to describe each server with the "medoid" description of one arbitrary tool:

```
wger (91): Create a per-iteration config record for a slot entry.
```

That is noise, and maintaining a hand-written list is worse. Since 1.1.0 the proxy generates **one semantic note per source** — in English, asking an OpenAI-compatible endpoint (LiteLLM in my deployment), passing **one MCP at a time** with its tools and descriptions. The note is stored in `data/purposes.json` together with a hash of the server's tool surface, so in steady state the LLM cost is **zero**.

| situation | action | LLM calls |
|---|---|---|
| hash unchanged | reuse the note from `purposes.json` | 0 |
| tool added/removed or description changed | regenerate **that source only** | 1 |
| new source | generate | 1 |
| source disappeared | row kept in the file, unused (its hash reactivates it if it comes back) | 0 |
| LLM down / timeout / invalid note | keep the previous note, else fall back to medoid | 0 |

Enrichment runs **after** catalog, manifest and instructions are already published, on a daemon thread: `tools/list` and the handshake never wait for an LLM. A brand-new source uses the medoid for the current session and the good note from the next one (instructions are read at `initialize`).

Priority of the purpose line shown to the model ("what it does / when to use it"):

1. `PURPOSES` (manual pin from env) — always wins, zero drift
2. generated note in `data/purposes.json` — automatic, hash-cached
3. the `[source]` block in the upstream's own `instructions`
4. medoid description (historical fallback)

### The obligations layer (1.1.1) — additive, never replacing

A purpose sentence is **not enough** and must not replace operating rules. Upstreams ship imperative instructions (`MUST`, `NEVER`, `IMPORTANT`, `start with X first`, `before building ... confirm`, `run \`nlm login\``), and an LLM that summarises them loses or softens them. Since 1.1.1 there is a **second, deterministic layer**, published in a separate section of the instructions, below the catalog:

```
Mandatory obligations by server (stated by the server itself; obey them in addition
 to the purpose above, and read any skill guide they point to BEFORE calling tools):
- Home_assistant: MUST call Home_assistant-ha_get_skill_guide() first: it returns the reference files ...
- zipchat: Start with zipchat_list_chats to discover chat_id; ... Never invent chat_id, ...
- shopify_matrixify: Before building an import or export, confirm column formats ...
```

How it works:

- **No LLM.** Sentences are extracted **verbatim** from the upstream's `[source]` block and never paraphrased. Same input text → same output text: no cache, no cost.
- **Markers**: `MUST|IMPORTANT|REQUIRED|NEVER|ALWAYS|DO NOT` (word-boundary, case-insensitive, so `never` inside `whenever` does not count) plus imperative forms (`do not`, `before calling|building`, `start with`, `call X first`, `never invent`, `only use`, `use only when`, `run \``).
- **Fixed rule for skill guides**: if a source exposes a `*get_skill_guide` tool and its notes talk about skills, the obligation is emitted mechanically (`MUST call <tool>() first`) even when the upstream prose changes. Covers Home Assistant without depending on wording.
- **Budget**: 400 chars per source, stop at the first sentence that would overflow (never cut a rule in half). A source that declares no obligations does not appear: in my live catalog that is 11 sources out of 28.
- **De-duplication**: if the purpose line (pin, generated note or truncated upstream) already contains a rule, that rule is not repeated below. Comparison is on a normalised 60-char prefix, because the purpose can be truncated with `…`.
- **Verified**: with generated notes the overhead is ~2.4 KB (~600 tokens) on the whole handshake; with the bare upstream as purpose the balance is **negative**, because the section replaces the 200-char truncation with normative sentences only.

Operational consequence: a `PURPOSES` pin **can no longer erase** a server's `MUST`/`NEVER`. Before 1.1.1 the generated note overrode the upstream and the obligation disappeared.

### Groups

```
GROUPS=workspace=Google_Workspace*,file_bridge,NotebookLm,Clickup;audit=audit*,scan*,browser*,chrome_devtools;dev=github,Context7,wsc_mcp,n8n_mcp,tika
```

The pattern is `fnmatch`-ed against the **tool name** and the **source prefix**: membership follows the upstreams by itself, no list to maintain when you add an MCP server. `mcp_search` and `mcp_describe` accept an optional `group` parameter; it is fail-closed (an unknown group, or a name outside the group, is never silently widened to the whole catalog). Live sizes in my deployment: `workspace` 149, `audit` 75, `dev` 49.

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

After changing `GROUPS`, `PURPOSES` or any `ENRICH_*` value, use `docker compose up -d <service>` — `docker compose restart` does **not** reload environment variables.

## OpenWebUI in Docker — which URL to use

| where OpenWebUI runs | URL to register |
|---|---|
| Same Docker network as the proxy | `http://mcp-search-proxy:8092/mcp` (or `http://<service-name>:8092/mcp`) |
| On the host / another host | `http://<HOST-IP-OR-NAME>:8092/mcp` |

- NEVER `http://0.0.0.0:...` — that's the listen address, not a routable target.
- NEVER `http://127.0.0.1:...` from inside another container — it points at the container itself.
- Watch for `http://` with a double slash (a single-slash `http:/...` is a typo and fails).
- Check ports: `docker ps` must show `0.0.0.0:8092->8092` (compose publishes `8092:8092`).

Minimal `.env` (recommended format, 1 no-auth upstream):

```dotenv
UPSTREAMS=http://upstream1:4000/mcp
CATALOG_TTL_SEC=1800
```

Two formats, same behaviour — pick one:

```dotenv
# Recommended: coupled url|token pairs (token optional after |).
# Tokens must not contain | or , (entries split on comma, pair on FIRST |).
# If UPSTREAMS is set, UPSTREAM_URLS/UPSTREAM_TOKENS below are IGNORED.
UPSTREAMS=https://opendata.cat/api/mcp,https://foo.bar/mcp|sk-abc123

# Legacy: positional tokens (empty entry = no auth; note the trailing comma:
# token for URL #1, none for URL #2). Ignored when UPSTREAMS is set.
# UPSTREAM_URLS=http://upstream1:4000/mcp,http://upstream2:8101/mcp
# UPSTREAM_TOKENS=sk-your-token-here,
# LITELLM_MASTER_KEY=
```

If `UPSTREAM_TOKENS` is unset, `LITELLM_MASTER_KEY` is sent only to URLs containing `litellm`. Tokens are never logged — startup logs only `auth=yes|no` per upstream.

## Configuration

| var | default | meaning |
|---|---|---|
| `UPSTREAMS` | (unset) | recommended: coupled `url|token` pairs, comma-separated. Token after `|` optional (absent = no auth). If set, `UPSTREAM_URLS`/`UPSTREAM_TOKENS` are ignored. |
| `UPSTREAM_URLS` | `http://litellm:4000/mcp` | legacy: comma-separated upstream MCP URLs (Streamable HTTP). First wins on name collisions. |
| `UPSTREAM_TOKENS` | (unset) | legacy: comma-separated, positional Bearer tokens matching `UPSTREAM_URLS`. Empty = no auth for that slot. |
| `LITELLM_MASTER_KEY` | (unset) | legacy single-token fallback, sent only to URLs containing `litellm`. Also the default enrichment key. |
| `UPSTREAM_TIMEOUT_A11Y` | `280` | timeout (s) for URLs containing `a11y`. |
| `CATALOG_TTL_SEC` | `1800` | safety-net TTL for background refresh. `0` = explicit/unknown-tool only. |
| `MANIFEST_PATH` | `/app/data/manifest.json` | persisted catalog (fingerprint + routing + tools). |
| `PORT` | `8092` | listen port (`/mcp`). |
| `LOG_LEVEL` | `INFO` | FastMCP log level. |
| `GROUPS` | (unset) | logical groups for the optional `group` param: `name=pattern[,pattern];name2=...`. Pattern is fnmatch on tool name and source prefix. |
| `PURPOSES` | (unset) | manual purpose pins: `source=text;source2=text`. Wins over generated notes. Since 1.1.1 a pin replaces the purpose **only** — obligations always come from the deterministic layer. |
| `ENRICH_BASE_URL` | (unset) | OpenAI-compatible base URL for the semantic notes, **including** the API version (e.g. `http://litellm:4000/v1`). Empty = feature off. |
| `ENRICH_MODEL` | (unset) | model used for the notes. Empty = feature off. |
| `ENRICH_API_KEY` | (unset) | bearer for the enrichment endpoint. Empty → falls back to `LITELLM_MASTER_KEY`. |
| `ENRICH_TIMEOUT` | `60` | per-request timeout (s). |
| `ENRICH_CONCURRENCY` | `4` | concurrent note generations. |

No proxy env vars are set in `docker-compose.yml` by design. If your environment forces `HTTP(S)_PROXY`, add the upstreams to `NO_PROXY` yourself — the proxy must reach upstreams directly. Note that Python's `urllib` does **not** understand CIDR ranges in `NO_PROXY` (curl does): list host **names**, not only networks.

## My live test results

Setup: Docker on ARM64, Python 3.12, FastMCP 4.1.8, 2 upstreams (MCP aggregator with 764 tools across 27 servers + Playwright accessibility scanner with 43 tools), client over Streamable HTTP.

**Catalog**: `807 tool {litellm:764, a11y:43}, 1630214ch (~407554tok), fp=807:f9e6926edc85825f`. `tools/list` on the proxy itself returns exactly the 4 `mcp_*` bridges.

**Retrieval** (live catalog):

```
'send gmail email'               -> Google_Workspace-send_gmail_message FIRST
'list gmail threads unread'      -> gmail/Google_Workspace thread tools (stem threads->thread)
'create calendar event tomorrow' -> calendar create_event FIRST
'github issue' / 'notebooklm query' / 'wger workout today' -> correct first hit
'navigate browser page screenshot' -> browser_* tools top-5 (cross-upstream search works)
'xyzzy plugh qqq'                -> [] + available_sources + hint (never a fake miss)
```

**Stateful E2E** (the reason for per-upstream sticky sessions): `browser_navigate https://example.com/` → `scan_page {wcag2a,wcag2aa,wcag21aa,wcag22aa}` on the **same** page → `Violations: 0, Incomplete: 2, Passes: 28` → `browser_snapshot` returns the **same** URL/title, no `No open pages`. Navigate → scan → snapshot share one session through the proxy.

**Refresh**: `mcp_refresh` returns the same fingerprint when upstreams are unchanged; a partial refresh with a missing/empty upstream is refused (same fp kept, `RuntimeError`), not overwritten; `mcp_describe` cross-upstream returns `not_found=[]`; unknown-tool triggers one re-list + retry.

**Semantic notes + obligations (1.1.1, live)**: `enrich done: 26 generated, 0 failed, 0 reused` in ~23 s with concurrency 4 (~45k prompt tokens total, `purposes.json` ~10 KB); obligations present for 11 sources out of 28; instructions 11,037 chars including the `Mandatory obligations by server` section. `_update_instructions()` rebuilds 20 times in 0.289 s — no cost on the handshake, byte-stable across calls (prompt-cache safe).

## Limitations (honest)

- Lexical only: synonyms across languages (`mail`/`email`/`posta`) have no alias table. Single-word `mail` resolves to `gmail-*` via provider-suffix boost; other ambiguous one-word queries can still misrank. Specific multi-word queries are fine.
- Manual stemmer, English only, zero dependencies by choice. Known over-stemming (`created`→`creat` vs `create`) is rare in real queries and mitigated by gate + limit. Swap in Snowball/nltk if you need full recall.
- `instructions` (tier-1 listing, purposes, obligations) update server-side on refresh, but already-connected clients keep the old text until their next handshake.
- `CATALOG_TTL_SEC` re-fetches fan out to every upstream (each aggregator `tools/list` fans out again downstream). Keep it long (default 30 min) or `0`.
- 1.1.0: a source whose listing row is split per service (e.g. `Google_Workspace`) is **not** enriched — it keeps per-service medoids, which are more precise than one sentence for the whole source anyway.
- 1.1.0: `purposes.json` is generated data — it belongs in `.gitignore`, never in the repo.
- 1.1.1: the obligations layer is always on; there is no env to disable it. If an upstream writes rules outside the recognised markers they are not extracted: add the marker to `_OBL_PHRASE_RE`, or use a `PURPOSES` pin containing the rule (the pin is de-duplicated).
- 1.1.1: the tool name in the fixed skill rule comes from the catalog (`<source>-get_skill_guide`), so the `<source>-<tool>` prefix format must be preserved.
- 1.1.1: generated notes are treated as untrusted input (single line, no markdown, no URLs, max 40 words / 240 chars) — a response breaking the shape is discarded and the previous note is kept.

## Attribution

Retrieval pattern inspired by NousResearch's Hermes agent `tool_search` (MIT). No Hermes code is vendored here — same idea (BM25 catalog, rarest-token admission, search/describe/call bridges), original implementation.

## Support

Published as-is, no assistance. I run this for my own stack and don't have time for setup help — issues and PRs are welcome but answers are not guaranteed.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history.

## License

MIT — see [LICENSE](LICENSE).
