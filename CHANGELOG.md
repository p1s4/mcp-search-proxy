# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-11-12

### Added

- **Obligations layer** (`Mandatory obligations by server`): a second,
  deterministic section of the MCP `instructions`, published below the tier-1
  catalog. Imperative rules stated by each upstream server in its own
  `instructions` (`MUST`, `IMPORTANT`, `REQUIRED`, `NEVER`, `ALWAYS`, `DO NOT`,
  plus `do not`, `before calling|building`, `start with`, `call X first`,
  `never invent`, `only use`, `use only when`, `run \``) are extracted
  **verbatim** — no LLM, no cache, no paraphrase — and emitted per source.
  Same input → same output, byte-stable across calls (prompt-cache safe).
- Fixed rule for skill guides: if a source exposes a `*get_skill_guide` tool
  and its notes mention skills, the obligation `MUST call <tool>() first` is
  emitted mechanically, with the tool name taken from the catalog so it cannot
  drift.
- `OBLIGATION_BUDGET` 400 chars per source, stop at the first sentence that
  would overflow (a rule is never cut in half). De-duplication against the
  purpose line via normalised 60-char prefix, so a truncated (`…`) purpose
  still suppresses the duplicate.

### Fixed

- **Regression from 1.1.0**: the generated semantic note replaced the upstream
  `[source]` block wholesale, so 10 of 11 upstream obligation blocks were
  being silently dropped (e.g. Home Assistant's `MUST read the skill guide
  first`). Purpose and obligations are now two additive layers: a `PURPOSES`
  pin can no longer erase a server's `MUST`/`NEVER`.
- Marker matching is word-boundary and case-insensitive, so `never` inside
  `whenever`/`whatever` no longer admits half sentences.
- Numbered/bulleted list prefixes (`2.`, `-`, `*`) are stripped before the
  length check, so rules written as list items are no longer discarded.
- Length floor lowered to 12 chars for all-uppercase rules: short but real
  rules (`Never invent IDs.`) are kept.
- `use only when|if|for|after` added to the imperative forms — catches credit
  and quota limits phrased that way.
- Quoted tool names (`Always call 'get_me' first`) are recognised.

## [1.1.0] - 2026-11-12

### Added

- **Semantic notes per server** (optional): at refresh, the proxy asks an
  OpenAI-compatible endpoint (e.g. LiteLLM) for one English purpose sentence
  per source, passing one MCP at a time. Notes are cached in
  `data/purposes.json` keyed by a hash of the server's tool surface, so steady
  state costs **zero** LLM calls; only a changed surface is regenerated.
  Enrichment runs on a daemon thread **after** catalog, manifest and
  instructions are published — the handshake never waits for an LLM.
  Controlled by `ENRICH_BASE_URL`, `ENRICH_MODEL`, `ENRICH_API_KEY`,
  `ENRICH_TIMEOUT`, `ENRICH_CONCURRENCY`. Off when base URL or model is empty
  (behaviour identical to 1.0.7).
- `PURPOSES` env: manual purpose pins (`source=text;source2=text`) that win
  over generated notes. Intended for exceptions that must not drift.
- **Groups** (optional): `GROUPS=name=pattern[,pattern];name2=...`, matched
  with `fnmatch` against both the tool name and the source prefix, so
  membership follows the upstreams automatically. `mcp_search` and
  `mcp_describe` accept an optional `group` parameter and are fail-closed: an
  unknown group, or a name outside the group, is never silently widened to the
  whole catalog.
- Purpose priority chain: `PURPOSES` pin → generated note → upstream
  `[source]` block → medoid description.
- Generated notes are treated as untrusted input: single line, no markdown, no
  URLs, max 40 words / 240 chars; a response breaking the shape is discarded
  and the previous note is kept.

### Changed

- Source rows in `instructions` are no longer described by the medoid tool of
  an arbitrary server member when a real note is available.

## [1.0.7] - 2026-10-27

### Fixed

- Fail-closed partial refresh: a refresh that would shrink a healthy cached
  catalog while any configured upstream is missing or returns 0 tools is now
  refused (`RuntimeError`, previous index + disk manifest kept). Fixes the
  reboot race where the proxy started in the same second as its upstream and
  the failed refresh overwrote a 631-tool catalog with the 28 tools of the
  surviving upstream until a manual `mcp_refresh`. A genuine shrink is still
  applied when ALL configured upstreams answer (even with fewer tools);
  cold boot with an empty cache applies whatever answers.

## [1.0.6] - 2026-10-27

### Changed

- Multi-token provider/operation boost: each query token (len>=3) matching the
  operation segment (+10) or the provider/source label (+6, suffix +4)
  disambiguates multi-intent queries where the old single-token branch never
  fired. Weights stay below the single-token branch (30/15/12) to avoid score
  explosion. Stemmed equality fallback makes the match plural-tolerant
  (`orders` -> `order`, `tasks` -> `task`) on both single and multi-token paths.
- Capped indexed text per tool: description clipped at 1500 chars, combined
  parameter descriptions at 800 chars. Long descriptions still add recall but
  no longer dominate BM25 via raw term frequency.

### Fixed

- Removed the per-term `+1.0` description bonus: it rewarded long bodies for
  generic tokens (`text`, `content`, `template`). Full-phrase `+20` match in
  description is kept as the strong relevance signal.

## [1.0.5] - 2026-10-26

### Changed

- Tier-1 listing now prefers real upstream `instructions` (server notes captured at `initialize`, refreshed on `mcp_refresh`) over truncated medoid descriptions; fallback medoid 80ch + `try:` keywords. `Google_Workspace` split into one row per service (gmail/drive/calendar/docs/sheets/slides/task, verb-agnostic).
- Clip helper `_word_clip`: first non-empty line, word-boundary clip + ellipsis (fixes mid-word cut of `_short_desc`, immune to `Next.js`/`notebook.google.com`/`e.g.`).

### Fixed

- Sticky-session safe notes refresh: probe `initialize` per upstream, probe sid discarded, live sticky sid (a11y tabs/pages) always restored; best-effort fail-soft per upstream.

## [1.0.4] - 2026-10-26

### Added

- `_entry_search_text` now also indexes parameter `description` text (first
  60 top-level properties, same cap as parameter names). Tools without
  parameter descriptions produce a bit-identical search text, so retrieval
  is unchanged until upstream servers add `Field(description=...)`.
  This unlocks BM25 ranking on parameter-level keywords for the upcoming
  custom-server schema pass (English descriptions + per-parameter docs).

## [1.0.3] - 2026-10-26

### Fixed

- Compound-operation search: single-word queries now match exact name
  segments (`issue` -> `create_issue`, `task` -> `create_task`). The previous
  code compared against the whole operation string, so any compound operation
  missed the +30 keyword boost. Match is segment equality with a len>=3
  guard — no substring noise (`api` still does not match `rapid`).
- Same segment-match on the provider/source label: compound sources like
  `wsc_mcp` split into segments, so `mcp` earns the +15 source boost.

### Added

- Anti-skip instruction in server `instructions` (dynamic + initial):
  "Never tell the user a capability is unavailable without searching first."
  Already-connected clients see the old text until reconnect.

## [1.0.2] - 2026-09-17

### Changed

- Full English translation of all user-facing strings: tool descriptions, error
  messages, hints, server `instructions`, log lines, comments and docstrings.
  Retrieval logic is bit-identical (verified: code token-stream identical,
  15/15 ranking parity on the live catalog). `User-Agent` is now
  `mcp-search-proxy/1.0.2`.
- Ellipsis truncation in search-result clipping is now ASCII `...` instead of `…`.

### Added

- Explicit anti-pattern guidance: `mcp_refresh` description now reads "Use ONLY
  when a tool comes back unknown/stale — never before every search/call".
  Same note in the server `instructions`. The proxy already re-lists once
  automatically on unknown-tool, so manual refresh before every call only adds
  downstream load.
- `mcp_call` description and unknown-tool hints now clarify that a server/source
  name alone is not callable — pick an exact tool name from `matches[]`.

## [1.0.1] - 2026-09-15

### Fixed

- Publish port `8092:8092` in `docker-compose.yml` (previously only `EXPOSE` in the
  Dockerfile, so the proxy was unreachable from the host).
- Handle stateless upstreams: `initialize` returning 200 without a `Mcp-Session-Id`
  header now marks the upstream as stateless and serves it with `session=None`
  (no `Mcp-Session-Id` header on list/call, no session reset on 400).
- Explicit `User-Agent: mcp-search-proxy/1.0.1` on all upstream requests
  (fixes Cloudflare 1010 `browser_signature_banned` on servers blocking the
  default `Python-urllib/*` signature).
- Startup validation with a clear `WARNING` on invalid URL scheme
  (e.g. `htps` typo) instead of a cryptic traceback; invalid entries are
  skipped without crashing.
- Robust body parsing: accept plain JSON responses as well as SSE `data:` lines.

### Added

- `UPSTREAMS=url|token,...` single-var format (recommended). Token after `|`
  is optional (absent = no auth). Fully backward compatible: when unset,
  legacy `UPSTREAM_URLS` / `UPSTREAM_TOKENS` behaviour is unchanged.
- OpenWebUI-in-Docker URL table in README (which URL to register depending on
  where OpenWebUI runs).
- Per-upstream startup log lines (`scheme_ok` / `auth=yes|no`, tokens never logged).

## [1.0.0] - 2026-09-11

Initial release: search-first proxy (`mcp_search` / `mcp_describe` / `mcp_call` /
`mcp_refresh`), local BM25 retrieval, sticky per-upstream sessions, on-disk
manifest, fail-soft fetch / fail-closed merge.
