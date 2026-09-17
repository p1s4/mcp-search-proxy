# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
