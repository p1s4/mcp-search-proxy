"""mcp-search-proxy — search-first in front of MCP upstreams.

The client sees 4 fixed tools (~2-3k tokens) instead of 532 full schemas (~179k):
  mcp_search(query, limit)  -> BM25 over cached manifest (names+desc+params, Hermes Layer B style)
  mcp_describe(names)      -> full inputSchema for N tools only
  mcp_call(name, arguments) -> tools/call proxied to the owning upstream, raw result back
  mcp_refresh()            -> upstream re-list + index rebuild (purely reactive, no polling)

Upstreams: any MCP server over Streamable HTTP (Docker DNS, e.g. http://litellm:4000/mcp). Bearer auth proxy->upstream only.
Names are mcp_* (not tool_*) to avoid the xAI 400 on the reserved tool_search name.
BM25: k1=1.5 b=0.75, rarest-token admission, exact-name=inf (like Hermes catalog.py).
POC: lowercase-split tokenize (no Snowball/nltk to stay at zero extra dependencies).
"""

import hashlib
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager

from fastmcp import FastMCP

# ---------------------------------------------------------------- config


VERSION = "1.0.7"
USER_AGENT = f"mcp-search-proxy/{VERSION}"


def _log(msg: str) -> None:
    print(f"[mcp-search-proxy] {msg}", flush=True)


# Generic multi-upstream (GitHub-ready): N servers via env.
# Recommended format: UPSTREAMS=url|token,url|token (token optional after |).
# Legacy format: UPSTREAM_URLS comma-separated + positional UPSTREAM_TOKENS.
# When UPSTREAMS is set, it IGNORES UPSTREAM_URLS/UPSTREAM_TOKENS/UPSTREAM_URL.
# Token must not contain "|" or "," (entries split on ",", pair on the FIRST "|").
# E.g. legacy prod: "http://litellm:4000/mcp,http://mcp-a11y-proxy:8101/mcp"
MASTER_KEY = os.environ.get("LITELLM_MASTER_KEY", "")
_raw_upstreams = os.environ.get("UPSTREAMS", "").strip()
if _raw_upstreams:
    _cfg_urls: list[str] = []
    _cfg_toks: list[str] = []
    for _entry in _raw_upstreams.split(","):
        _entry = _entry.strip()
        if not _entry:
            continue
        if "|" in _entry:
            _u, _t = _entry.split("|", 1)
            _u, _t = _u.strip(), _t.strip()
        else:
            _u, _t = _entry, ""
        if not _u:
            continue
        if _u in _cfg_urls:
            continue  # dedupe preserving order (keep first occurrence)
        _cfg_urls.append(_u)
        _cfg_toks.append(_t)
    UPSTREAM_URLS: list[str] = _cfg_urls
    _UPSTREAM_TOKENS: list[str] = _cfg_toks
else:
    _raw_urls = os.environ.get("UPSTREAM_URLS", "") or os.environ.get("UPSTREAM_URL", "http://litellm:4000/mcp")
    UPSTREAM_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()]
    # dedupe preserving order
    _seen: set[str] = set()
    UPSTREAM_URLS = [u for u in UPSTREAM_URLS if not (u in _seen or _seen.add(u))]
    # Legacy per-upstream auth: UPSTREAM_TOKENS comma-separated, positional
    # against UPSTREAM_URLS (empty entry = no-auth). When absent: MASTER_KEY only
    # on URLs containing "litellm", nothing elsewhere (a11y is no-auth).
    _raw_toks = os.environ.get("UPSTREAM_TOKENS", "")
    _UPSTREAM_TOKENS = [t.strip() for t in _raw_toks.split(",")] if _raw_toks else []
UPSTREAM_URL = UPSTREAM_URLS[0] if UPSTREAM_URLS else ""  # legacy alias (logs, single-upstream compat, realigned after validation)


def _scheme_ok(url: str) -> bool:
    try:
        return urllib.parse.urlparse(url).scheme in ("http", "https")
    except Exception:
        return False


def _validate_upstreams() -> None:
    """Startup validation: 1 line per upstream, never log tokens in clear.
    Non-http/https scheme -> WARNING + skip without crashing (e.g. htps typo)."""
    global UPSTREAM_URLS, _UPSTREAM_TOKENS, UPSTREAM_URL
    valid_urls: list[str] = []
    valid_toks: list[str] = []
    n = len(UPSTREAM_URLS)
    for i, url in enumerate(UPSTREAM_URLS, 1):
        tok = _UPSTREAM_TOKENS[i - 1] if (i - 1) < len(_UPSTREAM_TOKENS) else ""
        # the legacy MASTER_KEY fallback (litellm URLs only) counts as auth, as in _token_for
        has_auth = bool(tok) or bool(MASTER_KEY and "litellm" in url)
        ok = _scheme_ok(url)
        _log(f"[{i}/{n}] url={url} scheme_ok={str(ok).lower()} auth={'yes' if has_auth else 'no'}")
        if not ok:
            _log(f"WARNING: skipping upstream {url}: non-http/https scheme, check for typos (e.g. htps)")
            continue
        valid_urls.append(url)
        valid_toks.append(tok)
    UPSTREAM_URLS = valid_urls
    _UPSTREAM_TOKENS = valid_toks
    UPSTREAM_URL = UPSTREAM_URLS[0] if UPSTREAM_URLS else ""
    if not UPSTREAM_URLS:
        _log("WARNING: no valid upstream configured, catalog will stay empty until env fix + restart")


_validate_upstreams()
MANIFEST_PATH = os.environ.get("MANIFEST_PATH", "/app/data/manifest.json")
PORT = int(os.environ.get("PORT", "8092"))
TTL_SEC = int(os.environ.get("CATALOG_TTL_SEC", "1800"))  # 0 = off, explicit/unknown-tool refresh only
PROTOCOL = "2025-06-18"

SEARCH_DEFAULT_LIMIT = 5
SEARCH_MAX_LIMIT = 25
SEARCH_MAX_QUERIES_PER_CALL = 7  # aligned with Hermes _MAX_QUERIES_PER_CALL
DESCRIBE_MAX_NAMES = 10          # aligned with Hermes _MAX_DESCRIBE_NAMES_PER_CALL
DESC_CLIP = 500
CHARS_PER_TOKEN = 4.0
BM25_K1 = 1.5
BM25_B = 0.75
INDEX_DESC_CAP = 1500        # C (1.0.6): cap description chars indexed per tool (long-desc magnet cut)
INDEX_PARAM_DESC_CAP = 800   # C (1.0.6): cap combined param-description chars indexed per tool
TIER1_SHORT_DESC_LEN = 60
TIER1_NOTE_LEN = 200       # upstream server-note clip (tier-1 level 1/2)
TIER1_FALLBACK_LEN = 80    # medoid description clip (tier-1 level 3)
TIER1_TRY_KW = 5           # try-keywords per row, extracted from tool names
# Documented deviation from Hermes (which is pure BM25 over search-text): some
# catalog descriptions are in Italian (e.g. gmail-*), while model queries are
# in English. Without name weighting, a doc repeating 'email/send' in the body
# beats the right tool that has them in the name.
# NAME_BONUS adds 1.0 x idf per query-token present in the name.
NAME_BONUS_WEIGHT = 1.0

_token_re = re.compile(r"[A-Za-z0-9]+")
_split_re = re.compile(r"[_.:\-]+")

# ---------------------------------------------------------------- state (in-memory, thread-safe)

_LOCK = threading.RLock()
_TOOLS: list[dict] = []          # full upstream schemas (merged from all upstreams)
_BY_NAME: dict[str, dict] = {}
_DOC_TOKENS: dict[str, list[str]] = {}
_DF: dict[str, int] = {}
_AVGDL = 0.0
_N = 0
_FINGERPRINT = ""
_LAST_REFRESH = 0.0
_SESSIONS: dict[str, str] = {}       # sticky per-upstream: url -> sid (never mix)
_STATELESS: set[str] = set()          # upstreams without sid (initialize 200 without Mcp-Session-Id): session=None
_TOOL_UPSTREAM: dict[str, str] = {}  # routing: tool-name -> upstream url
_UPSTREAM_INSTRUCTIONS: dict[str, str] = {}  # upstream url -> initialize instructions (server notes)


def _fingerprint(tools: list[dict]) -> str:
    names = sorted(t.get("name", "") for t in tools)
    h = hashlib.sha256("\n".join(names).encode()).hexdigest()[:16]
    return f"{len(names)}:{h}"


def _estimate_tokens(chars: int) -> int:
    return math.ceil(chars / CHARS_PER_TOKEN)


# ---------------------------------------------------------------- upstream JSON-RPC (Streamable HTTP, SSE)

def _token_for(url: str) -> str:
    """Per-upstream auth (positional, legacy fallback).
    With UPSTREAMS=url|token: token paired to its entry. With legacy format:
    UPSTREAM_TOKENS comma-separated, same position as UPSTREAM_URLS
    (empty entry = no-auth). When absent: MASTER_KEY on litellm URLs only."""
    try:
        idx = UPSTREAM_URLS.index(url)
        if idx < len(_UPSTREAM_TOKENS) and _UPSTREAM_TOKENS[idx]:
            return _UPSTREAM_TOKENS[idx]
    except ValueError:
        pass
    if MASTER_KEY and "litellm" in url:
        return MASTER_KEY
    return ""


def _headers(url: str, session: str | None = None) -> dict:
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        # Explicit UA: urllib default "Python-urllib/3.12" is banned by some
        # site owners via Cloudflare (error 1010 browser_signature_banned, e.g. getbooyah).
        "User-Agent": USER_AGENT,
    }
    tok = _token_for(url)
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    if session:
        h["Mcp-Session-Id"] = session
    return h


def _parse_body(body: str) -> list[dict]:
    """Parse a Streamable HTTP response: SSE (data: {...}) or direct JSON
    (some stateless servers, e.g. getbooyah, answer pure application/json)."""
    out: list[dict] = []
    for line in body.split("\n"):
        line = line.strip()
        if not line.startswith("data:"):
            continue
        d = line[5:].strip()
        if not d or d == "[DONE]":
            continue
        try:
            out.append(json.loads(d))
        except json.JSONDecodeError:
            continue
    if out:
        return out
    body = body.strip()
    if body.startswith("{"):
        try:
            return [json.loads(body)]
        except json.JSONDecodeError:
            pass
    return []


def _parse_sse(body: str) -> list[dict]:
    return _parse_body(body)


def _timeout_for(url: str, default: int) -> int:
    """Per-upstream timeout: a11y behind nginx has proxy_read_timeout 300s."""
    try:
        v = int(os.environ.get("UPSTREAM_TIMEOUT_A11Y", "280"))
    except Exception:
        v = 280
    if "a11y" in url and default < v:
        return v
    return default


def _post(url: str, payload: dict, session: str | None, timeout: int) -> tuple[str | None, list[dict], int]:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=_headers(url, session), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            sid = r.headers.get("Mcp-Session-Id") or r.headers.get("mcp-session-id") or session
            body = r.read().decode("utf-8", "replace")
            return sid, _parse_body(body), r.status
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        msgs = _parse_sse(body) if body else []
        raise RuntimeError(f"upstream {url} HTTP {e.code}: {body[:500]}") from e


def _ensure_session(url: str) -> str | None:
    """Sticky per-upstream: strict sid reuse, never a fresh one per call.
    For stateful upstreams (e.g. a11y: tabs/pages bound to the sid) this is
    what avoids the lost-tab seen with LiteLLM on Jul 23 (new session
    per call -> navigated page no longer available). Reset only on
    400/404/session-expired, once, then retry.
    Stateless upstreams (initialize 200 without Mcp-Session-Id, e.g. gutenberg):
    no sid, session=None from here on (Mcp-Session-Id header omitted)."""
    with _LOCK:
        if url in _STATELESS:
            return None
        sid = _SESSIONS.get(url)
        if sid:
            return sid
    sid, msgs, _ = _post(
        url,
        {"jsonrpc": "2.0", "id": "init-1", "method": "initialize",
         "params": {"protocolVersion": PROTOCOL, "capabilities": {},
                    "clientInfo": {"name": "mcp-search-proxy", "version": VERSION}}},
        session=None, timeout=20,
    )
    _remember_instructions(url, msgs)
    if not sid:
        with _LOCK:
            _STATELESS.add(url)
        _log(f"upstream {url} stateless (initialize without session id), continuing without sid")
        # notifications/initialized best-effort even without sid (some servers want it)
        try:
            _post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session=None, timeout=10)
        except Exception as e:
            _log(f"initialized notification {url}: {e} (continuing)")
        return None
    # notifications/initialized (best-effort, 202 expected)
    try:
        _post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session=sid, timeout=10)
    except Exception as e:
        _log(f"initialized notification {url}: {e} (continuing)")
    with _LOCK:
        _STATELESS.discard(url)
        _SESSIONS[url] = sid
        return sid


def _remember_instructions(url: str, msgs: list) -> None:
    """Save upstream initialize instructions (server notes) for tier-1.
    The initialize result already arrives here; we keep only the text."""
    try:
        for m in msgs or []:
            res = m.get("result") if isinstance(m, dict) else None
            if isinstance(res, dict) and isinstance(res.get("instructions"), str):
                text = res["instructions"].strip()
                if text:
                    with _LOCK:
                        _UPSTREAM_INSTRUCTIONS[url] = text
                return
    except Exception as e:
        _log(f"remember instructions {url} failed: {e}")


def _refresh_upstream_instructions() -> None:
    """Re-run initialize on every upstream to refresh server notes.
    _ensure_session reuses the cached sid, so without this the notes would
    stay stale across refreshes. The probe session is DISCARDED: the live
    sticky sid (a11y tabs/pages) is always restored, never replaced.
    Best-effort: one call per upstream, fail-soft."""
    for url in UPSTREAM_URLS:
        try:
            with _LOCK:
                old = _SESSIONS.get(url)
            _, msgs, _ = _post(
                url,
                {"jsonrpc": "2.0", "id": "init-notes", "method": "initialize",
                 "params": {"protocolVersion": PROTOCOL, "capabilities": {},
                            "clientInfo": {"name": "mcp-search-proxy", "version": VERSION}}},
                session=None, timeout=20,
            )
            _remember_instructions(url, msgs)
            # NOTE: new probe sid (if any) is intentionally dropped — the
            # pre-existing sticky session in _SESSIONS stays authoritative.
            # The orphan probe session expires server-side (no initialized sent).
            _log(f"refreshed server notes for {url} (sticky session preserved)")
        except Exception as e:
            _log(f"refresh instructions {url} failed (keeping previous): {e}")


def _is_stateful(url: str) -> bool:
    with _LOCK:
        return url not in _STATELESS


def _reset_session(url: str) -> None:
    with _LOCK:
        # stateless: no session reset/retry, stays in _STATELESS
        if url in _STATELESS:
            return
        _SESSIONS.pop(url, None)


def _tools_list_one(url: str) -> tuple[str, list[dict]]:
    """tools/list on a single upstream. Returns (url, tools)."""
    try:
        sid = _ensure_session(url)
    except Exception as e:
        raise RuntimeError(f"initialize failed for {url}: {e}") from e
    for attempt in (1, 2):
        try:
            sid_now, msgs, _ = _post(
                url,
                {"jsonrpc": "2.0", "id": f"list-{attempt}", "method": "tools/list", "params": {}},
                session=sid, timeout=_timeout_for(url, 120),
            )
            if sid_now:
                with _LOCK:
                    _SESSIONS[url] = sid_now
                    sid = sid_now
            for m in msgs:
                res = m.get("result")
                if isinstance(res, dict) and isinstance(res.get("tools"), list):
                    return url, res["tools"]
            err = next((m.get("error") for m in msgs if m.get("error")), None)
            raise RuntimeError(f"tools/list without result.tools: {json.dumps(msgs)[:800]} err={err}")
        except RuntimeError as e:
            # expired/invalid session -> reset once and retry (stateful only;
            # for stateless a 400 is a normal error, no session reset/retry)
            if attempt == 1 and _is_stateful(url) and ("400" in str(e) or "404" in str(e) or "session" in str(e).lower()):
                _log(f"tools/list {url} retry after session reset ({e})")
                _reset_session(url)
                sid = _ensure_session(url)
                continue
            raise
    raise RuntimeError(f"tools/list {url}: attempts exhausted")


def _tools_list_live() -> list[tuple[str, list[dict]]]:
    """Sequential fan-out over all upstreams. Returns [(url, tools)].
    One upstream down does not block the others: log + empty list (fail-soft).
    If ALL fail, raise RuntimeError (fail-closed)."""
    results: list[tuple[str, list[dict]]] = []
    errors: list[str] = []
    for url in UPSTREAM_URLS:
        try:
            results.append(_tools_list_one(url))
        except Exception as e:
            _log(f"tools/list {url} failed, skipping upstream: {e}")
            errors.append(f"{url}: {e}")
    if not results:
        raise RuntimeError(f"all upstreams failed: {'; '.join(errors)}")
    return results


def _tools_call_live(name: str, arguments: dict, timeout: int = 180) -> dict:
    """Per-tool routing: the call goes to the upstream that provided the tool
    (_TOOL_UPSTREAM map). Fallback: try in order (fail-soft)."""
    with _LOCK:
        preferred = _TOOL_UPSTREAM.get(name)
    urls = [preferred] + [u for u in UPSTREAM_URLS if u != preferred] if preferred else list(UPSTREAM_URLS)
    last_err: Exception | None = None
    for url in urls:
        sid = _ensure_session(url)
        for attempt in (1, 2):
            try:
                _, msgs, _ = _post(
                    url,
                    {"jsonrpc": "2.0", "id": f"call-{attempt}",
                     "method": "tools/call", "params": {"name": name, "arguments": arguments or {}}},
                    session=sid, timeout=_timeout_for(url, timeout),
                )
                for m in msgs:
                    if "result" in m or "error" in m:
                        return m
                raise RuntimeError(f"tools/call without result/error: {json.dumps(msgs)[:800]}")
            except RuntimeError as e:
                last_err = e
                if attempt == 1 and _is_stateful(url) and ("400" in str(e) or "404" in str(e) or "session" in str(e).lower()):
                    _log(f"tools/call {url} retry after session reset ({e})")
                    _reset_session(url)
                    sid = _ensure_session(url)
                    continue
                break  # non-session error: move to next upstream only on fallback
        if preferred:
            break  # explicit routing: no spillover to other upstreams
    raise last_err or RuntimeError(f"tools/call '{name}': no upstream available")


# ---------------------------------------------------------------- catalog + BM25

def _source_of(name: str) -> str:
    if "-" in name:
        return name.split("-")[0]
    return name.split("_")[0]


def _entry_search_text(tool: dict) -> str:
    name = tool.get("name", "")
    desc = tool.get("description", "") or ""
    norm_name = _split_re.sub(" ", name).lower()
    parts = [norm_name]
    src = _source_of(name).lower().replace("_", " ")
    if src and src not in norm_name:
        parts.append(src)
    # C (1.0.6): clip body before indexing — long descriptions (learn_shopify_api
    # ~4k ch, mailchimp Args: blocks, Context7) otherwise dominate BM25 via TF.
    parts.append(desc[:INDEX_DESC_CAP])
    try:
        props = (tool.get("inputSchema") or {}).get("properties") or {}
        if isinstance(props, dict):
            keys = list(props.keys())[:60]
            parts.append(" ".join(str(k).replace("_", " ") for k in keys))
            descs = []
            for k in keys:
                v = props.get(k)
                if isinstance(v, dict):
                    d = v.get("description")
                    if d:
                        descs.append(str(d))
            if descs:
                # C (1.0.6): param descriptions capped — they add recall, not ranking mass.
                parts.append(" ".join(descs)[:INDEX_PARAM_DESC_CAP])
    except Exception:
        pass
    return "\n".join(parts)


def _tokenize(text: str) -> list[str]:
    return [_stem(t) for t in _token_re.findall(text.lower())]


def _stem(tok: str) -> str:
    """Minimal hand-rolled EN stemmer, zero dependencies (consistent with the POC).
    Light s/es/ed/ing stripping with a len<4 guard (avoids 'is'->'i').
    Applied to both docs (via _tokenize in _build_index) and queries
    (via _tokenize in _search_one), otherwise 'threads' vs 'thread'
    stay incomparable. Known over-stemming: 'created'->'creat'
    vs 'create'->'create' (they diverge); rare in real queries, the
    OOV-robust gate + limit+describe mitigate it. s/es plurals handled with
    sibilant rules to avoid 'messages'->'messag' (trailing s only)."""
    if len(tok) < 4:
        return tok
    if len(tok) > 5 and tok.endswith("ing") and len(tok) - 3 >= 4:
        base = tok[:-3]
        if len(base) >= 5 and base[-1] == base[-2]:
            base = base[:-1]
        return base
    if len(tok) > 4 and tok.endswith("ed") and len(tok) - 2 >= 4:
        base = tok[:-2]
        if len(base) >= 5 and base[-1] == base[-2]:
            base = base[:-1]
        return base
    if len(tok) > 4 and tok.endswith("ies") and len(tok) - 3 >= 3:
        return tok[:-3] + "y"
    if len(tok) > 5 and tok.endswith("sses"):
        return tok[:-2]
    if len(tok) > 5 and (tok.endswith("ches") or tok.endswith("shes")):
        return tok[:-2]
    if len(tok) > 4 and (tok.endswith("xes") or tok.endswith("zes") or tok.endswith("oes")):
        return tok[:-2]
    if len(tok) > 4 and tok.endswith("s") and not tok.endswith("ss") and not tok.endswith("us"):
        return tok[:-1]
    return tok


def _name_match(qtok: str, ntok: str) -> bool:
    """Guarded name match: equal, or qtok substring of ntok (len>=4).
    Covers 'mail' in 'gmail'/'mailchimp' without tokenizer splitting.
    Query->name direction only (not vice versa) + len guard to avoid
    false positives on short generic tokens."""
    if qtok == ntok:
        return True
    return len(qtok) >= 4 and qtok in ntok


def _segments(name: str) -> list[str]:
    """Provider-operation segments: split on -/_ (lowercased, len>=3 only).
    e.g. 'Clickup-create_issue' -> ['clickup', 'create', 'issue'];
    'wsc_mcp-search' -> ['wsc', 'mcp', 'search'].
    Exact segment equality (len>=3) admits 'issue' -> 'create_issue'
    without substring noise ('api' equals 'api', never matches 'rapid')."""
    return [p for p in re.split(r"[-_]+", name.lower()) if len(p) >= 3]


def _build_index(tools: list[dict], routing: dict[str, str] | None = None) -> None:
    global _TOOLS, _BY_NAME, _DOC_TOKENS, _DF, _AVGDL, _N, _FINGERPRINT, _TOOL_UPSTREAM
    by_name: dict[str, dict] = {}
    doc_tokens: dict[str, list[str]] = {}
    df: dict[str, int] = {}
    total_len = 0
    for t in tools:
        n = t.get("name", "")
        if not n or n in by_name:
            continue
        by_name[n] = t
        toks = _tokenize(_entry_search_text(t))
        doc_tokens[n] = toks
        total_len += len(toks)
        for tok in set(toks):
            df[tok] = df.get(tok, 0) + 1
    with _LOCK:
        _TOOLS = list(by_name.values())
        _BY_NAME = by_name
        _DOC_TOKENS = doc_tokens
        _DF = df
        _N = len(by_name)
        _AVGDL = (total_len / _N) if _N else 0.0
        _FINGERPRINT = _fingerprint(_TOOLS)
        if routing is not None:
            # routing only for names surviving dedupe (first upstream wins)
            _TOOL_UPSTREAM = {n: routing[n] for n in by_name if n in routing}


def _idf(tok: str) -> float:
    n = _N or 1
    df = _DF.get(tok, 0)
    return math.log(1 + (n - df + 0.5) / (df + 0.5))


def _bm25_score(qtoks: list[str], doc: list[str]) -> float:
    if not doc:
        return 0.0
    from collections import Counter
    tf = Counter(doc)
    dl = len(doc)
    avg = _AVGDL or 1.0
    s = 0.0
    for tok in qtoks:
        f = tf.get(tok, 0)
        if not f:
            continue
        idf = _idf(tok)
        denom = f + BM25_K1 * (1 - BM25_B + BM25_B * dl / avg)
        s += idf * (f * (BM25_K1 + 1)) / denom
    return s


def _search_one(query: str, limit: int) -> list[str]:
    with _LOCK:
        names = list(_BY_NAME.keys())
    if not names:
        return []
    qtoks = _tokenize(query)
    if not qtoks:
        return []
    # exact-name match = inf (like Hermes)
    qnorm = query.strip().lower()
    with _LOCK:
        for n in names:
            if n.lower() == qnorm:
                return [n]
    # rarest-token gate (like Hermes) with OOV robustness: gate selection
    # happens only among tokens present in the index (DF>0). OOV tokens (typos,
    # unstemmed plurals, Italian words on English descriptions and
    # vice versa) contribute 0 to BM25 but must not zero out results.
    # Without Snowball (zero-dependency POC) 'threads' does not match 'thread':
    # known limitation, mitigated by limit+describe. When no token is in
    # vocabulary, no doc can match -> [] with an available_sources hint.
    in_vocab = [t for t in set(qtoks) if _DF.get(t, 0) > 0]
    if not in_vocab:
        return []
    scored_q = sorted(in_vocab, key=lambda t: _idf(t), reverse=True)
    gate = scored_q[0]
    qset = set(qtoks)
    ranked: list[tuple[float, str]] = []
    with _LOCK:
        snapshot = [
            (n, _DOC_TOKENS.get(n, []), ((_BY_NAME.get(n) or {}).get("description") or ""))
            for n in names
        ]
    for n, doc, desc in snapshot:
        # NAME_BONUS gate bypass: the gate is picked over the whole query and
        # some descriptions are in IT while queries are in EN.
        # When the highest-IDF token (e.g. 'inbox') is missing from the doc, the
        # tool would be dropped before the name bonus could save it, even
        # when the name matches explicitly (e.g. 'gmail' in gmail-*).
        # name_hit admits the doc regardless of the gate; the
        # BM25+bonus ranking then decides the order. name_hit uses guarded
        # _name_match (query->name substring, len>=4): 'mail' in 'gmail' admits
        # gmail tools even when the 'mail' token is not in the doc (the tokenizer
        # does not split 'gmail' into 'g'+'mail'). Same rule for the bonus below.
        name_toks = set(_tokenize(_split_re.sub(" ", n)))
        name_hit = any(_name_match(t, nt) for t in qset for nt in name_toks)
        if gate and gate not in doc and not name_hit:
            continue
        s = _bm25_score(qtoks, doc)
        # name bonus: each query-token in the name adds idf x weight.
        # Guarded _name_match: 'mail' in 'gmail' counts (name substring,
        # len>=4); 'mail' also matches 'mailchimp' — ranking decides:
        # idf(mail) is high and shared, bonus equal for both, BM25 on the body wins.
        bonus = 0.0
        for t in qset:
            for nt in name_toks:
                if _name_match(t, nt):
                    bonus += _idf(t)
                    break
        bonus *= NAME_BONUS_WEIGHT
        # Keyword boost (original reimplementation, stdlib only, weights from
        # the handoff spec: +50/+20/+3/+1 on top of BM25+NAME_BONUS).
        # Idea: the explicit name/description match must weigh against
        # noisy bodies (long descriptions with high TF on rare tokens).
        # Additive boost only: no min_score gate (we do top-N here, the model
        # applies the gate). Admission and ordering unchanged.
        n_lower = n.lower()
        d_lower = desc.lower() if isinstance(desc, str) else ""
        keyword_boost = 0.0
        if qnorm:
            if qnorm in n_lower:
                keyword_boost += 50.0
            if qnorm in d_lower:
                keyword_boost += 20.0
            for term in qset:
                if not term:
                    continue
                if term in n_lower:
                    keyword_boost += 3.0
                # B (1.0.6): per-term +1.0 on description REMOVED — it rewarded
                # long bodies (learn_shopify_api, mailchimp Args:, Context7)
                # for generic tokens (text/content/template). Full-phrase +20
                # above is kept: phrase match in desc stays a strong signal.
            # Adaptation to the provider-operation namespaced catalog
            # (original, single-token queries only): the literal
            # +50/+20 boost ties all contenders (e.g. 'mail' in gmail-* and
            # mailchimp-*, 'search' in WebSearchAndCrawl-* and github-search-*).
            # The exact operation (suffix after -/_) and the provider (prefix
            # before -/_) disambiguate: 'mail' is a suffix of 'gmail'
            # (not a prefix of 'mailchimp'), 'search' is the exact operation
            # of WebSearchAndCrawl-search (not of search_analytics).
            qparts = re.findall(r"[a-z0-9_]+", qnorm)
            op_lower = n.rsplit("-", 1)[-1].lower() if "-" in n else n.rsplit("_", 1)[-1].lower()
            src_lower = _source_of(n).lower()
            # Segment-match (Punto 1, 2026-10-26): the operation may be
            # compound ('create_issue'); split on _ so 'issue' matches
            # the exact segment. Same for compound sources ('wsc_mcp').
            # Len>=3 guard: no boost on 1-2ch tokens.
            op_segs = _segments(op_lower)
            src_segs = _segments(src_lower)
            # Stemmed copies for plural-tolerant match (orders->order,
            # tasks->task): raw equality first, stemmed fallback.
            op_stems = {_stem(w) for w in op_segs}
            src_stems = {_stem(w) for w in src_segs}
            if len(qparts) == 1:
                q1 = qparts[0]
                q1s = _stem(q1)
                if len(q1) >= 3 and (q1 == op_lower or q1 in op_segs or q1s in op_stems):
                    keyword_boost += 30.0
                if len(q1) >= 3 and (q1 == src_lower or q1 in src_segs or q1s in src_stems):
                    keyword_boost += 15.0
                elif len(q1) >= 4 and src_lower.endswith(q1) and q1 != src_lower:
                    keyword_boost += 12.0
            else:
                # A (1.0.6): multi-token provider/operation boost. Each query
                # token matching the operation (+10) or the provider (+6,
                # suffix +4) disambiguates the tail on multi-intent queries
                # (send gmail email, list clickup tasks) where the old
                # single-token branch never fired. Weights are lower than the
                # single-token branch (30/15/12) to avoid score explosion.
                for qp in set(qparts):
                    if len(qp) < 3:
                        continue
                    qps = _stem(qp)
                    if qp == op_lower or qp in op_segs or qps in op_stems:
                        keyword_boost += 10.0
                    if qp == src_lower or qp in src_segs or qps in src_stems:
                        keyword_boost += 6.0
                    elif len(qp) >= 4 and src_lower.endswith(qp) and qp != src_lower:
                        keyword_boost += 4.0
        if s > 0 or bonus > 0:
            ranked.append((s + bonus + keyword_boost, n))
    ranked.sort(key=lambda x: (-x[0], x[1]))
    return [n for _, n in ranked[:limit]]


def _clip(s: str, n: int = DESC_CLIP) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "..."


def _required_of(tool: dict) -> list[str]:
    try:
        req = (tool.get("inputSchema") or {}).get("required") or []
        return [str(x)[:64] for x in req[:32]]
    except Exception:
        return []


def _save_manifest() -> None:
    try:
        os.makedirs(os.path.dirname(MANIFEST_PATH) or ".", exist_ok=True)
        with _LOCK:
            payload = {
                "fingerprint": _FINGERPRINT,
                "count": _N,
                "total_chars": len(json.dumps(_TOOLS)),
                "refreshed_at": _LAST_REFRESH,
                "upstreams": list(UPSTREAM_URLS),
                "routing": dict(_TOOL_UPSTREAM),
                "upstream_instructions": dict(_UPSTREAM_INSTRUCTIONS),
                "tools": _TOOLS,
            }
        tmp = MANIFEST_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, MANIFEST_PATH)
    except Exception as e:
        _log(f"save manifest failed: {e}")


def _load_manifest() -> bool:
    try:
        with open(MANIFEST_PATH) as f:
            payload = json.load(f)
        tools = payload.get("tools") or []
        if not tools:
            return False
        _build_index(tools, payload.get("routing") or None)
        with _LOCK:
            _UPSTREAM_INSTRUCTIONS.update(payload.get("upstream_instructions") or {})
            # legacy manifest without routing (single-upstream 1.0.0): everything to the first upstream
            if not _TOOL_UPSTREAM and _TOOLS and UPSTREAM_URLS:
                _TOOL_UPSTREAM.update({t.get("name", ""): UPSTREAM_URLS[0] for t in _TOOLS if t.get("name")})
        global _LAST_REFRESH
        with _LOCK:
            _LAST_REFRESH = float(payload.get("refreshed_at") or 0.0)
        _log(f"manifest from disk: {_N} tool fp={_FINGERPRINT}")
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        _log(f"load manifest failed: {e}")
        return False


def refresh_catalog(force: bool = False) -> dict:
    """Fetch-once + rebuild. Returns stats. Raises RuntimeError on failure.
    At the end it refreshes mcp.instructions with the tier-1 manifest (1 line per
    source: upstream server note or medoid short-desc + try keywords). Verified on
    fastmcp 3.4.2: FastMCP.instructions has a setter (server.py:445-450,
    delegating to _mcp_server.instructions), so it is runtime-mutable.
    Note: clients read instructions at the initialize handshake — a
    runtime mcp_refresh() updates the server but already-connected clients
    keep seeing the old text until their next handshake.
    1.0.7 fail-closed: a partial refresh (some upstream down/empty) that
    would shrink a good catalog is refused — the previous index + manifest
    are kept and RuntimeError is raised (lifespan/mcp_refresh fall back
    to disk/in-memory). A genuine shrink is applied only when ALL
    configured upstreams answer (even with fewer tools)."""
    global _LAST_REFRESH
    with _LOCK:
        _prev_n = _N
        _prev_fp = _FINGERPRINT
    per_up = _tools_list_live()  # [(url, tools)] fan-out
    merged: list[dict] = []
    routing: dict[str, str] = {}
    counts: dict[str, int] = {}
    for url, tools in per_up:
        counts[url] = len(tools)
        for t in tools:
            n = t.get("name", "")
            if not n or n in routing:
                if n in routing:
                    _log(f"name collision '{n}': keeping {routing[n]}, dropping {url} (fail-closed)")
                continue
            routing[n] = url
            merged.append(t)
    if not merged:
        raise RuntimeError("all upstreams returned 0 tools")
    # Fail-closed on partial upstream failure (reboot race: e.g. litellm
    # still down, only a11y answers -> 28 < 631 cached). Generic: compare
    # against the previous in-memory catalog, refuse shrinkage when any
    # configured upstream is missing or returned 0 tools.
    ok_urls = {url for url, tools in per_up if tools}
    missing = [u for u in UPSTREAM_URLS if u not in ok_urls]
    if missing and _prev_n and len(merged) < _prev_n:
        _log(f"WARNING: partial refresh refused ({len(merged)} < {_prev_n} cached, missing {missing}): keeping fp={_prev_fp}")
        raise RuntimeError(f"partial refresh refused: {len(merged)} < {_prev_n} cached, missing upstreams {missing}")
    _build_index(merged, routing)
    with _LOCK:
        _LAST_REFRESH = time.time()
    _refresh_upstream_instructions()
    _save_manifest()
    _update_instructions()
    total_chars = len(json.dumps(_TOOLS))
    _log(f"catalog refresh: {_N} tool {counts}, {total_chars}ch (~{_estimate_tokens(total_chars)}tok), fp={_FINGERPRINT}")
    return {
        "ok": True,
        "count": _N,
        "counts_per_upstream": counts,
        "upstreams": list(counts.keys()),
        "total_chars": total_chars,
        "est_tokens": _estimate_tokens(total_chars),
        "fingerprint": _FINGERPRINT,
        "refreshed_at": _LAST_REFRESH,
    }


def _stale() -> bool:
    if TTL_SEC <= 0:
        return False
    with _LOCK:
        last = _LAST_REFRESH
    return (time.time() - last) > TTL_SEC


def _word_clip(text: str, n: int) -> str:
    """First non-empty line, word-boundary clip at n chars + ellipsis (1.0.5).
    Line-based (not sentence-based): immune to abbreviations/domains
    ('Next.js', 'notebook.google.com', 'e.g.') that broke the naive
    split('.') simulation. Fixes also the mid-word cut of _short_desc."""
    first = ""
    for line in (text or "").splitlines():
        s = line.strip()
        if s:
            first = s
            break
    t = " ".join(first.split())  # collapse double spaces
    if len(t) <= n:
        return t
    cut = t[:n].rsplit(" ", 1)[0] or t[:n]
    return cut.rstrip(",;:") + "\u2026"


def _short_desc(text: str, n: int = TIER1_SHORT_DESC_LEN) -> str:
    """First sentence, max n chars with word-boundary (like Hermes
    _short_desc in tool_search_catalog.py:185, plus word-boundary fix)."""
    return _word_clip(text, n)


_TIER1_TRY_STOP = frozenset(
    "get list create update delete set add run fetch search show check make do "
    "use with for and the tool tools data info new all api mcp of to on in by from into via per "
    "google workspace".split()
)


def _source_keywords(names: list[str], source: str, k: int = TIER1_TRY_KW) -> list[str]:
    """Top-k tokens from tool names (prefix stripped), stopwords/CRUD removed.
    Deterministic: same manifest -> same keywords. Recomputed at each refresh,
    so adding/removing an MCP updates the row with zero manual maintenance."""
    from collections import Counter
    c: Counter = Counter()
    slow = source.lower()
    for n in names:
        body = n[len(source):] if n.startswith(source) else n
        for tok in _token_re.findall(body.lower()):
            if len(tok) < 3 or tok in _TIER1_TRY_STOP or tok == slow:
                continue
            c[tok] += 1
    return [w for w, _ in c.most_common(k)]


def _parse_server_notes(text: str) -> dict[str, str]:
    """Split upstream instructions into {source: note} on ^[name] blocks.
    Text without blocks returns {} (caller treats it as wildcard)."""
    notes: dict[str, str] = {}
    if not text or "[" not in text:
        return notes
    cur: str | None = None
    buf: list[str] = []

    def flush() -> None:
        if cur and buf:
            notes[cur] = "\n".join(buf).strip()

    for line in text.splitlines():
        m = re.match(r"^\[([^\]]+)\]\s*$", line.strip())
        if m:
            flush()
            cur = m.group(1)
            buf = []
        elif cur is not None:
            buf.append(line)
    flush()
    return notes


def _tier1_notes() -> dict[str, str]:
    """Merge upstream server notes: [source] blocks win; block-less text
    applies as wildcard ONLY when its URL serves a single source.
    Multi-source wildcard (e.g. a11y text cloned on audit+browser+scan)
    is ambiguous -> blocked, those sources fall back to medoid desc."""
    with _LOCK:
        instr = dict(_UPSTREAM_INSTRUCTIONS)
        routing = dict(_TOOL_UPSTREAM)
    notes: dict[str, str] = {}
    wild: dict[str, str] = {}  # url -> text (no blocks)
    for url, text in instr.items():
        parsed = _parse_server_notes(text)
        if parsed:
            for s, note in parsed.items():
                notes.setdefault(s, note)
        elif text.strip():
            wild[url] = text.strip()
    if wild:
        url_of: dict[str, str] = {}
        for tool, url in routing.items():
            url_of.setdefault(_source_of(tool), url)
        from collections import Counter
        per_url = Counter(url_of.values())
        for s, url in url_of.items():
            if s not in notes and url in wild and per_url[url] == 1:
                notes[s] = wild[url]
    return notes


_GW_SVC_ALIASES: dict[str, str] = {
    # token -> canonical service row
    "gmail": "gmail",
    "drive": "drive",
    "calendar": "calendar", "calendars": "calendar",
    "event": "calendar", "events": "calendar",
    "freebusy": "calendar", "focus": "calendar", "office": "calendar",
    "doc": "docs", "docs": "docs",
    "sheet": "sheets", "sheets": "sheets",
    "slide": "slides", "slides": "slides",
    "task": "task", "tasks": "task",
}


def _gw_service(tool_name: str, source: str = "Google_Workspace") -> str:
    """Canonical service for a Google_Workspace tool, scanning all tokens
    after the prefix (verb-agnostic). '' when no known service token."""
    rest = tool_name[len(source):].lstrip("-_") if tool_name.startswith(source) else tool_name
    for tok in [p.lower() for p in re.split(r"[-_]+", rest) if p]:
        if tok in _GW_SVC_ALIASES:
            return _GW_SVC_ALIASES[tok]
    return ""


def _tier1_needs_split(source: str) -> bool:
    """True when a source mixes several known services (Google_Workspace:
    gmail/drive/calendar/docs/sheets/slides/task). The split itself happens
    in _tier1_listing; this is only the gate."""
    with _LOCK:
        names = [n for n in _BY_NAME if _source_of(n) == source]
    if len(names) < 5:
        return False
    services = {_gw_service(n, source) for n in names}
    return "" not in services and len(services) >= 2


def _tier1_listing() -> str:
    """Tier-1 manifest: 1 line per source 'name: phrase | try: kw (N tool)',
    sorted byte-stable (prompt-cache safe, like Hermes). Phrase priority:
    1) upstream server note ([source] block, clipped to TIER1_NOTE_LEN),
    2) medoid tool description (tool whose name holds most top keywords),
    clipped to TIER1_FALLBACK_LEN. Google_Workspace (gmail/drive/calendar/
    task in one source) is split into one row per service."""
    with _LOCK:
        items = list(_BY_NAME.items())
    from collections import Counter
    by_source: dict[str, list[str]] = {}
    for n, _ in items:
        by_source.setdefault(_source_of(n), []).append(n)
    counts = Counter(_source_of(n) for n, _ in items)
    notes = _tier1_notes()
    # per-source rows: (sort_key, text); Google_Workspace splits per service
    rows: dict[str, str] = {}
    for s in sorted(by_source):
        names = by_source[s]
        kws = _source_keywords(names, s)
        try_part = f" | try: {', '.join(kws)}" if kws else ""
        if s == "Google_Workspace" and _tier1_needs_split(s):
            # split gmail/drive/calendar/docs/sheets/slides/task into sub-rows
            # (verb-agnostic: event/freebusy/focus/out_of_office -> calendar)
            subs: dict[str, list[str]] = {}
            for n in names:
                svc = _gw_service(n, s) or "other"
                subs.setdefault(svc, []).append(n)
            for svc in sorted(subs):
                snames = subs[svc]
                skws = _source_keywords(snames, f"{s}_{svc}")
                stry = f" | try: {', '.join(skws)}" if skws else ""
                # medoid: most keyword hits in name, tie-break longest desc
                if skws:
                    def _key(n: str) -> tuple:
                        with _LOCK:
                            d = (_BY_NAME.get(n) or {}).get("description", "") or ""
                        return (sum(1 for k in skws if k in n.lower()), len(d))
                    best = max(sorted(snames), key=_key)
                else:
                    best = sorted(snames)[0]
                with _LOCK:
                    bdesc = (_BY_NAME.get(best) or {}).get("description", "")
                phrase = _word_clip(bdesc, TIER1_FALLBACK_LEN)
                rows[f"{s}-{svc}"] = f"- {s} ({svc}): {phrase}{stry} ({len(snames)} tool)"
            continue
        if s in notes:
            phrase = _word_clip(notes[s], TIER1_NOTE_LEN)
        else:
            if kws:
                def _key(n: str) -> tuple:
                    with _LOCK:
                        d = (_BY_NAME.get(n) or {}).get("description", "") or ""
                    return (sum(1 for k in kws if k in n.lower()), len(d))
                best = max(sorted(names), key=_key)
            else:
                best = sorted(names)[0]
            with _LOCK:
                bdesc = (_BY_NAME.get(best) or {}).get("description", "")
            phrase = _word_clip(bdesc, TIER1_FALLBACK_LEN)
        rows[s] = f"- {s}: {phrase}{try_part} ({counts[s]} tool)"
    return "\n".join(rows[k] for k in sorted(rows))


def _update_instructions() -> None:
    try:
        listing = _tier1_listing()
        with _LOCK:
            n = _N
        mcp.instructions = (
            f"Search-first proxy in front of {len(UPSTREAM_URLS)} MCP upstreams ({n} tools). "
            "Flow: mcp_search to find names, mcp_describe for full schemas, "
            "mcp_call to execute. mcp_refresh only when a tool comes back unknown or stale — never refresh before every call. "
            "Never tell the user a capability is unavailable without searching first.\n"
            f"Catalog by server:\n{listing}"
        )
    except Exception as e:
        _log(f"update instructions failed: {e}")


def _ensure_catalog() -> None:
    with _LOCK:
        empty = _N == 0
    if empty:
        if not _load_manifest():
            refresh_catalog(force=True)
        else:
            _update_instructions()
            # manifest on disk but we still check upstream when TTL is stale
            if _stale():
                try:
                    refresh_catalog(force=True)
                except Exception as e:
                    _log(f"TTL refresh failed, using disk manifest: {e}")
    elif _stale():
        try:
            refresh_catalog(force=True)
        except Exception as e:
            _log(f"TTL refresh failed, using in-memory catalog: {e}")


# ---------------------------------------------------------------- FastMCP

@asynccontextmanager
async def _lifespan(app):
    try:
        if not _load_manifest():
            _log("no manifest, initial upstream fetch...")
            refresh_catalog(force=True)
        else:
            _log(f"starting with disk manifest ({_N} tools), best-effort upstream check...")
            try:
                refresh_catalog(force=True)
            except Exception as e:
                _log(f"initial fetch failed, staying on disk manifest: {e}")
                _update_instructions()
    except Exception as e:
        _log(f"lifespan: catalog unavailable at startup: {e}")
    yield


mcp = FastMCP(
    name="mcp-search-proxy",
    instructions=(
        "Search-first proxy in front of MCP upstreams. "
        "Flow: mcp_search to find names, mcp_describe for full schemas, "
        "mcp_call to execute. mcp_refresh only when a tool comes back unknown or stale — never refresh before every call. "
        "Never tell the user a capability is unavailable without searching first."
    ),
    lifespan=_lifespan,
)


@mcp.tool(description="Search MCP tools by topic (BM25 over names+descriptions+params). Returns names + mini-cards, never full schemas. If top results share one prefix (e.g. all thread_*) and the query has multiple intents, retry with a higher limit (e.g. 10).")
def mcp_search(query: str, limit: int = SEARCH_DEFAULT_LIMIT) -> dict:
    _ensure_catalog()
    if isinstance(query, list):  # tolerance: list -> first entry
        query = query[0] if query else ""
    query = (query or "").strip()
    if not query:
        return {"ok": False, "error": "empty query", "hint": "e.g. 'gmail send', 'github issue', 'calendar event'"}
    try:
        limit = int(limit)
    except Exception:
        limit = SEARCH_DEFAULT_LIMIT
    limit = max(1, min(limit, SEARCH_MAX_LIMIT))
    if len(query.split()) > 20:
        query = " ".join(query.split()[:20])
    names = _search_one(query, limit)
    with _LOCK:
        total = _N
    tools: dict[str, dict] = {}
    with _LOCK:
        for n in names:
            t = _BY_NAME.get(n, {})
            tools[n] = {
                "source": _source_of(n),
                "description": _clip(t.get("description", "")),
                "required": _required_of(t),
            }
    out: dict = {"ok": True, "query": query, "total_available": total,
                 "results": [{"query": query, "matches": names}], "tools": tools}
    if not names:
        with _LOCK:
            from collections import Counter
            c = Counter(_source_of(n) for n in _BY_NAME)
            out["available_sources"] = [{"name": k, "tool_count": v}
                                        for k, v in sorted(c.items())]
            out["hint"] = ("no lexical match: try synonyms or English terms "
                           "(e.g. 'email' instead of 'posta'), or mcp_refresh if the catalog is stale")
    return out


@mcp.tool(description="Full input schemas only for the listed names (max 10/call). If you see the exact name in the listing, skip mcp_search and come straight here.")
def mcp_describe(names: list[str]) -> dict:
    _ensure_catalog()
    if isinstance(names, str):
        names = [names]
    names = [str(x) for x in (names or []) if str(x).strip()]
    if not names:
        return {"ok": False, "error": "empty names"}
    if len(names) > DESCRIBE_MAX_NAMES:
        return {"ok": False, "error": f"max {DESCRIBE_MAX_NAMES} names per call", "received": len(names)}
    seen, uniq = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    tools: dict[str, dict] = {}
    not_found: list[str] = []
    with _LOCK:
        for n in uniq:
            t = _BY_NAME.get(n)
            if t is None:
                not_found.append(n)
            else:
                tools[n] = {"description": t.get("description", ""),
                            "parameters": t.get("inputSchema", {"type": "object"})}
    out: dict = {"ok": True, "tools": tools, "not_found": not_found, "errors": {}}
    if not_found:
        out["hint"] = "use mcp_search for the correct names, or mcp_refresh if the catalog is stale"
    return out


@mcp.tool(description="Execute an MCP tool on the upstream that provides it and return the raw result. Validates required params locally before sending. Needs the exact tool name from mcp_search — a server/source name alone is not callable.")
def mcp_call(name: str, arguments: dict | str | None = None) -> dict:
    _ensure_catalog()
    name = (name or "").strip()
    if not name:
        return {"ok": False, "error": "empty name"}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments) if arguments.strip() else {}
        except json.JSONDecodeError as e:
            return {"ok": False, "error": f"arguments is not valid JSON: {e}"}
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        return {"ok": False, "error": "arguments must be a JSON object"}

    with _LOCK:
        tool = _BY_NAME.get(name)
    if tool is None:
        # purely reactive: one-shot re-list on unknown-tool, then retry
        try:
            before = _FINGERPRINT
            refresh_catalog(force=True)
            with _LOCK:
                tool = _BY_NAME.get(name)
            _log(f"unknown tool '{name}': re-list fp {before}->{_FINGERPRINT}")
        except Exception as e:
            return {"ok": False, "error": f"tool '{name}' unknown and refresh failed: {e}",
                    "hint": "use mcp_search to find the correct name (a server/source name alone is not callable — pick an exact tool name from matches[])"}
        if tool is None:
            return {"ok": False, "error": f"tool '{name}' unknown even after refresh",
                    "hint": "use mcp_search to find the correct name (a server/source name alone is not callable — pick an exact tool name from matches[])"}

    # local required validation (fail-open on external $refs, like Hermes validation.py)
    try:
        schema = tool.get("inputSchema") or {}
        required = schema.get("required") or []
        missing = [r for r in required if r not in arguments]
        if missing:
            return {"ok": False, "error": f"missing required params: {missing}",
                    "required": [str(x) for x in required],
                    "hint": "use mcp_describe for the full schema"}
    except Exception:
        pass  # fail-open

    try:
        msg = _tools_call_live(name, arguments)
    except Exception as e:
        return {"ok": False, "error": f"upstream tools/call failed: {e}"}
    if isinstance(msg, dict) and msg.get("error"):
        return {"ok": False, "error": msg["error"], "raw": msg}
    result = msg.get("result", msg) if isinstance(msg, dict) else msg
    return {"ok": True, "tool": name, "result": result}


@mcp.tool(description="Re-list all upstreams + rebuild the BM25 index. Use ONLY when a tool comes back unknown/stale — never before every search/call. Returns count/chars/fingerprint.")
def mcp_refresh() -> dict:
    try:
        stats = refresh_catalog(force=True)
        return stats
    except Exception as e:
        with _LOCK:
            fallback = {"ok": False, "error": str(e), "cached_count": _N, "cached_fingerprint": _FINGERPRINT}
        return fallback


if __name__ == "__main__":
    level = os.environ.get("LOG_LEVEL", "INFO")
    _log(f"listening on 0.0.0.0:{PORT}/mcp -> {len(UPSTREAM_URLS)} upstreams {UPSTREAM_URLS} (TTL {TTL_SEC}s)")
    mcp.run(transport="streamable-http", host="0.0.0.0", port=PORT, path="/mcp", log_level=level)
