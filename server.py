"""mcp-search-proxy — search-first davanti a LiteLLM MCP.

OWUI vede solo 4 tool fissi (~2-3k token) invece di 532 schemi full (~179k):
  mcp_search(query, limit)  -> BM25 su manifest cached (nomi+desc+params, stile Hermes Layer B)
  mcp_describe(names)      -> full inputSchema solo per N tool
  mcp_call(name, arguments) -> tools/call proxata a LiteLLM, risultato grezzo a OWUI
  mcp_refresh()            -> re-list upstream + rebuild indice (reattivo puro, no polling)

Upstream: http://litellm:4000/mcp (DNS Docker, ai-network). Auth Bearer solo proxy->LiteLLM.
Nomi mcp_* (non tool_*) per evitare il 400 xAI su nome riservato tool_search.
BM25: k1=1.5 b=0.75, ammissione rarest-token, exact-name=inf (come Hermes catalog.py).
POC: tokenize lowercase-split (no Snowball/nltk per restare a zero dipendenze extra).
"""

import hashlib
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager

from fastmcp import FastMCP

# ---------------------------------------------------------------- config

# Multi-upstream generico (GitHub-ready): lista N server via env.
# UPSTREAM_URLS comma-separated (nuovo). UPSTREAM_URL singolo (legacy, fallback).
# Es. prod: "http://litellm:4000/mcp,http://mcp-a11y-proxy:8101/mcp"
_raw_urls = os.environ.get("UPSTREAM_URLS", "") or os.environ.get("UPSTREAM_URL", "http://litellm:4000/mcp")
UPSTREAM_URLS: list[str] = [u.strip() for u in _raw_urls.split(",") if u.strip()]
# dedupe preservando ordine
_seen: set[str] = set()
UPSTREAM_URLS = [u for u in UPSTREAM_URLS if not (u in _seen or _seen.add(u))]
UPSTREAM_URL = UPSTREAM_URLS[0]  # alias legacy (log, single-upstream compat)
MASTER_KEY = os.environ.get("LITELLM_MASTER_KEY", "")
# Auth per-upstream (GitHub-ready): UPSTREAM_TOKENS comma-separated, posizionale
# rispetto a UPSTREAM_URLS (voce vuota = no-auth). Se assente: MASTER_KEY solo
# sugli URL che contengono "litellm", niente altrove (a11y no-auth).
_raw_toks = os.environ.get("UPSTREAM_TOKENS", "")
_UPSTREAM_TOKENS: list[str] = [t.strip() for t in _raw_toks.split(",")] if _raw_toks else []
MANIFEST_PATH = os.environ.get("MANIFEST_PATH", "/app/data/manifest.json")
PORT = int(os.environ.get("PORT", "8092"))
TTL_SEC = int(os.environ.get("CATALOG_TTL_SEC", "1800"))  # 0 = off, solo refresh esplicito/unknown-tool
PROTOCOL = "2025-06-18"

SEARCH_DEFAULT_LIMIT = 5
SEARCH_MAX_LIMIT = 25
SEARCH_MAX_QUERIES_PER_CALL = 7  # allineato a Hermes _MAX_QUERIES_PER_CALL
DESCRIBE_MAX_NAMES = 10          # allineato a Hermes _MAX_DESCRIBE_NAMES_PER_CALL
DESC_CLIP = 500
CHARS_PER_TOKEN = 4.0
BM25_K1 = 1.5
BM25_B = 0.75
TIER1_SHORT_DESC_LEN = 60
# Deviazione documentata da Hermes (che è BM25 puro su search-text): le
# descrizioni di questo catalogo sono in parte in italiano (es. gmail-*),
# mentre le query del modello sono in inglese. Senza peso sul nome, un doc
# che ripete 'email/send' nel body batte il tool giusto che li ha nel nome.
# NAME_BONUS aggiunge 1.0 x idf per ogni query-token presente nel nome.
NAME_BONUS_WEIGHT = 1.0

_token_re = re.compile(r"[A-Za-z0-9]+")
_split_re = re.compile(r"[_.:\-]+")

# ---------------------------------------------------------------- state (in-memory, thread-safe)

_LOCK = threading.RLock()
_TOOLS: list[dict] = []          # schemi full upstream (merge tutti gli upstream)
_BY_NAME: dict[str, dict] = {}
_DOC_TOKENS: dict[str, list[str]] = {}
_DF: dict[str, int] = {}
_AVGDL = 0.0
_N = 0
_FINGERPRINT = ""
_LAST_REFRESH = 0.0
_SESSIONS: dict[str, str] = {}       # sticky per-upstream: url -> sid (mai mixare)
_TOOL_UPSTREAM: dict[str, str] = {}  # routing: tool-name -> upstream url


def _log(msg: str) -> None:
    print(f"[mcp-search-proxy] {msg}", flush=True)


def _fingerprint(tools: list[dict]) -> str:
    names = sorted(t.get("name", "") for t in tools)
    h = hashlib.sha256("\n".join(names).encode()).hexdigest()[:16]
    return f"{len(names)}:{h}"


def _estimate_tokens(chars: int) -> int:
    return math.ceil(chars / CHARS_PER_TOKEN)


# ---------------------------------------------------------------- upstream JSON-RPC (Streamable HTTP, SSE)

def _token_for(url: str) -> str:
    """Auth per-upstream (posizionale via UPSTREAM_TOKENS, fallback legacy).
    UPSTREAM_TOKENS comma-separated, stessa posizione di UPSTREAM_URLS
    (voce vuota = no-auth). Se assente: MASTER_KEY solo su URL litellm."""
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
    }
    tok = _token_for(url)
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    if session:
        h["Mcp-Session-Id"] = session
    return h


def _parse_sse(body: str) -> list[dict]:
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
    return out


def _timeout_for(url: str, default: int) -> int:
    """Timeout per-upstream: a11y dietro nginx ha proxy_read_timeout 300s."""
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
            sid = r.headers.get("Mcp-Session-Id") or session
            body = r.read().decode("utf-8", "replace")
            return sid, _parse_sse(body), r.status
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        msgs = _parse_sse(body) if body else []
        raise RuntimeError(f"upstream {url} HTTP {e.code}: {body[:500]}") from e


def _ensure_session(url: str) -> str:
    """Sticky per-upstream: riuso stretto della sid, mai una nuova per call.
    Per upstream stateful (es. a11y: tab/pagine legati alla sid) questo e il
    punto che evita il tab-perso visto con LiteLLM il 23 Lug (nuova sessione
    per ogni call -> pagina navigata non piu disponibile). Reset solo su
    400/404/session-expired, una volta, poi retry."""
    with _LOCK:
        sid = _SESSIONS.get(url)
        if sid:
            return sid
    sid, msgs, _ = _post(
        url,
        {"jsonrpc": "2.0", "id": "init-1", "method": "initialize",
         "params": {"protocolVersion": PROTOCOL, "capabilities": {},
                    "clientInfo": {"name": "mcp-search-proxy", "version": "1.0.0"}}},
        session=None, timeout=20,
    )
    if not sid:
        raise RuntimeError(f"upstream {url} initialize: session id mancante")
    # notifications/initialized (best-effort, 202 atteso)
    try:
        _post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session=sid, timeout=10)
    except Exception as e:
        _log(f"initialized notification {url}: {e} (proseguo)")
    with _LOCK:
        _SESSIONS[url] = sid
        return sid


def _reset_session(url: str) -> None:
    with _LOCK:
        _SESSIONS.pop(url, None)


def _tools_list_one(url: str) -> tuple[str, list[dict]]:
    """tools/list su un singolo upstream. Ritorna (url, tools)."""
    try:
        sid = _ensure_session(url)
    except Exception as e:
        raise RuntimeError(f"initialize fallita verso {url}: {e}") from e
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
            raise RuntimeError(f"tools/list senza result.tools: {json.dumps(msgs)[:800]} err={err}")
        except RuntimeError as e:
            # sessione scaduta/invalidata -> reset una volta e retry
            if attempt == 1 and ("400" in str(e) or "404" in str(e) or "session" in str(e).lower()):
                _log(f"tools/list {url} retry dopo reset sessione ({e})")
                _reset_session(url)
                sid = _ensure_session(url)
                continue
            raise
    raise RuntimeError(f"tools/list {url}: tentativi esauriti")


def _tools_list_live() -> list[tuple[str, list[dict]]]:
    """Fan-out sequenziale su tutti gli upstream. Ritorna [(url, tools)].
    Un upstream down non blocca gli altri: log + lista vuota (fail-soft).
    Se TUTTI falliscono, solleva RuntimeError (fail-closed)."""
    results: list[tuple[str, list[dict]]] = []
    errors: list[str] = []
    for url in UPSTREAM_URLS:
        try:
            results.append(_tools_list_one(url))
        except Exception as e:
            _log(f"tools/list {url} fallita, salto upstream: {e}")
            errors.append(f"{url}: {e}")
    if not results:
        raise RuntimeError(f"tutti gli upstream falliti: {'; '.join(errors)}")
    return results


def _tools_call_live(name: str, arguments: dict, timeout: int = 180) -> dict:
    """Routing per-tool: la call va all'upstream che ha fornito il tool
    (mappa _TOOL_UPSTREAM). Fallback: prova in ordine (fail-soft)."""
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
                raise RuntimeError(f"tools/call senza result/error: {json.dumps(msgs)[:800]}")
            except RuntimeError as e:
                last_err = e
                if attempt == 1 and ("400" in str(e) or "404" in str(e) or "session" in str(e).lower()):
                    _log(f"tools/call {url} retry dopo reset sessione ({e})")
                    _reset_session(url)
                    sid = _ensure_session(url)
                    continue
                break  # errore non-sessione: passa al prossimo upstream solo se fallback
        if preferred:
            break  # routing esplicito: non spillo su altri upstream
    raise last_err or RuntimeError(f"tools/call '{name}': nessun upstream disponibile")


# ---------------------------------------------------------------- catalogo + BM25

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
    parts.append(desc)
    try:
        props = (tool.get("inputSchema") or {}).get("properties") or {}
        if isinstance(props, dict):
            parts.append(" ".join(str(k).replace("_", " ") for k in list(props.keys())[:60]))
    except Exception:
        pass
    return "\n".join(parts)


def _tokenize(text: str) -> list[str]:
    return [_stem(t) for t in _token_re.findall(text.lower())]


def _stem(tok: str) -> str:
    """Stemmer manuale minimale EN, zero dipendenze (coerente col POC).
    Strip leggero s/es/ed/ing con guardia len<4 (evita 'is'->'i').
    Applicato sia ai doc (via _tokenize in _build_index) sia alle query
    (via _tokenize in _search_one), altrimenti 'threads' vs 'thread'
    restano non comparabili. Over-stemming noto: 'created'->'creat'
    vs 'create'->'create' (divergono); caso raro in query reali, il gate
    OOV-robusto + limit+describe mitigano. Plurali s/es gestiti con
    regole sibilanti per evitare 'messages'->'messag' (solo s finale)."""
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
    """Match nome guardato: uguale o qtok sottostringa di ntok (len>=4).
    Copre 'mail' in 'gmail'/'mailchimp' senza split del tokenizer.
    Solo direzione query->nome (non viceversa) + guardia len per evitare
    falsi positivi su token corti generici."""
    if qtok == ntok:
        return True
    return len(qtok) >= 4 and qtok in ntok


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
            # routing solo per nomi sopravvissuti al dedupe (primo upstream vince)
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
    # exact-name match = inf (come Hermes)
    qnorm = query.strip().lower()
    with _LOCK:
        for n in names:
            if n.lower() == qnorm:
                return [n]
    # rarest-token gate (come Hermes) con robustezza OOV: la scelta del gate
    # avviene solo tra token presenti nell'indice (DF>0). Token OOV (refusi,
    # plurali non stemmingati, parole italiane su descrizioni inglesi e
    # viceversa) contribuiscono 0 al BM25 ma non devono azzerare i risultati.
    # Senza Snowball (POC zero-dipendenze) 'threads' non matcha 'thread':
    # limitazione nota, mitigata da limit+describe. Se nessun token è in
    # vocabolario, nessun doc può matchare -> [] con hint available_sources.
    in_vocab = [t for t in set(qtoks) if _DF.get(t, 0) > 0]
    if not in_vocab:
        return []
    scored_q = sorted(in_vocab, key=lambda t: _idf(t), reverse=True)
    gate = scored_q[0]
    qset = set(qtoks)
    ranked: list[tuple[float, str]] = []
    with _LOCK:
        snapshot = [(n, _DOC_TOKENS.get(n, [])) for n in names]
    for n, doc in snapshot:
        # NAME_BONUS bypass sul gate: il gate è scelto sull'intera query e
        # le description sono in parte in IT mentre le query sono in EN.
        # Se il token a IDF più alta (es. 'inbox') manca nel doc, il tool
        # verrebbe scartato prima che il bonus-nome possa salvarlo, anche
        # se il nome matcha esplicitamente (es. 'gmail' in gmail-*).
        # name_hit ammette il doc a prescindere dal gate; il ranking
        # BM25+bonus decide poi l'ordine. name_hit usa _name_match guardato
        # (substring query->nome, len>=4): 'mail' in 'gmail' ammette i tool
        # gmail anche se il token 'mail' non è nel doc (il tokenizer non
        # spezza 'gmail' in 'g'+'mail'). Stessa regola per il bonus sotto.
        name_toks = set(_tokenize(_split_re.sub(" ", n)))
        name_hit = any(_name_match(t, nt) for t in qset for nt in name_toks)
        if gate and gate not in doc and not name_hit:
            continue
        s = _bm25_score(qtoks, doc)
        # bonus nome: ogni query-token nel nome aggiunge idf x peso.
        # _name_match guardato: 'mail' in 'gmail' conta (substring nome,
        # len>=4), ma 'mail' NON matcha 'mailchimp' diversamente da 'gmail'
        # solo se pesato uguale — il ranking decide: idf(mail) alto e
        # condiviso, bonus uguale per entrambi, vince BM25 sul body.
        bonus = 0.0
        for t in qset:
            for nt in name_toks:
                if _name_match(t, nt):
                    bonus += _idf(t)
                    break
        bonus *= NAME_BONUS_WEIGHT
        if s > 0 or bonus > 0:
            ranked.append((s + bonus, n))
    ranked.sort(key=lambda x: (-x[0], x[1]))
    return [n for _, n in ranked[:limit]]


def _clip(s: str, n: int = DESC_CLIP) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"


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
                "tools": _TOOLS,
            }
        tmp = MANIFEST_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, MANIFEST_PATH)
    except Exception as e:
        _log(f"save manifest fallita: {e}")


def _load_manifest() -> bool:
    try:
        with open(MANIFEST_PATH) as f:
            payload = json.load(f)
        tools = payload.get("tools") or []
        if not tools:
            return False
        _build_index(tools, payload.get("routing") or None)
        with _LOCK:
            # manifest legacy senza routing (single-upstream 1.0.0): tutto al primo upstream
            if not _TOOL_UPSTREAM and _TOOLS:
                _TOOL_UPSTREAM.update({t.get("name", ""): UPSTREAM_URLS[0] for t in _TOOLS if t.get("name")})
        global _LAST_REFRESH
        with _LOCK:
            _LAST_REFRESH = float(payload.get("refreshed_at") or 0.0)
        _log(f"manifest da disco: {_N} tool fp={_FINGERPRINT}")
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        _log(f"load manifest fallita: {e}")
        return False


def refresh_catalog(force: bool = False) -> dict:
    """Fetch-once + rebuild. Ritorna stats. Solleva RuntimeError su fallimento.
    Alla fine aggiorna mcp.instructions col manifest tier-1 (1 riga per
    source, stile Hermes Tier 1: nome + short-desc). Verificato su
    fastmcp 3.4.2: FastMCP.instructions ha setter (server.py:445-450,
    delega a _mcp_server.instructions), quindi mutabile a runtime.
    Nota: i client leggono instructions all'handshake initialize — un
    mcp_refresh() a runtime aggiorna il server ma i client già connessi
    vedono il testo vecchio fino al prossimo handshake."""
    global _LAST_REFRESH
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
                    _log(f"collisione nome '{n}': tengo {routing[n]}, scarto {url} (fail-closed)")
                continue
            routing[n] = url
            merged.append(t)
    if not merged:
        raise RuntimeError("tutti gli upstream hanno ritornato 0 tool")
    _build_index(merged, routing)
    with _LOCK:
        _LAST_REFRESH = time.time()
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


def _short_desc(text: str, n: int = TIER1_SHORT_DESC_LEN) -> str:
    """Prima frase, ≤60ch (come Hermes _short_desc in tool_search_catalog.py:185)."""
    t = (text or "").split(".")[0].strip().replace("\n", " ")
    return t if len(t) <= n else t[:n]


def _tier1_listing() -> str:
    """Manifest tier-1: 1 riga per source 'nome: short-desc', sorted byte-stable
    (prompt-cache safe, come Hermes). 21 righe / ~1.3kch / ~340tok misurati
    sul catalogo reale fp 532:6eee51ac20cd1972."""
    with _LOCK:
        items = list(_BY_NAME.items())
    from collections import Counter
    counts = Counter(_source_of(n) for n, _ in items)
    first: dict[str, str] = {}
    for n, t in sorted(items):
        s = _source_of(n)
        if s not in first:
            first[s] = _short_desc(t.get("description", ""))
    lines = [f"- {s}: {first[s]} ({counts[s]} tool)" for s in sorted(first)]
    return "\n".join(lines)


def _update_instructions() -> None:
    try:
        listing = _tier1_listing()
        with _LOCK:
            n = _N
        mcp.instructions = (
            f"Proxy search-first davanti a {len(UPSTREAM_URLS)} upstream MCP ({n} tool). "
            "Flusso: mcp_search per trovare i nomi, mcp_describe per gli schemi full, "
            "mcp_call per eseguire. mcp_refresh solo se un tool risulta unknown o datato.\n"
            f"Catalogo per server:\n{listing}"
        )
    except Exception as e:
        _log(f"update instructions fallito: {e}")


def _ensure_catalog() -> None:
    with _LOCK:
        empty = _N == 0
    if empty:
        if not _load_manifest():
            refresh_catalog(force=True)
        else:
            _update_instructions()
            # manifest su disco ma verifichiamo upstream se TTL scaduto
            if _stale():
                try:
                    refresh_catalog(force=True)
                except Exception as e:
                    _log(f"refresh TTL fallito, uso manifest disco: {e}")
    elif _stale():
        try:
            refresh_catalog(force=True)
        except Exception as e:
            _log(f"refresh TTL fallito, uso catalogo memoria: {e}")


# ---------------------------------------------------------------- FastMCP

@asynccontextmanager
async def _lifespan(app):
    try:
        if not _load_manifest():
            _log("nessun manifest, fetch iniziale upstream…")
            refresh_catalog(force=True)
        else:
            _log(f"avvio con manifest disco ({_N} tool), verifica upstream best-effort…")
            try:
                refresh_catalog(force=True)
            except Exception as e:
                _log(f"fetch iniziale fallita, resto su manifest disco: {e}")
                _update_instructions()
    except Exception as e:
        _log(f"lifespan: catalogo non disponibile all'avvio: {e}")
    yield


mcp = FastMCP(
    name="mcp-search-proxy",
    instructions=(
        "Proxy search-first davanti a LiteLLM MCP (532 tool). "
        "Flusso: mcp_search per trovare i nomi, mcp_describe per gli schemi full, "
        "mcp_call per eseguire. mcp_refresh solo se un tool risulta unknown o datato."
    ),
    lifespan=_lifespan,
)


@mcp.tool(description="Cerca tool MCP per argomento (BM25 su nomi+descrizioni+parametri). Ritorna nomi + mini-schede, mai schemi full. Se i top risultati condividono lo stesso prefisso (es. tutti thread_*) e la query ha piu intenti, riprova con limit piu alto (es. 10).")
def mcp_search(query: str, limit: int = SEARCH_DEFAULT_LIMIT) -> dict:
    _ensure_catalog()
    if isinstance(query, list):  # tolleranza: lista -> prima voce
        query = query[0] if query else ""
    query = (query or "").strip()
    if not query:
        return {"ok": False, "error": "query vuota", "hint": "es: 'gmail send', 'github issue', 'calendar event'"}
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
            out["hint"] = ("nessun match lessicale: prova sinonimi o termini inglesi "
                           "(es. 'email' invece di 'posta'), oppure mcp_refresh se il catalogo è datato")
    return out


@mcp.tool(description="Schemi input full solo per i nomi indicati (max 10/call). Se vedi il nome esatto nel listing, salta mcp_search e vieni qui.")
def mcp_describe(names: list[str]) -> dict:
    _ensure_catalog()
    if isinstance(names, str):
        names = [names]
    names = [str(x) for x in (names or []) if str(x).strip()]
    if not names:
        return {"ok": False, "error": "names vuota"}
    if len(names) > DESCRIBE_MAX_NAMES:
        return {"ok": False, "error": f"max {DESCRIBE_MAX_NAMES} nomi per call", "received": len(names)}
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
        out["hint"] = "usa mcp_search per i nomi corretti, o mcp_refresh se il catalogo è datato"
    return out


@mcp.tool(description="Esegue un tool MCP sull'upstream che lo fornisce e ritorna il risultato grezzo. Valida i required in locale prima dell'invio.")
def mcp_call(name: str, arguments: dict | str | None = None) -> dict:
    _ensure_catalog()
    name = (name or "").strip()
    if not name:
        return {"ok": False, "error": "name vuoto"}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments) if arguments.strip() else {}
        except json.JSONDecodeError as e:
            return {"ok": False, "error": f"arguments non è JSON valido: {e}"}
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        return {"ok": False, "error": "arguments deve essere un oggetto JSON"}

    with _LOCK:
        tool = _BY_NAME.get(name)
    if tool is None:
        # reattivo puro: una re-list una tantum su unknown-tool, poi retry
        try:
            before = _FINGERPRINT
            refresh_catalog(force=True)
            with _LOCK:
                tool = _BY_NAME.get(name)
            _log(f"unknown-tool '{name}': re-list fp {before}->{_FINGERPRINT}")
        except Exception as e:
            return {"ok": False, "error": f"tool '{name}' sconosciuto e refresh fallito: {e}",
                    "hint": "usa mcp_search per trovare il nome corretto"}
        if tool is None:
            return {"ok": False, "error": f"tool '{name}' sconosciuto anche dopo refresh",
                    "hint": "usa mcp_search per trovare il nome corretto"}

    # validazione required locale (fail-open su $ref esterni, come Hermes validation.py)
    try:
        schema = tool.get("inputSchema") or {}
        required = schema.get("required") or []
        missing = [r for r in required if r not in arguments]
        if missing:
            return {"ok": False, "error": f"parametri required mancanti: {missing}",
                    "required": [str(x) for x in required],
                    "hint": "usa mcp_describe per lo schema full"}
    except Exception:
        pass  # fail-open

    try:
        msg = _tools_call_live(name, arguments)
    except Exception as e:
        return {"ok": False, "error": f"upstream tools/call fallita: {e}"}
    if isinstance(msg, dict) and msg.get("error"):
        return {"ok": False, "error": msg["error"], "raw": msg}
    result = msg.get("result", msg) if isinstance(msg, dict) else msg
    return {"ok": True, "tool": name, "result": result}


@mcp.tool(description="Re-list di tutti gli upstream + rebuild indice BM25. Usala se un tool risulta unknown/datato. Ritorna count/chars/fingerprint.")
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
    _log(f"avvio su 0.0.0.0:{PORT}/mcp -> {len(UPSTREAM_URLS)} upstream {UPSTREAM_URLS} (TTL {TTL_SEC}s)")
    mcp.run(transport="streamable-http", host="0.0.0.0", port=PORT, path="/mcp", log_level=level)
