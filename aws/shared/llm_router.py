"""
aws/shared/llm_router.py — centralized, tiered LLM routing for the engine fleet.

Strategy (from the 2026-06 audit):
  - 52 engines already run on Claude Haiku 4.5 — cheapest tier. GLM is NOT
    worth it there (reasoning-token overhead erases the gap). Keep them.
  - 9 engines run on Sonnet 4.6 — the real GLM-5.1 target (~1/3 the cost,
    competitive reasoning). Route via tier="reason" with Claude fallback.
  - Proprietary data (Khalid Index internals, portfolio, private notes)
    NEVER leaves Claude — enforced by the data-classification guard.

Tiers:
  "bulk"     -> Claude Haiku 4.5   (default; summarize/classify/extract)
  "reason"   -> GLM-5.1 on Z.ai    (fallback: Claude Sonnet on any error)
  "critical" -> Claude Sonnet 4.6  (high-stakes; never offshored)

contains_proprietary=True forces Claude regardless of tier.

Keys: Anthropic from env (ANTHROPIC_API_KEY or ANTHROPIC_KEY).
      Z.ai from SSM /justhodl/zai-api-key (cached per warm container).
"""
import json
import os
import urllib.request
import urllib.error

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
GLM_REASON = "glm-5.1"

ZAI_BASE_URL = os.environ.get("ZAI_BASE_URL", "https://api.z.ai/api/paas/v4")
ZAI_SSM_NAME = "/justhodl/zai-api-key"
_ANTHROPIC_KEY = (os.environ.get("ANTHROPIC_API_KEY")
                  or os.environ.get("ANTHROPIC_KEY", "")).strip()
_anthropic_ssm_cache = None


def _anthropic_key():
    """Env key (stripped) with SSM fallback — some functions carry stale or
    whitespace-polluted env copies; /justhodl/anthropic/api-key is truth."""
    global _anthropic_ssm_cache
    if _ANTHROPIC_KEY:
        return _ANTHROPIC_KEY
    if _anthropic_ssm_cache is None:
        import boto3
        ssm = boto3.client("ssm", region_name="us-east-1")
        _anthropic_ssm_cache = ssm.get_parameter(
            Name="/justhodl/anthropic/api-key",
            WithDecryption=True)["Parameter"]["Value"].strip()
    return _anthropic_ssm_cache

_zai_key_cache = None


_DOWN = {}            # provider -> unix ts until which we skip it (circuit breaker)
_COOLDOWN = 600       # 10 min: a dead provider is skipped instantly on warm containers


def _tripped(kind):
    import time as _t
    return _DOWN.get(kind, 0) > _t.time()


def _trip(kind):
    import time as _t
    _DOWN[kind] = _t.time() + _COOLDOWN


def _zai_key():
    global _zai_key_cache
    if _zai_key_cache is None:
        import boto3
        ssm = boto3.client("ssm", region_name="us-east-1")
        _zai_key_cache = ssm.get_parameter(Name=ZAI_SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
    return _zai_key_cache


def _msgs(prompt):
    return [{"role": "user", "content": prompt}] if isinstance(prompt, str) else prompt


def _claude(prompt, model, max_tokens, system=None):
    payload = {"model": model, "max_tokens": max_tokens, "messages": _msgs(prompt)}
    if system:
        payload["system"] = system
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "x-api-key": _anthropic_key(),
                 "anthropic-version": "2023-06-01", "x-jh-internal": "router"},
    )
    with urllib.request.urlopen(req, timeout=35) as r:
        d = json.loads(r.read().decode())
    txt = "".join(b.get("text", "") for b in d.get("content", []))
    u = d.get("usage") or {}
    return txt, int(u.get("input_tokens") or 0), int(u.get("output_tokens") or 0)


def _glm(prompt, model, max_tokens, system=None):
    msgs = _msgs(prompt)
    if system:
        msgs = [{"role": "system", "content": system}] + msgs
    # GLM-5.1 is a reasoning model: give it room so reasoning tokens don't
    # starve the visible answer (the empty-content trap seen in testing).
    payload = {"model": model, "max_tokens": max(max_tokens, 1500), "messages": msgs}
    req = urllib.request.Request(
        f"{ZAI_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {_zai_key()}"},
    )
    # GLM-5.1 reasoning on real prompts routinely exceeds 40s (ops 3153
    # verbatim: read timeout -> breaker -> credit-dead Haiku -> empty).
    with urllib.request.urlopen(req, timeout=130) as r:
        d = json.loads(r.read().decode())
    msg = d["choices"][0]["message"]
    txt = msg.get("content") or msg.get("reasoning_content") or ""
    u = d.get("usage") or {}
    return txt, int(u.get("prompt_tokens") or 0), int(u.get("completion_tokens") or 0)


def complete(prompt, tier="bulk", max_tokens=1024, contains_proprietary=False, system=None,
             cache_ttl=None, no_cache=False, on_demand=False):
    """Single entry point. Returns the model's text.

    Cost-governed via aws/shared/llm_cost (content cache + daily budget cap +
    per-engine metering). All governance is fail-safe: any error degrades to a
    plain model call. `cache_ttl`/`no_cache` are optional; existing callers are
    unaffected. Preserves the GLM->Sonnet reason-tier fallback.
    """
    try:
        import llm_cost
    except Exception:
        llm_cost = None
    if llm_cost is not None:
        tier = llm_cost.economy_downgrade(tier)

    # resolve model + provider
    if contains_proprietary or tier == "critical":
        model, kind = SONNET, "claude"
    elif tier == "reason":
        model, kind = GLM_REASON, "glm"
    else:
        model, kind = HAIKU, "claude"

    # cost guard: clamp output for non-critical tiers so no single call can run
    # away to 16k tokens (the equity-research/ticker-deep-research class of drain).
    if tier != "critical" and not contains_proprietary:
        try:
            max_tokens = min(int(max_tokens or 1024), 6000)
        except Exception:
            max_tokens = 6000

    msgs = _msgs(prompt)
    key = None
    if llm_cost is not None and not no_cache:
        try:
            key = llm_cost.make_key(model, msgs, system, max_tokens)
            hit = llm_cost.cache_get(key, cache_ttl)
            if hit is not None:
                try:
                    import json as _j
                    llm_cost.log_cost(model, llm_cost.estimate_tokens(_j.dumps(msgs, default=str)),
                                      llm_cost.estimate_tokens(hit), cached=True)
                except Exception:
                    pass
                return hit
            if not llm_cost.budget_ok():
                print("[llm_router] daily LLM budget cap hit (or mode=off) -> empty; engine uses deterministic fallback")
                return ""
            if not llm_cost.within_daily_cap():
                print("[llm_router] engine daily call cap reached -> empty; engine uses deterministic fallback")
                return ""
        except Exception:
            key = None

    # on-demand mode: scheduled/background calls stop here (cache above stays free);
    # only user-initiated calls (ask / ai-chat / page-AI button) pass on_demand=True.
    if llm_cost is not None and not on_demand:
        try:
            if llm_cost.mode() == "on_demand":
                print("[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback")
                return ""
        except Exception:
            pass

    # real provider call (usage-instrumented, breaker-aware, never raises:
    # total provider failure returns "" so engines take their deterministic path)
    try:
        if kind == "glm":
            if _tripped("glm"):
                raise TimeoutError("glm circuit open")
            txt, it, ot = _glm(prompt, GLM_REASON, max_tokens, system)
        else:
            if _tripped("claude"):
                raise TimeoutError("claude circuit open")
            txt, it, ot = _claude(prompt, model, max_tokens, system)
    except Exception as e:
        if kind == "glm":
            _trip("glm")
            print(f"[llm_router] GLM failed ({e!r}); falling back to Haiku (cost-safe — NOT Sonnet)")
            try:
                if _tripped("claude"):
                    raise TimeoutError("claude circuit open")
                txt, it, ot = _claude(prompt, HAIKU, max_tokens, system)
                model = HAIKU
            except Exception as e2:
                _trip("claude")
                print(f"[llm_router] ALL providers down ({e2!r}) -> empty; engine uses deterministic fallback")
                return ""
        else:
            _trip("claude")
            print(f"[llm_router] {kind} failed ({e!r}) -> empty; engine uses deterministic fallback")
            return ""

    if llm_cost is not None:
        try:
            llm_cost.log_cost(model, it, ot, cached=False)
            # ops 4434 C2: per-engine attribution (EMF, alarmable)
            try:
                import os as _os
                _usd = llm_cost.est_usd(model, it, ot) if hasattr(llm_cost, 'est_usd') else 0.0
                llm_cost.attribute(_os.environ.get('AWS_LAMBDA_FUNCTION_NAME'), model, _usd, it, ot)
            except Exception:
                pass
            if key and txt and txt.strip():
                llm_cost.cache_put(key, txt, model)
        except Exception:
            pass
    return txt

# deploy-nudge: 2026-07-05 circuit-breaker propagation to ALL transitive importers (ops 2897).


# ═══════════ ops 4374: AI COUNCIL — cross-provider consultation rail ═══════════
# The March-2026 workflow (Khalid manually bridging Claude<->Perplexity audits)
# becomes machine-bridged: any engine or ops script can convene a council of
# independent AIs and get per-provider answers + an auditable record.
PPLX_SSM_NAME = "/justhodl/perplexity/api-key"
PPLX_MODEL = os.environ.get("PPLX_MODEL", "sonar-pro")
_pplx_key_cache = None


def _pplx_key():
    global _pplx_key_cache
    if _pplx_key_cache is None:
        import boto3
        ssm = boto3.client("ssm", region_name="us-east-1")
        _pplx_key_cache = ssm.get_parameter(
            Name=PPLX_SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
    return _pplx_key_cache


def _perplexity(prompt, model=None, max_tokens=1400, system=None):
    """Perplexity chat/completions (OpenAI-compatible). Returns
    (text, citations)."""
    msgs = _msgs(prompt)
    if system:
        msgs = [{"role": "system", "content": system}] + msgs
    payload = {"model": model or PPLX_MODEL, "max_tokens": max_tokens,
               "messages": msgs}
    req = urllib.request.Request(
        "https://api.perplexity.ai/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + _pplx_key()},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read().decode())
    txt = (((d.get("choices") or [{}])[0].get("message") or {})
           .get("content") or "")
    cites = d.get("citations") or []
    return txt, cites


def council(question, providers=None, system=None, max_tokens=1400,
            context=""):
    """Fan a question out to independent AIs; return per-provider answers.

    providers: subset of ("perplexity", "glm", "claude"). Missing keys or
    provider outages degrade to per-provider errors, never a crash — the
    council convenes with whoever shows up.
    """
    import time as _t
    providers = list(providers or ("perplexity", "glm", "claude"))
    q = (context + "\n\n" + question).strip() if context else question
    out = {}
    for p in providers:
        t0 = _t.time()
        rec = {"ok": False, "provider": p}
        try:
            if _tripped(p):
                raise RuntimeError("circuit-open (recent failure)")
            if p == "perplexity":
                txt, cites = _perplexity(q, max_tokens=max_tokens,
                                         system=system)
                rec.update(ok=bool(txt), answer=txt, citations=cites[:12],
                           model=PPLX_MODEL)
            elif p == "glm":
                txt, ti, to = _glm(q, GLM_REASON, max_tokens, system=system)
                rec.update(ok=bool(txt), answer=txt, model=GLM_REASON)
            elif p == "claude":
                txt, ti, to = _claude(q, SONNET, max_tokens, system=system)
                rec.update(ok=bool(txt), answer=txt, model=SONNET)
            else:
                raise ValueError("unknown provider")
            if not rec["ok"]:
                rec["error"] = "empty answer"
        except Exception as e:
            body = ""
            try:
                body = e.read().decode()[:200]        # HTTPError detail
            except Exception:
                pass
            retried = False
            if "429" in str(e):
                _t.sleep(4)
                try:
                    if p == "glm":
                        txt, ti, to = _glm(q, GLM_REASON, max_tokens,
                                           system=system)
                        rec.update(ok=bool(txt), answer=txt,
                                   model=GLM_REASON)
                        retried = rec["ok"]
                except Exception as e2:
                    e = e2
            if p == "claude" and "400" in str(e) and not retried:
                try:
                    global _ANTHROPIC_KEY, _anthropic_ssm_cache
                    _ANTHROPIC_KEY = ""
                    _anthropic_ssm_cache = None       # force SSM truth
                    txt, ti, to = _claude(q, SONNET, max_tokens,
                                          system=system)
                    rec.update(ok=bool(txt), answer=txt, model=SONNET)
                    retried = rec["ok"]
                except Exception as e3:
                    e = e3
                    try:
                        body = e3.read().decode()[:200]
                    except Exception:
                        pass
            if not retried:
                _trip(p)
                rec["error"] = (f"{type(e).__name__}: {str(e)[:120]} "
                                f"| body: {body}" if body
                                else f"{type(e).__name__}: {str(e)[:160]}")
                rec["error_class"] = classify_provider_error(
                    rec["error"] + " " + body)
        rec["latency_s"] = round(_t.time() - t0, 1)
        out[p] = rec
    return out
# ═══════════ end AI COUNCIL ═══════════


def classify_provider_error(err):
    """Route-able error taxonomy (A2A spec): config_missing | auth_failed |
    quota_exhausted | rate_limited | provider_5xx | timeout | unknown."""
    e = (err or "").lower()
    if "parameternotfound" in e or "config" in e and "missing" in e:
        return "config_missing"
    if "credit balance" in e or "insufficient balance" in e or        "recharge" in e or "quota" in e or "1113" in e:
        return "quota_exhausted"
    if "401" in e or "403" in e or "invalid x-api-key" in e or        "authentication" in e or "unauthorized" in e:
        return "auth_failed"
    if "429" in e or "too many requests" in e or "rate" in e:
        return "rate_limited"
    if "timed out" in e or "timeout" in e:
        return "timeout"
    if "500" in e or "502" in e or "503" in e or "504" in e:
        return "provider_5xx"
    return "unknown"
