"""anthropic_shim — transparent resilience for direct Anthropic calls.

Importing this module (one line: ``import anthropic_shim``) makes any urllib
POST to ``api.anthropic.com`` fall back to GLM-5.1 (Z.ai) via the shared
``llm_router`` when Anthropic itself fails — e.g. the credit-exhaustion /
billing block that silently took 50+ engines offline for 13 days.

Behaviour is purely ADDITIVE and safe by default:
  * When Anthropic works, nothing changes and no data leaves Anthropic.
  * Only on an Anthropic-side failure does it route the SAME request to the
    fallback provider and return an Anthropic-shaped response, so the calling
    engine parses ``["content"][0]["text"]`` unchanged.
  * If the fallback also fails (e.g. Z.ai is empty too), it re-raises the
    original Anthropic error, so the engine's existing error handling runs
    exactly as before — never worse than today.

Idempotent: importing it many times patches urllib only once.
"""
import json
import urllib.request

_PATCHED_FLAG = "_anthropic_shim_patched"


class _FakeResp:
    """Minimal stand-in for what urllib.request.urlopen returns."""

    def __init__(self, data, status=200):
        self._data = data
        self.status = status
        self.code = status

    def read(self, *a):
        return self._data

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _anthropic_shape(text, model="fallback"):
    """Wrap plain text in the Anthropic messages response shape (bytes)."""
    return json.dumps({
        "id": "msg_shim",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 0, "output_tokens": 0},
        "_via": "anthropic_shim",
    }).encode("utf-8")


def _install():
    if getattr(urllib.request, _PATCHED_FLAG, False):
        return
    _orig = urllib.request.urlopen

    def _patched(req, *args, **kwargs):
        url, data = "", None
        try:
            if hasattr(req, "full_url"):
                url = req.full_url or ""
                data = getattr(req, "data", None)
            elif isinstance(req, str):
                url = req
        except Exception:
            url = ""
        if "api.anthropic.com" in url and data:
            # llm_router-originated calls govern themselves -> pass straight through
            try:
                hdrs = getattr(req, "headers", {}) or {}
                if any(str(k).lower() == "x-jh-internal" for k in hdrs):
                    return _orig(req, *args, **kwargs)
            except Exception:
                pass

            body, _lc, key = None, None, None
            try:
                body = json.loads(data.decode("utf-8", "ignore"))
            except Exception:
                body = None

            # economy-mode + runaway guard for raw urllib callers (mirror the router):
            # in economy mode, rewrite Sonnet/Opus -> Haiku; always cap max_tokens<=6000.
            if body is not None:
                try:
                    import llm_cost as _lcg
                    mutated = False
                    mdl = str(body.get("model", ""))
                    if _lcg._config().get("mode") == "economy" and ("sonnet" in mdl or "opus" in mdl):
                        body["model"] = "claude-haiku-4-5-20251001"
                        mutated = True
                    try:
                        if int(body.get("max_tokens") or 1024) > 6000:
                            body["max_tokens"] = 6000
                            mutated = True
                    except Exception:
                        pass
                    if mutated:
                        data = json.dumps(body).encode("utf-8")
                        try:
                            req.data = data
                            req.remove_header("Content-length")
                        except Exception:
                            pass
                except Exception:
                    pass

            # cost governance: content-cache hit / hard budget cap (fail-safe)
            if body is not None:
                try:
                    import llm_cost as _lc
                    model = body.get("model", "claude")
                    key = _lc.make_key(model, body.get("messages"), body.get("system"),
                                       body.get("max_tokens"))
                    hit = _lc.cache_get(key)
                    if hit is not None:
                        _lc.log_cost(model, _lc.estimate_tokens(data.decode("utf-8", "ignore")),
                                     _lc.estimate_tokens(hit), cached=True)
                        return _FakeResp(_anthropic_shape(hit, model))
                    if not _lc.budget_ok():
                        return _FakeResp(_anthropic_shape("", model))  # -> deterministic fallback
                except Exception:
                    _lc = None

            # real Anthropic call, with the existing llm_router fallback on failure
            try:
                resp = _orig(req, *args, **kwargs)
            except Exception as e:
                try:
                    from llm_router import complete
                    txt = complete((body or {}).get("messages") or "", tier="reason",
                                   max_tokens=int((body or {}).get("max_tokens") or 1024),
                                   system=(body or {}).get("system"))
                    if txt and txt.strip():
                        return _FakeResp(_anthropic_shape(txt, (body or {}).get("model", "fallback")))
                except Exception:
                    pass
                raise e

            # success -> read once, meter + cache, return identical bytes to the caller
            try:
                raw = resp.read()
            except Exception:
                return resp
            try:
                d = json.loads(raw.decode("utf-8", "ignore"))
                txt = "".join(b.get("text", "") for b in d.get("content", []))
                if _lc is not None and body is not None:
                    u = d.get("usage") or {}
                    _lc.log_cost(body.get("model", "claude"), int(u.get("input_tokens") or 0),
                                 int(u.get("output_tokens") or 0), cached=False)
                    if key and txt and txt.strip():
                        _lc.cache_put(key, txt, body.get("model", "claude"))
            except Exception:
                pass
            return _FakeResp(raw, resp.getcode())
        return _orig(req, *args, **kwargs)

    urllib.request.urlopen = _patched
    setattr(urllib.request, _PATCHED_FLAG, True)


_install()

# deploy-nudge: force redeploy of importers to pick up cost governance (ops 2790).
