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
            try:
                return _orig(req, *args, **kwargs)          # Anthropic first
            except Exception as e:
                try:
                    body = json.loads(data.decode("utf-8", "ignore"))
                    from llm_router import complete
                    txt = complete(
                        body.get("messages") or "",
                        tier="reason",                       # GLM-5.1 primary
                        max_tokens=int(body.get("max_tokens") or 1024),
                        system=body.get("system"),
                    )
                    if txt and txt.strip():
                        payload = json.dumps({
                            "id": "msg_shim_fallback",
                            "type": "message",
                            "role": "assistant",
                            "model": body.get("model", "fallback"),
                            "content": [{"type": "text", "text": txt}],
                            "stop_reason": "end_turn",
                            "_via": "anthropic_shim->llm_router",
                        }).encode("utf-8")
                        return _FakeResp(payload)
                except Exception:
                    pass
                raise e                                       # both down → original error
        return _orig(req, *args, **kwargs)

    urllib.request.urlopen = _patched
    setattr(urllib.request, _PATCHED_FLAG, True)


_install()
