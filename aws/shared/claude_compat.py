"""Drop-in resilience shim for engines that call the Anthropic /v1/messages
endpoint directly.

The 2026-06 outage showed that 50+ engines were hard-wired to Anthropic with no
fallback, so they all went silent for ~13 days when credits ran out. This shim
lets any of them become provider-resilient with a one-line change:

    # before:
    req  = urllib.request.Request("https://api.anthropic.com/v1/messages", ...)
    data = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
    # after:
    import claude_compat
    data = claude_compat.messages(body)        # body = the same request dict

`messages()` routes through the shared llm_router (GLM-5.1/Z.ai primary with a
Claude fallback for the reason tier), and returns an **Anthropic-shaped**
response dict, so existing `data["content"][0]["text"]` parsing is unchanged.
If the router is unavailable it falls back to a direct Anthropic call, so the
shim is never worse than the original code.
"""
import json
import os
import urllib.request


def _flatten(body):
    """Extract (system, prompt) text from an Anthropic request body."""
    system = body.get("system")
    parts = []
    for m in body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(b.get("text", "") for b in c if isinstance(b, dict))
        if c:
            parts.append(str(c))
    return system, "\n\n".join(parts)


def text(body, tier="reason"):
    """Return just the model's text (or "" on total failure)."""
    return (messages(body, tier=tier).get("content") or [{}])[0].get("text", "")


def messages(body, tier="reason"):
    """Anthropic-shaped {"content":[{"type":"text","text":...}]} via llm_router,
    with a direct-Anthropic last resort. `tier` defaults to 'reason' (GLM primary,
    Claude fallback); pass tier='critical' to force Claude for proprietary data."""
    system, prompt = _flatten(body)
    max_tokens = body.get("max_tokens", 1024)
    # 1) shared router (GLM/Z.ai primary or Claude — survives a single-provider outage)
    try:
        from llm_router import complete
        out = complete(prompt, tier=tier, max_tokens=max_tokens, system=system)
        if out and out.strip():
            return {"content": [{"type": "text", "text": out}],
                    "stop_reason": "end_turn", "_via": "llm_router"}
    except Exception as e:
        print(f"[claude_compat] router unavailable: {str(e)[:90]}")
    # 2) last resort: direct Anthropic (preserves original behaviour)
    try:
        key = (os.environ.get("ANTHROPIC_KEY")
               or os.environ.get("ANTHROPIC_API_KEY", ""))
        b = dict(body)
        b.setdefault("model", "claude-haiku-4-5-20251001")
        b.setdefault("max_tokens", max_tokens)
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(b).encode(),
            headers={"Content-Type": "application/json", "x-api-key": key,
                     "anthropic-version": "2023-06-01"})
        return json.loads(urllib.request.urlopen(req, timeout=40).read().decode())
    except Exception as e:
        print(f"[claude_compat] direct anthropic err: {str(e)[:90]}")
        return {"content": [{"type": "text", "text": ""}], "_error": str(e)[:120]}
