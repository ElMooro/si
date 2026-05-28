"""jhcore.claude — Anthropic API client for JustHodl Lambdas.

Default model: claude-haiku-4-5-20251001 (the platform standard).
"""
import json
import os
import urllib.request

DEFAULT_MODEL = "claude-haiku-4-5-20251001"
API_URL = "https://api.anthropic.com/v1/messages"
API_VERSION = "2023-06-01"


def _key():
    return os.environ.get("ANTHROPIC_API_KEY", "")


def complete(prompt, system=None, model=None, max_tokens=1024, temperature=0.3, api_key=None, timeout=30):
    """Single-turn completion. Returns the text content string, or "" on error.

    prompt: user message (str)
    system: optional system prompt
    """
    key = api_key or _key()
    if not key:
        return ""
    body = {
        "model": model or DEFAULT_MODEL,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system
    headers = {
        "x-api-key": key,
        "anthropic-version": API_VERSION,
        "content-type": "application/json",
    }
    req = urllib.request.Request(API_URL, data=json.dumps(body).encode("utf-8"), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
        # Combine all text blocks
        parts = []
        for blk in data.get("content", []):
            if blk.get("type") == "text":
                parts.append(blk.get("text", ""))
        return "\n".join(parts)
    except Exception as e:
        print(f"[jhcore.claude] err: {e}")
        return ""


def complete_json(prompt, system=None, model=None, max_tokens=2048, temperature=0.2, api_key=None):
    """Like complete but parses JSON from the response (strips ```json fences). Returns dict or None."""
    txt = complete(prompt + "\n\nRespond ONLY with valid JSON, no preamble or markdown.",
                   system=system, model=model, max_tokens=max_tokens, temperature=temperature, api_key=api_key)
    if not txt:
        return None
    txt = txt.strip()
    if txt.startswith("```"):
        # strip fences
        lines = [l for l in txt.split("\n") if not l.strip().startswith("```")]
        txt = "\n".join(lines).strip()
    try:
        return json.loads(txt)
    except Exception as e:
        print(f"[jhcore.claude] json parse err: {e} | head: {txt[:200]}")
        return None
