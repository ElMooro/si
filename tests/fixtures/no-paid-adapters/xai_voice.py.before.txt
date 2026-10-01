"""Grok voice for justhodl-ai. Key only from env XAI_API_KEY or SSM /justhodl/xai/api-key.
Never logs the key. Empty string if unset — caller uses deterministic desk.
"""
from __future__ import annotations
import json
import os
import urllib.request

MODEL = os.environ.get("XAI_MODEL", "grok-4.3")
BASE = os.environ.get("XAI_BASE_URL", "https://api.x.ai/v1")
SSM = "/justhodl/xai/api-key"
_key = None


def _key_get():
    global _key
    if _key is not None:
        return _key
    env = (os.environ.get("XAI_API_KEY") or os.environ.get("XAI_KEY") or "").strip()
    if env:
        _key = env
        return _key
    try:
        import boto3
        _key = boto3.client("ssm", region_name="us-east-1").get_parameter(
            Name=SSM, WithDecryption=True)["Parameter"]["Value"].strip()
    except Exception:
        _key = ""
    return _key


def complete(prompt, system=None, max_tokens=1800):
    key = _key_get()
    if not key:
        return ""
    body = {
        "model": MODEL,
        "temperature": 0.2,
        "max_tokens": int(max_tokens or 1800),
        "messages": [
            {"role": "system", "content": system or (
                "You are Grok teaching JustHodl's market desk. Use only the board "
                "facts given. Missing stays missing. Plumbing gates size. Dollar first. "
                "Do not invent prints. Return compact JSON if asked, else short prose."
            )},
            {"role": "user", "content": prompt},
        ],
    }
    req = urllib.request.Request(
        BASE.rstrip("/") + "/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as h:
            data = json.loads(h.read().decode())
        return (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    except Exception as e:
        print("[xai_voice] fail", type(e).__name__)
        return ""
