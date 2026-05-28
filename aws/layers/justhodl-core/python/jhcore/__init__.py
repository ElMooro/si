"""
jhcore — JustHodl shared Lambda layer.
=======================================
Import with:  from jhcore import fred, s3io, notify, claude, kb

Modules:
    fred    — FRED API client (caching + retry + multi-series fetch)
    s3io    — S3 read/write helpers (JSON-aware, cache-control aware)
    notify  — Telegram helper + minimal SES email
    claude  — Anthropic API client (model="claude-haiku-4-5-20251001" default)
    kb      — Crisis knowledge-base lookup (reads s3://.../config/crisis-knowledge-base.json)
"""
__version__ = "1.0.0"
from . import fred, s3io, notify, claude, kb  # noqa: F401
