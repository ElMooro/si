"""jhcore.kb — Crisis Knowledge Base router.

Loads s3://justhodl-dashboard-live/data/crisis-knowledge-base.json (or
config/crisis-knowledge-base.json if newer), routes keywords to relevant
framework chunks, returns text to inject into prompts.

This is the macro framework codification: Hugh Hendry yield curve,
Dollar Milkshake, eurodollar mechanics, 1997 Asian Crisis, Fed corridor,
BTC cycles, permanent portfolio, US10Y 5%, etc.
"""
import re
from . import s3io

_cache = {"kb": None}
_CANDIDATE_KEYS = [
    "data/crisis-knowledge-base.json",
    "config/crisis-knowledge-base.json",
]


def load():
    """Load the KB (cached after first call). Returns dict or {}."""
    if _cache["kb"] is not None:
        return _cache["kb"]
    for k in _CANDIDATE_KEYS:
        d = s3io.get_json(k)
        if d:
            _cache["kb"] = d
            return d
    _cache["kb"] = {}
    return {}


def lookup(keywords, max_chunks=3, max_chars=2000):
    """Given a keyword string or list, return relevant KB framework excerpts.

    Returns: list of {framework, excerpt} (capped) or [] if no KB / no match.
    """
    kb = load()
    if not kb:
        return []
    if isinstance(keywords, str):
        kws = re.findall(r"[a-zA-Z]+", keywords.lower())
    else:
        kws = [str(k).lower() for k in keywords]
    frameworks = kb.get("frameworks") or kb.get("rules") or []
    if isinstance(frameworks, dict):
        frameworks = [{"name": k, **(v if isinstance(v, dict) else {"text": str(v)})}
                      for k, v in frameworks.items()]

    scored = []
    for fw in frameworks:
        name = fw.get("name", "")
        text = fw.get("text") or fw.get("excerpt") or fw.get("summary") or ""
        if not text:
            continue
        body = (name + " " + text).lower()
        score = sum(1 for kw in kws if kw and kw in body)
        if score:
            scored.append((score, name, text[:max_chars]))
    scored.sort(reverse=True)
    return [{"framework": n, "excerpt": e} for _, n, e in scored[:max_chunks]]


def context_for_prompt(keywords, max_chunks=3):
    """Return a formatted text block ready to inject into a Claude prompt."""
    chunks = lookup(keywords, max_chunks=max_chunks)
    if not chunks:
        return ""
    parts = ["[Relevant macro frameworks from the JustHodl knowledge base]"]
    for c in chunks:
        parts.append(f"\n--- {c['framework']} ---\n{c['excerpt']}")
    return "\n".join(parts)
