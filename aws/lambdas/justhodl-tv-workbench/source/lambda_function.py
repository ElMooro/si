"""
justhodl-tv-workbench v1.0 — the TradingView mirror, assembled server-side.

Khalid (2026-07-28): "extract each watchlist I have with all the indicators
inside it, all the notes inside those indicators, and the data source
TradingView pulled it from — all inside an engine and a page organized by
watchlist name, just like in TradingView."

Joins four artifacts the fleet already owns (audit-first — nothing here is
re-fetched from TradingView):
  data/tv-watchlists.json  — 207 lists w/ symbols (extension v1.4+ capture)
  data/brain.json          — 12k+ notes; [TV:EX:SYM] prefix per note
  data/tradingview.json    — live value/status per vault symbol
  data/tv-sources.json     — TV attribution (fills on the v1.5.0 upload;
                             absent = honest empty, never guessed)

Shape (size-disciplined: symbols deduped into one details map, watchlists
reference keys — Black Swan's 500 symbols cost bytes once, not per list):
  { watchlists: [{name,id,n,symbols:[...keys]}],
    symbols: {KEY: {value,status,source_engine,tv_source,description,
                    n_notes,notes:[{t,ts}]}} }
"""
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

MARKER = "tv-workbench v1.0 ops4019"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/tv-workbench.json"
s3 = boto3.client("s3")
TV_RX = re.compile(r"\[TV:([A-Z0-9_:!\.\-]+)\]")
MAX_NOTES_PER = 6
NOTE_CHARS = 420


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"[tv-workbench] {MARKER}")

    wl_doc = gj("data/tv-watchlists.json", {}) or {}
    brain = gj("data/brain.json", {}) or {}
    vault = gj("data/tradingview.json", {}) or {}
    srcs = gj("data/tv-sources.json", {}) or {}

    # ── notes by symbol: full key AND bare tail both index the same list ──
    notes_by = defaultdict(list)
    for n in brain.get("notes", []):
        txt = n.get("text") or ""
        m = TV_RX.match(txt)
        if not m:
            continue
        full = m.group(1)
        bare = full.split(":")[-1]
        body = txt[m.end():].strip()[:NOTE_CHARS]
        rec = {"t": body, "ts": n.get("ts") or n.get("created") or None}
        notes_by[full].append(rec)
        if bare != full:
            notes_by[bare].append(rec)

    vidx = {r.get("symbol"): r for r in vault.get("symbols") or []}
    smap = srcs.get("sources") or {}

    def attribution(key, bare):
        for k in (key, bare):
            v = smap.get(k)
            if isinstance(v, dict) and v.get("source"):
                return v.get("source"), v.get("description") or ""
        return None, ""

    wls_raw = wl_doc.get("watchlists") or wl_doc.get("lists") or []
    if isinstance(wls_raw, dict):
        wls_raw = list(wls_raw.values())

    symbols, watchlists = {}, []
    for w in wls_raw:
        name = str(w.get("name") or "").strip()
        syms = [str(x).strip() for x in (w.get("symbols") or []) if x]
        keys = []
        for full in syms:
            bare = full.split(":")[-1]
            key = full
            keys.append(key)
            if key in symbols:
                continue
            vr = vidx.get(bare) or vidx.get(full) or {}
            src, desc = attribution(full, bare)
            nl = notes_by.get(full) or notes_by.get(bare) or []
            symbols[key] = {
                "bare": bare,
                "value": vr.get("value"),
                "status": vr.get("status"),
                "source_engine": (str(vr.get("source"))[:40]
                                  if vr.get("source") else None),
                "tv_source": src,
                "description": desc[:160],
                "n_notes": len(nl),
                "notes": nl[:MAX_NOTES_PER],
            }
        watchlists.append({"name": name or f"(unnamed {w.get('id')})",
                           "id": str(w.get("id") or "")[:24],
                           "named": bool(name and not name.isdigit()),
                           "n": len(keys), "symbols": keys})

    watchlists.sort(key=lambda x: (not x["named"], x["name"].lower()))
    n_notes_joined = sum(1 for v in symbols.values() if v["n_notes"])
    n_with_src = sum(1 for v in symbols.values() if v["tv_source"])

    out = {
        "engine": "justhodl-tv-workbench", "version": "1.0", "marker": MARKER,
        "generated_at": now.isoformat(),
        "inputs": {
            "watchlists_generated": wl_doc.get("generated_at"),
            "brain_notes": len(brain.get("notes") or []),
            "sources_generated": srcs.get("generated_at"),
            "sources_available": bool(smap),
        },
        "totals": {"watchlists": len(watchlists),
                   "unique_symbols": len(symbols),
                   "symbols_with_notes": n_notes_joined,
                   "symbols_with_tv_source": n_with_src},
        "honesty": "tv_source is TradingView's own attribution captured by "
                   "extension v1.5.0; empty means not yet browsed+uploaded, "
                   "never inferred. Notes are Khalid's verbatim brain notes, "
                   "capped at 6/symbol for size.",
        "watchlists": watchlists,
        "symbols": symbols,
        "elapsed_s": round(time.time() - t0, 1),
    }
    body = json.dumps(out, default=str)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[tv-workbench] DONE {out['elapsed_s']}s wl={len(watchlists)} "
          f"syms={len(symbols)} noted={n_notes_joined} sourced={n_with_src} "
          f"bytes={len(body)}")
    return {"ok": True, **out["totals"], "bytes": len(body)}
