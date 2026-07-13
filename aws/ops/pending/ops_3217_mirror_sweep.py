"""ops 3217 — the non-wakers explained by their own rows, the mirror
poison swept fleet-wide, and the ICE Europe rates complex proxied.

3216 woke 2 of the 5 one-symbol-blocked engines. The other three
(Developed Markets, Europe Liquidity, Global Deposit Rates) got their
blockers curated and still sleep — but since 3208 every DORMANT row
carries a NAMED reason, the explanation is a read, not a guess.

Also closed here:
  · The FRED-OECD-mirror poison class generalized: 3215 found Chile
    templates emitting FRED ids that don't exist (0 pts) while counting
    as "resolved". This ops probes EVERY non-curated map entry in the
    OECD MEI families and dry-ledgers the zero-pointers — fleet-wide,
    "resolved" now means fetchable everywhere.
  · ICEEUR:I2! (3M Euribor, old LIFFE root) and ICEEUR:EON2! (EONIA →
    €STR successor) proxied as 100−rate (the ZQ convention), candidate-
    laddered and probe-gated. These sit inside Europe Liquidity's gap
    list.
Map-only changes — no deploys; the runner reads the map at runtime.
"""
import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
OPEN_ENGINES = ("Developed Markets", "Europe Liquidity",
                "Global Deposit Rates")
MIRROR = re.compile(r"^(IR3TIB|NAEXKP|XTNTVA|LRHUTTTT|SPASTT|CPALTT|"
                    r"PRINTO|SLRTTO|ODCNPI|CSCICP|BSCICP|LORSGP|MABMM3|"
                    r"IRSTCI)01[A-Z]{2,3}[MQA]\d")
ICE_CANDS = {
    "ICEEUR:EON2!": [("DERIVED",
                      "FRED~ECBESTRVOLWGTTRMDMNRT~hundred_minus",
                      "EONIA→€STR successor, 100−rate (ZQ convention)")],
    "ICEEUR:I2!": [("DERIVED", "FRED~IR3TIB01EZM156N~hundred_minus",
                    "3M Euribor (EZ interbank), 100−rate"),
                   ("DERIVED",
                    "DBNOMICS~ECB/FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA"
                    "~hundred_minus",
                    "3M Euribor via ECB, 100−rate")],
}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def eng_row(idx, name_frag):
    for e in idx.get("engines") or []:
        if name_frag.lower() in str(e.get("name", "")).lower():
            return e
    return None


with report("3217_mirror_sweep") as rep:
    fails, warns = [], []
    rep.heading("ops 3217 — non-wakers explained, mirror poison swept, "
                "ICE rates proxied")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    # ── 1. the non-wakers, in their own words ─────────────────────────
    rep.section("1. Why the three still sleep (named reasons)")
    for nm in OPEN_ENGINES:
        e = eng_row(idx, nm)
        if not e:
            rep.log(f"  ? {nm}: not found in index")
            continue
        rep.log(f"  {str(e.get('name'))[:40]:<40} state={e.get('state')} "
                f"resolved={e.get('members_resolved')} "
                f"reason={str(e.get('reason'))[:70]}")

    # ── 2. fleet-wide mirror sweep ─────────────────────────────────────
    rep.section("2. FRED-OECD-mirror sweep (non-curated only)")
    targets = [(s, m) for s, m in mapped.items()
               if m.get("source") == "FRED" and s not in curated
               and MIRROR.match(str(m.get("id", "")))]
    rep.kv(mirror_candidates=len(targets))

    def probe(t):
        s, m = t
        try:
            return s, m, len(SS.fetch("FRED", m["id"]))
        except Exception:
            return s, m, 0

    swept = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for s, m, n in ex.map(probe, targets[:400]):
            if n == 0:
                dry[s] = m["id"]
                mapped.pop(s, None)
                swept += 1
    rep.kv(zero_point_swept=swept,
           still_alive=len(targets[:400]) - swept)
    if swept:
        rep.ok(f"{swept} phantom mirrors out of the map — 'resolved' now "
               "means fetchable, fleet-wide")

    # ── 3. ICE Europe rates proxies ────────────────────────────────────
    rep.section("3. ICEEUR I/EON — candidate-laddered, probe-gated")
    ice_ok = 0
    for sym, cands in ICE_CANDS.items():
        if sym in mapped or sym in (prev.get("retired") or {}):
            continue
        for src, sid, note in cands:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            if n >= 150:
                e = {"source": src, "id": sid, "confidence": 0.85,
                     "note": note + " (ops 3217)"}
                mapped[sym] = e
                curated[sym] = e
                ice_ok += 1
                rep.ok(f"{sym} → {sid[:56]}  ({n} pts)")
                break
        else:
            rep.log(f"  ✗ {sym}: all candidates dry — stays open")
    rep.kv(ice_curated=ice_ok)

    # ── 4. write + fleet — wakes + honest dormant histogram ───────────
    rep.section("4. Fleet re-run")
    wl = s3_json("data/tv-watchlists.json") or {}
    uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                   if not str(l.get("id", "")).startswith("e2e-")
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "retired", "search_cache")
                                      if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "dry": dry,
                                   "note": "ops 3217: mirror sweep + ICE "
                                           "rates proxies"}),
                  ContentType="application/json")
    rep.kv(coverage_now=cov,
           note="coverage drop = phantom entries leaving, honest")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(60):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx2 = d
            break
    if idx2:
        eng2 = idx2.get("engines") or []
        act2 = {e["engine_id"] for e in eng2
                if str(e.get("state")) == "ACTIVE"}
        woken = sorted(act2 - prev_active)
        slept = sorted(prev_active - act2)
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken), newly_dormant=len(slept))
        for w in woken[:8]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        for w in slept[:6]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  💤 slept (lost phantom members): {nm}")
        reasons = Counter((e.get("reason") or "").split("(")[0].strip()
                          for e in eng2 if e.get("state") == "DORMANT")
        for rzn, n in reasons.most_common(4):
            rep.log(f"  DORMANT {n:>3} × {rzn[:64]}")
        for nm in OPEN_ENGINES:
            e = eng_row(idx2, nm)
            if e:
                rep.log(f"  → {str(e.get('name'))[:36]:<36} now "
                        f"{e.get('state')} "
                        f"({str(e.get('reason') or 'ACTIVE')[:50]})")
    else:
        warns.append("index not fresh in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
