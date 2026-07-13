"""ops 3240 — the MARKET-miss sub-census + two rescue ladders.

772 of the fleet's misses are MARKET-class. This ops groups them by
exchange prefix (with dictionary names as evidence), then runs the two
ladders the census menu already hinted at:

  · BER:* (Börse Berlin cross-listings) → Yahoo {t}.BE, then {t}.DE
    (the liquid German line), then bare {t} (US original).
  · NASDAQ index tiles (NQ*/CRSP*) → Yahoo ^{t}.

Probe-gated (≥200 pts), budgeted, curated with the winning path named;
fleet run; wakes by name. Whatever stays dry stays honestly in the
census for the next family.
"""
import gzip
import json
import re
import sys
import time
from collections import Counter, defaultdict
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


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3240_market_rescue") as rep:
    fails, warns = [], []
    rep.heading("ops 3240 — MARKET-miss sub-census + rescue ladders")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    names_doc = s3_json("data/symbol-dictionary.json") or {}
    names = names_doc.get("dictionary") or names_doc.get("symbols") or {}
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    def nm(s):
        v = names.get(s)
        return (v.get("name") if isinstance(v, dict) else v) or ""

    # ── 1. sub-census by exchange prefix ───────────────────────────────
    rep.section("1. MARKET misses by exchange")
    mkt = [s for s, m in misses.items()
           if str((m or {}).get("id", "")).startswith("MARKET|")
           or "MARKET" in str((m or {}).get("id", ""))[:8]]
    if not mkt:
        mkt = [s for s, m in misses.items()
               if (mapped.get(s) or {}).get("source") == "MARKET"
               or "=" not in str((m or {}).get("id", ""))]
    ex = Counter(s.split(":")[0] if ":" in s else "BARE" for s in mkt)
    for e2, n in ex.most_common(12):
        sample = next((s for s in mkt if s.startswith(e2 + ":")), "")
        rep.log(f"  {n:>4} × {e2:<10} e.g. {sample[:22]} "
                f"'{nm(sample)[:34]}'")

    # ── 2. rescue ladders ──────────────────────────────────────────────
    rep.section("2. BER + NASDAQ-index ladders (probe-gated)")
    landed, t0 = 0, time.time()

    def ladder(sym, cands, note):
        global landed
        for cid in cands:
            try:
                n = len(SS.fetch("MARKET", cid))
            except Exception:
                n = 0
            if n >= 200:
                e = {"source": "MARKET", "id": cid, "confidence": 0.6,
                     "note": f"{note} (ops 3240)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.log(f"  ✓ {sym:<18} → {cid:<12} ({n})  "
                        f"'{nm(sym)[:30]}'")
                return True
        return False

    ber = sorted(s for s in mkt if s.startswith("BER:"))[:25]
    for s in ber:
        if time.time() - t0 > 150:
            break
        t = s.split(":", 1)[1]
        ladder(s, [f"{t}.BE", f"{t}.DE", t], "Börse Berlin → Yahoo")
    nas = sorted(s for s in mkt if s.startswith("NASDAQ:")
                 and re.match(r"NASDAQ:(NQ|CRSP)", s))[:25]
    for s in nas:
        if time.time() - t0 > 300:
            break
        t = s.split(":", 1)[1]
        ladder(s, [f"^{t}"], "Nasdaq index tile → Yahoo ^")
    rep.kv(ber_tried=len(ber), nasdaq_tried=len(nas), curations=landed)

    # ── 3. fleet ───────────────────────────────────────────────────────
    if landed:
        rep.section("3. Fleet — wakes by name")
        wl = s3_json("data/tv-watchlists.json") or {}
        uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only", "retired",
                                           "search_cache") if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "dry": dry, "note": "ops 3240"}),
                      ContentType="application/json")
        rep.kv(coverage_now=cov)
        mark = datetime.now(timezone.utc).isoformat()
        try:
            LAM.invoke(FunctionName="justhodl-wl-engines",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            fails.append(f"invoke: {str(e)[:70]}")
        idx2 = None
        for _ in range(70):
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
            rep.kv(active_before=len(prev_active),
                   active_now=len(act2), woken=len(woken))
            for w in woken[:10]:
                nm2 = next((e.get("name") for e in eng2
                            if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm2}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("index not fresh in window")
    else:
        warns.append("no rescues landed — clusters above are the honest "
                     "residue map")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
