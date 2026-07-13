"""ops 3190 — ECONOMICS 383: the biggest residual bucket, closed on
DBnomics (IMF IFS + OECD MEI), free, probe-gated.

3185 routed OECD members to FRED's MEI mirrors and everything else to the
World Bank. What is LEFT is the families neither carries: central-bank
balance sheets (CBBS), interbank/policy rates (INBR/BR), quarterly GDP for
non-OECD (GDPQQ), confidence/orders/utilization/trade for the long tail.
IMF IFS covers ~190 countries monthly/quarterly and DBnomics serves it free
— the fetcher has existed since the canary grid.

Mechanism: ECON_DBN in series_source.py is a LADDER of candidate series-id
templates per family. map_symbol emits rung 0; THIS ops probes every new
entry with a real fetch, climbs the ladder on a dry hit, promotes the first
rung that returns a real series, prunes families that never do. Winners are
written into a `curated` section of data/symbol-map.json which every future
rebuild MERGES FIRST — probe-proven mappings survive rebuilds (new
convention, this ops establishes it).

Gate: coverage must not regress; measured per-family hit table; verdict
measured, not assumed.
"""
import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")
PROBE_BUDGET_S = 380
MIN_POINTS = 40
ECON_RE = re.compile(r"^ECONOMICS:([A-Z]{2})([A-Z0-9]+)$")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3190_econ_dbnomics") as rep:
    fails, warns = [], []
    rep.heading("ops 3190 — ECONOMICS residuals on IMF/OECD via DBnomics "
                "($0, probe-gated ladders)")

    # ── 1. census the ECONOMICS residue by family ─────────────────────
    rep.section("1. Family census (post-3189)")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    prev_map = prev.get("map") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)
    curated = dict(prev.get("curated") or {})
    econ_left = [s for s in uniq if s not in prev_map
                 and s.startswith("ECONOMICS:")]
    fam_count = Counter()
    for s in econ_left:
        m = ECON_RE.match(s)
        fam_count[m.group(2) if m else "UNPARSED"] += 1
    rep.kv(economics_unmapped=len(econ_left),
           families=len(fam_count),
           ladder_covered=sum(n for f, n in fam_count.items()
                              if f in SS.ECON_DBN))
    for f, n in fam_count.most_common(16):
        tag = "ladder" if f in SS.ECON_DBN else "-"
        rep.log(f"  {f:<10} {n:>4}  {tag}")

    # ── 2. rebuild map (curated merges FIRST — new convention) ────────
    rep.section("2. Re-map with econ ladders live")
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, used, t0 = {}, 0, time.time()
    for s in uniq:
        if s in curated:                       # probe-proven survives
            mapped[s] = curated[s]
            continue
        allow = used < 200 and time.time() - t0 < 150
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src and src != "EODHD":
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
    new_econ = {s: m for s, m in mapped.items()
                if s not in prev_map and m["source"] == "DBNOMICS"
                and "econ ladder" in m["note"]}
    rep.kv(new_ladder_entries=len(new_econ))

    # ── 3. probe rung 0, climb the ladder on dry ──────────────────────
    rep.section("3. Probe + ladder climb (real fetches)")

    def probe_one(sym):
        m0 = ECON_RE.match(sym)
        if not m0:
            return sym, None, 0
        i2, ind = m0.groups()
        i3 = SS.ISO2_ISO3.get(i2) or i2
        for rung, tpl in enumerate(SS.ECON_DBN.get(ind, [])):
            sid = tpl.format(i2=i2, i3=i3)
            try:
                ser = SS.fetch("DBNOMICS", sid)
            except Exception:
                ser = {}
            if len(ser) >= MIN_POINTS:
                return sym, {"source": "DBNOMICS", "id": sid,
                             "confidence": 0.7,
                             "note": f"econ ladder {ind} rung{rung} "
                                     "(probe-proven)"}, len(ser)
        return sym, None, 0

    hits, dry, t0 = {}, [], time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(probe_one, s) for s in new_econ]
        for f in as_completed(futs):
            if time.time() - t0 > PROBE_BUDGET_S:
                break
            sym, entry, n = f.result()
            if entry:
                hits[sym] = (entry, n)
            else:
                dry.append(sym)
    for sym in dry:
        mapped.pop(sym, None)
    for sym, (entry, _n) in hits.items():
        mapped[sym] = entry
        curated[sym] = entry
    fam_h, fam_d = Counter(), Counter()
    for sym in hits:
        fam_h[ECON_RE.match(sym).group(2)] += 1
    for sym in dry:
        m0 = ECON_RE.match(sym)
        fam_d[m0.group(2) if m0 else "?"] += 1
    for f in sorted(set(fam_h) | set(fam_d)):
        rep.log(f"  {f:<10} hit {fam_h.get(f, 0):>3}  dry {fam_d.get(f, 0):>3}")
        if fam_h.get(f, 0) == 0:
            warns.append(f"family {f}: every rung dry — IFS/MEI code "
                         "guess wrong or no data; needs a curated look")
    for sym, (entry, n) in sorted(hits.items(),
                                  key=lambda kv: -kv[1][1])[:5]:
        rep.log(f"    ✓ {sym} → {entry['id']}  ({n} pts)")
    rep.kv(probed=len(new_econ), survivors=len(hits), pruned=len(dry))

    # ── 4. write map (with curated carry-forward section) ─────────────
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov, "map": mapped,
                      "curated": curated, "search_cache": cache,
                      "note": ("ops 3190: ECONOMICS via IMF IFS/OECD MEI on "
                               "DBnomics; `curated` = probe-proven entries, "
                               "MERGE FIRST on every rebuild")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           mapped_now=len(mapped), curated_total=len(curated))
    if cov < prev_cov - 0.05:
        fails.append(f"coverage regressed {prev_cov} → {cov}")

    # ── 5. redeploy shared bundle + kick the fleet ────────────────────
    rep.section("4. Redeploy shared bundle")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
        rep.log("  fleet kicked (Event) — full verification lands in "
                "ops 3195 after all six residue waves")
    except Exception as e:
        warns.append(f"fleet kick: {str(e)[:70]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
