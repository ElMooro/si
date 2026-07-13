"""ops 3268 — RATIO FAMILIES: div transform + per-country mapping.

The four computed-ratio panels (FX-Reserves/GDP, FER/External-Debt,
FER/M3, GDP/M3 — ~61 members, 0 resolved) sat dead because no ratio
transform existed. Now: series_source gains a 5-part `div` (mirrors
minus, zero-guarded); this ops BROWSES (never guesses) FRED per
country — TRESEG{cc}M052N reserves, CLVMNACSCAB1GQ{cc} /
NGDPRSAXDC{cc}Q GDP, MABMM301{cc}M189S broad money — writes only
probe-verified pairs into data/symbol-map.json (merged, entry shape
copied from a live minus entry), redeploys the SHARED_CONSUMERS, runs
the fleet, and reports the four engines' states. Composite mode means
≥1 resolved member + 60 weeks wakes an engine. External-debt has no
free per-country template — expected to stay dormant, said plainly.
"""
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
FRED_KEY = "2f057499936072679d8843d7fce99989"
SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")
FAMS = [
    ("fx-reserves-gdp", "reserves/gdp", ("FER", "GDP")),
    ("fer-external-debt", "external debt", ("FER", "EXD")),
    ("fer-money-supply", "reserves / money supply", ("FER", "M3")),
    ("gdp-m3", "gdp to money supply", ("GDP", "M3")),
]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


_probe_cache = {}


def fred_ok(sid):
    """Alive (obs past 2023) + >=120 obs. Cached per id."""
    if sid in _probe_cache:
        return _probe_cache[sid]
    ok = False
    try:
        u = ("https://api.stlouisfed.org/fred/series/observations"
             f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
             "&sort_order=desc&limit=1")
        j = json.loads(urllib.request.urlopen(u, timeout=12).read())
        obs = j.get("observations") or []
        last = obs[0]["date"] if obs else ""
        n = int(j.get("count") or 0)
        ok = last >= "2023-01-01" and n >= 120
    except Exception:
        ok = False
    _probe_cache[sid] = ok
    time.sleep(0.55)          # FRED politeness gate (doctrine)
    return ok


def gdp_id(cc):
    for cand in (f"CLVMNACSCAB1GQ{cc}", f"NGDPRSAXDC{cc}Q"):
        if fred_ok(cand):
            return cand
    return None


def fer_id(cc):
    c = f"TRESEG{cc}M052N"
    return c if fred_ok(c) else None


def m3_id(cc):
    c = f"MABMM301{cc}M189S"
    return c if fred_ok(c) else None


with report("3268_ratio_families") as rep:
    fails, warns = [], []
    rep.heading("ops 3268 — ratio families: div transform + browsed "
                "country pairs")

    rep.section("1. The four lists — raw member symbols")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = wl.get("lists") or []
    fam_syms = {}
    for fam, needle, _sides in FAMS:
        L = next((l for l in lists
                  if needle in str(l.get("name", "")).lower()), None)
        syms = [str(x) for x in ((L or {}).get("symbols") or [])]
        fam_syms[fam] = syms
        rep.log(f"  [{fam}] {len(syms)} tiles — e.g. "
                + " · ".join(syms[:5]))

    rep.section("2. Country parse + FRED browse (never guess)")
    cc_rx = re.compile(r"ECONOMICS:([A-Z]{2})[A-Z0-9]*")
    smap_doc = s3_json("data/symbol-map.json") or {}
    smap = smap_doc.get("map") or {}
    donor = next((v for v in smap.values()
                  if "~minus~" in str(v.get("id", ""))), None)
    rep.log(f"  minus-entry donor shape: "
            f"{json.dumps(donor)[:120] if donor else 'NONE'}")
    getters = {"FER": fer_id, "GDP": gdp_id, "M3": m3_id,
               "EXD": lambda cc: None}
    added, per_fam = 0, {}
    for fam, _needle, (sa, sb) in FAMS:
        n_ok = 0
        for sym in fam_syms.get(fam, []):
            ccs = cc_rx.findall(sym)
            cc = ccs[0] if ccs else None
            if not cc or sym in smap and "~div~" in \
                    str(smap[sym].get("id", "")):
                if sym in smap:
                    n_ok += 1
                continue
            a, b = getters[sa](cc), getters[sb](cc)
            if a and b:
                smap[sym] = {
                    "source": (donor or {}).get("source") or "FRED",
                    "id": f"FRED~{a}~div~FRED~{b}",
                    "name": f"{cc} {sa}/{sb} ratio (ops 3268)"}
                added += 1
                n_ok += 1
        per_fam[fam] = n_ok
        rep.kv(**{f"{fam}_resolved": n_ok,
                  f"{fam}_tiles": len(fam_syms.get(fam, []))})
    rep.kv(map_entries_added=added)
    if added:
        smap_doc["map"] = smap
        smap_doc["updated_at"] = datetime.now(
            timezone.utc).isoformat()
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps(smap_doc),
                      ContentType="application/json")
        rep.ok(f"symbol-map merged (+{added} div entries)")
    elif not any(per_fam.values()):
        fails.append("no country pairs resolved — templates dead?")

    if not fails:
        rep.section("3. Shared-consumer redeploy (series_source "
                    "changed)")
        for fn in SHARED_CONSUMERS:
            cfg = {}
            pc = AWS_DIR / "lambdas" / fn / "config.json"
            if pc.exists():
                cfg = json.loads(pc.read_text())
            sch = cfg.get("schedule")
            rule, cron = (sch.get("rule_name"), sch.get("cron")) \
                if isinstance(sch, dict) else (None, None)
            env = (LAM.get_function_configuration(FunctionName=fn)
                   .get("Environment") or {}).get("Variables") or {}
            try:
                deploy_lambda(report=rep, function_name=fn,
                              source_dir=AWS_DIR / "lambdas" / fn
                              / "source",
                              env_vars=env, eb_rule_name=rule,
                              eb_schedule=cron,
                              timeout=cfg.get("timeout", 900),
                              memory=cfg.get("memory", 1536),
                              description=str(
                                  cfg.get("description", ""))[:250],
                              smoke=False)
                LAM.get_waiter("function_updated_v2").wait(
                    FunctionName=fn,
                    WaiterConfig={"Delay": 2, "MaxAttempts": 40})
            except Exception as e:
                fails.append(f"deploy {fn}: {str(e)[:70]}")

    if not fails:
        rep.section("4. Fleet run + the four engines")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
        idx = None
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx = d
                break
        if not idx:
            fails.append("index not fresh in window")
        else:
            eng = idx.get("engines") or []
            act = sum(1 for e in eng
                      if str(e.get("state")) == "ACTIVE")
            rep.kv(active_total=act)
            woke = 0
            for fam, needle, _s in FAMS:
                e = next((x for x in eng if needle in
                          str(x.get("name", "")).lower()), None)
                if not e:
                    continue
                st = str(e.get("state"))
                if st == "ACTIVE":
                    woke += 1
                    rep.ok(f"{str(e.get('name'))[:44]}: ACTIVE "
                           f"(mode={e.get('mode')}) "
                           f"members={e.get('members_resolved')}"
                           f"/{e.get('members_total')} z="
                           f"{e.get('activation_now')} pct="
                           f"{e.get('activation_pctile')}"
                           f"{' FIRING' if e.get('firing') else ''}")
                else:
                    rep.log(f"  {str(e.get('name'))[:44]}: {st} — "
                            f"{str(e.get('reason'))[:56]}")
            rep.kv(ratio_engines_awake=woke)
            if woke < 2:
                fails.append(f"only {woke}/4 ratio engines woke")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
