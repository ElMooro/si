"""ops 3204 — FUSION WAVE 3: his COT tiles widen the extremes scanner +
the rail chip re-verified.

The extremes scanner z-scores positioning for a hardcoded 31-contract
universe. The symbol map now carries ~48 CFTC contract codes Khalid
actually watches — every one probe-proven against publicreporting.cftc.gov
in ops 3189. This ops GENERATES cot/universe-ext.json from the map (names
from the symbol dictionary), which the scanner merges additively at cold
start (hardcoded contracts win collisions, a bad row changes nothing),
then deploys, invokes, and verifies the widened universe in the live
snapshot. Also re-checks the site-wide HIS RESEARCH rail chip that 3203's
window missed by one bake cycle.
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
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-cot-extremes-scanner"
SNAP = "cot/extremes/current.json"
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3204)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def get(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


with report("3204_cot_widen") as rep:
    fails, warns = [], []
    rep.heading("ops 3204 — COT universe widened from his tiles + rail "
                "chip verified")

    # ── 1. generate the extension from the map ─────────────────────────
    rep.section("1. Build cot/universe-ext.json from probe-proven codes")
    smap = s3_json("data/symbol-map.json") or {}
    entries = smap.get("map") or {}
    sdict = (s3_json("data/symbol-dictionary.json") or {})
    names = sdict.get("dictionary") or sdict.get("symbols") or {}
    src = (AWS_DIR / "lambdas" / FN / "source"
           / "lambda_function.py").read_text()
    have = set(re.findall(r'"cftc_code":\s*"([0-9A-Z]{5,6})"', src))
    codes = {}
    for sym, m in entries.items():
        if m.get("source") != "COT":
            continue
        parts = str(m.get("id", "")).split("|")
        if len(parts) != 3:
            continue
        code = parts[1]
        if code in have:
            continue
        nm = names.get(sym)
        if isinstance(nm, dict):
            nm = nm.get("name") or nm.get("title")
        nm = re.split(r"\s+[—-]\s+", str(nm or ""))[0].strip() or None
        cur = codes.get(code)
        if not cur or (nm and (not cur["name"] or len(nm) < len(cur["name"]))):
            codes[code] = {"name": nm or f"CFTC {code}",
                           "cftc_code": code, "category": "watchlist"}
    ext = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": "ops 3204: symbol-map COT entries (probe-proven 3189)",
           "contracts": {f"C{c}": spec for c, spec in codes.items()}}
    S3.put_object(Bucket=BUCKET, Key="cot/universe-ext.json",
                  Body=json.dumps(ext), ContentType="application/json")
    rep.kv(hardcoded_universe=len(have), new_watchlist_codes=len(codes))
    for c, spec in list(codes.items())[:5]:
        rep.log(f"  + {c}  {spec['name'][:44]}")
    if not codes:
        warns.append("no new codes — his COT tiles already inside the "
                     "hardcoded universe")

    # ── 2. deploy + run + verify widened snapshot ──────────────────────
    rep.section("2. Deploy scanner + verify the widened snapshot")
    cfg = {}
    p = AWS_DIR / "lambdas" / FN / "config.json"
    if p.exists():
        cfg = json.loads(p.read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 300),
                      memory=cfg.get("memory", 512),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
    except Exception as e:
        fails.append(f"deploy: {str(e)[:90]}")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:80]}")
    widened = False
    for _ in range(30):
        time.sleep(10)
        d = s3_json(SNAP) or {}
        if str(d.get("generated_at", d.get("as_of", ""))) > mark or \
                str(d.get("updated_at", "")) > mark:
            blob = json.dumps(d)
            hits = [c for c in codes if f"C{c}" in blob or f'"{c}"' in blob]
            n_contracts = (len(d.get("contracts") or [])
                           or len(d.get("by_sector") or {}))
            rep.kv(snapshot_refreshed=True, widened_codes_present=len(hits),
                   snapshot_size=len(blob))
            if hits or not codes:
                widened = True
                rep.ok(f"scanner running the widened universe "
                       f"({len(hits)} watchlist contracts in the snapshot)")
            break
    if not widened and codes:
        warns.append("snapshot not refreshed in window or watchlist codes "
                     "absent — verify at the scanner's next schedule")

    # ── 3. rail chip re-verify (3203 missed it by one bake) ───────────
    rep.section("3. HIS RESEARCH chip on the live rail")
    chip = False
    for _ in range(10):
        try:
            h = get("https://justhodl.ai/flows.html")
            if '"research"' in h and "panels.html" in h:
                chip = True
                rep.ok("research chip live in flows.html rail payload")
                break
        except Exception:
            pass
        time.sleep(20)
    if not chip:
        warns.append("chip still absent from flows.html — the */15 cron "
                     "re-bake will carry it; check bake logs if not by "
                     "next hour")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
