"""ops 3301 — PRIMARY DEALER POSITIONING desk (Khalid: Bloomberg/Crisil
say dealers are net SHORT corporate bonds, first since 1998 — verify and
give dealers their own engine + page, join the credit engine).
Engine: justhodl-nyfed-pd v2 adds the PDPOS* net-outright family —
corporate bonds by maturity bucket (stitched, IG+HY summed) with regime/
first-negative/ytd-avg analytics + Telegram tripwire, plus the full
dealer ledger (UST/MBS/ABS/agency/munis). Page: primary-dealers.html.
Join: credit-stress payload gains dealer_positioning; credit-desk card.
Assertions are data-INTEGRITY (buckets resolved, decomposition sums,
freshness); market numbers are REPORTED vs the headline, not asserted —
the Fed file is the truth, the article is the hypothesis."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def http(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-ops-3301"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


def live_env(fn):
    c = LAM.get_function_configuration(FunctionName=fn)
    return c, (c.get("Environment") or {}).get("Variables") or {}


with report("3301_primary_dealers") as rep:
    fails, warns = [], []

    rep.section("1. Deploy nyfed-pd v2 (+TELEGRAM tripwire env)")
    cfg, env = live_env("justhodl-nyfed-pd")
    if "TELEGRAM_BOT_TOKEN" not in env:
        try:
            _, tenv = live_env("justhodl-dollar-radar")
            for k in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
                if tenv.get(k):
                    env[k] = tenv[k]
            rep.log("telegram env copied from dollar-radar")
        except Exception as e:
            warns.append("telegram env copy failed: %s" % str(e)[:80])
    deploy_lambda(report=rep, function_name="justhodl-nyfed-pd",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-nyfed-pd" / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=420, memory=int(cfg.get("MemorySize") or 512),
                  description="NY Fed PD net positions: Treasury tenor ladder "
                              "+ PDPOS outright family (corporate by maturity "
                              "+ full dealer ledger).",
                  smoke=False)
    mark = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-nyfed-pd", InvocationType="Event",
               Payload=b"{}")

    rep.section("2. Poll fresh output + corporate integrity")
    d = None
    for _ in range(40):
        time.sleep(12)
        d = s3_json("data/nyfed-primary-dealer.json")
        if d and d.get("generated_at", "") >= mark.isoformat(timespec="seconds"):
            break
    if not (d and d.get("generated_at", "") >= mark.isoformat(timespec="seconds")):
        fails.append("nyfed-pd output never freshened")
        d = d or {}
    c = d.get("corporate") or {}
    spec = s3_json("data/config/nyfed-pd-spec.json") or {}
    corp_spec = (spec.get("pos") or {}).get("corp") or []
    rep.kv(version=d.get("version"),
           corp_series_discovered=len(corp_spec),
           corp_series_bucketed=sum(1 for x in corp_spec if x.get("bucket")),
           corp_unbucketed=[x["keyid"] for x in corp_spec
                            if not x.get("bucket") and not x.get("tot")][:12],
           buckets_latest=c.get("buckets_latest_b"),
           ledger_classes=sorted((d.get("positions_ledger") or {}).keys()))
    if not c:
        fails.append("corporate block missing")
    if len([b for b in (c.get("buckets_latest_b") or {})
            if b != "cp"]) < 3:
        fails.append("fewer than 3 corporate bond maturity buckets resolved")
    a, b5, u5 = c.get("net_bonds_b"), c.get("net_5yplus_b"), c.get("net_under5y_b")
    if None in (a, b5, u5):
        fails.append("decomposition fields missing: total=%s 5y+=%s <5y=%s"
                     % (a, b5, u5))
    elif abs((b5 + u5) - a) > 0.6:
        fails.append("decomposition inconsistent: %.2f + %.2f != %.2f"
                     % (b5, u5, a))
    if a is not None and abs(a) > 80:
        fails.append("corp net %.1fB outside sanity (|x|<=80B)" % a)
    if (c.get("n_weeks") or 0) < 100:
        warns.append("only %s weeks of corporate history — stitch shallow, "
                     "older series breaks may need adding" % c.get("n_weeks"))
    hist = s3_json("data/history/nyfed-pd-corp.json") or {}
    rep.kv(hist_weeks=len(hist.get("totals") or {}),
           hist_start=c.get("history_start"))

    rep.section("3. THE HEADLINE CHECK — Fed file vs Bloomberg/Crisil claim")
    rep.kv(net_bonds_b=a, ytd_avg_b=c.get("ytd_avg_b"),
           net_5yplus_b=b5, net_under5y_b=u5, cp_b=c.get("cp_b"),
           regime=c.get("regime"),
           prior_negative_date=c.get("prior_negative_date"),
           all_time_min=(c.get("all_time_min_b"),
                         c.get("all_time_min_date")),
           pctile_history=c.get("pctile_history"),
           z_52w=c.get("z_52w"), as_of=c.get("as_of"),
           claim="net~-4B ytd-avg; 5y+~-13.7B; <5y~+9.66B; first since 1998")
    if b5 is not None and b5 > 0:
        warns.append("5y+ book is LONG %.1fB — headline decomposition not "
                     "reproduced; check bucket mapping" % b5)

    rep.section("4. Credit engine join")
    cs_cfg, cs_env = live_env("justhodl-credit-stress")
    deploy_lambda(report=rep, function_name="justhodl-credit-stress",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-credit-stress" / "source",
                  env_vars=cs_env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(cs_cfg.get("Timeout") or 300),
                  memory=int(cs_cfg.get("MemorySize") or 512),
                  description=str(cs_cfg.get("Description") or "")[:250],
                  smoke=False)
    m2 = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-credit-stress",
               InvocationType="Event", Payload=b"{}")
    csd = None
    for _ in range(25):
        time.sleep(10)
        csd = s3_json("data/credit-stress.json")
        if csd and csd.get("generated_at", "") >= m2:
            break
    dp = (csd or {}).get("dealer_positioning")
    rep.kv(credit_stress_fresh=bool(csd and csd.get("generated_at", "") >= m2),
           dealer_positioning=dp)
    if not (csd and csd.get("generated_at", "") >= m2):
        fails.append("credit-stress never freshened")
    if not dp or dp.get("net_bonds_b") is None:
        fails.append("dealer_positioning join missing/empty in credit-stress")

    rep.section("5. Page markers")
    raw = http("https://raw.githubusercontent.com/ElMooro/si/main/primary-dealers.html")
    for mk in ("Positioning Desk", "buckets_latest_b", "jh-nav-drawer"):
        if mk not in raw:
            fails.append("marker %r missing from repo primary-dealers.html" % mk)
    cd = http("https://raw.githubusercontent.com/ElMooro/si/main/credit-desk.html")
    if "DEALER CORP INVENTORY" not in cd:
        fails.append("credit-desk dealer card missing in repo")
    live_ok = False
    for _ in range(16):
        pg = http("https://justhodl.ai/primary-dealers.html?ops=3301")
        if "Positioning Desk" in pg:
            live_ok = True
            break
        time.sleep(30)
    rep.kv(live_page=live_ok)
    if not live_ok:
        warns.append("live primary-dealers.html not serving yet (CDN lag) — "
                     "repo verified")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3301 PASS — dealers have their own desk: net corp %sB "
            "(%s), 5y+ %sB / <5y %sB, wired into credit-stress + Telegram "
            "tripwire armed." % (a, c.get("regime"), b5, u5))
sys.exit(0)
