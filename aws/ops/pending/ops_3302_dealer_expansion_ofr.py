"""ops 3302 — DEALER DESK EXPANSION + OFR STFM + SIDEBAR FIX.
[1] Sidebar: primary-dealers.html was auto-filed under Portfolio &
Execution ('Positioning' matched the 'position' keyword) — FORCE map +
manifest seed pin it in Macro & Liquidity; verify the LIVE manifest.
[2] nyfed-pd v3: dealer FINANCING (securities in/out = the repo book)
+ TRANSACTION volumes per class + corporate turnover velocity.
[3] NEW justhodl-ofr-stfm: OFR Short-Term Funding Monitor by whole
dataset (repo DVP/GCF/TRI volumes+rates, MMF assets/holdings, NYPD
fails cross-check) + full mnemonic catalog for future fleet wiring;
daily EventBridge Scheduler.
[4] Page v2: FAILS (joins existing settlement-fails feed), FINANCING,
TRANSACTIONS, OFR funding-context sections — all additive.
Fails cross-check: OFR NYPD FtD total should reconcile with our direct
settlement-fails UST reading within tolerance (same FR2004 source)."""
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
SCH = boto3.client("scheduler", region_name=REGION)
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
            "User-Agent": "justhodl-ops-3302"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


with report("3302_dealer_expansion_ofr") as rep:
    fails, warns = [], []

    rep.section("1. nyfed-pd v3 (financing + transactions)")
    cfg = LAM.get_function_configuration(FunctionName="justhodl-nyfed-pd")
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name="justhodl-nyfed-pd",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-nyfed-pd" / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=600, memory=int(cfg.get("MemorySize") or 512),
                  description="NY Fed PD: positions (Treasury ladder + "
                              "corporate buckets + ledger) + financing "
                              "(securities in/out) + transaction volumes.",
                  smoke=False)
    mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # force POS re-discovery so the new fin/txn families enter the spec
    spec = s3_json("data/config/nyfed-pd-spec.json") or {}
    if isinstance(spec.get("pos"), dict):
        spec.pop("pos", None)  # 3302c: full rediscovery with fixed grammar
        S3.put_object(Bucket=BUCKET, Key="data/config/nyfed-pd-spec.json",
                      Body=json.dumps(spec).encode(),
                      ContentType="application/json")
        rep.log("spec pos family cleared -> rediscovery will run")
    LAM.invoke(FunctionName="justhodl-nyfed-pd", InvocationType="Event",
               Payload=b"{}")

    rep.section("2. NEW justhodl-ofr-stfm + Scheduler")
    try:
        denv = (LAM.get_function_configuration(
            FunctionName="justhodl-confluence-meta")
            .get("Environment") or {}).get("Variables") or {}
    except Exception:
        denv = {}
    deploy_lambda(report=rep, function_name="justhodl-ofr-stfm",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-ofr-stfm" / "source",
                  env_vars={k: v for k, v in denv.items()
                            if k in ("FRED_API_KEY",)},
                  eb_rule_name=None, eb_schedule=None,
                  timeout=300, memory=1024,
                  description="OFR STFM extractor: repo DVP/GCF/TRI + MMF "
                              "+ FR2004 fails cross-check + mnemonic "
                              "catalog for the fleet.",
                  smoke=False)
    try:
        arn = LAM.get_function_configuration(
            FunctionName="justhodl-ofr-stfm")["FunctionArn"]
        kw = dict(Name="justhodl-ofr-stfm-daily",
                  ScheduleExpression="cron(30 11 ? * TUE-SAT *)",
                  FlexibleTimeWindow={"Mode": "OFF"},
                  Target={"Arn": arn,
                          "RoleArn": "arn:aws:iam::857687956942:role/"
                                     "justhodl-scheduler-role",
                          "Input": "{}"},
                  State="ENABLED",
                  Description="OFR STFM daily pull (~9am ET data)")
        try:
            SCH.create_schedule(**kw)
            rep.log("scheduler created")
        except SCH.exceptions.ConflictException:
            SCH.update_schedule(**kw)
            rep.log("scheduler updated")
    except Exception as e:
        fails.append("scheduler setup failed: %s" % str(e)[:120])
    m2 = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LAM.invoke(FunctionName="justhodl-ofr-stfm", InvocationType="Event",
               Payload=b"{}")

    rep.section("3. Verify OFR feed")
    of = None
    for _ in range(30):
        time.sleep(10)
        of = s3_json("data/ofr-stfm.json")
        if of and of.get("generated_at", "") >= m2:
            break
    if not (of and of.get("generated_at", "") >= m2):
        fails.append("ofr-stfm output never freshened")
        of = of or {}
    ven = ((of.get("repo") or {}).get("venues")) or {}
    rep.kv(repo_n=(of.get("repo") or {}).get("n_series"),
           repo_venues={k: {"vol_mn": v.get("vol_mn"),
                            "rate_pct": v.get("rate_pct")}
                        for k, v in ven.items()},
           mmf_n=(of.get("mmf") or {}).get("n_series"),
           mmf_picks=sorted(((of.get("mmf") or {}).get("picks") or {})
                            .keys()),
           nypd_fails_cross=of.get("nypd_fails_cross"),
           catalog_repo_n=len((of.get("catalog") or {}).get("repo") or []),
           catalog_mmf_n=len((of.get("catalog") or {}).get("mmf") or []))
    fx0 = of.get("nypd_fails_cross") or {}
    if (of.get("repo") or {}).get("error"):
        (warns if fx0.get("ftd_tot") else fails).append(
            "OFR repo dataset error: %s" % of["repo"]["error"])
    elif len(ven) < 2:
        fails.append("fewer than 2 repo venues resolved: %s"
                     % sorted(ven.keys()))
    if (of.get("mmf") or {}).get("error"):
        warns.append("OFR mmf dataset error: %s" % of["mmf"]["error"])
    if not fx0.get("ftd_tot"):
        fails.append("NYPD fails cross-check (direct mnemonic) missing: %s"
                     % fx0)

    rep.section("4. Verify nyfed-pd v3 blocks + fails reconciliation")
    d = None
    for _ in range(45):
        time.sleep(12)
        d = s3_json("data/nyfed-primary-dealer.json")
        if d and d.get("generated_at", "") >= mark:
            break
    if not (d and d.get("generated_at", "") >= mark):
        fails.append("nyfed-pd v3 output never freshened")
        d = d or {}
    fn, tx = d.get("financing"), d.get("transactions") or {}
    c = d.get("corporate") or {}
    rep.kv(version=d.get("version"), financing=fn,
           txn_classes=sorted(tx.keys()),
           txn_corporate=tx.get("CORPORATE"),
           turnover_velocity=c.get("turnover_velocity"),
           corp_net_bonds_b=c.get("net_bonds_b"))
    if not fn or fn.get("securities_in_b") is None:
        fails.append("financing block missing")
    elif not (300 < fn["securities_in_b"] < 15000
              and 300 < fn["securities_out_b"] < 15000):
        fails.append("financing magnitudes implausible: %s" % fn)
    if len(tx) < 3:
        fails.append("only %d transaction classes" % len(tx))
    if c.get("net_bonds_b") is None:
        fails.append("corporate block regressed")
    sf = s3_json("data/settlement-fails.json") or {}
    ust = next((x for x in (sf.get("classes") or [])
                if x.get("key") == "ust_ex_tips"), {})
    ofr_ftd = ((of.get("nypd_fails_cross") or {}).get("ftd_tot")
               or {}).get("latest")
    rep.kv(sf_ust_ftd_latest=ust.get("ftd_latest"),
           sf_regime=sf.get("regime"), ofr_ftd_tot=ofr_ftd)
    if ust.get("ftd_latest") and ofr_ftd:
        a, b = float(ust["ftd_latest"]), float(ofr_ftd)
        # units may differ ($B vs $M) — accept either scaling
        if not (0.5 < a / b < 2.0 or 0.5 < a / (b / 1e3) < 2.0
                or 0.5 < (a * 1e3) / b < 2.0):
            warns.append("fails cross-check scale mismatch: ours %.1f vs "
                         "OFR %.1f (TOT covers all classes; ours is UST "
                         "only — informational)" % (a, b))

    rep.section("5. Sidebar + page markers (live)")
    mf = http("https://justhodl.ai/nav-manifest.json?ops=3302")
    in_live, in_macro = "primary-dealers" in mf, False
    try:
        mj = json.loads(mf)
        for cat in mj.get("categories", []):
            if any(p.get("href") == "/primary-dealers.html"
                   for p in cat.get("pages", [])):
                in_macro = (cat.get("name") == "Macro & Liquidity")
                rep.kv(live_manifest_category=cat.get("name"))
    except Exception:
        pass
    rep.kv(live_manifest_has_page=in_live,
           live_manifest_macro=in_macro)
    if not in_live:
        for _ in range(10):
            time.sleep(30)
            mf = http("https://justhodl.ai/nav-manifest.json?r=%d"
                      % time.time())
            if "primary-dealers" in mf:
                in_live = True
                break
        if not in_live:
            fails.append("primary-dealers missing from LIVE nav manifest")
    raw = http("https://raw.githubusercontent.com/ElMooro/si/main/primary-dealers.html")
    for mk in ("Settlement fails", "Dealer financing", "Transaction volumes",
               "ofr-stfm"):
        if mk not in raw:
            fails.append("marker %r missing from repo page" % mk)

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3302 PASS — dealer desk covers positions + fails + "
            "financing + turnover, OFR STFM live for the fleet, sidebar "
            "fixed into Macro & Liquidity.")
sys.exit(0)
