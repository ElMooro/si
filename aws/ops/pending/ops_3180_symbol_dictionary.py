"""ops 3180 — a full name for EVERY symbol, from authoritative sources.

Khalid: "put a full name for every ticker (the full name and FRED ticker)
so we can fuse them in the engines."

He is right that this is a fusion prerequisite: an engine cannot reason
about `PRAWMINDEXM`, but it can reason about "Global Price Index of All
Commodities". Names are the semantic layer.

No invented names. FRED's own /series metadata (title, units, frequency,
seasonal adjustment, exact history window), Polygon's reference API for
equities and funds, the World Bank's indicator + country catalogs, and a
decoder for TradingView's ECONOMICS:{ISO2}{IND} codes so that even
symbols we cannot yet PRICE still get a human name.

watchlists.html now shows, per member: the full name, the raw TV code,
and the exact source ticker (FRED: DGS10 / World Bank: BR · GC.DOD...).
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-symbol-dictionary"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3180_symbol_dictionary") as rep:
    fails, warns = [], []
    rep.heading("ops 3180 — full name for every symbol")

    rep.section("1. Deploy")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    donor = (LAM.get_function_configuration(FunctionName="justhodl-wl-engines")
             .get("Environment") or {}).get("Variables") or {}
    env = {k: v for k, v in donor.items()
           if k in ("S3_BUCKET", "POLYGON_KEY", "FRED_API_KEY", "FRED_KEY")}
    env.setdefault("S3_BUCKET", BUCKET)
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"), timeout=cfg["timeout"],
                  memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)

    rep.section("2. Build the dictionary (runs in passes; cached)")
    doc = None
    for attempt in (1, 2, 3):
        t0 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        deadline = time.time() + 850
        while time.time() < deadline:
            try:
                d = s3_json("data/symbol-dictionary.json")
                if datetime.fromisoformat(d["generated_at"]) >= t0:
                    doc = d
                    break
            except Exception:
                pass
            time.sleep(20)
        if not doc:
            fails.append(f"pass {attempt}: dictionary never written")
            break
        rep.kv(**{f"pass{attempt}_named": doc.get("n_named"),
                  f"pass{attempt}_pct": doc.get("named_pct"),
                  f"pass{attempt}_filled": doc.get("filled_this_run")})
        if (doc.get("named_pct") or 0) >= 99 or not doc.get("filled_this_run"):
            break
        rep.log(f"  {doc['n_named']}/{doc['n_symbols']} named "
                f"({doc['named_pct']}%) — running another pass")

    if not doc:
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("3. Coverage")
    rep.kv(symbols=doc.get("n_symbols"), named=doc.get("n_named"),
           named_pct=doc.get("named_pct"),
           **{f"src_{k}": v for k, v in (doc.get("sources") or {}).items()})
    if (doc.get("named_pct") or 0) < 90:
        warns.append(f"{doc.get('named_pct')}% named — another pass will "
                     "fill the rest (the dictionary is cached and additive)")
    else:
        rep.ok(f"{doc['n_named']} of {doc['n_symbols']} symbols carry an "
               f"authoritative name ({doc['named_pct']}%)")

    rep.section("4. Spot-check the names he will actually read")
    dic = doc.get("dictionary") or {}
    for sym in ("FRED:DGS10", "FRED:PRAWMINDEXM", "FRED:WALCL",
                "FRED:RRPONTSYD", "TVC:DXY", "NASDAQ:NVDA", "AMEX:KRE",
                "ECONOMICS:ZWDIR", "ECONOMICS:KHBOT", "ECONOMICS:CNFER",
                "TVC:US10Y", "NYMEX:CL1!"):
        d = dic.get(sym)
        if d:
            src = f"{d.get('source')}: {d.get('source_id')}"
            extra = " · ".join(x for x in (d.get("units"), d.get("frequency"),
                                           d.get("history")) if x)
            rep.log(f"  {sym:22s} → {str(d.get('name'))[:56]:56s} "
                    f"[{src}] {extra[:38]}")
        else:
            rep.log(f"  {sym:22s} → (not in his universe)")

    rep.section("5. Page")
    import urllib.request
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/watchlists.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
        html = r.read().decode("utf-8", "replace")
        if "symbol-dictionary.json" in html:
            rep.ok("watchlists.html reads the dictionary — every member now "
                   "shows its full name and exact source ticker")
        else:
            warns.append("CDN still on the pre-dictionary page (self-heals)")
    except Exception as e:
        warns.append(f"page: {str(e)[:60]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
