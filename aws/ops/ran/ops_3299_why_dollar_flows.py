"""ops 3299 — why.html DOLLAR FLOWS + top vitals. Engine: 13f-positions
now mirrors a WHALE-subset $ trio (clone-alpha skill>=55 else top-3
positive-skill) inside the same dv loop as the headline (reconciles by
construction) and publishes data/13f-flows-by-ticker.json — full
per-ticker b/s/n + wb/ws/wn + top named buyers/sellers. Page: vitals
strip (P/E·PEG·P/S·P/B·EV/EBITDA·FCF·shares out) at the TOP with
FLASHING-RED dilution when share count keeps rising, plus the
institutions/whales/retail $-buying-vs-$-selling panel (retail = FINRA
off-exchange internalizer weekly tape, $ est = shares x px). Truth
bands from the proven 2026-Q1 prints: GOOGL bought ~$11.6B, ICLN sold
~$28.5B, TSLA sold ~$16.1B; mirror identity vs dollar_flows boards."""
import json
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
FN = "justhodl-13f-positions"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
FEED = "data/13f-flows-by-ticker.json"


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def http(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-ops-3299"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


with report("3299_why_dollar_flows") as rep:
    fails, warns = [], []

    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 600),
                  memory=int(live.get("MemorySize") or 1024),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")

    rep.section("2. Poll the new per-ticker $ ledger")
    fd = None
    for _ in range(75):
        time.sleep(15)
        fd = s3_json(FEED)
        if fd and fd.get("as_of", "") >= mark:
            break
    if not (fd and fd.get("as_of", "") >= mark):
        fails.append("flows-by-ticker never freshened")
        fd = fd or {}

    tmap = fd.get("t") or {}
    rep.kv(n_tickers=fd.get("n_tickers"),
           whale_funds=fd.get("whale_funds"))
    if (fd.get("n_tickers") or 0) < 800:
        fails.append("n_tickers %s < 800" % fd.get("n_tickers"))

    wf = fd.get("whale_funds") or []
    if len(wf) < 3:
        fails.append("whale set < 3 funds: %s" % wf)
    if "RENAISSANCE" not in json.dumps(wf).upper():
        warns.append("Renaissance not in whale set: %s" % wf)

    def band(tk, field, lo, hi):
        v = (tmap.get(tk) or {}).get(field)
        rep.kv(**{"%s_%s" % (tk, field): v})
        if v is None or not (lo <= v <= hi):
            fails.append("%s.%s=%s outside [%.1e, %.1e]"
                         % (tk, field, v, lo, hi))

    band("GOOGL", "b", 5e9, 2.2e10)      # ~ $11.6B bought
    band("ICLN", "s", 1.2e10, 4.5e10)    # ~ $28.5B sold
    band("TSLA", "s", 8e9, 3.0e10)       # ~ $16.1B sold

    n_whale_active = sum(1 for r in tmap.values()
                         if (r.get("wb") or 0) + (r.get("ws") or 0) > 0)
    n_named = sum(1 for r in tmap.values() if r.get("fb") or r.get("fs"))
    rep.kv(tickers_with_whale_prints=n_whale_active,
           tickers_with_named_contributors=n_named)
    if n_whale_active < 25:
        fails.append("only %d tickers carry whale prints" % n_whale_active)
    if n_named < 200:
        fails.append("only %d tickers carry named buyers/sellers" % n_named)

    rep.section("3. Mirror identity vs main doc boards")
    d = s3_json("data/13f-positions.json") or {}
    if d.get("generated_at", "") < mark:
        warns.append("main doc older than invoke mark (boards may be "
                     "prior run) — identity checked anyway")
    top = ((d.get("dollar_flows") or {}).get("most_bought_usd") or [])
    if top:
        tk0 = top[0].get("ticker")
        want = top[0].get("bought_usd")
        got = (tmap.get(tk0) or {}).get("b")
        rep.kv(identity_ticker=tk0, board_bought=want, feed_bought=got)
        if tk0 and want and got != round(want):
            fails.append("mirror identity broken: %s board=%s feed=%s"
                         % (tk0, want, got))
    else:
        warns.append("no most_bought_usd board in main doc")

    rep.section("4. Page deploy markers")
    raw = http("https://raw.githubusercontent.com/ElMooro/si/main/why.html")
    for mk in ("jhVitalsTop", "fillJHDollarFlows", "jh-flashred"):
        if mk not in raw:
            fails.append("marker %s missing from repo why.html" % mk)
    live_ok = False
    for _ in range(16):
        pg = http("https://justhodl.ai/why.html?ops=3299")
        if "fillJHDollarFlows" in pg:
            live_ok = True
            break
        time.sleep(30)
    rep.kv(live_page_marker=live_ok)
    if not live_ok:
        warns.append("live why.html not showing marker yet (CDN/pages "
                     "lag) — repo copy verified")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3299 PASS — why.html now answers WHO is buying vs "
            "selling in dollars, and dilution flashes red at the top.")
sys.exit(0)
