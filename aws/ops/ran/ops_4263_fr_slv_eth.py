"""
ops_4263 -- forward-returns v1.1.0: SLV + ETH join the 10y ER universe.

Closes the gap disclosed in ops 4262: SILVER and ETH ladder rows had no
strategic leg because forward-returns carried no SLV/ETH rows. v1.1.0
adds both with honest models -- SLV via Erb-Harvey (1.0% real +
breakeven, wider bands than gold), ETH as a conservative flat 12%
below BTC's 15% with the uncertainty living in p10/p90 bands, per the
engine's own BTC convention.

Chain: rebuild forward-returns -> verify SLV/ETH rows real -> re-run
quantum-desk -> assert SILVER + ETH strategic legs now live -> check
the page finally on edge (4257's CDN-lag warn).
"""
import json, sys, time, urllib.request
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=330, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

fails = []
with report("4263_fr_slv_eth") as r:
    r.heading("ops 4263 -- forward-returns v1.1.0 (SLV + ETH) chained "
              "into quantum-desk")

    r.section("1. forward-returns v1.1.0 rebuild")
    fr = None
    for i in range(40):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-forward-returns")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lam.invoke(FunctionName="justhodl-forward-returns",
                           InvocationType="RequestResponse", Payload=b"{}")
                fr = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/forward-returns.json")["Body"].read())
                if str(fr.get("version")) == "1.1.0":
                    break
        except Exception as e:
            r.log("wait: %s" % str(e)[:90])
        time.sleep(10)
    if not fr or str(fr.get("version")) != "1.1.0":
        fails.append("forward-returns v1.1.0 never landed (saw %s)"
                     % (fr or {}).get("version"))
    else:
        aset = fr.get("assets") or {}
        for sym in ("SLV", "ETH"):
            row = aset.get(sym)
            if not row:
                fails.append("%s row missing from forward-returns" % sym)
                continue
            r.ok("%s: er_10y=%s%% pctile=%s verdict=%s price=%s "
                 "vol=%s worst12m=%s"
                 % (sym, row.get("forward_er_10y_pct"),
                    row.get("current_vs_history_percentile"),
                    row.get("verdict"), row.get("current_price"),
                    row.get("vol_10y_pct"), row.get("worst_12mo_pct")))
        rk = (fr.get("rankings") or {}).get("by_forward_er") or []
        r.log("rankings now (%d): %s" % (len(rk), rk))

    r.section("2. quantum-desk picks them up")
    qd = None
    if not fails:
        try:
            lam.invoke(FunctionName="justhodl-quantum-desk",
                       InvocationType="RequestResponse", Payload=b"{}")
            qd = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/quantum-desk.json")["Body"].read())
        except Exception as e:
            fails.append("quantum-desk rerun: %s" % str(e)[:150])
    if qd:
        by = {x["class"]: x for x in qd.get("asset_ladder") or []}
        for cls in ("SILVER", "ETH"):
            row = by.get(cls)
            st = (row or {}).get("legs", {}).get("strategic")
            if st is None:
                fails.append("%s strategic leg still absent" % cls)
            else:
                r.ok("%s strategic leg live: %.3f (pctile %s) -- score "
                     "%s %s, %s legs"
                     % (cls, st, (row.get("audit") or {}).get("strategic"),
                        row.get("score"), row.get("verdict"),
                        len(row.get("legs_used", []))))
        n_s = sum(1 for x in (qd.get("asset_ladder") or [])
                  if x["legs"].get("strategic") is not None)
        r.log("strategic coverage: %d/%d rows"
              % (n_s, len(qd.get("asset_ladder") or [])))
        lad = qd.get("asset_ladder") or []
        r.log("ladder now: %s"
              % ", ".join("%s %.3f %s" % (x["class"], x["score"],
                                          x["verdict"]) for x in lad[:8]))

    r.section("3. page edge (closing 4257's CDN warn)")
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/quantum-desk.html",
            headers={"User-Agent": "justhodl-ops/4263",
                     "Cache-Control": "no-cache"})
        body = urllib.request.urlopen(req, timeout=25).read().decode(
            "utf-8", "ignore")
        if "Quantum Desk" in body and "quantum-desk.json" in body:
            r.ok("page LIVE on edge (%d bytes)" % len(body))
        else:
            r.warn("page serves but content unexpected (%d bytes)"
                   % len(body))
    except Exception as e:
        r.warn("edge still lagging: %s -- pages.yml carried it, CDN "
               "will settle" % str(e)[:100])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4263 PASS -- SLV + ETH in the 10y ER universe and "
             "wired through the desk")
if fails:
    sys.exit(1)
