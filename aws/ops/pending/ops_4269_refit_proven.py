"""
ops_4269 (delta 2) -- refit proven end-to-end [import re now deployed] (4268's gate passed vacuously).

4268 moved 86 official senate txns into the engine and 0 came out: eFD
dates are MM/DD/YYYY and the 90d cutoff compares ISO strings -- every
row silently dropped, and my gate accepted the empty result. Fixed:
_efd_date normalizer, truthful provenance on every writer (Quiver
narrative fully removed), and THIS gate refuses vacuous success:
artifact fresh + trade_source=congress_direct_official + >=20 trades +
party attribution printed with real names.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=330, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

fails = []
with report("4269_refit_proven") as r:
    r.heading("ops 4269 -- political-stocks v2, non-vacuous proof")
    ok_dep = False
    for _ in range(45):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-political-stocks")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm_dt = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm_dt).total_seconds() < 12 * 60:
                    ok_dep = True
                    break
        except Exception:
            pass
        time.sleep(8)
    if not ok_dep:
        fails.append("deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        body = (p["Payload"].read() or b"")[:200].decode("utf-8", "ignore")
        r.log("invoked: %s" % body)
        try:
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/political-stocks.json")["Body"].read())
            h = s3.head_object(Bucket=BUCKET,
                               Key="data/political-stocks.json")
            a = (datetime.now(timezone.utc)
                 - h["LastModified"]).total_seconds() / 60.0
            src2 = str(doc.get("trade_source") or "")
            cong = doc.get("congress") or {}
            n_total = cong.get("n_trades_total") or 0
            n_tick = cong.get("n_tickers") or 0
            if a >= 20:
                fails.append("artifact stale %.0f min" % a)
            if "congress_direct" not in src2:
                fails.append("trade_source=%r" % src2[:50])
            if n_total < 20 or n_tick < 5:
                fails.append("rows dropping again: total=%s tickers=%s"
                             % (n_total, n_tick))
            r.log("provenance: trade_source=%s status=%s data_source=%s"
                  % (src2[:40], doc.get("source_status"),
                     str(doc.get("data_source"))[:60]))
            r.log("congress block: trades=%s tickers=%s senate=%s "
                  "house=%s clusters=%s"
                  % (n_total, n_tick, cong.get("n_trades_senate"),
                     cong.get("n_trades_house"),
                     len(doc.get("clusters") or [])))
            samp = []
            for t in (doc.get("top_buys") or [])[:4] + \
                     (doc.get("clusters") or [])[:3]:
                for tr in (t.get("recent_trades") or [])[:1]:
                    samp.append((t.get("ticker"), tr))
            withp = 0
            for tk, tr in samp[:7]:
                if tr.get("party") not in (None, "", "?"):
                    withp += 1
                r.kv(ticker=tk, who=tr.get("politician"),
                     party=tr.get("party"), type=tr.get("type"),
                     amount=str(tr.get("amount"))[:22],
                     date=tr.get("date"))
            if samp and withp == 0:
                r.warn("zero party attribution in sample -- name_map "
                       "matching needs widening (disclosed)")
            if not fails:
                r.ok("OFFICIAL FEED LIVE: %s trades / %s tickers, "
                     "fresh %.1f min" % (n_total, n_tick, a))
        except Exception as e:
            fails.append("verify: %s" % str(e)[:120])
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4269 PASS -- refit proven with rows through the "
             "whole pipeline")
if fails:
    sys.exit(1)
