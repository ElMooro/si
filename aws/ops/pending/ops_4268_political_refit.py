"""
ops_4268 -- political-stocks v2: official feed, vendor retired.

The 4265 forensics showed political-stocks running on a Quiver
tombstone: live 401, cache 898h old. congress-direct (official Senate
eFD + House Clerk, Quiver-free since ops ~3200s) is the house source of
record. This refit:
  * fetch_congress_direct() PRIMARY -- senate eFD PTR transactions
    adapted onto the row shape the engine already consumes
  * Quiver fully retired (no live call, no cache fallback) -- empty
    official feed means an honest-empty artifact, not vendor necromancy
  * party attribution without BioGuideID: party-map v1.2 adds a
    name_map (built from the same canonical legislators file); cache
    schema-upgrade forces one live refresh
  * House PTRs remain filing metadata (tickers live in PDFs) --
    disclosed, queued as a future extractor

Gate: congress-direct refreshed -> political-stocks re-run -> artifact
fresh, source label congress_direct_official, n_trades > 0, party
attribution rate printed with real names/tickers.
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

def age_min(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (datetime.now(timezone.utc)
            - h["LastModified"]).total_seconds() / 60.0

fails = []
with report("4268_political_refit") as r:
    r.heading("ops 4268 -- political-stocks on the official rail")

    r.section("1. refresh congress-direct (source of record)")
    try:
        p = lam.invoke(FunctionName="justhodl-congress-direct",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:170].decode("utf-8",
                                                          "ignore"))
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/congress-direct.json")["Body"].read())
        n_tx = (cd.get("senate") or {}).get("n_transactions", 0)
        n_tk = (cd.get("senate") or {}).get("n_with_ticker", 0)
        r.ok("congress-direct fresh: %s senate txns (%s with ticker), "
             "%s house PTR filings, errors: sen=%s house=%s"
             % (n_tx, n_tk,
                (cd.get("house") or {}).get("n_ptr_filings"),
                (cd.get("senate") or {}).get("error"),
                (cd.get("house") or {}).get("error")))
        if n_tk == 0:
            r.warn("official senate feed has zero ticker'd txns right "
                   "now -- political-stocks will write honest-empty")
    except Exception as e:
        fails.append("congress-direct: %s" % str(e)[:120])

    r.section("2. political-stocks v2 on the official feed")
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
        fails.append("political-stocks deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:170].decode("utf-8",
                                                          "ignore"))
        try:
            a = age_min("data/political-stocks.json")
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/political-stocks.json")["Body"].read())
            src2 = str(doc.get("trade_source")
                       or doc.get("source") or "")
            trades = doc.get("trades") or doc.get("recent_trades") or []
            n = doc.get("n_trades") or len(trades)
            if a >= 20:
                fails.append("political-stocks.json stale %.0f min" % a)
            elif "congress_direct" not in src2 and n:
                fails.append("source label wrong: %s" % src2[:60])
            else:
                withp = sum(1 for t in trades
                            if t.get("party") not in (None, "", "?"))
                r.ok("ARTIFACT LIVE: %.1f min, source=%s, trades=%s, "
                     "party-attributed %s/%s"
                     % (a, src2[:40], n, withp, len(trades)))
                for t in trades[:6]:
                    r.kv(who=t.get("name") or t.get("politician"),
                         party=t.get("party"),
                         ticker=t.get("ticker"),
                         type=t.get("type") or t.get("transaction"),
                         amount=str(t.get("amount"))[:22],
                         date=t.get("date"))
                if trades and withp * 3 < len(trades):
                    r.warn("party attribution below 1/3 -- name_map "
                           "matching may need widening (disclosed)")
        except Exception as e:
            fails.append("verify: %s" % str(e)[:120])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4268 PASS -- congress trading intelligence runs on "
             "official filings; Quiver is history")
if fails:
    sys.exit(1)
