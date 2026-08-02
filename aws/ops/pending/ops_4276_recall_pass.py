"""
ops_4276 -- recall pass: the glued-date generator, reparsed.

4275's probe showed the Clerk variant that concatenates dates
("P 06/12/202607/08/2026$1,001") -- word-boundary anchors made every
row invisible. v1.1 drops the anchors (regression: both generators
pass), adds a noise-line filter for the \\x00-padded Filing-Status /
Subholding rows, and a reparse_zero mode to re-walk the 12 marked
docs. Gate: histogram's zero-bucket shrinks materially, trades_total
jumps, political-stocks n_house rises, attribution sampled from the
congress dict (4275 shape dump).
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

def fresh_deploy(fn, minutes=15):
    for _ in range(45):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < minutes * 60:
                    return True
        except Exception:
            pass
        time.sleep(8)
    return False

fails = []
with report("4276_recall_pass") as r:
    r.heading("ops 4276 -- glued-date recall pass")

    r.section("1. reparse the zero-row dozen on v1.1")
    if not fresh_deploy("justhodl-house-ptr-extract"):
        fails.append("extractor deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-house-ptr-extract",
                       InvocationType="RequestResponse",
                       Payload=json.dumps({"reparse_zero": True}).encode())
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:220].decode("utf-8",
                                                          "ignore"))
        try:
            led = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/house-ptr-trades.json")["Body"].read())
            hist = {}
            for d in (led.get("docs") or {}).values():
                if d.get("status") == "parsed":
                    n = d.get("n_rows", 0)
                    hist[n] = hist.get(n, 0) + 1
            st = led.get("stats") or {}
            zeros = hist.get(0, 0)
            r.log("histogram now: %s -- trades_total=%s"
                  % (dict(sorted(hist.items())), st.get("trades_total")))
            if zeros > 5:
                fails.append("zero-bucket still %d after v1.1" % zeros)
            else:
                r.ok("zero-bucket 12 -> %d; recall recovered" % zeros)
            for t in (led.get("trades") or [])[:6]:
                r.kv(who=t.get("Representative"), ticker=t.get("Ticker"),
                     tx=t.get("Transaction"),
                     date=t.get("TransactionDate"),
                     amount=str(t.get("Range"))[:22])
        except Exception as e:
            fails.append("ledger: %s" % str(e)[:110])

    r.section("2. both chambers, attribution visible")
    if not fails:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:180].decode("utf-8",
                                                          "ignore"))
        try:
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/political-stocks.json")["Body"].read())
            cong = doc.get("congress") or {}
            nh = cong.get("n_trades_house") or 0
            r.log("chambers: senate=%s house=%s tickers=%s"
                  % (cong.get("n_trades_senate"), nh,
                     cong.get("n_tickers")))
            if nh < 10:
                fails.append("n_house=%s after recall pass" % nh)
            n_att = n_tot = shown = 0
            pools = [v for v in cong.values()
                     if isinstance(v, list) and v
                     and isinstance(v[0], dict)
                     and "recent_trades" in v[0]]
            for pool in pools:
                for tk in pool:
                    for tr in (tk.get("recent_trades") or []):
                        n_tot += 1
                        if tr.get("party") in ("D", "R", "I"):
                            n_att += 1
                            if shown < 6:
                                shown += 1
                                r.kv(who=tr.get("politician"),
                                     party=tr.get("party"),
                                     chamber=tr.get("chamber"),
                                     ticker=tk.get("ticker"),
                                     tx=tr.get("type"))
            if n_tot:
                rate = 100.0 * n_att / n_tot
                (r.ok if rate >= 40 else r.warn)(
                    "party attribution: %d/%d (%.0f%%) via name_map"
                    % (n_att, n_tot, rate))
            else:
                r.warn("no recent_trades pools inside congress dict "
                       "either -- shape note stands")
        except Exception as e:
            fails.append("political verify: %s" % str(e)[:110])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4276 PASS -- recall recovered, two chambers live "
             "with visible attribution")
if fails:
    sys.exit(1)
