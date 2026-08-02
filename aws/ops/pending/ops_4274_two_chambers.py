"""
ops_4274 -- two chambers proven + parser recall inspected.

4273 landed the extractor (17/20 parsed, 3 honest no_text, real names:
Fields/TSM, Liccardo/NVDA) but (a) 5 trades from 17 docs demands a
recall histogram, and (b) political-stocks raced its own deploy AGAIN
so n_house=0 reflected old code. This op: histogram -> wait -> re-run
-> HARD gate n_house>0 -> attribution sample from ANY recent_trades
pool (score gates were hiding the sample).
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
with report("4274_two_chambers") as r:
    r.heading("ops 4274 -- two chambers, one tape")

    r.section("1. parser recall histogram")
    try:
        led = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/house-ptr-trades.json")["Body"].read())
        hist, zero_docs = {}, []
        for did, d in (led.get("docs") or {}).items():
            if d.get("status") != "parsed":
                continue
            n = d.get("n_rows", 0)
            hist[n] = hist.get(n, 0) + 1
            if n == 0:
                zero_docs.append((did, (d.get("filer") or "?")[:22]))
        r.log("rows-per-parsed-doc: %s" % dict(sorted(hist.items())))
        if zero_docs:
            r.warn("%d/%d parsed docs yielded ZERO rows -- recall gap, "
                   "sample doc_ids for layout study: %s"
                   % (len(zero_docs),
                      sum(hist.values()), zero_docs[:4]))
        else:
            r.ok("every parsed doc yielded rows")
    except Exception as e:
        fails.append("histogram: %s" % str(e)[:100])

    r.section("2. political-stocks with the merge actually deployed")
    ok2 = False
    for _ in range(45):
        try:
            c2 = lam.get_function_configuration(
                FunctionName="justhodl-political-stocks")
            if c2.get("LastUpdateStatus") in (None, "Successful") \
                    and c2.get("State") == "Active":
                lm2 = datetime.strptime(
                    c2["LastModified"].split(".")[0],
                    "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                # merge shipped in the 4273 push ~17:30Z; accept 60 min
                if (RUN_START - lm2).total_seconds() < 60 * 60:
                    ok2 = True
                    break
        except Exception:
            pass
        time.sleep(8)
    if not ok2:
        fails.append("political-stocks deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:190].decode("utf-8",
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
            if nh < 1:
                fails.append("house ledger has trades but n_house=%s"
                             % nh)
            n_att = n_tot = shown = 0
            pools = [doc.get(g) or [] for g in
                     ("top_buys", "top_sells", "clusters")]
            if not any(pools):
                pools = [v for v in doc.values()
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
                    "party attribution via name_map: %d/%d (%.0f%%)"
                    % (n_att, n_tot, rate))
            else:
                r.warn("no recent_trades pools found to sample "
                       "(artifact shape note for next pass)")
        except Exception as e:
            fails.append("verify: %s" % str(e)[:110])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4274 PASS -- both chambers merged on official rails "
             "with attribution visible")
if fails:
    sys.exit(1)
