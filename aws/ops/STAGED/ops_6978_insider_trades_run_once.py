"""ops 6978 - ONE run of justhodl-insider-trades by hand, then the document read back.

Khalid 2026-11-26 11:28 ET: "look into : research. insiders, they seem broken". The Insiders page is broken to the eye
because one misfiled Form 4 line (3,149,470 LONA shares "at $3,149.47" = $9.92B) is the whole bar chart and nine tenths
of the total. The price check that sets it aside was deployed at 14:57 UTC, but the engine's own run is 21:10 UTC, so
the page would stay wrong for five more hours. This op invokes the function once (its normal event, its own code, its
own writes) and reads the result. The 21:10 UTC run still happens; this is a second run today, disclosed.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6978_insider_trades_run_once"
REGION, LIVE, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-insider-trades"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=330, connect_timeout=20, retries={"max_attempts": 0}))
    fails = []
    with report(NAME) as r:
        r.heading("6978 - one run of the insider engine by hand, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)

        before = json.loads(s3.get_object(Bucket=LIVE, Key="data/insider-trades.json")["Body"].read())
        r.log("before: version %s, written %s, %s buys, total $%s" % (before.get("version"), before.get("generated_at"), (before.get("stats") or {}).get("total_buys"), format((before.get("stats") or {}).get("total_value_usd") or 0, ",.0f")))
        if before.get("version") == "3.1" and "price_check" in before:
            r.log("the document already carries the price check: the function is not invoked again")
        else:
            out = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
            body = out["Payload"].read().decode("utf-8", "replace")
            r.log("invoked once: status %s, function error %s, answer %s" % (out.get("StatusCode"), out.get("FunctionError"), body[:300]))
            ck(out.get("StatusCode") == 200 and not out.get("FunctionError"), "the run finished without an error")
        d = json.loads(s3.get_object(Bucket=LIVE, Key="data/insider-trades.json")["Body"].read())
        st, aside = d.get("stats") or {}, d.get("set_aside") or []
        r.log("after: version %s, written %s, %s buys, total $%s, clusters %s, big buys %s" % (d.get("version"), d.get("generated_at"), st.get("total_buys"), format(st.get("total_value_usd") or 0, ",.0f"), st.get("cluster_count"), st.get("big_buy_count")))
        r.log("price check: " + json.dumps(d.get("price_check")))
        for a in aside[:8]:
            r.log("   set aside: %s %s %s shares at %s = $%s | tape %s | %s" % (a.get("ticker"), a.get("side"), format(int(a.get("shares") or 0), ","), a.get("price"), format(float(a.get("value") or 0), ",.0f"), a.get("tape_price"), a.get("reason")))
        top = sorted([t for t in d.get("transactions") or [] if isinstance(t, dict) and t.get("value")], key=lambda t: -t["value"])[:6]
        r.log("largest purchases now: " + ", ".join("%s $%s" % (t["ticker"], format(t["value"], ",.0f")) for t in top))
        ck(d.get("version") == "3.1" and isinstance(d.get("price_check"), dict) and "error" not in d["price_check"], "the document is 3.1 and carries the price check")
        ck(any(a.get("ticker") == "LONA" for a in aside) and all(t.get("ticker") != "LONA" or (t.get("value") or 0) < 1e9 for t in d.get("transactions") or [] if isinstance(t, dict)), "the LONA line is set aside and no longer among the purchases")
        ck((st.get("total_value_usd") or 9e12) < 3e9, "the 30-day buying total no longer carries it ($%s)" % format(st.get("total_value_usd") or 0, ",.0f"))
        ck(len(d.get("set_aside_as_filed") or []) == len(aside) and (d.get("price_check") or {}).get("tickers_on_the_book", 0) > 50, "the rows set aside are kept as filed and the factor book answered")
        r.kv(failed_checks=len(fails), buys=st.get("total_buys"), total_usd=st.get("total_value_usd"), set_aside=len(aside))
        if fails:
            r.fail("; ".join(fails)[:600])
            sys.exit(1)
        r.ok("the insider document is right as of this run")


if __name__ == "__main__":
    main()
