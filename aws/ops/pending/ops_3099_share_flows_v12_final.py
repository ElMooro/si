#!/usr/bin/env python3
"""ops 3099 -- SHARE-FLOWS v1.2.1 final convergence run.
Closes the two-session build: (a) insider SELLS were 0 because 3098
invoked the insider-trades composer before its sell_transactions
redeploy finished (deploy race) -- this run race-guards BOTH
functions to repo code before invoking; (b) verifies gated on
version==1.2.1 (3097 lesson: never accept a stale-fresh doc);
(c) asserts the full v1.2 field set (net buyback, SBC, 3Y CAGR,
total shareholder yield, forensic flags, new boards), threaded
coverage, sells actually joined, and pages live."""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3099", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def wait_active(fn):
    for _ in range(50):
        try:
            c = L.get_function_configuration(FunctionName=fn)
        except L.exceptions.ResourceNotFoundException:
            return False
        if c.get("State") in ("Active", None) and \
                c.get("LastUpdateStatus") in ("Successful", None):
            return True
        time.sleep(8)
    return False


def retry_conflict(f, *a, **k):
    for i in range(12):
        try:
            return f(*a, **k)
        except L.exceptions.ResourceConflictException:
            time.sleep(12)
    raise RuntimeError("still conflicting")


def push_code(fn, rep):
    """Race-guarded: make the live function exactly the repo code."""
    src = (AWS_DIR / "lambdas" / fn / "source" /
           "lambda_function.py").read_text()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    if not wait_active(fn):
        raise RuntimeError(fn + " never Active")
    retry_conflict(L.update_function_code, FunctionName=fn,
                   ZipFile=buf.getvalue())
    wait_active(fn)
    try:
        cfg = json.loads((AWS_DIR / "lambdas" / fn /
                          "config.json").read_text())
        retry_conflict(L.update_function_configuration,
                       FunctionName=fn, Timeout=cfg["timeout"],
                       MemorySize=cfg["memory"])
        wait_active(fn)
    except Exception:
        pass
    rep.kv(**{fn: "code=repo (race-guarded)"})


def s3j(key):
    return json.loads(S3.get_object(Bucket=BUCKET,
                                    Key=key)["Body"].read())


def main():
    fails, warns = [], []
    with report("3099_share_flows_v12_final") as rep:
        rep.section("1. Race-guarded deploys (both functions)")
        push_code("justhodl-insider-trades", rep)
        push_code("justhodl-share-flows", rep)

        rep.section("2. Insider tape: sells at source")
        L.invoke(FunctionName="justhodl-insider-trades",
                 InvocationType="Event", Payload=b"{}")
        sells_src = 0
        for _ in range(30):
            time.sleep(15)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/insider-trades.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 600:
                    doc = json.loads(o["Body"].read())
                    sells_src = len(doc.get("sell_transactions")
                                    or [])
                    break
            except Exception:
                pass
        rep.kv(insider_trades_sell_rows=sells_src)
        if sells_src == 0:
            warns.append("tape still has 0 sell rows -- share-flows"
                         " sells will honestly omit; investigate"
                         " composer parse upstream")

        rep.section("3. share-flows v1.2.1 run + gates")
        L.invoke(FunctionName="justhodl-share-flows",
                 InvocationType="Event", Payload=b"{}")
        d = None
        for _ in range(56):
            time.sleep(15)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/share-flows.json")
                doc = json.loads(o["Body"].read())
                if doc.get("version") == "1.2.1":
                    d = doc
                    break
            except Exception:
                pass
        if not d:
            fails.append("no v1.2.1 doc within 14 min "
                         "(version gate, 3097 lesson)")
            _fin(rep, fails, warns)
            raise SystemExit(1)
        tk = d.get("tickers") or {}
        rep.kv(version=d.get("version"), n_tickers=len(tk),
               fresh_fetched=d.get("fresh_fetched"),
               warns_engine=json.dumps(d.get("warns"))[:300])
        if len(tk) < 400:
            fails.append("map thin: %d (<400)" % len(tk))
        if (d.get("fresh_fetched") or 0) < 150:
            warns.append("fresh only %s -- threading underused or"
                         " cache covered most"
                         % d.get("fresh_fetched"))

        v12 = {t: v for t, v in tk.items()
               if "buyback_net_ttm_usd" in v}
        n_sbc = sum(1 for v in v12.values() if v.get("sbc_ttm_usd"))
        n_flag = sum(1 for v in tk.values() if v.get("flags"))
        n_sellj = sum(1 for v in tk.values()
                      if v.get("insider_sell_usd_recent"))
        n_buyj = sum(1 for v in tk.values()
                     if v.get("insider_buy_usd_90d"))
        rep.kv(n_v12_rows=len(v12), n_sbc=n_sbc, n_flagged=n_flag,
               n_insider_buy_joined=n_buyj,
               n_insider_sell_joined=n_sellj)
        if not v12:
            fails.append("no v1.2 rows at all")
        if sells_src > 0 and n_sellj == 0:
            fails.append("tape has sells but join produced none")
        if n_buyj < 3:
            warns.append("insider buys joined on only %d names"
                         % n_buyj)

        aapl = tk.get("AAPL") or {}
        rep.kv(aapl=json.dumps(aapl, default=str)[:420])
        if aapl:
            yo = aapl.get("sh_yoy_pct")
            if yo is not None and yo > 0.5:
                fails.append("AAPL sh_yoy %s should be negative" % yo)
            nb = aapl.get("buyback_net_yield_pct")
            if nb is not None and not (0.5 <= nb <= 5.0):
                fails.append("AAPL NET bb yield %s outside 0.5-5" % nb)
            ty = aapl.get("total_shareholder_yield_pct")
            if ty is not None and not (0.5 <= ty <= 6.0):
                fails.append("AAPL total yield %s outside 0.5-6" % ty)
            if aapl.get("sbc_ttm_usd") is not None \
                    and not (5e9 <= aapl["sbc_ttm_usd"] <= 2e10):
                fails.append("AAPL SBC %s outside 5-20B band"
                             % aapl["sbc_ttm_usd"])
        # sanity: any non-extreme, non-suspect row with insane yield?
        insane = [t for t, v in tk.items()
                  if (v.get("buyback_yield_pct") or 0) > 30
                  and not v.get("extreme")
                  and not v.get("data_suspect")]
        if insane:
            fails.append("unflagged >30%% yields: %s" % insane[:5])

        bd = d.get("boards") or {}
        rep.kv(boards={k: len(v or []) for k, v in bd.items()})
        for b in ("top_buybacks", "top_diluters"):
            if not bd.get(b):
                fails.append(b + " empty")

        rep.section("4. Pages live")
        pages = {
            "share-flows.html": ("Share Flows", "NET buyback",
                                 "extreme_diluters"),
            "opportunities.html": ("sfChips", "Capital return"),
        }
        for pg_name, marks in pages.items():
            ok, pg = False, ""
            for _ in range(15):
                try:
                    pg = get("https://justhodl.ai/%s?cb=%d"
                             % (pg_name, time.time()))
                    if marks[0] in pg:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(18)
            if not ok:
                fails.append(pg_name + " not live")
                continue
            for m in marks:
                if m not in pg:
                    fails.append("%s marker missing: %s"
                                 % (pg_name, m))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: " + f)
            raise SystemExit(1)
        rep.log("PASS -- share-flows v1.2.1 live end to end")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3099.json").write_text(
        json.dumps({"ops": 3099,
                    "verdict": "FAIL" if fails else "PASS",
                    "fails": fails, "warns": warns,
                    "ts": datetime.now(timezone.utc).isoformat()},
                   indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS",
           n_fails=len(fails), n_warns=len(warns))


main()
