"""ops_5124 -- every symbol from the warehouse: tv-bars universe lane + equity warehouse probe.

Khalid: "all my data is on data.html so it should be pulled from there ... every
single ticker should be pulled ... i have symbols that say this symbol is
available on tradingview and it shouldnt say that".

The message is the free TradingView widget's paywall text, shown whenever
chart-pro's AUTO engine falls back to the widget (any symbol Polygon does not
carry: TVC:VIX, ECONOMICS:*, foreign listings...). The fix is to never chart
through the widget: bank every symbol's bars in the warehouse and serve from
S3. justhodl-tv-bars already speaks TradingView's chart socket with Khalid's
session (full history since inception); v1.1 adds
  mode=pull    : bank any EXCHANGE:SYMBOL on demand -> data/warm/tv-bars/universe/
  mode=refresh : nightly fan-out, countback 120, append-only union

  S1 deploy tv-bars v1.1 (code only; schedule/lease untouched)
  S2 prove pulls across symbol families (index, macro, foreign, fx, crypto,
     US equity/ETF, OTC) -- bars, first/last date, latency
  S3 probe data/warm/us-equities-daily/ (per-ticker daily bars?) and
     data/warm/tv-bars/ (existing ICE lane) layouts
  S4 nightly refresh schedule (02:30 UTC, fan-out 4)
Gates: at least 6 of the 10 probe symbols bank with >1000 bars; TVC:VIX and
NASDAQ:AAPL must succeed with history before 2015.
"""
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-tv-bars"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)


def main():
    with report("5124-tv-bars-universe") as r:
        r.heading("ops 5124 -- every symbol from the warehouse: tv-bars universe lane")
        fails = []
        r.section("S1 deploy tv-bars v1.1")
        cur = lam.get_function_configuration(FunctionName=FN)
        env = (cur.get("Environment") or {}).get("Variables") or {"S3_BUCKET": B}
        desc = (cur.get("Description") or "")[:200]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=600, memory=1024, create_function_url=False, smoke=False,
                      description=(desc + " | v1.1 universe pull/refresh (ops 5124)")[:255])
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        r.kv(step="S1", state=cfg.get("State"), update=cfg.get("LastUpdateStatus"))

        r.section("S2 prove pulls across symbol families")
        probes = ["TVC:VIX", "NASDAQ:AAPL", "ECONOMICS:DEUR", "SSE:000001", "FX:EURUSD", "COINBASE:BTCUSD", "AMEX:SPY", "SP:SPX", "OTC:AAAIF", "CME_MINI:ES1!"]
        ok_n = 0
        for chunk in (probes[:5], probes[5:]):
            t = time.time()
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"mode": "pull", "tv_symbols": chunk, "budget": 35}).encode())
            body = json.loads(resp["Payload"].read() or b"{}")
            r.log(f"  invoke {chunk} -> {int((time.time() - t) * 1000)}ms ok={body.get('ok')} err={body.get('error')} universe={body.get('universe')}")
            for sym, res in (body.get("results") or {}).items():
                r.log(f"    {sym:<18} ok={res.get('ok')} n={res.get('n')} first={res.get('first')} last={res.get('last')} err={res.get('error')}")
                if res.get("ok") and (res.get("n") or 0) > 1000:
                    ok_n += 1
                if sym in ("TVC:VIX", "NASDAQ:AAPL"):
                    if not res.get("ok"):
                        fails.append(f"{sym} did not bank: {res.get('error')}")
                    elif (res.get("first") or "9999") > "2015-01-01":
                        fails.append(f"{sym} history starts {res.get('first')} (expected long history)")
        r.kv(step="S2", banked_over_1000=ok_n, probes=len(probes))
        if ok_n < 6:
            fails.append(f"only {ok_n}/10 probe symbols banked with >1000 bars")
        # a banked doc's shape
        try:
            o = s3.get_object(Bucket=B, Key="data/warm/tv-bars/universe/TVC__VIX.json.gz")
            d = json.loads(gzip.decompress(o["Body"].read()))
            r.log(f"  TVC__VIX doc: keys={list(d.keys())} n={d.get('n')} first={d.get('first_date')} last={d.get('last_date')} bars[0]={d.get('bars', [None])[0]} bars[-1]={d.get('bars', [None])[-1]}")
        except Exception as e:  # noqa: BLE001
            r.warn(f"  TVC__VIX doc unreadable: {str(e)[:100]}")

        r.section("S3 warehouse layouts: us-equities-daily, tv-bars")
        for pre in ("data/warm/us-equities-daily/", "data/warm/tv-bars/"):
            d = s3.list_objects_v2(Bucket=B, Prefix=pre, Delimiter="/", MaxKeys=40)
            subs = [c["Prefix"] for c in d.get("CommonPrefixes", [])]
            keys = [(o["Key"], o["Size"]) for o in d.get("Contents", [])][:12]
            tok, n = None, 0
            for _ in range(5):
                kw = {"Bucket": B, "Prefix": pre, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                dd = s3.list_objects_v2(**kw)
                n += len(dd.get("Contents", []))
                tok = dd.get("NextContinuationToken")
                if not tok:
                    break
            r.log(f"  {pre}: objects={n}{'+' if tok else ''} sub={subs[:10]} keys={keys}")
            sample = next((k for k, sz in keys if sz > 300 and not k.endswith("_index.json") and not k.endswith("_state.json") and "manifest" not in k), None)
            if sample:
                raw = s3.get_object(Bucket=B, Key=sample)["Body"].read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                r.log(f"    HEAD {sample}: {raw[:500].decode('utf-8', 'replace')}")
        try:
            idx = json.loads(s3.get_object(Bucket=B, Key="data/warm/tv-bars/_index.json")["Body"].read())
            r.log(f"  ICE lane index: n_symbols={idx.get('n_symbols')} sample={list((idx.get('symbols') or {}).items())[:2]}")
        except Exception as e:  # noqa: BLE001
            r.log(f"  ICE lane index unreadable: {str(e)[:80]}")

        r.section("S4 nightly refresh schedule")
        sd = {"Name": FN + "-universe-refresh", "ScheduleExpression": "cron(30 2 * * ? *)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": json.dumps({"mode": "refresh", "fanout": 4}),
                         "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
              "State": "ENABLED", "Description": "TradingView universe bars: nightly append-only refresh (countback 120) of every symbol chart-pro has opened"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-tv-bars-universe-refresh cron(30 2 * * ? *)")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: tv-bars v1.1 banks any TradingView symbol on demand and refreshes nightly; chart-pro can now serve every symbol from S3")


if __name__ == "__main__":
    main()
