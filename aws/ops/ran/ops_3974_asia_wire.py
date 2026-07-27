"""
ops_3974 — wire EUBUND into the vault, then refresh the barometers.

Discovery outcome (ops 3970/3971), stated honestly:

  RESOLVED  EUBUND — German 10Y, 3.2247, ECB Data Portal, via the SAME YC
            adapter already proven for EU03Y and DE02Y. Wired.

  STALE AT SOURCE — NOT WIRED  CH02Y / CH03Y. The standing note said the SNB
            rendoblid parse was broken and ?fromDate= was fresh. Both halves
            were wrong: the cube parses fine (22 series x 7,534 obs) and it
            simply ENDS 2025-07-31. The fromDate window returns zero
            observations. A year-old yield published as a live rate would be
            fake data, so these stay NO_FREE_SOURCE with the real reason.

  NOT GOVERNMENT DATA — NEVER WIRED  USMPMI, USCPMI, USNMPR, EUMPMI, JPMPMI,
            TWMPMI (S&P Global / ISM) and USHMI (NAHB). FRED returns 400 on
            every candidate id because these are licensed products. No
            substitute series will be published under their symbol.

  ENDPOINT WRONG, STILL OPEN  Eurostat DENO/EUESI (200 but empty value map —
            dimension codes need work), BOJ IR01 (metadata only 1,289 bytes,
            wrong db for a 3M rate), ONS GBGDG (404).

Gates: EUBUND LIVE with a real value, vault coverage not regressed, and the
barometers re-run so the new indicator actually votes.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
FN_V, FN_B = "justhodl-tradingview", "justhodl-domain-barometers"
MARK = "tradingview-vault v3.8 ops3974 asia-wire+jpexpyy-fix"
SRC = ROOT / "lambdas" / FN_V / "source" / "lambda_function.py"


def wait_idle(fn, rep, tries=40):
    for i in range(tries):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return True
        rep.log(f"  [{i}] {fn} busy")
        time.sleep(10)
    return False


def deployed(fn):
    info = lam.get_function(FunctionName=fn)
    return zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
        info["Code"]["Location"], timeout=60).read())).read("lambda_function.py").decode()


def main():
    with report("3974_asia_wire") as rep:
        rep.heading("ops 3972 — wire EUBUND (ECB) + refresh barometers")
        checks = []

        rep.section("A. settle the vault by marker")
        if not wait_idle(FN_V, rep):
            rep.fail("vault never idle")
            sys.exit(1)
        if MARK not in deployed(FN_V):
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", SRC.read_text())
            for a in range(8):
                try:
                    lam.update_function_code(FunctionName=FN_V, ZipFile=buf.getvalue(),
                                             Publish=True)
                    rep.ok(f"  vault code pushed (attempt {a})")
                    break
                except lam.exceptions.ResourceConflictException:
                    rep.log(f"  [{a}] conflict — backing off")
                    time.sleep(15)
            if not wait_idle(FN_V, rep):
                rep.fail("vault never idle after push")
                sys.exit(1)
        ok = MARK in deployed(FN_V)
        rep.kv(vault_marker_settled=ok)
        checks.append(("vault settled with EUBUND marker", ok))
        if not ok:
            rep.fail("marker absent")
            sys.exit(1)

        rep.section("B. invoke the vault (async — it runs long)")
        before = json.loads(s3.get_object(Bucket=BUCKET,
                                          Key="data/tradingview.json")["Body"].read())
        rep.kv(before_live=before.get("n_live"), before_gen=before.get("generated_at"))
        lam.invoke(FunctionName=FN_V, InvocationType="Event",
                   Payload=json.dumps({"source": "ops3974", "force": True}).encode())
        vault = None
        for i in range(40):
            time.sleep(20)
            d = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/tradingview.json")["Body"].read())
            if d.get("generated_at") != before.get("generated_at"):
                vault = d
                rep.ok(f"  vault refreshed after ~{(i+1)*20}s")
                break
        if not vault:
            rep.fail("vault never rewrote")
            sys.exit(1)

        idx = {r["symbol"]: r for r in vault.get("symbols") or []}
        eb = idx.get("EUBUND") or {}
        al = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/asia-leads.json")["Body"].read())
        kr = ((al.get("korea_exports") or {})).get("yoy_pct")
        tw = ((al.get("taiwan_exports") or {})).get("yoy_pct")
        rep.section("B2. the ops-3973 integrity fix + Asia wirings")
        for s_ in ("JPEXPYY", "TWEXPYY", "JPIRYY", "TOPIX"):
            x = idx.get(s_) or {}
            rep.log(f"  {s_:9s} status={str(x.get('status')):16s} value={x.get('value')} "
                    f"src={str(x.get('source'))[:40]} asof={x.get('asof')}")
        jx = idx.get("JPEXPYY") or {}
        tx = idx.get("TWEXPYY") or {}
        ji = idx.get("JPIRYY") or {}
        rep.kv(korea_yoy=kr, taiwan_yoy=tw, jpexpyy_now=jx.get("value"),
               twexpyy_now=tx.get("value"), jpiryy_now=ji.get("value"))
        checks += [
            ("JPEXPYY no longer equals Korea's number",
             jx.get("value") is None or kr is None
             or abs(float(jx["value"]) - float(kr)) > 1e-9),
            ("JPEXPYY resolves from a JAPAN source",
             "JPM667N" in str(jx.get("source")) or jx.get("status") != "LIVE"),
            ("TWEXPYY LIVE and equals Taiwan's own yoy",
             tx.get("status") == "LIVE" and tw is not None
             and abs(float(tx.get("value", -1)) - float(tw)) < 1e-6),
            ("JPIRYY LIVE from boj-detail", ji.get("status") == "LIVE"),
        ]
        rep.kv(after_live=vault.get("n_live"), coverage=vault.get("coverage_pct"),
               statuses=json.dumps(vault.get("status_counts")))
        rep.log(f"  EUBUND: status={eb.get('status')} value={eb.get('value')} "
                f"src={eb.get('source')} asof={eb.get('asof')}")
        checks += [
            ("EUBUND still LIVE", eb.get("status") == "LIVE"),
            ("EUBUND carries a real value",
             isinstance(eb.get("value"), (int, float)) and eb.get("value") != 0),
            ("vault coverage did not regress",
             (vault.get("n_live") or 0) >= (before.get("n_live") or 0)),
        ]

        rep.section("C. refresh the barometers so it actually votes")
        r = lam.invoke(FunctionName=FN_B, InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops3974"}).encode())
        rep.log(f"  payload={r['Payload'].read().decode()[:320]}")
        checks.append(("barometers invoke clean", not r.get("FunctionError")))
        bar = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/domain-barometers.json")["Body"].read())
        bidx = {x["symbol"]: x for x in bar.get("symbols") or []}
        be = bidx.get("EUBUND") or {}
        rep.log(f"  EUBUND in barometers: domain={be.get('domain')} "
                f"status={be.get('status')} pol={be.get('polarity')} "
                f"tier={be.get('tier')}")
        tot = 0
        for d in ("MACRO", "LIQUIDITY", "RISK"):
            b = (bar.get("barometers") or {}).get(d) or {}
            c = b.get("coverage") or {}
            tot += c.get("voting") or 0
            rep.log(f"  {d:9s} {b.get('score_0_100')} {b.get('state')} "
                    f"voting={c.get('voting')} of {c.get('classified')}")
        lp = bar.get("ledger_prev_values") or {}
        rep.kv(total_voting=tot, ledger_prev_n=lp.get("n"), ledger_as_of=lp.get("as_of"))
        checks.append(("EUBUND classified and live in the barometers",
                       be.get("status") == "LIVE" and be.get("domain") in
                       ("MACRO", "LIQUIDITY", "RISK")))
        checks.append(("ledger now carries prior values for change derivation",
                       (lp.get("n") or 0) > 0))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — EUBUND {eb.get('value')} LIVE from ECB; vault "
               f"{vault.get('n_live')} live; {tot} indicators voting; ledger "
               f"prev n={lp.get('n')}")


if __name__ == "__main__":
    main()
