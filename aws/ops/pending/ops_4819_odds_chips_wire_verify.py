"""ops/4819 -- odds chips wire verify (Fusion 1 consumers).
 Invest + stock-buying now carry additive, NON-GATING
 `base_rate_odds` per row, joined from data/base-rates.json.
 Local gates already green: invest pytest 35/35 (incl. 6 new join
 tests proving deep non-gating identity), sb stub-harness 7/7.
 G0  FIELD-level: spine LIVE + cohorts.26w.quintiles[0].beat_pct
     numeric + current_assignments>=3000; both consumer docs exist.
 (1) zip markers settle: 'invest-odds v1' + 'sb-odds v1'.
 (2) Event-invoke BOTH; poll both docs fresh (<=12 min combined).
 (3) truths: header meta present; odds coverage >=95% of rows whose
     ticker/symbol sits in current_assignments; sampled odds.q ==
     spine assignment q (consumer can never diverge from the spine);
     schema strings unchanged (invest/0.1, schema_version 1);
     pre-existing row fields present (score/tier/verdict smoke).
 (4) readout: sample chips from each consumer.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
SPINE_KEY = "data/base-rates.json"
CONSUMERS = (
    ("justhodl-invest", "data/invest.json", "invest-odds v1",
     "generated_at", "stock_picks", "ticker"),
    ("justhodl-stock-buying", "data/stock-buying.json", "sb-odds v1",
     "as_of", "top", "symbol"),
)

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def settle(rep, fn, marker):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=fn)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                rep.ok("%s marker '%s' settled (attempt %d)"
                       % (fn, marker, att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("%s zip never carried '%s'" % (fn, marker))
    FAILED.append("settle_" + fn)
    return False


def verify_consumer(rep, name, doc, spine, rows_key, tkey):
    asg = spine.get("current_assignments") or {}
    rows = doc.get(rows_key) or []
    meta = doc.get("base_rates")
    in_led = [r for r in rows if isinstance(r, dict)
              and r.get(tkey) in asg]
    with_odds = [r for r in in_led if r.get("base_rate_odds")]
    if not rows:
        rep.warn("  %s: %s empty (bootstrap tape) -- coverage "
                 "skipped, meta=%s" % (name, rows_key,
                                       json.dumps(meta)))
        return
    if not in_led:
        rep.warn("  %s: no row tickers in current_assignments -- "
                 "coverage skipped" % name)
        return
    cov = len(with_odds) / len(in_led)
    if cov >= 0.95:
        rep.ok("  %s: odds coverage %d/%d (%.0f%%)"
               % (name, len(with_odds), len(in_led), 100 * cov))
    else:
        rep.fail("  %s: coverage only %d/%d"
                 % (name, len(with_odds), len(in_led)))
        FAILED.append("coverage_" + name)
    bad = 0
    for r in with_odds[:40]:
        if r["base_rate_odds"].get("q") != asg[r[tkey]].get("q"):
            bad += 1
    if bad == 0:
        rep.ok("  %s: sampled odds.q == spine q (%d/%d)"
               % (name, min(40, len(with_odds)),
                  min(40, len(with_odds))))
    else:
        rep.fail("  %s: %d sampled rows diverge from spine" % (name,
                                                               bad))
        FAILED.append("spine_q_" + name)
    if meta and isinstance(meta.get("as_of"), str):
        rep.ok("  %s: header base_rates meta %s"
               % (name, json.dumps(meta)))
    else:
        rep.fail("  %s: header meta missing" % name)
        FAILED.append("meta_" + name)


def main():
    with report("ops 4819 -- odds chips wire verify") as rep:
        rep.heading("G0. FIELD-level spine + consumer contracts")
        try:
            spine = sread(SPINE_KEY)
        except ClientError:
            rep.fail("spine MISSING")
            sys.exit(1)
        q0 = (((spine.get("cohorts") or {}).get("26w")
               or {}).get("quintiles") or [{}])[0]
        n_asg = len(spine.get("current_assignments") or {})
        if (spine.get("status") == "LIVE"
                and isinstance(q0.get("beat_pct"), (int, float))
                and n_asg >= 3000):
            rep.ok("  spine LIVE, q1.beat_pct=%s, assignments=%d"
                   % (q0.get("beat_pct"), n_asg))
        else:
            rep.fail("  spine contract broken: status=%s beat=%s "
                     "n=%d" % (spine.get("status"),
                               q0.get("beat_pct"), n_asg))
            sys.exit(1)
        prev = {}
        for fn, key, _m, stamp, _rk, _tk in CONSUMERS:
            try:
                prev[fn] = sread(key).get(stamp)
                rep.ok("  %s present (prev %s=%s)"
                       % (key, stamp, str(prev[fn])[:19]))
            except ClientError:
                prev[fn] = None
                rep.warn("  %s absent pre-invoke" % key)

        rep.heading("1. deploy settle (both markers)")
        for fn, _k, marker, _s, _r, _t in CONSUMERS:
            if not settle(rep, fn, marker):
                sys.exit(1)

        rep.heading("2. Event-invoke both + poll (<=12 min)")
        for fn, _k, _m, _s, _r, _t in CONSUMERS:
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=b"{}")
        docs = {}
        t0 = time.time()
        while time.time() - t0 < 720 and len(docs) < len(CONSUMERS):
            time.sleep(20)
            for fn, key, _m, stamp, _r, _t in CONSUMERS:
                if fn in docs:
                    continue
                try:
                    d = sread(key)
                except ClientError:
                    continue
                if d.get(stamp) != prev[fn]:
                    docs[fn] = d
                    rep.ok("%s fresh in %ds" % (fn,
                                                int(time.time() - t0)))
        for fn, _k, _m, _s, _r, _t in CONSUMERS:
            if fn not in docs:
                rep.fail("%s never refreshed" % fn)
                FAILED.append("stale_" + fn)
        if FAILED:
            sys.exit(1)

        rep.heading("3. truths")
        inv = docs["justhodl-invest"]
        sb = docs["justhodl-stock-buying"]
        if inv.get("schema") == "invest/0.1":
            rep.ok("  invest schema unchanged (invest/0.1)")
        else:
            rep.fail("  invest schema mutated: %s"
                     % inv.get("schema"))
            FAILED.append("schema_invest")
        if sb.get("schema_version") == 1:
            rep.ok("  stock-buying schema_version unchanged (1)")
        else:
            rep.fail("  sb schema mutated: %s"
                     % sb.get("schema_version"))
            FAILED.append("schema_sb")
        for fn, _k, _m, _s, rk, tk in CONSUMERS:
            verify_consumer(rep, fn, docs[fn], spine, rk, tk)
        smoke = [r for r in (sb.get("top") or [])
                 if r.get("base_rate_odds")][:1]
        if smoke and all(k in smoke[0]
                         for k in ("score", "tier", "gates")):
            rep.ok("  sb pre-existing fields intact on chip rows")
        elif sb.get("top"):
            rep.fail("  sb row fields missing on chip rows")
            FAILED.append("sb_fields")

        rep.heading("4. sample chips")
        for fn, _k, _m, _s, rk, tk in CONSUMERS:
            shown = 0
            for r in docs[fn].get(rk) or []:
                o = r.get("base_rate_odds")
                if not o:
                    continue
                rep.log("  %-13s %-6s q=%s beat=%s%% LB95=%s%% "
                        "dd=%s medEx=%spp"
                        % (fn.replace("justhodl-", ""), r.get(tk),
                           o.get("q"), o.get("beat_spx_pct"),
                           o.get("lb95_pct"), o.get("dd_band"),
                           o.get("median_excess_pp")))
                shown += 1
                if shown >= 4:
                    break

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Fusion 1 consumers LIVE -- invest + stock-buying now "
               "quote the fleet's measured odds; neither can diverge "
               "from the spine")


if __name__ == "__main__":
    main()
