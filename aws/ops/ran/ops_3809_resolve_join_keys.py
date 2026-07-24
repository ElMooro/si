#!/usr/bin/env python3
"""ops 3809 — exact join keys + industry-boom shape (G0 before the build).

3808 ranked by overlap. This resolves the EXACT container key and per-row field
names for each joiner, plus how industry-boom keys its league table, so the
v5.0 build has zero guessed keys. Six bugs in this arc came from guessing.
"""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def main():
    with report("3809_resolve_join_keys") as rep:
        rep.heading("ops 3809 — resolve exact join keys")

        def load(k):
            return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())

        rep.section("dark-pool (912 overlap) — which container holds all names?")
        dp = load("data/dark-pool.json")
        for k, v in dp.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rep.log("  list '%s' n=%d keys=%s" % (k, len(v), sorted(v[0].keys())[:14]))
            elif isinstance(v, dict) and len(v) > 20:
                rep.log("  dict '%s' n=%d (keyed by ticker?)" % (k, len(v)))

        rep.section("finra-short (494) — containers")
        fs = load("data/finra-short.json")
        for k, v in fs.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rep.log("  list '%s' n=%d keys=%s" % (k, len(v), sorted(v[0].keys())[:14]))
            elif isinstance(v, dict) and len(v) > 20:
                rep.log("  dict '%s' n=%d" % (k, len(v)))

        rep.section("earnings-pead (241) — containers + metric shape")
        pe = load("data/earnings-pead.json")
        for k, v in pe.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rep.log("  list '%s' n=%d keys=%s" % (k, len(v), sorted(v[0].keys())[:14]))
                if k == "all_qualifying":
                    r0 = v[0]
                    rep.log("    metrics: %s" % json.dumps(r0.get("metrics"))[:200])
                    rep.log("    tier=%s score=%s flags=%s" % (
                        r0.get("tier"), r0.get("score"), str(r0.get("flags"))[:80]))

        rep.section("estimate-revisions (236) — containers + revision field")
        er = load("data/estimate-revisions.json")
        for k, v in er.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rep.log("  list '%s' n=%d keys=%s" % (k, len(v), sorted(v[0].keys())[:16]))
            elif isinstance(v, dict) and len(v) > 20:
                fk = list(v.keys())[0]
                rep.log("  dict '%s' n=%d sample['%s']=%s" % (k, len(v), fk, str(v[fk])[:120]))

        rep.section("industry-boom — how is the league keyed?")
        ib = load("data/industry-boom.json")
        for k, v in ib.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rep.log("  list '%s' n=%d keys=%s" % (k, len(v), sorted(v[0].keys())[:14]))
                if k == "league":
                    rep.log("    sample: %s" % json.dumps(v[0])[:260])
        rep.log("  n_industries=%s coverage=%s" % (ib.get("n_industries"), str(ib.get("coverage"))[:90]))

        rep.section("Ledger industry names must MATCH industry-boom league names")
        ck = load("data/chokepoint.json")
        rows = (ck.get("capture_gap") or {}).get("all_rows") or []
        led_ind = {r.get("industry") for r in rows if r.get("industry")}
        lg = ib.get("league") or []
        bkey = None
        for cand in ("industry", "name", "group"):
            if lg and cand in lg[0]:
                bkey = cand
                break
        boom_ind = {x.get(bkey) for x in lg if x.get(bkey)} if bkey else set()
        rep.kv(ledger_industries=len(led_ind), boom_industries=len(boom_ind),
               boom_key=bkey, industry_overlap=len(led_ind & boom_ind))
        rep.log("  ledger-only sample: %s" % sorted(list(led_ind - boom_ind))[:6])
        rep.log("  matched sample    : %s" % sorted(list(led_ind & boom_ind))[:6])

        if not (led_ind & boom_ind):
            rep.fail("industry names do not match between ledger and boom league — "
                     "a by-industry join would silently produce nothing")
            sys.exit(1)
        rep.ok("PASS_ALL — keys resolved")


if __name__ == "__main__":
    main()
