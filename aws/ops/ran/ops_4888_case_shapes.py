"""ops/4888 -- industry-case shape probe (report-only).
Dump the exact shapes the case engine will bind:
 readthrough.json (value-chain edges), industry boom league,
 sp500.json member fields + sector_context, universe row,
 13F flows per-ticker key, earnings picks row.  Bind only
 what prints.
"""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def dump(rep, key, picker):
    try:
        j = g(key)
    except Exception as e:  # noqa: BLE001
        rep.log("[%s] ABSENT %s" % (key, str(e)[:60]))
        return
    rep.log("[%s] top=%s" % (key, list(j)[:12]))
    try:
        picker(rep, j)
    except Exception as e:  # noqa: BLE001
        rep.log("  picker died: %s" % str(e)[:80])


def main():
    with report("ops 4888 -- case shapes") as rep:
        def p_rt(rep, j):
            for k in ("map", "edges", "by_ticker", "rows",
                      "companies"):
                if k in j:
                    v = j[k]
                    rep.log("  %s type=%s n=%s" %
                            (k, type(v).__name__,
                             len(v) if hasattr(v, "__len__")
                             else "-"))
                    it = (list(v.items())[0] if
                          isinstance(v, dict) else v[0])
                    rep.log("  sample=%s" % json.dumps(
                        it, default=str)[:500])
                    return
        dump(rep, "data/readthrough.json", p_rt)

        def p_boom(rep, j):
            lg = j.get("league") or j.get("rows") or []
            rep.log("  league n=%d row0=%s"
                    % (len(lg), json.dumps(
                        lg[0], default=str)[:400]
                       if lg else "-"))
        for k in ("data/industry-boom.json",
                  "data/boom-league.json",
                  "data/industry-league.json"):
            dump(rep, k, p_boom)

        def p_sp(rep, j):
            m = j.get("members") or []
            rep.log("  members n=%d" % len(m))
            if m:
                nv = next((x for x in m
                           if x.get("t") == "NVDA"), m[0])
                rep.log("  NVDA keys=%s" % list(nv)[:44])
                rep.log("  NVDA sample=%s" % json.dumps(
                    {k: nv[k] for k in list(nv)[:18]},
                    default=str)[:520])
            sc = j.get("sector_context") or j.get("sectors")
            if sc:
                it = (list(sc.items())[0]
                      if isinstance(sc, dict) else sc[0])
                rep.log("  sector_context sample=%s"
                        % json.dumps(it, default=str)[:300])
        dump(rep, "data/sp500.json", p_sp)

        def p_uni(rep, j):
            rows = j.get("stocks") or j.get("rows") or []
            rep.log("  n=%d row0=%s" % (len(rows),
                                        json.dumps(
                    rows[0], default=str)[:360] if rows
                    else "-"))
        dump(rep, "data/universe.json", p_uni)

        def p_13f(rep, j):
            for k in ("by_ticker", "dollar_flows", "rows",
                      "tickers"):
                if k in j:
                    v = j[k]
                    it = (list(v.items())[0]
                          if isinstance(v, dict) else v[0])
                    rep.log("  %s sample=%s"
                            % (k, json.dumps(
                                it, default=str)[:360]))
                    return
        for k in ("data/13f-flows.json",
                  "data/clone-alpha.json",
                  "data/whale-flows.json"):
            dump(rep, k, p_13f)

        def p_earn(rep, j):
            pk = (j.get("growth_calls") or {}).get("picks") \
                or []
            rep.log("  picks n=%d keys=%s"
                    % (len(pk), list(pk[0])[:14]
                       if pk else []))
        dump(rep, "data/earnings.json", p_earn)
        try:
            g("data/universe.json")
        except Exception:  # noqa: BLE001
            rep.fail("universe.json unreadable -- case "
                     "engine has no spine")
            sys.exit(1)
        rep.ok("shapes on record")


if __name__ == "__main__":
    main()
