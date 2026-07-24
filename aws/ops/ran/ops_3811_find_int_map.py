#!/usr/bin/env python3
"""ops 3811 — find which joined feed returns ints (v5.0 verdict threw at runtime).

3810 deployed v5.0 but the verdict block raised:
    'int' object has no attribute 'get'
so mispricing_verdict never populated. My G0 gates confirmed each CONTAINER key
existed (direction_map, dark_map, tickers, all_qualifying, league) but never
checked the VALUE TYPE inside those containers. That is the same producer/
consumer assumption gap as the earlier field-drop bugs, in a new form: I
verified the shape one level too shallow.

Suspects: I call .get() on values from dark_map, tickers (finra-short) and
direction_map. direction_map is known to hold strings ("FLAT") and I handle that.
finra-short's `tickers` map values were never inspected — a count map would
explain the error exactly.

This ops inspects the VALUE TYPE of every map I dereference, and writes no code.
"""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


def main():
    with report("3811_find_int_map") as rep:
        rep.heading("ops 3811 — value types of every map the verdict dereferences")

        def load(k):
            return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())

        checks = [
            ("data/estimate-revisions.json", "direction_map"),
            ("data/dark-pool.json", "dark_map"),
            ("data/finra-short.json", "tickers"),
        ]
        culprits = []
        for key, cont in checks:
            try:
                j = load(key)
                m = j.get(cont)
                rep.section("%s -> %s" % (key, cont))
                rep.log("  container type: %s  n=%s" % (type(m).__name__,
                        len(m) if hasattr(m, "__len__") else "?"))
                if isinstance(m, dict):
                    types = {}
                    sample = None
                    for k2, v2 in list(m.items())[:400]:
                        types[type(v2).__name__] = types.get(type(v2).__name__, 0) + 1
                        if sample is None:
                            sample = (k2, v2)
                    rep.log("  value types: %s" % types)
                    rep.log("  sample: %s -> %s" % (sample[0], str(sample[1])[:160]))
                    if "dict" not in types:
                        culprits.append((key, cont, types))
                        rep.warn("  ** .get() on these values WILL throw — not dicts **")
                    else:
                        # even if mostly dicts, a single non-dict breaks the loop
                        bad = {t: c for t, c in types.items() if t != "dict"}
                        if bad:
                            culprits.append((key, cont, bad))
                            rep.warn("  ** mixed types present: %s **" % bad)
                elif isinstance(m, list):
                    rep.log("  LIST not dict — indexing by ticker would fail")
                    culprits.append((key, cont, "list"))
            except Exception as e:
                rep.log("  ERR %s" % str(e)[:120])

        rep.section("all_qualifying / league are lists of dicts (already row-mapped)")
        for key, cont in (("data/earnings-pead.json", "all_qualifying"),
                          ("data/industry-boom.json", "league")):
            j = load(key)
            v = j.get(cont) or []
            rep.log("  %-34s %s n=%d first=%s" % (
                cont, type(v).__name__, len(v),
                type(v[0]).__name__ if v else "-"))

        rep.section("VERDICT")
        if culprits:
            for key, cont, t in culprits:
                rep.log("  CULPRIT %-34s %s -> %s" % (key, cont, t))
            rep.warn("Fix = guard every map dereference with isinstance(x, dict) "
                     "before .get(), and skip the leg rather than crashing the whole "
                     "verdict block. One bad value type currently kills all 2,393 rows.")
        gate(rep, "DIAG.found", bool(culprits), "%d culprit map(s) identified" % len(culprits))
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — culprit isolated")


if __name__ == "__main__":
    main()
