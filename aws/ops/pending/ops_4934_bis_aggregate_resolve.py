"""ops/4934 -- BIS LBS: resolve the ambiguous aggregate (read-only).

The live board published USD claims $3,934bn against USD liabilities
$49bn. An 80:1 claims/liabilities ratio is not a real number for the
LBS reporting population -- their USD books are broadly two-sided. So
one of those legs is not the aggregate it claims to be.

Cause, found by reading my own code rather than the data: the key
`Q.S.{pos}.A.USD.......` pins FREQ, L_MEASURE, L_POSITION, L_INSTR and
L_DENOM, then leaves L_CURR_TYPE, L_PARENT_CTY, L_REP_BANK_TYPE and
L_POS_TYPE as wildcards. Several distinct series therefore survive the
L_REP_CTY=5A / L_CP_COUNTRY=5J / L_CP_SECTOR=A filter, and the parse
does `best[period] = val` -- last row wins, arbitrarily. Claims landed
on the aggregate; liabilities landed on a sub-slice. The engine was
never summing wrong rows (that guard held); it was choosing between
them at random, which is the quieter version of the same sin.

This op does NOT guess the fix. It enumerates, for each position,
EVERY distinct dimension tuple that survives the filter, with its
latest value, so the canonical row is chosen against evidence and then
pinned explicitly. It also cross-checks the chosen pair for two-sided
plausibility, because a ratio test is what would have caught this on
day one.
"""
import json
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "JustHodl research ops4934 admin@justhodl.ai"}
BIS = "https://stats.bis.org/api/v1/data/WS_LBS_D_PUB"
OUT = {"generated": datetime.now(timezone.utc).isoformat(), "op": 4934}

FREE_DIMS = ("L_CURR_TYPE", "L_PARENT_CTY", "L_REP_BANK_TYPE",
             "L_POS_TYPE", "L_INSTR", "L_MEASURE")


def get(url, timeout=70, cap=14_000_000):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(cap).decode("utf-8", "replace")


def enumerate_rows(pos, r):
    """Every distinct dim tuple surviving 5A/5J/A/USD, newest value."""
    url = ("%s/Q.S.%s.A.USD......./all?format=csv&lastNObservations=6"
           % (BIS, pos))
    txt = get(url)
    lines = txt.splitlines()
    cols = [c.strip().strip('"') for c in lines[0].split(",")]
    ix = {c: i for i, c in enumerate(cols)}
    groups = defaultdict(dict)
    total = 0
    for ln in lines[1:]:
        f = ln.split(",")
        if len(f) < len(cols) - 4:
            continue
        total += 1
        if f[ix["L_DENOM"]] != "USD":
            continue
        if f[ix["L_REP_CTY"]] != "5A":
            continue
        if f[ix["L_CP_COUNTRY"]] != "5J":
            continue
        if f[ix["L_CP_SECTOR"]] != "A":
            continue
        try:
            val = float(f[ix["OBS_VALUE"]])
        except (ValueError, IndexError):
            continue
        tup = tuple(f[ix[d]] if d in ix and ix[d] < len(f) else "?"
                    for d in FREE_DIMS)
        groups[tup][f[ix["TIME_PERIOD"]]] = val
    r.kv(**{"pos_%s_rows_returned" % pos: total,
            "pos_%s_surviving_series" % pos: len(groups)})
    return groups


def main():
    t0 = datetime.now(timezone.utc)
    with report("4934-bis-aggregate-resolve") as r:
        r.heading("ops 4934 — BIS LBS: which row is actually THE aggregate")
        r.log("free dims (wildcarded in v1.0.1): %s" % ", ".join(FREE_DIMS))

        found = {}
        for pos, label in (("C", "claims"), ("L", "liabilities")):
            r.section("%s (L_POSITION=%s)" % (label.upper(), pos))
            try:
                g = enumerate_rows(pos, r)
            except Exception as e:  # noqa: BLE001
                r.fail("%s fetch failed: %s: %s"
                       % (label, type(e).__name__, str(e)[:120]))
                continue
            rows = []
            for tup, per in sorted(g.items()):
                latest = sorted(per)[-1]
                rows.append({"dims": dict(zip(FREE_DIMS, tup)),
                             "latest_period": latest,
                             "usd_mn": per[latest],
                             "usd_bn": round(per[latest] * 1e6 / 1e9, 1),
                             "n_periods": len(per)})
            rows.sort(key=lambda x: -abs(x["usd_bn"]))
            found[label] = rows
            for x in rows[:14]:
                d = x["dims"]
                r.log("  %-9s %-5s %-5s %-5s %-5s %-5s  $%12s bn  %s"
                      % (d.get("L_CURR_TYPE"), d.get("L_PARENT_CTY"),
                         d.get("L_REP_BANK_TYPE"), d.get("L_POS_TYPE"),
                         d.get("L_INSTR"), d.get("L_MEASURE"),
                         "{:,.1f}".format(x["usd_bn"]), x["latest_period"]))
            if len(rows) > 14:
                r.log("  … %d more series" % (len(rows) - 14))

        OUT["series"] = found

        # Which tuple exists on BOTH sides? That pair is the only
        # defensible claims/liabilities comparison — comparing an
        # aggregate against a sub-slice is what produced the 80:1.
        r.section("Two-sided candidates (same dim tuple on both sides)")
        ck = {tuple(sorted(x["dims"].items())): x
              for x in found.get("claims", [])}
        lk = {tuple(sorted(x["dims"].items())): x
              for x in found.get("liabilities", [])}
        both = []
        for k in set(ck) & set(lk):
            c, li = ck[k]["usd_bn"], lk[k]["usd_bn"]
            ratio = (abs(c) / abs(li)) if li else None
            both.append({"dims": dict(k), "claims_bn": c,
                         "liabilities_bn": li,
                         "net_bn": round(c - li, 1),
                         "ratio": round(ratio, 2) if ratio else None,
                         "two_sided": bool(ratio and 0.2 <= ratio <= 5.0)})
        both.sort(key=lambda x: -abs(x["claims_bn"]))
        OUT["two_sided"] = both
        r.kv(tuples_on_both_sides=len(both),
             passing_ratio_test=sum(1 for x in both if x["two_sided"]))
        for x in both[:14]:
            d = x["dims"]
            r.log("  %-9s %-5s %-5s %-5s %-5s %-5s  C $%11s  L $%11s  "
                  "ratio %-7s %s"
                  % (d.get("L_CURR_TYPE"), d.get("L_PARENT_CTY"),
                     d.get("L_REP_BANK_TYPE"), d.get("L_POS_TYPE"),
                     d.get("L_INSTR"), d.get("L_MEASURE"),
                     "{:,.1f}".format(x["claims_bn"]),
                     "{:,.1f}".format(x["liabilities_bn"]),
                     x["ratio"], "✓" if x["two_sided"] else "✗"))

        best = next((x for x in both if x["two_sided"]), None)
        r.section("Recommendation")
        if best:
            r.ok("canonical tuple: %s" % json.dumps(best["dims"]))
            r.kv(claims_bn=best["claims_bn"],
                 liabilities_bn=best["liabilities_bn"],
                 net_bn=best["net_bn"], ratio=best["ratio"])
            OUT["recommended"] = best
        else:
            r.warn("no two-sided tuple passed the 0.2–5.0 ratio test — "
                   "pin nothing, report the ambiguity on the page")
            OUT["recommended"] = None

        (ROOT / "aws" / "ops" / "reports" / "4934.json").write_text(
            json.dumps(OUT, indent=1, default=str))
        r.kv(status="RESOLVED",
             duration_s=int((datetime.now(timezone.utc) - t0)
                            .total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
