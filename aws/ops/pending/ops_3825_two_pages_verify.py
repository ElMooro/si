"""
ops_3825 — edge-verify btc-cycle.html + global-recession.html, field-coverage both

Closes two page-contract obligations opened by ops 3822 and 3824.

⚠ THE GAP THIS FIXES: cycle_bands was added to justhodl-onchain-ratios, which
writes data/onchain-ratios.json — but onchain.html reads cryptoquant-onchain.json.
Only crypto/index.html and signals.html read onchain-ratios, and neither renders
cycle_bands. The Rainbow and Pi Cycle were computing correctly and were invisible
to every human. An engine whose data no one can see is half-built.

Both pages verified AT THE EDGE with a unique marker, then field-coverage audited
against the LIVE artifacts.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
MARKER = "v1-ops3825"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
s3 = boto3.client("s3", region_name="us-east-1")

TARGETS = [
    ("btc-cycle.html", "data/onchain-ratios.json", "cycle_bands",
     [("rainbow band chart", "bands are"), ("pi cycle block", "Pi Cycle Top"),
      ("fit disclosure", "ln(price)"), ("sample warning", "Sample size warning"),
      ("context-not-target", "never a target")]),
    ("global-recession.html", "data/global-recession.json", None,
     [("not-macromicro", "What this is not"), ("methodology table", "Base hazard"),
      ("excluded list", "never imputed"), ("breadth", "Breadth at risk"),
      ("us separate", "separate")]),
]


def fetch(path, attempt):
    req = urllib.request.Request(
        f"https://justhodl.ai/{path}?v={int(time.time())}-{attempt}",
        headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def keys_of(obj, prefix=""):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.append(k)
            if isinstance(v, dict):
                out += keys_of(v)
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                out += keys_of(v[0])
    return out


def main():
    with report("3825_two_pages_verify") as rep:
        rep.heading("ops 3825 — btc-cycle + global-recession pages")
        allfail = []

        WAIVED = {"engine", "version", "generated_at", "layer", "name", "basis",
                  "method", "note", "notes", "errors", "ok", "body", "headers",
                  "as_of_period", "build_seconds", "duration_s", "terms",
                  "raw_score", "squashed_pct", "phase_base", "modifiers",
                  "aggregation", "clamp", "comp_period", "supplement_value",
                  "supplement_date", "history_n", "composite_pct", "mom_change",
                  "yoy_change", "three_month_change", "joins", "caveats",
                  "not_macromicro", "equity_beta_guidance", "rule", "caveat",
                  "interpretation", "reason", "v1_1_note", "squash",
                  "n_daily_closes", "historical_n", "band_prices", "slope_a",
                  "intercept_b", "residual_sigma", "r_squared", "n_days",
                  "history_starts", "available", "latest_date"}

        for page, feed, sub, markers in TARGETS:
            rep.section(f"── {page}")
            html = ""
            for a in range(1, 14):
                try:
                    html = fetch(page, a)
                except Exception as e:
                    rep.log(f"  attempt {a}: {str(e)[:70]}")
                    time.sleep(20); continue
                if MARKER in html:
                    rep.ok(f"  served on attempt {a} ({len(html):,} bytes)")
                    break
                rep.log(f"  attempt {a}: {len(html):,} bytes, marker absent")
                time.sleep(20)
            else:
                rep.fail(f"  {page} never reached the edge")
                allfail.append(page); continue

            d = json.loads(s3.get_object(Bucket=BUCKET, Key=feed)["Body"].read())
            scope = (d.get(sub) or {}) if sub else d
            ks = sorted(set(keys_of(scope)))
            missing = [k for k in ks if k not in WAIVED and k not in html]
            rep.log(f"  keys checked {len([k for k in ks if k not in WAIVED])} · "
                    f"missing {len(missing)}")
            if missing:
                rep.fail(f"  NO RENDER PATH: {missing}")
                allfail.append(f"{page}:keys")
            else:
                rep.ok("  every non-waived key rendered")

            for label, m in markers:
                if m in html:
                    rep.ok(f"  marker · {label}")
                else:
                    rep.fail(f"  marker MISSING · {label} ('{m}')")
                    allfail.append(f"{page}:{label}")

        rep.section("Nav manifest (served)")
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/nav-manifest.json?v={int(time.time())}",
                headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                nav = json.loads(r.read().decode())
            found = {}
            for c in nav.get("categories", []):
                for p in c.get("pages", []):
                    for t in ("btc-cycle", "global-recession"):
                        if t in p.get("href", ""):
                            found[t] = c["name"]
            for t in ("btc-cycle", "global-recession"):
                (rep.ok if t in found else rep.warn)(
                    f"  {t}: {found.get(t, 'not listed yet (CI regenerates on a lag)')}")
        except Exception as e:
            rep.warn(f"  manifest unreadable: {str(e)[:70]}")

        if allfail:
            rep.fail(f"FAILED: {allfail}")
            sys.exit(1)
        rep.ok("PASS_ALL — both pages served, all keys rendered, all markers present")


if __name__ == "__main__":
    main()
