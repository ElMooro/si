"""
ops_3900 — PROBE: before touching trade-evaluator's code, verify empirically
whether its declared source (justhodl-options-flow-scanner's POLY_KEY) is
actually a real, working key — not repeating the confluence-meta mistake
earlier this session, where a "documented known-good" source turned out to
have zero Anthropic key at all. Checks the LIVE (not just config.json)
environment value, and makes a real, minimal Polygon API call with it to
confirm the whole chain actually resolves a price. Writes no code.
"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")


def get_live_env(fn_name):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def main():
    with report("3900_polygon_key_verify") as rep:
        rep.heading("ops 3900 — verify options-flow-scanner's POLY_KEY is real before fixing anything")

        rep.section("1. live env on justhodl-options-flow-scanner (the declared inherit source)")
        env1 = get_live_env("justhodl-options-flow-scanner")
        poly_key = env1.get("POLY_KEY", "")
        rep.kv(present=bool(poly_key), length=len(poly_key),
               looks_like_placeholder="PLACEHOLDER" in poly_key.upper() if poly_key else None,
               prefix=poly_key[:8] + "..." if poly_key else None,
               all_env_keys=str(sorted(env1.keys())))

        rep.section("2. live env on trade-evaluator itself — did the declared inherit actually resolve")
        env2 = get_live_env("justhodl-trade-evaluator")
        te_poly_key = env2.get("POLY_KEY", "")
        rep.kv(trade_evaluator_poly_key_present=bool(te_poly_key),
               trade_evaluator_poly_key_length=len(te_poly_key),
               matches_source=(te_poly_key == poly_key) if poly_key else None,
               all_env_keys=str(sorted(env2.keys())))

        rep.section("3. cross-check — does ANY other lambda have a confirmed-different POLYGON_KEY "
                    "(the OTHER naming convention) worth comparing")
        env3 = get_live_env("justhodl-equity-research")
        eq_poly = env3.get("POLYGON_KEY", "")
        rep.kv(equity_research_POLYGON_KEY_present=bool(eq_poly), length=len(eq_poly),
               same_value_as_POLY_KEY=(eq_poly == poly_key) if (eq_poly and poly_key) else None)

        rep.section("4. THE REAL TEST — does a live Polygon call with THIS key actually work")
        test_key = poly_key or te_poly_key or eq_poly
        if not test_key:
            rep.fail("  no candidate key found anywhere to even test")
            sys.exit(1)
        url = (f"https://api.polygon.io/v2/aggs/ticker/AAPL/prev?adjusted=true&apiKey={test_key}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ops-verify/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            rep.ok(f"  SUCCESS — real Polygon response: {json.dumps(data, default=str)[:400]}")
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "ignore")
            except Exception:
                body = "(no body)"
            rep.fail(f"  HTTP {e.code} {e.reason} — Polygon error body: {body[:400]}")
            sys.exit(1)
        except Exception as e:
            rep.fail(f"  non-HTTP failure: {str(e)[:200]}")
            sys.exit(1)

        rep.ok("PROBE COMPLETE — key confirmed genuinely working end to end")


if __name__ == "__main__":
    main()
