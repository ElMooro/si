"""
ops_3995 — PROBE: why does the vault walk still truncate under v2.1?

Resolved by inspection: the "JPLG paradox" was MY verifier — srckey "bo" is
not a substring of "bank-of-japan" (b->a). JPLG is genuinely in the BOJ
family from the vault. Remaining engine question: vault n_paths < 3000 and
EUBUND/JP02Y/NO03Y/PETOT absent even from the barometers fallback, with a
~245s runtime. This probe reads the v2.1 artifact + ledger and answers,
with evidence: exception vs cap vs something else.
"""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("3995_vault_walk_probe") as rep:
        rep.heading("ops 3995 — vault-walk truncation forensics (v2.1)")
        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/data-census.json")["Body"].read())
        t = doc.get("totals") or {}
        rep.kv(marker=doc.get("marker"), generated_at=doc.get("generated_at"),
               elapsed_s=doc.get("elapsed_s"), **{k: t.get(k) for k in
               ("artifacts", "scalar_paths", "artifacts_truncated_by_time_budget",
                "parse_errors")})

        rep.section("A. the two priority artifacts, as the engine recorded them")
        arts = {a.get("key"): a for a in doc.get("artifacts") or []}
        for k in ("data/tradingview.json", "data/domain-barometers.json",
                  "data/risk-gate.json", "data/rotation-dashboard.json"):
            a = arts.get(k) or {}
            rep.log(f"  {k}")
            rep.log(f"      n_paths={a.get('n_paths')} skipped={a.get('skipped')} "
                    f"error={a.get('error')} size={a.get('size')} "
                    f"age_h={a.get('age_h')}")

        rep.section("B. ledger: per-artifact capture + WHERE the vault stopped")
        led = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data-census/paths-ledger.json")["Body"].read())
        paths = led.get("paths") or []
        rep.kv(ledger_n=len(paths), n_total_indexed=led.get("n_total_indexed"))
        byk = Counter(r.get("k") for r in paths)
        for k, n in byk.most_common(8):
            rep.log(f"  {n:6d}  {k}")
        v = [r for r in paths if r.get("k") == "data/tradingview.json"]
        rep.kv(vault_rows_in_ledger=len(v))
        rep.log("  first 3 vault paths: " +
                str([r.get("p") for r in v[:3]]))
        rep.log("  LAST 6 vault paths (truncation point evidence):")
        for r in v[-6:]:
            rep.log(f"      {r.get('p')} v={r.get('v')} src={str(r.get('src'))[:30]}")
        tags = [r.get("p", "").split("[")[1].split("]")[0]
                for r in v if "[" in r.get("p", "")]
        uniq = list(dict.fromkeys(tags))
        rep.kv(distinct_vault_symbols_captured=len(uniq))
        rep.log(f"  last symbols reached: {uniq[-8:]}")
        for w in ("EUBUND", "JP02Y", "NO03Y", "PETOT", "JPLG", "US10Y"):
            rep.log(f"  {w} captured: {w in uniq}")

        rep.section("C. barometers same question")
        b = [r for r in paths if r.get("k") == "data/domain-barometers.json"]
        btags = list(dict.fromkeys(r.get("p", "").split("[")[1].split("]")[0]
                                   for r in b if "[" in r.get("p", "")))
        rep.kv(barometers_rows=len(b), distinct_symbols=len(btags))
        rep.log(f"  barometers last symbols: {btags[-6:]}")

        rep.ok("PROBE DONE — evidence above decides cap vs exception vs order")


if __name__ == "__main__":
    main()
