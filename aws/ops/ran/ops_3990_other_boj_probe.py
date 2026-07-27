"""
ops_3990 — PROBE: decompose the OTHER family + find the missing BOJ.

Two residuals from the census close: (1) OTHER holds 3,139 points whose
source strings matched no family — dump the top strings so FAMS grows from
EVIDENCE, not guesses; (2) BOJ/MOF/NORGES/BCRP are absent as families even
though the vault carries JPLG (src boj:MD11...), JP02Y (mof), NO03Y
(norges), PETOT (bcrp) — locate those exact entries in the paths ledger and
print what the walk actually captured for them. Read-only.
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
    with report("3990_other_boj_probe") as rep:
        rep.heading("ops 3990 — probe: OTHER decomposition + missing-BOJ forensics")

        led = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data-census/paths-ledger.json")["Body"].read())
        paths = led.get("paths") or []
        rep.kv(ledger_n=len(paths), n_total_indexed=led.get("n_total_indexed"))

        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/data-census.json")["Body"].read())
        bs = doc.get("by_source") or {}
        rep.kv(marker=doc.get("marker"),
               families=json.dumps({k: (v or {}).get("n") for k, v in bs.items()}))

        rep.section("A. what is inside OTHER — top source strings")
        FAMK = ("fred", "ecb", "boj", "stat-search", "mof", "jgbcme", "yahoo",
                "moea", "gov.tw", "getpointdata", "bcrp", "norges", "boe",
                "iadb", "snb", "imf", "eurostat", "ec.europa", "treasury",
                "ust:", "pboc", "polygon", "poly:", "fmp", "cftc",
                "coinmetrics", "edgar", "sec.gov", "gnews", "fleet:", "data/")
        other = Counter()
        for r in paths:
            src = (r.get("src") or "")
            t = src.lower()
            if src and not any(k in t for k in FAMK):
                other[src[:60]] += 1
        rep.kv(distinct_other_srcs=len(other))
        for s_, n in other.most_common(40):
            rep.log(f"  {n:5d}  {s_}")

        rep.section("B. the vault's BOJ/MOF/NORGES/BCRP rows — what the walk saw")
        want = ("JPLG", "JP02Y", "NO03Y", "PETOT", "EUBUND", "US10Y", "SOFR")
        found = {w: [] for w in want}
        for r in paths:
            p = r.get("p") or ""
            for w in want:
                if f"[{w}]" in p:
                    found[w].append(r)
        for w in want:
            rows = found[w]
            rep.log(f"  {w}: {len(rows)} ledger entries")
            for r in rows[:3]:
                rep.log(f"      k={r.get('k')} p={r.get('p')} v={r.get('v')} "
                        f"leaf={r.get('leaf')} src={str(r.get('src'))[:44]} "
                        f"name={r.get('name')}")

        rep.section("C. are they in by_source anywhere?")
        for w in ("JPLG", "NO03Y", "PETOT", "JP02Y"):
            hits = []
            for fam, b in bs.items():
                for m in (b or {}).get("metrics") or []:
                    if str(m.get("name")).upper().startswith(w):
                        hits.append((fam, m.get("value"), m.get("pulled_from")))
            rep.log(f"  {w}: {hits[:4] if hits else 'ABSENT from every family'}")

        rep.section("D. ledger truncation check")
        vault_in_ledger = sum(1 for r in paths
                              if r.get("k") == "data/tradingview.json")
        rep.kv(vault_rows_in_ledger=vault_in_ledger,
               note="ledger is named-first capped 150k — vault rows have no "
                    "name sibling, so absence HERE does not mean absence from "
                    "the in-memory by_source pool; section C is the truth for that")
        rep.ok("PROBE DONE — evidence recorded for the FAMS patch")


if __name__ == "__main__":
    main()
