"""ops 6729 - READ-ONLY probe of the two failure classes the 13F security master showed at first light (ops 6727):
  (a) 'ambiguous' with exchCode US (Exxon 30231G102, Electronic Arts 285512109, Kenvue 49177J102, Warner Bros
      Discovery 934423104 ...): print the candidate rows the master retained, so the collapse rule can be designed
      from the data rather than guessed;
  (b) 'no_match' for letter-prefixed identifiers (CINS: Chubb H1467J104, Seagate G7997R103, ASML N07059210, Linde
      G54950103 ...): query OpenFIGI with idType ID_CINS (and ID_CUSIP again) for a handful, keyed via SSM, and
      print the rows.
Also prints the status mix of the master and the 30 largest unmapped identities with their statuses and reasons.
Network: <= 4 OpenFIGI requests. Writes nothing but this report.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import openfigi  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
MASTER, BOOK = "data/13f-security-master.json", "data/13f-book.json"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
CINS = ["H1467J104", "G7997R103", "N07059210", "G54950103", "L8681T102", "G5960L103", "G1151C101", "Y2573F102", "G25457105", "N97284108"]


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6729_13f_master_ambiguity_probe") as r:
        r.heading("ops 6729 - 13F security master: ambiguity + CINS probe (read-only)")
        master = json.loads(s3.get_object(Bucket=LIVE, Key=MASTER)["Body"].read())
        rows = master.get("by_cusip") or {}
        book = json.loads(s3.get_object(Bucket=LIVE, Key=BOOK)["Body"].read())
        unmapped = (((book.get("book") or {}).get("review") or {}).get("unmapped_top") or [])
        r.section("1. master status mix")
        mix = {}
        for c, row in rows.items():
            mix[row.get("status")] = mix.get(row.get("status"), 0) + 1
        letter = sum(1 for c in rows if c[:1].isalpha())
        letter_nm = sum(1 for c, row in rows.items() if c[:1].isalpha() and row.get("status") == "no_match")
        r.log("rows %d: %s | letter-prefixed identifiers %d (no_match %d) | reasons: %s" % (
            len(rows), json.dumps(mix), letter, letter_nm, json.dumps(sorted({(row.get("status"), row.get("reason")) for row in rows.values() if row.get("status") != "resolved"}))))
        r.section("2. ambiguous candidates (largest unmapped first)")
        amb = [u for u in unmapped if u.get("master") == "ambiguous"][:20]
        for u in amb:
            row = rows.get(u["cusip"]) or {}
            r.log("%s %s %s holders %s value %s | query %s | candidates: %s" % (u["cusip"], u.get("class"), u.get("issuer"), u.get("n_holders"), u.get("value_usd"), row.get("query"),
                                                                              json.dumps(row.get("candidates"))))
        # candidate shape statistics across all ambiguous rows
        shapes = {}
        for c, row in rows.items():
            if row.get("status") != "ambiguous":
                continue
            cands = row.get("candidates") or []
            key = "|".join(sorted({"%s/%s/%s" % (x.get("exchCode"), x.get("securityType"), "comp=figi" if x.get("compositeFIGI") == x.get("figi") else "comp!=figi") for x in cands}))
            shapes[key] = shapes.get(key, 0) + 1
        r.log("ambiguous candidate shapes (exch/type/composite-ness): %s" % json.dumps(sorted(shapes.items(), key=lambda kv: -kv[1])[:15]))
        r.section("3. CINS: ID_CINS vs ID_CUSIP for letter-prefixed identifiers (keyed)")
        key = openfigi.get_api_key()
        r.log("api key present: %s" % bool(key))
        deadline = time.monotonic() + 60
        jobs = [{"idType": "ID_CINS", "idValue": c, "exchCode": "US"} for c in CINS] + [{"idType": "ID_CINS", "idValue": c} for c in CINS[:5]] + [{"idType": "ID_CUSIP", "idValue": c} for c in CINS[:3]]
        res = openfigi.mapping(jobs, api_key=key, deadline=deadline) or []
        for j, item in zip(jobs, res):
            if isinstance(item, dict) and "data" in item:
                r.log("%s %s %s -> %d rows: %s" % (j["idType"], j["idValue"], j.get("exchCode", "-"), len(item["data"]),
                                                   json.dumps([{k: x.get(k) for k in ("ticker", "exchCode", "name", "securityType", "securityType2", "marketSector", "figi", "compositeFIGI")} for x in item["data"][:4]])))
            else:
                r.log("%s %s %s -> %s" % (j["idType"], j["idValue"], j.get("exchCode", "-"), json.dumps(item)[:200]))
        r.section("4. largest unmapped, with status + reason")
        for u in unmapped[:30]:
            row = rows.get(u["cusip"]) or {}
            r.log("%-9s %-10s %-40s holders %2s value %s | %s %s %s" % (u["cusip"], (u.get("class") or "")[:10], (u.get("issuer") or "")[:40], u.get("n_holders"), u.get("value_usd"),
                                                                      row.get("status"), row.get("reason"), row.get("query")))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
