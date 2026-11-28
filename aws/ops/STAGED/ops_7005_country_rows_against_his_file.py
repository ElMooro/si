"""ops 7005 - read-only: the country document's highest premiums, against the cells of his own file as banked.

ops 7004's sort check put Barbados first at 31.40%. That is the premium of a country in default (rating C); his January
file had Barbados at B2 and 11.35%. Either his July file says so, or a row was misread. This prints the document's
rows above 20% and the same rows' cells straight from the banked file (first nine columns), nothing else.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-damodaran" / "source"))
NAME = "ops_7005_country_rows_against_his_file"
LIVE = "justhodl-dashboard-live"


def main():
    import boto3
    import dam_core as C
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    with report(NAME) as r:
        r.heading("7005 - the highest country premiums against his file (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        K = json.loads(s3.get_object(Bucket=LIVE, Key="data/country-risk-premiums.json")["Body"].read())
        high = [x for x in K["rows"] if x[4] is not None and x[4] >= 0.2]
        names = [x[0] for x in high] + ["Barbados", "Belarus", "Bahrain", "Bangladesh"]
        r.log("document: file %s, as of %s, %d countries; rows at 20%% or more: %d" % (K["file"], K["as_of"], K["n"], len(high)))
        for x in high:
            r.log("  document: %s" % x)
        fails = 0
        for fname in [K["file"]] + list(K.get("other_files") or []):
            bk = C.book(s3.get_object(Bucket=LIVE, Key="damodaran/raw/" + fname)["Body"].read())
            rows = next(v for k, v in bk["sheets"].items() if "by country" in k.lower())
            r.log("his file %s:" % fname)
            for row in rows:
                if row and isinstance(row[0], str) and row[0].strip() in names:
                    r.log("  cells: %s" % [(round(c, 6) if isinstance(c, float) else c) for c in row[:9]])
                    if fname == K["file"]:
                        doc = next((x for x in K["rows"] if x[0] == row[0].strip()), None)
                        if doc and doc[2] != (row[2] or "").strip() or doc and doc[4] is not None and abs(doc[4] - float(row[4])) > 1e-6:
                            fails += 1
                            r.log("  ✗ the document's row differs: %s" % doc)
        r.kv(rows_at_20pct_or_more=len(high), differ=fails)
        if fails:
            r.fail("%d row(s) of the document differ from his file" % fails)
            sys.exit(1)
        r.ok("every row checked is the row of his file; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
