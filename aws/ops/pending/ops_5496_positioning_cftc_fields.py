"""ops_5496 -- add CFTC join provenance onto positioning-brief fields. Additive."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"


def main():
    with report("ops_5496_positioning_cftc_fields") as R:
        R.heading("ops 5496 CFTC on positioning brief")
        s3 = boto3.client("s3", region_name="us-east-1")
        brief = json.loads(s3.get_object(Bucket=B, Key="data/positioning-brief.json")["Body"].read())
        cftc = json.loads(s3.get_object(Bucket=B, Key="data/cftc-join.json")["Body"].read())
        fields = brief.setdefault("fields", {})
        # do not change breadth / score inputs
        fields["cftc_n_files"] = cftc.get("n_files")
        fields["cftc_source"] = cftc.get("source")
        fields["cftc_generated_at"] = cftc.get("generated_at")
        live = [f for f in (cftc.get("files") or []) if f.get("status") == "LIVE"]
        fields["cftc_n_live"] = len(live)
        fields["cftc_rows"] = sum(int(f.get("n") or 0) for f in live)
        inputs = brief.setdefault("inputs", {})
        inputs["data/cftc-join.json"] = {
            "required": False,
            "last_modified": None,
            "as_of": cftc.get("generated_at"),
            "freshness": "FRESH",
            "error": None,
        }
        why = brief.get("why") or ""
        extra = " | CFTC files=%s live=%s rows=%s" % (
            fields.get("cftc_n_files"), fields.get("cftc_n_live"), fields.get("cftc_rows"))
        if "CFTC files=" not in why:
            brief["why"] = why + extra
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(
            Bucket=B, Key="data/positioning-brief.json",
            Body=json.dumps(brief).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("cftc_n_files=%s live=%s rows=%s breadth %s/%s/%s" % (
            fields.get("cftc_n_files"), fields.get("cftc_n_live"), fields.get("cftc_rows"),
            fields.get("accumulating"), fields.get("distributing"), fields.get("flat")))
        if fields.get("cftc_n_live", 0) < 1:
            R.fail("no live CFTC files")
            sys.exit(1)


if __name__ == "__main__":
    main()
