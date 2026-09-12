"""ops_5439 -- rebuild positioning-brief after by_ticker parse fix. Runner only."""
from __future__ import annotations

import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_compiler import run  # noqa: E402


def main():
    with report("ops_5439_recompile_positioning") as R:
        R.heading("ops 5439 positioning by_ticker")
        s3 = boto3.client("s3", region_name="us-east-1")
        doc = run(s3, "positioning", source="ops_5439")
        fields = doc.get("fields") or {}
        R.ok("status=%s acc=%s dist=%s flat=%s n=%s pct=%s" % (
            doc.get("status"), fields.get("accumulating"), fields.get("distributing"),
            fields.get("flat"), fields.get("n_with_inst_trans"), fields.get("breadth_pct")))
        R.ok(doc.get("why"))
        if (fields.get("n_with_inst_trans") or 0) < 500:
            R.warn("still thin -- universe parse may still be wrong")


if __name__ == "__main__":
    main()
