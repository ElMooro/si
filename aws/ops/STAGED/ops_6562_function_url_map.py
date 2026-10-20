"""ops 6562 - READ-ONLY: Function URL -> function name for every function that has one, so the pages' hard-coded
Lambda URLs can be matched to functions before anything is switched off or deleted (Khalid, 2026-10-20)."""
from pathlib import Path
import json
import os
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"


def main():
    with report("ops_6562_function_url_map") as r:
        r.heading("ops 6562 - Function URL map (read-only)")
        lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 10, "mode": "standard"}))
        fns = []
        for p in lam.get_paginator("list_functions").paginate():
            fns += [f["FunctionName"] for f in p["Functions"]]
        urls = {}
        for n in fns:
            try:
                u = lam.get_function_url_config(FunctionName=n)
                urls[u["FunctionUrl"].split("//")[1].split("/")[0]] = {"function": n, "auth": u.get("AuthType")}
            except lam.exceptions.ResourceNotFoundException:
                continue
        out = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest" / "ops_6562_function_url_map_data.md"
        out.write_text("# Function URL host -> function\n\n```json\n" + json.dumps(urls, indent=1, sort_keys=True) + "\n```\n", encoding="utf-8")
        r.log("%d function URLs mapped -> %s" % (len(urls), out.name))
        r.ok("done")


if __name__ == "__main__":
    main()
