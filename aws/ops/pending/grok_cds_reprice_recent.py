"""Reprice recent CDS prints once the inferred-own-level anchor is deployed.

Waits until justhodl-cds-desk contains _median_branch_picks, then backfills
2026-09-01 through today. Earlier history is left as stored. Capped blocks and
an inferred own level can then sign names the previous writer left unpriced.
"""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3

FN = "justhodl-cds-desk"
REGION = "us-east-1"
MARKER = b"_median_branch_picks"
START = "2026-09-01"


def code_has_marker(lam):
    url = lam.get_function(FunctionName=FN)["Code"]["Location"]
    raw = urllib.request.urlopen(url, timeout=60).read()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    return any(name.endswith("cds_desk.py") and MARKER in zf.read(name) for name in zf.namelist())


def main():
    lam = boto3.client("lambda", region_name=REGION)
    deadline = time.time() + 25 * 60
    while time.time() < deadline:
        if code_has_marker(lam):
            print("deployed code has the inferred-anchor marker")
            break
        print("waiting for justhodl-cds-desk deploy")
        time.sleep(30)
    else:
        raise SystemExit("lambda never picked up the inferred-anchor fix")
    end = datetime.now(timezone.utc).date().isoformat()
    cur = START
    for _ in range(8):
        payload = {"action": "backfill", "start": cur, "end": end}
        print("invoke", payload)
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
        raw = resp["Payload"].read()
        if resp.get("FunctionError"):
            raise SystemExit("lambda error " + raw[:800].decode("utf-8", "replace"))
        body = json.loads(raw)
        keep = ("ok", "action", "files", "last_file", "budget_exhausted", "next_start", "as_of", "error", "sovereign_coverage", "n_liquid")
        print(json.dumps({k: body.get(k) for k in keep}, default=str))
        if not body.get("ok"):
            raise SystemExit("backfill not ok")
        nxt = body.get("next_start")
        if not body.get("budget_exhausted") or not nxt:
            print("reprice done")
            return
        cur = nxt
    raise SystemExit("still budget-exhausted after 8 chunks")


if __name__ == "__main__":
    main()
