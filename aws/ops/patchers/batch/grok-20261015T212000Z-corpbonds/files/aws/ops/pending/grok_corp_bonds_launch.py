"""Wait for justhodl-corp-bonds, publish once, and prove data/corp-bonds.json."""
import json
import time

import boto3

FN = "justhodl-corp-bonds"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/corp-bonds.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_function():
    deadline = time.time() + 25 * 60
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            print("function", cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("CodeSha256", "")[:16])
            if cfg.get("State") == "Active":
                return cfg
        except Exception as exc:
            print("waiting", type(exc).__name__)
        time.sleep(20)
    raise SystemExit("function never became active")


def main():
    wait_for_function()
    resp = lam.invoke(FunctionName=FN, Payload=json.dumps({"action": "publish"}).encode())
    raw = resp["Payload"].read().decode()
    print("invoke", resp.get("StatusCode"), resp.get("FunctionError"), raw[:1500])
    if resp.get("FunctionError"):
        raise SystemExit("invoke failed")
    body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    doc = json.loads(body)
    euro = doc.get("euro") or {}
    print(json.dumps({
        "engine": doc.get("engine"),
        "as_of": doc.get("as_of"),
        "bytes": len(body),
        "sleeves": len(doc.get("sleeves") or []),
        "euro_bonds": euro.get("n_bonds"),
        "euro_issuers": euro.get("n_issuers"),
        "cds_matched": doc.get("cds_matched_issuers"),
        "read": doc.get("read"),
    }))
    if doc.get("engine") != FN and doc.get("engine") != "justhodl-corp-bonds":
        raise SystemExit("wrong engine")
    if not doc.get("sleeves") and not euro.get("n_bonds"):
        raise SystemExit("packet has neither sleeves nor euro bonds")
    if euro.get("n_bonds", 0) < 50:
        raise SystemExit("euro book too small: %s" % euro.get("n_bonds"))


if __name__ == "__main__":
    main()
