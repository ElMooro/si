#!/usr/bin/env python3
"""ops 5640 — publish missing public aliases from live packets.
/data/ciss.json 403 ← copy data/ciss-stress.json
/data/primary-dealers.json 403 ← copy data/nyfed-primary-dealer.json
Does not replace settlement-fails or ciss-stress.
"""
import json
import boto3

BUCKET = "justhodl-dashboard-live"
COPIES = [
    ("data/ciss-stress.json", "data/ciss.json"),
    ("data/nyfed-primary-dealer.json", "data/primary-dealers.json"),
]


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    out = {"ok": True, "copied": []}
    for src, dst in COPIES:
        obj = s3.get_object(Bucket=BUCKET, Key=src)
        body = obj["Body"].read()
        json.loads(body)  # must be JSON
        s3.put_object(
            Bucket=BUCKET,
            Key=dst,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
        out["copied"].append({"src": src, "dst": dst, "bytes": len(body)})
        print("copied", src, "->", dst, len(body))
    print(json.dumps(out))
    return out


if __name__ == "__main__":
    main()
