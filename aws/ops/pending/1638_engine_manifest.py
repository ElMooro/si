# ops 1638 — generate engine manifest from repo scan, upload to S3, verify
import json, subprocess, os
import boto3
from botocore.config import Config
s3 = boto3.client("s3", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
B = "justhodl-dashboard-live"
out = {"ops": 1638}
r = subprocess.run(["python3", "ops/tools/gen_engine_manifest.py"],
                    capture_output=True, text=True, timeout=120)
assert r.returncode == 0, r.stderr[:300]
man = json.loads(r.stdout)
s3.put_object(Bucket=B, Key="data/engine-manifest.json", Body=r.stdout.encode(),
              ContentType="application/json", CacheControl="public, max-age=3600")
inv = [e["engine"] for e in man["engines"] if not e["page"] and not e["on_board"]]
out["verify"] = {"n_engines": man["n_engines"], "n_no_page": man["n_no_page"],
                  "n_board_only": man["n_board_only"], "n_invisible": man["n_invisible"],
                  "invisible_full": inv}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1638_engine_manifest.json", "w").write(json.dumps(out, indent=1))
print(json.dumps(out["verify"] | {"invisible_full": len(inv)}))
