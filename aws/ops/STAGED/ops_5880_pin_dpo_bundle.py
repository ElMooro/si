"""ops 5880 -- pin the trainer bundle with mode=dpo and count preference pairs (Claude, 2026-09-22). Direct lane.
1. re-pin factory/training/ (train_qlora.py with the dpo mode) under the SAME pinned image (factory_training_pin.py --image <current>)
2. count verified preference_pair rows (the doctrine harvest) and pairs derivable from traces
3. control: train_mode=auto, min_pairs=50 (a launch becomes dpo only when the dataset carries >= 50 pairs)
"""
import json, subprocess, sys
from collections import Counter
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]; PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5880_pin_dpo_bundle") as r:
        r.heading("ops 5880 -- pin the dpo-capable trainer; count pairs; train_mode=auto")
        pin = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        image = pin.get("training_image")
        r.kv(current_bundle=str(pin.get("bundle_uri"))[-40:], image=str(image)[-60:], require_digest=pin.get("require_digest"))
        p = subprocess.run([sys.executable, "scripts/factory_training_pin.py", "--image", image] + (["--require-digest"] if pin.get("require_digest") else []), cwd=REPO, capture_output=True, text=True, timeout=300)
        for line in (p.stdout or "").strip().splitlines()[-6:]:
            r.log(line[:300])
        if p.returncode != 0:
            r.fail("pin failed: %s" % (p.stderr or "")[-400:]); sys.exit(1)
        new = json.loads(s3.get_object(Bucket=PRI, Key="factory/training/current.json")["Body"].read())
        r.ok("pinned bundle %s (was %s)" % (str(new.get("bundle_sha256"))[:12], str(pin.get("bundle_sha256"))[:12]))
        r.section("2. Preference pairs available")
        kinds, pairs = Counter(), 0
        token = None
        n = 0
        while True:
            kw = {"Bucket": PRI, "Prefix": "factory/curriculum/code/verified/", "MaxKeys": 1000}
            if token: kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents", []):
                n += 1
                if n % 3 == 0 and n < 9000:        # sample a third of the prefix for the kind histogram
                    try:
                        d = json.loads(s3.get_object(Bucket=PRI, Key=o["Key"])["Body"].read())
                        kinds[str(d.get("kind"))] += 1
                        if isinstance(d.get("rejected"), str) and d["rejected"]:
                            pairs += 1
                    except Exception:  # noqa: BLE001
                        pass
            token = page.get("NextContinuationToken")
            if not token: break
        r.kv(verified_objects=n, sampled_kinds=json.dumps(dict(kinds)), sampled_pairs=pairs, pairs_estimate=pairs * 3)
        traces = 0
        token = None
        while True:
            kw = {"Bucket": PRI, "Prefix": "factory/traces/code/", "MaxKeys": 1000}
            if token: kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw); traces += len(page.get("Contents", [])); token = page.get("NextContinuationToken")
            if not token: break
        r.kv(trace_objects_private=traces)
        r.section("3. Control")
        ctl = json.loads(s3.get_object(Bucket=PRI, Key="factory/control/gearb.json")["Body"].read())
        ctl["train_mode"] = "auto"; ctl["min_pairs"] = 50
        s3.put_object(Bucket=PRI, Key="factory/control/gearb.json", Body=json.dumps(ctl, indent=1).encode(), ContentType="application/json")
        r.ok("train_mode=auto, min_pairs=50")


if __name__ == "__main__":
    main()
