"""ops 5561 -- exam forensics (read-only): why 0/164 with 21 critical failures twice? Prints the failure-reason histogram
from the latest gen-0 result and the first completions the exam job actually produced. Launches nothing."""
from __future__ import annotations

import io
import json
import sys
import tarfile
from collections import Counter
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PRI = "justhodl-ai-857687956942"
BURST = "jh-exam-gen0-20260914-213415"


def main() -> int:
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("ops_5561_exam_forensics") as R:
        R.heading("ops 5561 -- exam forensics: failure reasons + raw completions of the base exam job")
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/exams/code/results/gen-0-").get("Contents", [])]
        latest = json.loads(s3.get_object(Bucket=PRI, Key=sorted(keys)[-1])["Body"].read())
        reasons = Counter(str(v)[:60] for v in (latest.get("failure_reasons") or {}).values())
        R.ok("latest gen-0 result %s: reasons %s" % (sorted(keys)[-1].split("/")[-1], json.dumps(reasons.most_common(12))))
        R.ok("verifier stderr tail: %s" % (latest.get("verifier_stderr") or "")[:300])
        rec = json.loads(s3.get_object(Bucket=PRI, Key="factory/bursts/jobs/%s.json" % BURST)["Body"].read())
        key = rec["out_uri"].split(PRI + "/", 1)[1] + BURST + "/output/model.tar.gz"
        blob = s3.get_object(Bucket=PRI, Key=key)["Body"].read()
        tf = tarfile.open(fileobj=io.BytesIO(blob))
        names = tf.getnames()
        R.ok("job output members: %s" % names[:10])
        member = [n for n in names if n.endswith("exam.jsonl")]
        if member:
            lines = tf.extractfile(member[0]).read().decode("utf-8", "replace").splitlines()
            R.ok("exam.jsonl rows: %d" % len(lines))
            for ln in lines[:3]:
                d = json.loads(ln)
                R.log("  task=%s finish=%s tokens=%s completion[:400]=%r" % (d.get("task_id"), d.get("finish"), d.get("tokens"), str(d.get("completion"))[:400]))
        man = [n for n in names if n.endswith("burst_manifest.json")]
        if man:
            R.log("  manifest: %s" % tf.extractfile(man[0]).read().decode()[:600])
        R.ok("GREEN -- forensics recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
