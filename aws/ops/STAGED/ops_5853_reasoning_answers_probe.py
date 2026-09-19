"""ops 5853 -- why 0/20 on program-output prediction? (Claude, 2026-09-19). READ-ONLY: print 6 code_reading rows
(gold vs the model's answer tail) and 2 wrong gsm8k rows from the latest reasoning exam."""
import json, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5853_reasoning_answers_probe") as r:
        r.heading("ops 5853 -- reasoning exam answers")
        latest = json.loads(s3.get_object(Bucket=PRI, Key="factory/exams/reasoning/latest.json")["Body"].read())
        doc = json.loads(s3.get_object(Bucket=PRI, Key="factory/exams/reasoning/%s.json" % latest["run_id"])["Body"].read())
        shown = 0
        for row in doc["rows"]:
            if row["family"] == "code_reading" and shown < 6:
                shown += 1
                r.log("CODE %s gold=%r" % (row["id"], row["gold"][:120]))
                r.log("     answer=%r" % (row.get("answer_head") or "")[-220:])
        wrong = [row for row in doc["rows"] if row["family"] == "gsm8k" and row["correct"] is False][:2]
        for row in wrong:
            r.log("GSM %s gold=%s answer_tail=%r" % (row["id"], row["gold"], (row.get("answer_head") or "")[-200:]))
        r.ok("done")


if __name__ == "__main__":
    main()
