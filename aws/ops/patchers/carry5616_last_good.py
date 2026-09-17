#!/usr/bin/env python3
"""ops 5616 — last-good snapshot on justhodl-carry-surface."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-carry-surface/source/lambda_function.py"
OLD = "def lambda_handler(event=None, context=None):\n"
NEW = '''def lambda_handler(event=None, context=None):
    # ops 5616 last-good
    try:
        return _lambda_handler_inner(event, context)
    except Exception as exc:
        s3c = boto3.client("s3")
        prev = None
        try:
            prev = json.loads(s3c.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        except Exception:
            prev = None
        if isinstance(prev, dict) and prev:
            prev = dict(prev)
            q = dict(prev.get("quality") or {})
            q.update({"status": "stale", "publish_error": type(exc).__name__, "note": "last-good snapshot"})
            prev["quality"] = q
            prev["ok"] = False
            s3c.put_object(Bucket=BUCKET, Key=OUT_KEY,
                           Body=json.dumps(prev).encode(),
                           ContentType="application/json", CacheControl="no-cache")
            return {"statusCode": 200, "body": json.dumps({"ok": False, "used_last_good": True})}
        raise

def _lambda_handler_inner(event=None, context=None):
'''


def main() -> None:
    t = TARGET.read_text()
    if "ops 5616 last-good" in t:
        print("already patched")
        return
    if OLD not in t:
        raise SystemExit("handler signature not found")
    t = t.replace(OLD, NEW, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched", len(t))


if __name__ == "__main__":
    main()
