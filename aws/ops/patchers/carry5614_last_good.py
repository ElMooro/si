#!/usr/bin/env python3
"""ops 5614 — justhodl-carry-surface: last-good snapshot if publish or build fails."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-carry-surface/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    needle = "    # Save main output\n    s3.put_object("
    if needle not in t:
        needle = "    s3.put_object(\n        Bucket=BUCKET, Key=OUT_KEY,"
    if needle not in t:
        raise SystemExit("carry put_object block not found")
    insert = '''    def _read_last_good():
        try:
            return json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        except Exception:
            return None

'''
    if "_read_last_good" not in t:
        t = t.replace(needle, insert + needle, 1)

    # Wrap handler failures: if the function already has a top-level except, skip.
    marker = "def lambda_handler(event, context):"
    if marker not in t:
        raise SystemExit("no lambda_handler")
    if "ops 5614 last-good" not in t:
        wrap = '''def lambda_handler(event, context):
    # ops 5614 last-good
    try:
        return _lambda_handler_inner(event, context)
    except Exception as exc:
        s3 = boto3.client("s3")
        prev = None
        try:
            prev = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        except Exception:
            prev = None
        if isinstance(prev, dict) and prev:
            prev = dict(prev)
            q = dict(prev.get("quality") or {})
            q.update({"status": "stale", "publish_error": type(exc).__name__, "note": "last-good snapshot"})
            prev["quality"] = q
            prev["ok"] = False
            s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                          Body=json.dumps(prev).encode(),
                          ContentType="application/json", CacheControl="no-cache")
            return {"statusCode": 200, "body": json.dumps({"ok": False, "used_last_good": True})}
        raise

def _lambda_handler_inner(event, context):
'''
        t = t.replace(marker, wrap, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched carry-surface", len(t))


if __name__ == "__main__":
    main()
