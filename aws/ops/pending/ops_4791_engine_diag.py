"""ops/4791 -- raw engine diagnostic: invoke justhodl-repo once,
print StatusCode / FunctionError / ExecutedVersion and the FIRST 1400
chars of the payload verbatim (traceback lives there on Unhandled).
No parsing, no assumptions."""
import sys
import time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=920, connect_timeout=10,
                                   retries={"max_attempts": 0}))


def main():
    with report("4791_engine_diag") as rep:
        rep.heading("ops 4791 -- justhodl-repo raw invoke diagnostic")
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName="justhodl-repo",
                            InvocationType="RequestResponse",
                            Payload=b"{}")
            rep.kv(status=r.get("StatusCode"),
                    fn_error=r.get("FunctionError"),
                    secs=round(time.time() - t0, 1))
            body = r["Payload"].read().decode("utf-8", "replace")
            rep.log("payload[:1400]:\n" + body[:1400])
        except Exception as e:
            rep.kv(status="EXC", fn_error=type(e).__name__,
                    secs=round(time.time() - t0, 1))
            rep.log(str(e)[:600])


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
