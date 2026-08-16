"""ops/4773 -- after 4772's map lands and v1.3 deploys: retry-invoke
justhodl-repo until v=1.3, then verify PD rows' new floors on the LIVE
board: count NYPD rows with first < 2015, < 2013, < 2005; print the
deepest ten; confirm a spliced row's history file really carries the
old dates."""
import gzip
import json
import sys
import time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=900, connect_timeout=10,
                                   retries={"max_attempts": 0}))


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4783_names_verify") as rep:
        rep.heading("ops 4783 -- splice live on the board?")
        d = None
        for i in range(9):
            r = lam.invoke(FunctionName="justhodl-repo",
                            InvocationType="RequestResponse", Payload=b"{}")
            body = json.loads(json.loads(
                r["Payload"].read().decode())["body"])
            rep.log(f"attempt {i}: v={body.get('v')} "
                    f"series={body.get('series')}")
            if body.get("v") == "1.5":
                d = sread("data/repo.json")
                break
            time.sleep(22)
        rep.kv(check="v13_answered", value=bool(d))
        if not d:
            return
        ny = [s0 for g in d["groups"] for s0 in g["series"]
               if s0["id"].startswith("NYPD-")]
        for cut in ("2015-01-01", "2013-01-01", "2005-01-01", "1999-01-01"):
            n = sum(1 for s0 in ny if (s0["first"] or "9") < cut)
            rep.kv(**{f"rows_before_{cut[:4]}": n})
        deep = sorted(ny, key=lambda s0: s0["first"] or "9")[:10]
        for s0 in deep:
            rep.ok(f"  {s0['id']}: {s0['first']} -> {s0['last']} "
                    f"n={s0['n_obs']}")
        noname = sum(1 for g in d["groups"] for s0 in g["series"]
                      if not (s0.get("label") or "").strip())
        rep.kv(check="rows_without_name", value=noname)
        if deep:
            h = sread(f"data/repo-history/{deep[0]['sid']}.json")
            rep.kv(check="deepest_history_first", value=h["dates"][0])
            rep.kv(check="deepest_history_n", value=len(h["dates"]))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
