"""ops/4766 -- verify the dollar fix: retry-invoke justhodl-repo until
the v1.1 code answers (deploy lag), then confirm dollar rows > 0 in
data/repo.json and open DTWEXBGS's history file for its real span."""
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
    with report("4766_repo_dollar_verify") as rep:
        rep.heading("ops 4766 -- dollar fix verify (v1.1 retry-invoke)")
        got = None
        for i in range(7):
            r = lam.invoke(FunctionName="justhodl-repo",
                            InvocationType="RequestResponse", Payload=b"{}")
            body = json.loads(json.loads(
                r["Payload"].read().decode())["body"])
            rep.log(f"attempt {i}: v={body.get('v')} series={body.get('series')} "
                    f"secs={body.get('secs')}")
            if body.get("v") == "1.1":
                got = body
                break
            time.sleep(25)
        rep.kv(check="v11_answered", value=bool(got))
        if not got:
            rep.warn("deploy of v1.1 not observed after ~3 min of retries")
            return
        d = sread("data/repo.json")
        dollar = [s for g in d["groups"] for s in g["series"]
                   if g["name"].startswith("Dollar")]
        rep.kv(check="dollar_rows", value=len(dollar))
        for m in d.get("dollar_missing") or []:
            rep.log(f"  still missing: {m['id']} cats_tried={m['cats_tried']}")
        for s0 in dollar:
            rep.log(f"  {s0['id']}: last={s0['last_value']} "
                    f"m%={(s0['chg'] or {}).get('m')} "
                    f"y%={(s0['chg'] or {}).get('y')} n={s0['n_obs']}")
        if dollar:
            h = sread(f"data/repo-history/{dollar[0]['sid']}.json")
            rep.ok(f"{dollar[0]['id']} history: {len(h['dates'])} obs, "
                    f"{h['dates'][0]} -> {h['dates'][-1]}")
        b = d.get("barometer") or {}
        comp = next((c for c in b.get("components") or []
                      if "dollar" in c["name"].lower()), None)
        rep.kv(check="barometer_dollar_component",
                value=(comp or {}).get("value"))
        rep.kv(check="barometer_score", value=b.get("score"))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
