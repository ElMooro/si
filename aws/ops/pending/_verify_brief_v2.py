"""Final verify: call-verb extraction now correct + brief.html shows new tiles."""
import json
import time
import urllib.request
import boto3
from ops_report import report

UA = {"User-Agent": "justhodl-audit/1.0"}
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_brief_v2") as r:
        # 1. Wait for Lambda
        r.heading("0) Wait for ai-brief redeploy")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ ready, mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        # 2. Re-invoke
        r.heading("1) Re-invoke ai-brief")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  brief_chars: {inner.get('brief_chars')}")
            r.log(f"  duration_s:  {inner.get('duration_s')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 3. Verify call extraction
        r.heading("2) Verify call_verb is now extracted correctly")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  n_snapshots: {d.get('n_snapshots')}")
            r.log(f"  recent snapshots:")
            for snap in (d.get("snapshots") or [])[-5:]:
                r.log(f"    ts={snap.get('timestamp')[:19]}  call={snap.get('call_verb'):20s}  phase={snap.get('phase')}  ki={snap.get('khalid_score')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Pull DECISIVE CALL section to confirm verb is in it
        r.heading("3) Decisive Call section content (last 1500 chars)")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.md")
            md = obj["Body"].read().decode("utf-8")
            r.log(f"  brief size: {len(md):,}b")
            r.log(md[-1500:])
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. Verify brief.html live + new tiles
        r.heading("4) brief.html live with new tiles")
        code, body, err = fetch("https://justhodl.ai/brief.html")
        if err:
            r.log(f"  ✗ {err}")
        else:
            checks = [
                ("17 systems lede", "17 live systems" in body),
                ("calibration_v2 tile key", "calibration_v2" in body),
                ("paper_portfolio tile key", "paper_portfolio" in body),
                ("decisive-call-history mentioned", "decisive-call-history" in body),
                ("Trust Ranking label", "Trust Ranking" in body),
                ("Paper Portfolio label", "Paper Portfolio" in body),
            ]
            for label, ok in checks:
                r.log(f"  {'✓' if ok else '✗'} {label}")


if __name__ == "__main__":
    main()
