"""ops/4899 -- failures classification on the 214 set: zero unexplained.

The 214/214 walk (ops 4898) ledgered 7 failures; the kv sample showed
five as HTTP 404. Khalid's standard is EVERY flow accounted for, so
this op probes ALL of them with lastNObservations=1 and classifies:
404 = SOURCE_EMPTY (registry entry with no data behind the flowRef --
proven, documented); 200 = ALIVE (a real gap -> one retry_failures
blitz, must end zero). The classification is written permanently to
data/warm/ecb/failures-classified.json (deny-Delete'd prefix) so the
coverage story is complete: 214 banked = 207 with data + N proven
empty-at-source + 0 unexplained.
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def probe(fid):
    fr = str(fid).replace(":", ",")
    u = ("https://data-api.ecb.europa.eu/service/data/"
         f"{fr}?format=csvdata&lastNObservations=1")
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=45)
        body = r.read(4096)
        return 200, len(body)
    except urllib.error.HTTPError as e:
        return e.code, 0
    except Exception as e:
        return type(e).__name__, 0


def main():
    verdict = {"ops": 4899, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4899 -- failures classified zero unexplained"
                ) as rep:
        rep.heading("ops 4899 — 214-set failures: zero unexplained")
        walk = g("data/_state/sdmx-walk-ecb.json")
        fails = dict(walk.get("failures") or {})
        rep.kv(stage="ledger", n=len(fails),
               ids=",".join(list(fails)[:12]))

        cls, alive = {}, []
        for fid in fails:
            code, nb = probe(fid)
            if code == 200 and nb > 40:
                cls[fid] = {"code": 200, "class": "ALIVE"}
                alive.append(fid)
            elif code == 404:
                cls[fid] = {"code": 404, "class": "SOURCE_EMPTY"}
            else:
                cls[fid] = {"code": code, "class": "OTHER"}
                alive.append(fid)
            time.sleep(0.3)
        rep.kv(stage="probe",
               source_empty=sum(1 for c in cls.values()
                                if c["class"] == "SOURCE_EMPTY"),
               alive=",".join(alive) or "none",
               detail=json.dumps(cls)[:400])

        if alive:
            before = walk.get("as_of")
            lam.invoke(FunctionName="justhodl-sdmx-walker",
                       InvocationType="Event",
                       Payload=json.dumps(
                           {"agency": "ecb", "retry_failures": 1,
                            "per": 120, "cap_mb": 150,
                            "budget": 500}).encode())
            t = time.time()
            while time.time() - t < 640:
                time.sleep(25)
                w2 = g("data/_state/sdmx-walk-ecb.json")
                if w2.get("as_of") != before and \
                        float(w2.get("lease_until") or 0) \
                        <= time.time():
                    walk = w2
                    break
            fails2 = dict(walk.get("failures") or {})
            alive = [f for f in alive if f in fails2]
            rep.kv(stage="retry", remaining_alive=",".join(alive)
                   or "none", ledger_after=len(fails2))
            for f in alive:
                cls[f]["class"] = "UNRESOLVED"

        unresolved = [f for f, c in cls.items()
                      if c["class"] in ("ALIVE", "OTHER",
                                        "UNRESOLVED")]
        s3.put_object(
            Bucket=B, Key="data/warm/ecb/failures-classified.json",
            Body=json.dumps({
                "as_of": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"),
                "ops": 4899,
                "n_ledger": len(fails),
                "n_source_empty": sum(
                    1 for c in cls.values()
                    if c["class"] == "SOURCE_EMPTY"),
                "n_unresolved": len(unresolved),
                "entries": cls,
                "note": ("SOURCE_EMPTY = registry entry with no data "
                         "behind the flowRef, proven by "
                         "lastNObservations=1 probe")},
                default=str).encode(),
            ContentType="application/json", CacheControl="no-cache")
        rep.kv(stage="written",
               key="data/warm/ecb/failures-classified.json",
               unresolved=len(unresolved))
        verdict["gates"]["zero_unexplained"] = (
            "PASS" if not unresolved else "FAIL")
        verdict["classification"] = cls
        verdict["overall"] = ("PASS" if not unresolved else "FAIL")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"])
        out = ROOT / "aws" / "ops" / "reports" / "4899.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4899.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
