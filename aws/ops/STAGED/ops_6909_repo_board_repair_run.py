"""ops 6909 - ACTING: one repair run of the repo board after FRED refused its 06:10 UTC run.

At 06:10 UTC on 2026-11-22 FRED refused every request for about half a minute (ops 6907, 6908).  The engine asked its
FRED discovery once, got nothing, and published 766 series instead of 1,800: no FRED group, no policy rates, none of
the spreads built on them.  The live desk is missing those tiles until the board is rebuilt, and its own schedule is
24 hours away.  The engine now asks a short discovery again and, if FRED still refuses, serves the group from its
bank.  One invoke outside the schedule (disclosed to Khalid).

  1. Guard: the deployed package must hold this checkout's repo_sources.py and repo_scope.py; otherwise no invoke.
  2. One synchronous invoke.
  3. Read back: series and groups, the FRED group and how its inventory was obtained, the policy rows.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE, FN = "justhodl-dashboard-live", "justhodl-repo"


def selftest():
    assert FN == "justhodl-repo"


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=900))
    s3 = boto3.client("s3", region_name="us-east-1")

    def board():
        o = s3.get_object(Bucket=LIVE, Key="data/repo.json")
        return json.loads(o["Body"].read()), o["LastModified"]

    with report("ops_6909_repo_board_repair_run") as r:
        r.heading("ops 6909 - the repo board rebuilt after FRED's refusal, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in ("repo_sources.py", "repo_scope.py"):
            hits = [x for x in z.namelist() if x == name or x.endswith("/" + name)]
            same[name] = bool(hits) and z.read(hits[0]) == (ROOT / "aws/lambdas/justhodl-repo/source" / name).read_bytes()
        handler = [x for x in z.namelist() if x.endswith("lambda_function.py")]
        wired = bool(handler) and b"fred_group_inventory(" in z.read(handler[0])
        r.log("%s updated %s | modules as in this checkout: %s | the handler calls the inventory check: %s | timeout %s s" % (
            FN, (g["Configuration"].get("LastModified") or "")[:19], same, wired, g["Configuration"].get("Timeout")))
        if not (all(same.values()) and wired):
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        old, when0 = board()
        r.log("before: written %s | engine %s | %d groups, %s series" % (when0.strftime("%Y-%m-%d %H:%M"), old.get("engine_v"), len(old.get("groups") or []), (old.get("counts") or {}).get("series")))
        t0 = time.time()
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = res["Payload"].read().decode("utf-8", "replace")
        took = time.time() - t0
        try:
            body = json.loads(json.loads(raw).get("body") or "{}")
        except Exception:  # noqa: BLE001
            body = {}
        r.log("invoke: HTTP %s%s, %.0f s | %s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", took, json.dumps(body)[:400] if body else raw[:400]))
        new, when = board()
        rows = {s["id"]: dict(s, group=gr["name"]) for gr in new.get("groups") or [] for s in gr["series"]}
        diag = new.get("diag") or {}
        fred_n = sum(1 for x in rows.values() if x["group"] == "FRED repo & SOFR complex")
        r.log("after: written %s | engine %s | %d groups, %d series | counts %s" % (when.strftime("%Y-%m-%d %H:%M"), new.get("engine_v"), len(new.get("groups") or []), len(rows), json.dumps(new.get("counts"))))
        r.log("FRED group: %d series | inventory: %s | own sources: %s" % (fred_n, json.dumps(diag.get("fred_group")), json.dumps({k: v.get("status") for k, v in (diag.get("own_sources") or {}).items()})))
        for sid in ("SOFR", "IORB", "RRPONTSYAWARD", "DFEDTARU", "DFEDTARL", "DFEDTAR", "WRESBAL", "GC_SURVEY", "D_SOFR_ADMIN", "D_EFFR_TARGET", "D_REPO_TARGET", "D_TARGET_REPO"):
            x = rows.get(sid) or {}
            r.log("  %-16s %s obs, %s to %s: %s" % (sid, x.get("n_obs"), x.get("first"), x.get("last"), x.get("last_value")))
        try:
            inv = json.loads(s3.get_object(Bucket=LIVE, Key="data/repo-state/fred-repo-inventory.json")["Body"].read())
            r.log("recorded inventory: %d ids at %s" % (len(inv.get("ids") or {}), inv.get("at")))
        except Exception as e:  # noqa: BLE001
            r.log("recorded inventory: %s" % type(e).__name__)
        ok = (when > when0 and fred_n >= 900 and len(rows) >= 1800 and (diag.get("fred_group") or {}).get("inventory") in ("fred", "fred, asked again")
              and all(s in rows for s in ("IORB", "DFEDTARU", "D_SOFR_ADMIN", "D_REPO_TARGET", "D_TARGET_REPO")))
        r.kv(series=len(rows), groups=len(new.get("groups") or []), fred_group=fred_n, inventory=(diag.get("fred_group") or {}).get("inventory"), run_seconds=round(took))
        if ok:
            r.ok("the board is whole again: %d series in %d groups, the FRED group asked of FRED itself" % (len(rows), len(new.get("groups") or [])))
        else:
            r.fail("the board is not whole: %d series, FRED group %d, inventory %s" % (len(rows), fred_n, json.dumps(diag.get("fred_group"))))
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
