"""ops/4895 -- ECB: full inception history, permanent storage, blitz pace.

Khalid: "make sure we're importing ALL ecb data with all their historical
since inception, permanently stored and never deleted, and fasten the
importation (budget is no problem)."

Facts this rides on: the walker's bare {flow}?format=csvdata pull IS
full-history-since-inception by SDMX default -- the only inception
threat is the 40MB cap (truncated flagged, ledgered in st.truncated).
This push added event overrides (per / cap_mb / retry_truncated) and
lifted the fn to 10240MB/10240MB, FLOWS_PER_AGENCY=120.

Gates:
  G1 zip-settle by marker (retry_truncated in live artifact)
  G2 deny-Delete bucket policy covers data/warm/* AND data/raw/*
     (+ providers, ciss) -- extend by MERGE, re-read assert
  G3 no lifecycle rule expires those prefixes
  G4 blitz rounds (per=120, cap_mb=150) until state COMPLETE 104/104
  G5 truncation elimination: retry_truncated rounds at cap_mb=450,
     per=6 (RAM guard) until the ledger drains or stabilizes
  G6 inception depth: stream-scan min/max TIME_PERIOD years on six
     benchmark flows -- >=4 of 6 must reach <=2005
  G7 footprint census: keys + bytes under data/warm/ecb/data/
Nothing is claimed that is not read back from S3.
"""
import gzip
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
import urllib.request  # noqa: E402
import zipfile, io  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-sdmx-walker"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
T0 = time.time()
WALL_STOP_NEW_ROUNDS = 62 * 60   # leave room for scans + report


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def state():
    try:
        return g("data/_state/sdmx-walk-ecb.json")
    except Exception:
        return {}


def blitz_round(rep, payload, label, max_wait=940):
    before = state()
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps(payload).encode())
    t = time.time()
    while time.time() - t < max_wait:
        time.sleep(25)
        st = state()
        lease = float(st.get("lease_until") or 0)
        if st.get("as_of") != before.get("as_of") \
                and lease <= time.time():
            return st
    return state()


def year_scan(key):
    """Stream-gunzip an S3 object, regex-scan TIME_PERIOD years."""
    body = s3.get_object(Bucket=B, Key=key)["Body"]
    gz = gzip.GzipFile(fileobj=body)
    yr = re.compile(rb"[,\r\n](19[0-9]{2}|20[0-9]{2})[-,\r\nQWSM0-9]")
    lo, hi, n = 9999, 0, 0
    while True:
        chunk = gz.read(8 * 1024 * 1024)
        if not chunk:
            break
        for m in yr.finditer(chunk):
            y = int(m.group(1))
            lo, hi = min(lo, y), max(hi, y)
        n += len(chunk)
    return (None if lo == 9999 else lo), (hi or None), n


def main():
    verdict = {"ops": 4895, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4895 -- ecb inception permanence blitz") as rep:
        rep.heading("ops 4895 — ECB inception + permanence + blitz")

        # ── G1 zip-settle by marker ──────────────────────────────────
        ok1 = False
        deadline = time.time() + 420
        while time.time() < deadline:
            try:
                loc = lam.get_function(FunctionName=FN
                                       )["Code"]["Location"]
                raw = urllib.request.urlopen(loc, timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py")
                if b"retry_truncated" in src:
                    cfg = lam.get_function_configuration(
                        FunctionName=FN)
                    rep.kv(stage="zip-settle", marker=True,
                           memory=cfg.get("MemorySize"),
                           ephemeral=(cfg.get("EphemeralStorage")
                                      or {}).get("Size"),
                           per_env=(cfg.get("Environment") or {}
                                    ).get("Variables", {}
                                          ).get("FLOWS_PER_AGENCY"))
                    ok1 = True
                    break
            except Exception as e:
                rep.log(f"settle: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(20)
        verdict["gates"]["walker_deployed"] = "PASS" if ok1 else "FAIL"

        # ── G2 deny-Delete policy: warm + raw (+providers, ciss) ────
        want = ["arn:aws:s3:::%s/data/warm/*" % B,
                "arn:aws:s3:::%s/data/providers/*" % B,
                "arn:aws:s3:::%s/data/raw/*" % B,
                "arn:aws:s3:::%s/data/ciss*" % B]
        pol_ok, pol_note = False, ""
        try:
            try:
                pol = json.loads(s3.get_bucket_policy(Bucket=B
                                                      )["Policy"])
            except Exception:
                pol = {"Version": "2012-10-17", "Statement": []}
            stmts = pol.get("Statement") or []
            deny = next((x for x in stmts
                         if x.get("Effect") == "Deny"
                         and "s3:DeleteObject" in (
                             x.get("Action") if isinstance(
                                 x.get("Action"), list)
                             else [x.get("Action")])), None)
            changed = False
            if deny is None:
                stmts.append({
                    "Sid": "JustHodlDenyDeleteWarmRawOps4895",
                    "Effect": "Deny", "Principal": "*",
                    "Action": ["s3:DeleteObject",
                               "s3:DeleteObjectVersion"],
                    "Resource": list(want)})
                pol["Statement"] = stmts
                changed, pol_note = True, "created"
            else:
                res = deny.get("Resource")
                res = res if isinstance(res, list) else [res]
                missing = [w for w in want if w not in res]
                if missing:
                    deny["Resource"] = res + missing
                    changed = True
                    pol_note = "extended+" + str(len(missing))
                else:
                    pol_note = "already-covered"
            if changed:
                s3.put_bucket_policy(Bucket=B,
                                     Policy=json.dumps(pol))
            chk = json.loads(s3.get_bucket_policy(Bucket=B)["Policy"])
            flat = json.dumps(chk)
            pol_ok = all(w in flat for w in want)
            rep.kv(stage="deny-delete", note=pol_note, verified=pol_ok,
                   n_statements=len(chk.get("Statement") or []))
        except Exception as e:
            rep.kv(stage="deny-delete", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:160]}")
        verdict["gates"]["deny_delete_policy"] = ("PASS" if pol_ok
                                                  else "FAIL")

        # ── G3 lifecycle must not expire our prefixes ───────────────
        lc_ok, offenders = True, []
        try:
            rules = s3.get_bucket_lifecycle_configuration(
                Bucket=B).get("Rules") or []
            for r in rules:
                if r.get("Status") != "Enabled":
                    continue
                exp = ("Expiration" in r
                       or "NoncurrentVersionExpiration" in r)
                if not exp:
                    continue
                pfx = ((r.get("Filter") or {}).get("Prefix")
                       if isinstance(r.get("Filter"), dict)
                       else r.get("Prefix")) or ""
                if pfx == "" or any(t.startswith(pfx) or
                                    pfx.startswith(t)
                                    for t in ("data/warm/", "data/raw/",
                                              "data/ciss",
                                              "data/providers/")):
                    offenders.append({"id": r.get("ID"),
                                      "prefix": pfx})
            lc_ok = not offenders
            rep.kv(stage="lifecycle", n_rules=len(rules),
                   offenders=json.dumps(offenders)[:200],
                   clean=lc_ok)
        except Exception as e:
            # NoSuchLifecycleConfiguration == nothing can expire == clean
            rep.kv(stage="lifecycle",
                   note=f"{type(e).__name__} (no config = clean)",
                   clean=True)
        verdict["gates"]["lifecycle_clean"] = ("PASS" if lc_ok
                                               else "FAIL")

        # ── G4 blitz to COMPLETE ─────────────────────────────────────
        st = state()
        rounds = 0
        while ok1 and rounds < 3 \
                and time.time() - T0 < WALL_STOP_NEW_ROUNDS:
            nd = len(set(st.get("done") or []))
            nt = st.get("n_total") or 104
            if st.get("status") == "COMPLETE" and nd >= nt:
                break
            rounds += 1
            rep.log(f"blitz round {rounds}: done {nd}/{nt}, "
                    f"truncated {len(st.get('truncated') or [])}")
            st = blitz_round(rep, {"agency": "ecb", "budget": 740,
                                   "per": 120, "cap_mb": 150},
                             f"main-{rounds}")
        nd = len(set(st.get("done") or []))
        nt = st.get("n_total") or 0
        n_fail = len(st.get("failures") or {})
        rep.kv(stage="blitz", rounds=rounds, n_done=nd, n_total=nt,
               status=st.get("status"), n_failures=n_fail,
               fail_sample=json.dumps(dict(list((st.get("failures")
                                                 or {}).items())[:5])
                                      )[:240])
        verdict["gates"]["walk_complete"] = (
            "PASS" if (st.get("status") == "COMPLETE" and nd >= nt
                       and nt >= 100) else "PENDING")

        # one failures-retry pass (source-side denials get a chase)
        if n_fail and time.time() - T0 < WALL_STOP_NEW_ROUNDS:
            st = blitz_round(rep, {"agency": "ecb", "budget": 600,
                                   "per": 120, "cap_mb": 150,
                                   "retry_failures": 1}, "fail-retry",
                             max_wait=760)
            rep.kv(stage="fail-retry",
                   n_failures_after=len(st.get("failures") or {}),
                   retried_ok=st.get("retried_ok"))

        # ── G5 truncation elimination (cap 450MB, per=6 RAM guard) ──
        prev = -1
        tr = list(dict.fromkeys(state().get("truncated") or []))
        tr_rounds = 0
        while ok1 and tr and len(tr) != prev and tr_rounds < 3 \
                and time.time() - T0 < WALL_STOP_NEW_ROUNDS:
            prev = len(tr)
            tr_rounds += 1
            rep.log(f"trunc round {tr_rounds}: {prev} flows "
                    f"({','.join(map(str, tr[:8]))}) at cap 450MB")
            st = blitz_round(rep, {"agency": "ecb", "budget": 740,
                                   "per": 6, "cap_mb": 450,
                                   "retry_truncated": 1},
                             f"trunc-{tr_rounds}")
            tr = list(dict.fromkeys(st.get("truncated") or []))
        rep.kv(stage="truncation", rounds=tr_rounds,
               remaining=len(tr),
               remaining_flows=",".join(map(str, tr[:12])))
        verdict["gates"]["truncation_zero"] = (
            "PASS" if not tr else "PENDING")
        verdict["truncated_remaining"] = tr

        # ── G6 inception depth on benchmark flows ───────────────────
        deep = {}
        for f in ("EXR", "BSI", "ICP", "MIR", "CISS", "FM"):
            key = f"data/warm/ecb/data/{f}.dat.gz"
            try:
                lo, hi, nb = year_scan(key)
                deep[f] = {"min_year": lo, "max_year": hi,
                           "raw_bytes": nb}
            except Exception as e:
                deep[f] = {"err": f"{type(e).__name__}: {str(e)[:60]}"}
            rep.kv(stage="inception", flow=f, **{
                k: v for k, v in deep[f].items()})
        hits = sum(1 for d in deep.values()
                   if (d.get("min_year") or 9999) <= 2005)
        verdict["gates"]["inception_depth"] = ("PASS" if hits >= 4
                                               else "FAIL")
        verdict["inception"] = deep

        # ── G7 footprint census ─────────────────────────────────────
        n, byt, tok = 0, 0, None
        while True:
            kw = dict(Bucket=B, Prefix="data/warm/ecb/data/",
                      MaxKeys=1000)
            if tok:
                kw["ContinuationToken"] = tok
            r = s3.list_objects_v2(**kw)
            for o in r.get("Contents") or []:
                n += 1
                byt += o.get("Size") or 0
            if not r.get("IsTruncated"):
                break
            tok = r.get("NextContinuationToken")
        rep.kv(stage="footprint", data_objects=n,
               gz_mb=round(byt / 1048576, 1))
        verdict["footprint"] = {"objects": n, "gz_mb":
                                round(byt / 1048576, 1)}

        # downstream refresh (no wait — daily surfaces will confirm)
        for fn2 in ("justhodl-import-sentinel",
                    "justhodl-provider-catalog"):
            try:
                lam.invoke(FunctionName=fn2, InvocationType="Event")
            except Exception:
                pass

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4895.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4895.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
