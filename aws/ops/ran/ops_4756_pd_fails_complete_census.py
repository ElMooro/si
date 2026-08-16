"""
ops/4756 -- the complete fails census: every PD fails series, both
catalogs, verified banked, depth-proven, stragglers banked.

Khalid: 'OFR's NY Fed Primary Dealer Statistics has tons and tons of
different fails datasets -- did we import them ALL?' Ground truth:
OFR's NYPD = a 194-mnemonic curated mirror whose fails subset is
exactly 18 (the complete 2x9 FtD/FtR grid) -- all held. The 'tons and
tons' lives in NY Fed's UNDERLYING FR2004 catalog (1,543 series held
by markets-full, multi-break history from a prior arc). This op:

  A. OFR side: broad-pattern fails scan over all 194 NYPD mnemonics
     (FtD|FtR|FAIL|DELIV|RECEI|SETT) -- confirm 18 is truly the whole
     subset, grid-check 2 directions x 9 classes
  B. NY Fed side: load pd-state.json's 1,5xx-key catalog; broad fails
     scan over keyids; list EVERY match -- the real fails universe
  C. bank-verify: for each NY Fed fails keyid, HEAD
     data/warm/nyfed-markets/pd/{keyid}.json.gz; report held/missing
  D. depth-proof 3: one current-format fails series, one legacy-format
     (different survey break), one MBS-class -- earliest dates prove
     the multi-break history actually reaches back
  E. bank any missing fails keyids via the engine's own proven path
     (/api/pd/get/{seriesbreak}/timeseries/{key}.json across the
     survey-break candidates recorded in state), same warm key shape

Read-mostly; writes only if C finds stragglers. Everything lands under
the deny-Delete prefix.
"""
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org/api"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
FAILS_RE = re.compile(r"FtD|FtR|FAIL|DELIV|RECEI|SETT", re.I)
started = time.time()
TIME_CAP_S = 70 * 60

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(12_000_000)
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status, "raw": raw,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "raw": b"", "body": ""}
    except Exception as e:
        return {"ok": False, "status": None, "raw": b"", "body": "",
                 "error": f"{type(e).__name__}: {str(e)[:110]}"}


def s3_json_gz(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def deep_dates(obj, out=None, depth=0):
    if out is None:
        out = []
    if depth > 7:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:40000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def main():
    with report("4756_pd_fails_complete_census") as rep:
        rep.heading("ops 4756 -- the complete PD fails census (OFR mirror + NY Fed full)")

        rep.section("A. OFR NYPD mirror -- is 18 the whole fails subset?")
        st = s3_json_gz("data/warm/ofr/state.json")
        banked_ofr = sorted(map(str, st.get("banked") or st.get("done") or []))
        nypd = [x for x in banked_ofr if x.startswith("NYPD")]
        ofr_fails = sorted(x for x in nypd if FAILS_RE.search(x))
        rep.kv(check="ofr_nypd_total", value=len(nypd))
        rep.kv(check="ofr_fails_broad_scan", value=len(ofr_fails))
        dirs = {"FtD": 0, "FtR": 0}
        for x in ofr_fails:
            for d in dirs:
                if d in x:
                    dirs[d] += 1
        rep.kv(check="ofr_fails_FtD", value=dirs["FtD"])
        rep.kv(check="ofr_fails_FtR", value=dirs["FtR"])
        rep.log("OFR fails grid: " + ", ".join(ofr_fails))

        rep.section("B. NY Fed full PD catalog -- the real fails universe")
        pdstate = s3_json_gz("data/warm/nyfed-markets/pd-state.json")
        catalog = sorted(map(str, pdstate.get("catalog") or []))
        done = set(map(str, pdstate.get("done") or []))
        rep.kv(check="nyfed_pd_catalog", value=len(catalog))
        rep.kv(check="nyfed_pd_done", value=len(done))
        nyf_fails = sorted(k for k in catalog if FAILS_RE.search(k))
        rep.kv(check="nyfed_fails_keyids", value=len(nyf_fails))
        for k in nyf_fails:
            rep.log(f"  {k}")

        rep.section("C. Bank-verify every NY Fed fails keyid on S3")
        missing = []
        for k in nyf_fails:
            key = f"data/warm/nyfed-markets/pd/{k}.json.gz"
            try:
                s3.head_object(Bucket=BUCKET, Key=key)
            except Exception:
                missing.append(k)
        rep.kv(check="fails_held_on_s3", value=len(nyf_fails) - len(missing))
        rep.kv(check="fails_missing_on_s3", value=len(missing))
        for k in missing[:30]:
            rep.log(f"  MISSING: {k}")

        rep.section("D. Depth proof -- current, legacy, MBS-class")
        picks = []
        cur = [k for k in nyf_fails if not missing or k not in missing]
        if cur:
            picks.append(cur[0])
            mbs = [k for k in cur if "MBS" in k.upper()]
            if mbs:
                picks.append(mbs[0])
            legacy = [k for k in cur if re.search(r"PDFTD|PDFTR|-OLD|_L[12]?$",
                                                    k, re.I)]
            if legacy:
                picks.append(legacy[0])
            elif len(cur) > 2:
                picks.append(cur[-1])
        for k in dict.fromkeys(picks):
            try:
                doc = s3_json_gz(f"data/warm/nyfed-markets/pd/{k}.json.gz")
                dd = sorted(set(deep_dates(doc)))
                rep.kv(keyid=k, n=len(dd), earliest=dd[0] if dd else None,
                       latest=dd[-1] if dd else None)
                rep.ok(f"{k}: {len(dd)} obs, "
                        f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
            except Exception as e:
                rep.warn(f"{k}: {type(e).__name__}: {str(e)[:100]}")

        rep.section("E. Bank stragglers via the engine's own path")
        if not missing:
            rep.ok("no stragglers -- every fails series in the full NY Fed "
                    "catalog is already banked; nothing to write")
            return
        # survey-break candidates: reuse whatever the state recorded, else
        # the documented break set
        breaks = (pdstate.get("breaks") or pdstate.get("seriesbreaks") or
                   ["SBP2022", "SBP2015", "SBP2013", "SBN2013", "SBN2001"])
        banked_now = 0
        for k in missing:
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap during straggler bank")
                break
            got = False
            for br in breaks:
                r = get(f"{NYF}/pd/get/{br}/timeseries/{k}.json")
                if r["ok"] and r["body"].strip()[:1] in "[{":
                    try:
                        d = json.loads(r["body"])
                    except Exception:
                        continue
                    if deep_dates(d):
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=f"data/warm/nyfed-markets/pd/{k}.json.gz",
                            Body=gzip.compress(r["raw"]),
                            ContentType="application/json",
                            ContentEncoding="gzip")
                        rep.ok(f"banked straggler {k} (break {br})")
                        banked_now += 1
                        got = True
                        break
                time.sleep(0.2)
            if not got:
                rep.warn(f"{k}: no survey break yielded data -- left unbanked, "
                         "logged for follow-up")
        rep.kv(check="stragglers_banked", value=banked_now)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
