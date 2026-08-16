"""
ops/4738 -- overnight-repo super-monitor recon (read-only).

Khalid wants everything monitorable about the repo market -- rates,
volumes, haircuts, Treasury rehypothecation, all overnight lending --
with full history permanently on S3. The fleet already runs 8 repo
engines; ofr-stfm was explicitly designed to converge on OFR's full
mnemonic catalog (its comments name NCCBR as the expected late
arrival). So before building anything, measure the actual gaps:

  A. warm/ofr/state.json -- banked count vs worklist; are NCCBR-*
     mnemonics (the public repo-HAIRCUT source: OFR's non-centrally-
     cleared bilateral repo collection) discovered/banked yet?
  B. Live OFR catalog -- total mnemonics today, prefix breakdown,
     every NCCBR-* listed, any name containing HAIRCUT.
  C. Depth proof -- pull one banked series from warm and report its
     real earliest/latest obs (is "full history" actually banked?).
  D. NY Fed securities lending -- live probe; no engine pulls it and
     it's the rehypothecation-adjacent dataset (SOMA lending ops).
  E. Permanence -- bucket policy (deny-Delete on warm/*) + versioning
     status, so "permanently stored" is verified, not assumed.

Read-only: S3 GETs, public API GETs, get_bucket_policy/versioning.
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OFR = "https://data.financialresearch.gov/v1"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-repo-recon/1.0)"}

s3 = boto3.client("s3", region_name=REGION)


def get_url(url, timeout=25):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status,
                     "elapsed_ms": round((time.time() - t0) * 1000),
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code,
                 "elapsed_ms": round((time.time() - t0) * 1000),
                 "body": (e.read()[:200].decode("utf-8", "replace")
                          if e.fp else "")}
    except Exception as e:
        return {"ok": False, "status": None,
                 "error": f"{type(e).__name__}: {str(e)[:120]}",
                 "elapsed_ms": round((time.time() - t0) * 1000)}


def s3_json(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4738_repo_super_monitor_recon") as rep:
        rep.heading("ops 4738 -- overnight-repo super-monitor recon (read-only)")

        rep.section("A. warm/ofr state -- banked vs worklist, NCCBR presence")
        banked = []
        try:
            st = s3_json("data/warm/ofr/state.json")
            banked = sorted(st.get("banked") or st.get("done") or [])
            work = st.get("worklist") or st.get("pending") or []
            rep.kv(check="ofr_state_banked", value=len(banked))
            rep.kv(check="ofr_state_worklist_remaining", value=len(work))
            rep.kv(check="ofr_state_keys", value=",".join(sorted(st.keys()))[:200])
            nccbr_banked = [m for m in banked if str(m).upper().startswith("NCCBR")]
            rep.kv(check="nccbr_banked_count", value=len(nccbr_banked))
            if nccbr_banked:
                rep.log("NCCBR banked: " + ", ".join(map(str, nccbr_banked[:40])))
        except Exception as e:
            rep.warn(f"state read failed: {type(e).__name__}: {str(e)[:120]}")
        # independent count of actual series objects on disk
        n_series_keys = 0
        token = None
        while True:
            kw = dict(Bucket=BUCKET, Prefix="data/warm/ofr/series/", MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            n_series_keys += len(resp.get("Contents") or [])
            token = resp.get("NextContinuationToken")
            if not token:
                break
        rep.kv(check="ofr_series_objects_on_s3", value=n_series_keys)

        rep.section("B. Live OFR catalog -- total, prefixes, NCCBR, haircut mentions")
        cat = []
        for cand in (f"{OFR}/metadata/mnemonics",
                      f"{OFR}/metadata/query?output=by_mnemonic"):
            r = get_url(cand)
            if r["ok"]:
                try:
                    d = json.loads(r["body"])
                    if isinstance(d, list):
                        cat = [x if isinstance(x, str)
                               else (x.get("mnemonic") or x.get("id")) for x in d]
                    elif isinstance(d, dict):
                        cat = [x.get("mnemonic") for x in
                               (d.get("mnemonics") or d.get("results") or [])]
                    cat = sorted({str(m) for m in cat if m})
                    if cat:
                        rep.kv(check="catalog_endpoint_used", value=cand)
                        break
                except Exception:
                    continue
        rep.kv(check="ofr_catalog_total_live", value=len(cat))
        if cat:
            pref = Counter(m.split("-")[0] for m in cat)
            rep.log("prefix breakdown: " +
                    ", ".join(f"{k}={v}" for k, v in pref.most_common(15)))
            nccbr = [m for m in cat if m.upper().startswith("NCCBR")]
            rep.kv(check="nccbr_in_live_catalog", value=len(nccbr))
            for m in nccbr[:60]:
                rep.log(f"  NCCBR: {m}")
            hc = [m for m in cat if "HAIRCUT" in m.upper() or "HC" == m.split("-")[-1].upper()]
            rep.kv(check="haircut_named_mnemonics", value=len(hc))
            for m in hc[:30]:
                rep.log(f"  HAIRCUT-named: {m}")
            if banked:
                missing = sorted(set(cat) - set(map(str, banked)))
                rep.kv(check="in_catalog_not_banked", value=len(missing))
                for m in missing[:40]:
                    rep.log(f"  NOT BANKED: {m}")
        # search endpoint, defensive
        r = get_url(f"{OFR}/metadata/search?query=haircut")
        rep.log(f"metadata/search?query=haircut -> status={r.get('status')} "
                f"body[:300]={r.get('body','')[:300]}")

        rep.section("C. Depth proof on one banked series")
        probe_m = None
        for m in banked:
            if str(m).upper().startswith("REPO-TRI"):
                probe_m = str(m)
                break
        if not probe_m and banked:
            probe_m = str(banked[0])
        if probe_m:
            try:
                doc = s3_json(f"data/warm/ofr/series/{probe_m}.json.gz")
                ts = (doc.get("timeseries") or doc.get("data") or
                      (doc.get("series") or {}).get("timeseries") or [])
                dates = []
                if isinstance(ts, list):
                    for row in ts:
                        if isinstance(row, (list, tuple)) and row:
                            dates.append(str(row[0]))
                        elif isinstance(row, dict):
                            dates.append(str(row.get("date") or row.get("d") or ""))
                dates = sorted(d for d in dates if d and d[0].isdigit())
                rep.kv(check="depth_probe_mnemonic", value=probe_m)
                rep.kv(check="depth_probe_n_obs", value=len(dates))
                rep.kv(check="depth_probe_earliest", value=dates[0] if dates else None)
                rep.kv(check="depth_probe_latest", value=dates[-1] if dates else None)
            except Exception as e:
                rep.warn(f"depth probe on {probe_m}: {type(e).__name__}: {str(e)[:120]}")

        rep.section("D. NY Fed securities lending -- live probe (no engine pulls this)")
        for url in ("https://markets.newyorkfed.org/api/seclending/all/results/last/5.json",
                     "https://markets.newyorkfed.org/api/seclending/all/results/latest.json"):
            r = get_url(url)
            rep.log(f"{url.rsplit('/api/',1)[1]} -> ok={r['ok']} status={r.get('status')} "
                    f"body[:260]={r.get('body','')[:260]}")
            if r["ok"]:
                rep.kv(check="seclending_reachable", value=True)
                break

        rep.section("E. Permanence -- bucket policy + versioning")
        try:
            pol = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
            deny_warm = False
            for stmt in pol.get("Statement", []):
                if stmt.get("Effect") == "Deny":
                    acts = stmt.get("Action")
                    acts = [acts] if isinstance(acts, str) else (acts or [])
                    res = stmt.get("Resource")
                    res = [res] if isinstance(res, str) else (res or [])
                    if any("Delete" in a for a in acts) and any("warm" in r for r in res):
                        deny_warm = True
                        rep.log(f"deny stmt: actions={acts} resource={res}")
            rep.kv(check="deny_delete_on_warm", value=deny_warm)
        except Exception as e:
            rep.warn(f"bucket policy read: {type(e).__name__}: {str(e)[:120]}")
        try:
            v = s3.get_bucket_versioning(Bucket=BUCKET)
            rep.kv(check="bucket_versioning", value=v.get("Status"))
        except Exception as e:
            rep.warn(f"versioning read: {type(e).__name__}: {str(e)[:120]}")

        rep.section("Summary")
        rep.log("Read-only. Findings decide the build: if NCCBR/haircut series exist "
                 "in the live catalog but aren't banked, the fix is convergence + a "
                 "haircut panel, not a new engine; seclending result decides whether "
                 "a small securities-lending pull gets added for the rehypo picture.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("RECON ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
