"""
ops/4759 -- OLE fix + DFA archive + THE haircut workbooks + PD metadata
+ scheduled-engine verification.

Five jobs, one op:

  A. OLE-aware verifier (legacy .xls = OLE2 magic D0CF11E0, not ZIP --
     ops 4758's PK-only check wrongly rejected every one). Grid-bank
     the NY Fed quarterly dealer archive markets/DFA/{stem}_{Y}_q{N}.xls
     discovered in 4758: stem discovery first (probe 2020q1 across
     candidate stems), then the full year grid 2005->present for stems
     that exist; 404s reported per-year, never papered over.
  B. THE HAIRCUT DATA -- NY Fed tri-party repo statistics workbooks,
     both methodology vintages (current + pre-Nov-2025 history back to
     May 2010): monthly haircut p10/median/p90, dispersion, volumes,
     concentration by collateral class. Extensionless URLs -- detect
     magic, bank verbatim under data/warm/nyfed-research/haircuts/.
  C. hhdc re-crawl with the fixed verifier -- automatically catches the
     three .xls files 4758 rejected. Manifest-deduped, idempotent.
  D. PD metadata: pd/list/timeseries.csv + seriesbreaks.csv + asof.csv
     banked verbatim -- the series-break definitions are what make the
     1998->present multi-format history safely concatenable.
  E. Verification reads: soma-cusip hot status (did the new engine's
     first tranche fire?) and repo-deep-summary (has v2's self-extend
     run yet, or is the first run tomorrow 05:22 UTC?).
"""
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://www.newyorkfed.org"
MKT = "https://markets.newyorkfed.org"
RES_PREFIX = "data/warm/nyfed-research/"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
CAP = 200_000_000
TIME_CAP_S = 70 * 60
started = time.time()
OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

s3 = boto3.client("s3", region_name=REGION)


def stream_get(url, timeout=120):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        ct = r.headers.get("Content-Type", "")
        chunks = []
        total = 0
        while total < CAP:
            c = r.read(1_048_576)
            if not c:
                break
            chunks.append(c)
            total += len(c)
        truncated = total >= CAP
    return b"".join(chunks), ct, truncated


def office_ok(raw, ext):
    if ext in ("xlsx", "zip"):
        return raw[:2] == b"PK" and b"PK\x05\x06" in raw[-65536:]
    if ext == "xls":
        return raw[:8] == OLE_MAGIC or (
            raw[:2] == b"PK" and b"PK\x05\x06" in raw[-65536:])
    return len(raw) > 200


def load_manifest():
    try:
        m = json.loads(s3.get_object(
            Bucket=BUCKET, Key=f"{RES_PREFIX}_manifest.json")["Body"].read())
    except Exception:
        m = {"files": {}}
    m.setdefault("files", {})
    return m


def save_manifest(m):
    m["swept_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key=f"{RES_PREFIX}_manifest.json",
                   Body=json.dumps(m, indent=1),
                   ContentType="application/json")


def bank_file(rep, manifest, tag, url, name=None):
    try:
        raw, ct, truncated = stream_get(url)
    except urllib.error.HTTPError as e:
        return ("http", e.code)
    except Exception as e:
        rep.warn(f"  {url[:90]} -> {type(e).__name__}: {str(e)[:80]}")
        return ("err", None)
    if truncated:
        rep.warn(f"  {url[:90]} hit 200MB cap -- NOT banked")
        return ("cap", None)
    ext = (name or url).rsplit(".", 1)[-1].lower()
    if not name:
        name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                       url.split(".org", 1)[-1].strip("/"))[:110]
    if ext in ("xlsx", "xls", "zip") and not office_ok(raw, ext):
        rep.warn(f"  {name} failed {ext} magic check ({len(raw)} b) -- NOT banked")
        return ("badmagic", None)
    if len(raw) < 300:
        return ("small", None)
    key = f"{RES_PREFIX}{tag}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                   ContentType=ct or "application/octet-stream")
    manifest["files"][f"{tag}/{name}"] = {
        "source_url": url, "bytes": len(raw),
        "office_verified": ext in ("xlsx", "xls", "zip"),
        "banked_at": datetime.now(timezone.utc).isoformat()}
    rep.ok(f"  banked {tag}/{name} ({len(raw)} bytes)")
    return ("ok", len(raw))


def main():
    with report("4759_ole_fix_dfa_haircuts_pdmeta") as rep:
        rep.heading("ops 4759 -- OLE fix + DFA archive + haircut workbooks + PD meta")
        manifest = load_manifest()

        rep.section("A. DFA quarterly dealer archive (OLE-aware)")
        stems = []
        for stem in ("tsy_data", "agency_data", "mbs_data", "abs_data",
                       "corp_data"):
            st, _ = bank_file(rep, manifest, "dfa",
                               f"{NYF}/medialibrary/media/markets/DFA/"
                               f"{stem}_2020_q1.xls",
                               name=f"{stem}_2020_q1.xls")
            rep.log(f"stem probe {stem}: {st}")
            if st == "ok":
                stems.append(stem)
        rep.kv(check="dfa_stems_live", value=",".join(stems) or "none")
        year_now = int(time.strftime("%Y"))
        n404 = {}
        n_banked = 0
        for stem in stems:
            for y in range(2005, year_now + 1):
                for q in (1, 2, 3, 4):
                    if time.time() - started > TIME_CAP_S:
                        rep.warn("time cap in DFA grid")
                        break
                    name = f"{stem}_{y}_q{q}.xls"
                    if f"dfa/{name}" in manifest["files"]:
                        continue
                    st, _ = bank_file(
                        rep, manifest, "dfa",
                        f"{NYF}/medialibrary/media/markets/DFA/{name}",
                        name=name)
                    if st == "ok":
                        n_banked += 1
                    elif st == "http":
                        n404[y] = n404.get(y, 0) + 1
                    time.sleep(0.18)
        rep.kv(check="dfa_banked_this_run", value=n_banked)
        if n404:
            rep.log("DFA 404s by year: " + ", ".join(
                f"{y}={n}" for y, n in sorted(n404.items())))
        save_manifest(manifest)

        rep.section("B. THE haircut workbooks -- tri-party repo statistics")
        for label, url in (
                ("tri-party-repo_data_current",
                 f"{NYF}/medialibrary/Research/Interactives/Data/"
                 "tri-party-repo/tri-party-repo_data"),
                ("tri-party-repo_preNov25_history",
                 f"{NYF}/medialibrary/Research/Interactives/Data/"
                 "tri-party-repo/tri-party-repo-preNov25_data")):
            try:
                raw, ct, truncated = stream_get(url)
            except Exception as e:
                rep.warn(f"{label}: {type(e).__name__}: {str(e)[:90]}")
                continue
            ext = ("xlsx" if raw[:2] == b"PK" else
                    "xls" if raw[:8] == OLE_MAGIC else "bin")
            if truncated or ext == "bin" or not office_ok(raw, ext):
                rep.warn(f"{label}: magic={raw[:4].hex()} bytes={len(raw)} "
                         f"truncated={truncated} -- NOT banked")
                continue
            name = f"{label}.{ext}"
            key = f"{RES_PREFIX}haircuts/{name}"
            s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                           ContentType=ct or "application/octet-stream")
            manifest["files"][f"haircuts/{name}"] = {
                "source_url": url, "bytes": len(raw),
                "office_verified": True,
                "banked_at": datetime.now(timezone.utc).isoformat()}
            rep.ok(f"HAIRCUTS: banked {name} ({len(raw)} bytes, {ext}) -- "
                    "monthly p10/median/p90 by collateral class")
            rep.kv(haircut_file=name, bytes=len(raw))
        save_manifest(manifest)

        rep.section("C. hhdc re-crawl with fixed verifier")
        r0 = None
        try:
            raw, _, _ = stream_get(f"{NYF}/microeconomics/hhdc", timeout=60)
            r0 = raw.decode("utf-8", "replace")
        except Exception as e:
            rep.warn(f"hhdc seed: {type(e).__name__}")
        n_new = 0
        if r0:
            corpus = r0
            subs = set()
            for m in re.findall(r'href="(/[A-Za-z0-9_\-/.%]+)"', r0):
                if any(w in m.lower() for w in
                        ("interactive", "medialibrary", "householdcredit",
                         "data")) and not m.endswith((".pdf", ".jpg", ".png")):
                    subs.add(m)
            for h in sorted(subs)[:15]:
                try:
                    sraw, _, _ = stream_get(NYF + h, timeout=60)
                    corpus += "\n" + sraw.decode("utf-8", "replace")
                except Exception:
                    pass
                time.sleep(0.12)
            cands = set()
            for m in re.findall(
                    r'["\'=]((?:https?://[^"\'\s>]+|/[A-Za-z0-9_\-/.%]+)\.'
                    r'(?:xlsx|xls|csv|zip|json))["\'\s>]', corpus, re.I):
                cands.add(m if m.startswith("http") else NYF + m)
            for u in sorted(cands):
                if "newyorkfed.org" not in u:
                    continue
                if any(v.get("source_url") == u
                        for v in manifest["files"].values()):
                    continue
                st, _ = bank_file(rep, manifest, "hhdc", u)
                if st == "ok":
                    n_new += 1
                time.sleep(0.2)
        rep.kv(check="hhdc_new_banked", value=n_new)
        save_manifest(manifest)

        rep.section("D. PD metadata -- series list, SERIES BREAKS, as-of dates")
        for name in ("timeseries", "seriesbreaks", "asof"):
            url = f"{MKT}/api/pd/list/{name}.csv"
            try:
                raw, ct, _ = stream_get(url, timeout=60)
                if len(raw) < 50:
                    rep.warn(f"pd/list/{name}.csv: only {len(raw)} bytes")
                    continue
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"data/warm/nyfed-markets/pd/_meta/{name}.csv.gz",
                    Body=gzip.compress(raw), ContentType="text/csv",
                    ContentEncoding="gzip")
                first = raw.split(b"\n", 2)[:2]
                rep.ok(f"pd meta {name}.csv banked ({len(raw)} bytes); "
                        f"header: {first[0][:120].decode('utf-8', 'replace')}")
            except Exception as e:
                rep.warn(f"pd/list/{name}: {type(e).__name__}: {str(e)[:90]}")

        rep.section("E. Scheduled-engine verification")
        try:
            hot = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/soma-cusip-status.json")["Body"].read())
            rep.kv(check="soma_cusip_fired", value=True)
            for k in ("as_of", "banked_this_run", "done_pairs", "total_pairs",
                       "progress_pct", "endpoint"):
                rep.log(f"  soma {k}: {json.dumps(hot.get(k))[:120]}")
        except Exception:
            rep.kv(check="soma_cusip_fired", value=False)
            rep.log("  soma-cusip hot status absent -- first 3h tick "
                    "hasn't fired yet (cron :41 past every 3rd hour)")
        try:
            summ = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/warm/nyfed-markets/repo-deep-summary.json")
                ["Body"].read())
            v2 = any(k in summ for k in
                      ("fxs", "ambs", "tsy", "seclending", "soma_summary"))
            rep.kv(check="repo_deep_v2_ran", value=v2)
            if v2:
                for k in ("fxs", "ambs", "tsy", "seclending", "soma_summary"):
                    if k in summ:
                        rep.log(f"  v2 {k}: {json.dumps(summ[k])[:140]}")
            else:
                rep.log("  summary is pre-v2 -- first self-extend runs "
                        "tomorrow 05:22 UTC as scheduled")
        except Exception as e:
            rep.warn(f"repo-deep summary read: {type(e).__name__}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
