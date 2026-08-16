"""
ops/4754 -- financialresearch.gov: the site-wide sweep. Every dataset,
full history, permanent, on data.html.

Held already (verified this session): STFM 442 (incl. ALL 194 Primary
Dealer series), HFM 497, BSRM's two workbooks, the 18-mnemonic fails
grid. This op goes after everything else on the site:

  A. Primary Dealer re-proof: count the NYPD family and depth-check two
     non-fails series -- 'import all the Primary Dealer data' answered
     with fresh evidence, not a claim
  B. FSI full history: the official fsi.csv (known path, referenced in
     gap-metrics) banked verbatim + parsed date-span reported
  C. Sibling API roots, PER-TAG this time (4752's hardcoded-/hf/ bug
     fixed): {mmf,bsrm,fsi,nccbr,fird}/v1/metadata/mnemonics -- any
     that validates gets its whole catalog banked (cap 800/tag) into
     data/warm/ofr-{tag}/series/
  D. Site vacuum: monitoring-tools index + STFM datasets index + the
     data page -- every first-party .csv/.xlsx/.json/.zip href fetched
     and banked verbatim (cap 80 files x 15MB) into
     data/warm/ofr-site/ with a manifest mapping file -> source_url.
     Analytics/touchpoints hosts excluded.

Everything under data/warm/* = deny-Delete + versioned. REG entries for
the new prefixes ride in the same push, so data.html shows them.
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
FRG = "https://www.financialresearch.gov"
DATA = "https://data.financialresearch.gov"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
TIME_CAP_S = 70 * 60
started = time.time()
EXCLUDE_HOSTS = ("touchpoints", "google-analytics", "dap.digitalgov",
                  "youtube", "twitter", "facebook")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(16_000_000)
            ct = r.headers.get("Content-Type", "")
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status, "raw": raw, "ct": ct,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "raw": b"", "body": "", "ct": ""}
    except Exception as e:
        return {"ok": False, "status": None, "raw": b"", "body": "", "ct": "",
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
    with report("4754_ofr_sitewide_sweep") as rep:
        rep.heading("ops 4754 -- financialresearch.gov site-wide sweep")

        rep.section("A. Primary Dealer re-proof (already banked; fresh evidence)")
        st = s3_json_gz("data/warm/ofr/state.json")
        banked = sorted(map(str, st.get("banked") or st.get("done") or []))
        nypd = [x for x in banked if x.startswith("NYPD")]
        rep.kv(check="nypd_series_banked", value=len(nypd))
        non_fails = [x for x in nypd if not re.search(r"FtR|FtD|FAIL", x, re.I)]
        for probe in non_fails[:2]:
            try:
                doc = s3_json_gz(f"data/warm/ofr/series/{probe}.json.gz")
                dd = sorted(set(deep_dates(doc)))
                rep.kv(mnemonic=probe, n=len(dd),
                       earliest=dd[0] if dd else None,
                       latest=dd[-1] if dd else None)
                rep.ok(f"{probe}: {len(dd)} obs, "
                        f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
            except Exception as e:
                rep.warn(f"{probe}: {type(e).__name__}: {str(e)[:90]}")

        rep.section("B. FSI full-history file")
        for u in (f"{FRG}/financial-stress-index/data/fsi.csv",
                   f"{FRG}/financial-stress-index/data/fsi.xlsx"):
            r = get(u)
            rep.log(f"{u.rsplit('.gov', 1)[-1]} -> status={r.get('status')} "
                    f"bytes={len(r.get('raw', b''))}")
            if r["ok"] and len(r["raw"]) > 1000:
                name = u.rsplit("/", 1)[-1]
                s3.put_object(Bucket=BUCKET, Key=f"data/warm/ofr-fsi/{name}",
                               Body=r["raw"],
                               ContentType=r.get("ct") or "text/csv")
                dates = sorted(set(re.findall(
                    r"\b(19\d{2}|20\d{2})-(\d{2})-(\d{2})\b",
                    r["body"][:4_000_000])))
                span = (f"{'-'.join(dates[0])} -> {'-'.join(dates[-1])}"
                         if dates else "n/a")
                rep.ok(f"FSI {name}: {len(r['raw'])} bytes banked, "
                        f"{len(dates)} distinct dates, span {span}")
                rep.kv(check="fsi_banked", value=name)
                break

        rep.section("C. Sibling API roots -- per-tag this time")
        for tag in ("mmf", "bsrm", "fsi", "nccbr", "fird"):
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap")
                break
            cat = f"{DATA}/{tag}/v1/metadata/mnemonics"
            r = get(cat)
            rep.log(f"{tag}/v1/metadata/mnemonics -> status={r.get('status')} "
                    f"bytes={len(r.get('body', ''))}")
            if not (r["ok"] and r["body"].strip()[:1] in "[{"):
                continue
            try:
                d = json.loads(r["body"])
                mnems = ([x if isinstance(x, str) else
                           (x.get("mnemonic") or x.get("id")) for x in d]
                          if isinstance(d, list) else
                          [x.get("mnemonic") for x in
                           (d.get("mnemonics") or d.get("results") or [])])
                mnems = sorted({str(m) for m in mnems if m})
            except Exception:
                continue
            if not mnems:
                continue
            if len(mnems) > 800:
                rep.warn(f"{tag}: {len(mnems)} mnemonics exceeds 800 cap -- "
                         "NEEDS_DEDICATED_ENGINE, nothing partial banked")
                rep.kv(tag=tag, banked=False, reason="catalog_cap",
                       catalog=len(mnems))
                continue
            rep.ok(f"{tag}: catalog VALIDATED -- {len(mnems)} mnemonics; banking")
            base_full = f"{DATA}/{tag}/v1/series/full?mnemonic="
            n = 0
            for m in mnems:
                if time.time() - started > TIME_CAP_S:
                    rep.warn(f"time cap mid-{tag}")
                    break
                sr = get(base_full + m)
                if sr["ok"] and sr["body"].strip()[:1] in "[{":
                    s3.put_object(Bucket=BUCKET,
                                   Key=f"data/warm/ofr-{tag}/series/{m}.json.gz",
                                   Body=gzip.compress(sr["raw"]),
                                   ContentType="application/json",
                                   ContentEncoding="gzip")
                    n += 1
                time.sleep(0.15)
            rep.kv(tag=tag, banked=True, series_banked=n, catalog=len(mnems))

        rep.section("D. Site vacuum -- every first-party data file linked")
        pages = [f"{FRG}/monitoring-tools/",
                  f"{FRG}/short-term-funding-monitor/datasets/",
                  f"{FRG}/data/",
                  f"{FRG}/hedge-fund-monitor/",
                  f"{FRG}/bank-systemic-risk-monitor/",
                  f"{FRG}/money-market-fund-monitor/"]
        hrefs = set()
        for pg in pages:
            r = get(pg)
            rep.log(f"{pg.rsplit('.gov', 1)[-1]} -> status={r.get('status')} "
                    f"bytes={len(r.get('body', ''))}")
            if not r["ok"]:
                continue
            for m in re.findall(
                    r'href="([^"]+\.(?:csv|xlsx|json|zip))"', r["body"], re.I):
                hrefs.add(m)
        clean = []
        for h in sorted(hrefs):
            absu = h if h.startswith("http") else FRG + (
                h if h.startswith("/") else "/" + h)
            if any(x in absu for x in EXCLUDE_HOSTS):
                continue
            clean.append(absu)
        rep.kv(check="site_files_discovered", value=len(clean))
        manifest = {"swept_at": datetime.now(timezone.utc).isoformat(),
                     "files": {}}
        n_banked = 0
        for absu in clean[:80]:
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap during vacuum")
                break
            r = get(absu)
            raw = r.get("raw", b"")
            if not (r["ok"] and len(raw) > 200):
                rep.log(f"  skip {absu[:95]} -> status={r.get('status')} "
                        f"bytes={len(raw)}")
                continue
            name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                           absu.split(".gov", 1)[-1].strip("/"))[:100]
            key = f"data/warm/ofr-site/{name}"
            s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                           ContentType=r.get("ct") or
                           "application/octet-stream")
            manifest["files"][name] = {"source_url": absu, "bytes": len(raw)}
            n_banked += 1
            rep.ok(f"  banked {name} ({len(raw)} bytes)")
            time.sleep(0.25)
        s3.put_object(Bucket=BUCKET, Key="data/warm/ofr-site/_manifest.json",
                       Body=json.dumps(manifest, indent=1),
                       ContentType="application/json")
        rep.kv(check="site_files_banked", value=n_banked)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
