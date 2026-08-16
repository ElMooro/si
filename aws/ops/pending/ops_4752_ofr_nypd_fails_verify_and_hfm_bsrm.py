"""
ops/4752 -- the linked NYPD fails series verified + the REST of
financialresearch.gov (Hedge Fund Monitor, Bank Systemic Risk Monitor)
discovered and banked.

Khalid linked NYPD-PD_AFtR_TOT-A (primary-dealer aggregate fails-to-
receive) and asked for everything financialresearch.gov holds, full
history, permanent. Ground truth going in: the STFM catalog is 442/442
converged (ops 4738) and margin-lending's old HFM probe ended in a
documented dead end ("no stable public JSON found") -- so HFM/BSRM
need real discovery, not re-guessing.

  A. read data/warm/ofr/series/NYPD-PD_AFtR_TOT-A.json.gz -- n_obs,
     earliest, latest: is his exact series banked deep?
  B. enumerate the whole NYPD fails family (FtR/FtD/FAIL patterns) in
     the banked catalog; depth-check two of them -- closes the queued
     'verify NYPD fails mnemonics' item
  C. live /v1/metadata/mnemonics recount -- still 442, or grown (the
     stfm engine auto-banks growth; report either way)
  D. HFM discovery: fetch the monitor page + its JS bundles, extract
     every quoted URL touching data.financialresearch / .json / .csv /
     /api/; validate candidates; if a mnemonic catalog validates, bank
     each series full-history (cap 500, paced); if direct data files
     validate, bank verbatim (cap 60 x 12MB) -> data/warm/ofr-hfm/
  E. BSRM: identical treatment -> data/warm/ofr-bsrm/

Everything under data/warm/* = deny-Delete + versioned = permanent.
Per-fetch status/bytes logging throughout -- no silent zeros.
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
FRG = "https://www.financialresearch.gov"
DATA = "https://data.financialresearch.gov"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
TIME_CAP_S = 70 * 60
started = time.time()

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(13_000_000)
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


def depth_of(key):
    try:
        doc = s3_json_gz(key)
        dd = sorted(set(deep_dates(doc)))
        return {"n": len(dd), "earliest": dd[0] if dd else None,
                 "latest": dd[-1] if dd else None}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:90]}"}


def discover_and_bank(rep, tag, page_url, warm_prefix):
    """Page+bundle URL extraction -> validate -> bank catalogs or files."""
    rep.section(f"{tag}: discovery via page + bundles")
    page = get(page_url)
    rep.log(f"{page_url} -> status={page.get('status')} "
            f"bytes={len(page.get('body', ''))}")
    corpus = page.get("body", "")
    srcs = re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', corpus)
    rep.kv(check=f"{tag}_script_tags", value=len(srcs))
    for s_url in srcs[:6]:
        absu = s_url if s_url.startswith("http") else FRG + (
            s_url if s_url.startswith("/") else "/" + s_url)
        r = get(absu)
        rep.log(f"bundle {absu.rsplit('/', 1)[-1][:55]} -> "
                f"status={r.get('status')} bytes={len(r.get('body', ''))}")
        if r["ok"]:
            corpus += "\n" + r["body"]
    urls = set()
    for m in re.findall(
            r'["\']((?:https?://[^"\']+|/[A-Za-z0-9_\-/{}.]+)\.'
            r'(?:json|csv|xlsx))["\']', corpus):
        urls.add(m)
    for m in re.findall(r'["\'](https?://data\.financialresearch[^"\']+)["\']',
                          corpus):
        urls.add(m)
    for m in re.findall(r'["\'](/[A-Za-z0-9_\-/]*api/[A-Za-z0-9_\-/{}.]+)["\']',
                          corpus):
        urls.add(m)
    rep.kv(check=f"{tag}_candidate_urls", value=len(urls))
    for u in sorted(urls)[:25]:
        rep.log(f"  cand: {u[:110]}")

    # also try the STFM-style data host for a sibling catalog
    for cat in (f"{DATA}/hf/v1/metadata/mnemonics",
                 f"{DATA}/{tag.lower()}/v1/metadata/mnemonics"):
        r = get(cat)
        rep.log(f"catalog probe {cat.rsplit('.gov', 1)[-1]} -> "
                f"status={r.get('status')} bytes={len(r.get('body', ''))}")
        if r["ok"] and r["body"].strip()[:1] in "[{":
            try:
                d = json.loads(r["body"])
                mnems = ([x if isinstance(x, str) else
                           (x.get("mnemonic") or x.get("id")) for x in d]
                          if isinstance(d, list) else
                          [x.get("mnemonic") for x in
                           (d.get("mnemonics") or d.get("results") or [])])
                mnems = sorted({str(m) for m in mnems if m})[:500]
                if mnems:
                    rep.ok(f"{tag}: sibling catalog VALIDATED -- "
                            f"{len(mnems)} mnemonics; banking each")
                    base_full = cat.rsplit("/metadata", 1)[0] + \
                        "/series/full?mnemonic="
                    banked = 0
                    for m in mnems:
                        if time.time() - started > TIME_CAP_S:
                            rep.warn("time cap during catalog bank")
                            break
                        sr = get(base_full + m)
                        if sr["ok"] and sr["body"].strip()[:1] in "[{":
                            s3.put_object(
                                Bucket=BUCKET,
                                Key=f"{warm_prefix}series/{m}.json.gz",
                                Body=gzip.compress(sr["raw"]),
                                ContentType="application/json",
                                ContentEncoding="gzip")
                            banked += 1
                        time.sleep(0.15)
                    rep.kv(check=f"{tag}_catalog_series_banked", value=banked)
                    return
            except Exception:
                pass

    # bank direct data files that validate
    banked = 0
    for u in sorted(urls):
        if banked >= 60 or time.time() - started > TIME_CAP_S:
            break
        absu = u if u.startswith("http") else FRG + u
        if "{" in absu:
            continue
        r = get(absu)
        body = r.get("raw", b"")
        looks_data = (r["ok"] and len(body) > 200 and
                       (body.lstrip()[:1] in (b"[", b"{") or
                        absu.endswith(".csv") or body[:2] == b"PK"))
        rep.log(f"  fetch {absu[:95]} -> status={r.get('status')} "
                f"bytes={len(body)} data={looks_data}")
        if looks_data:
            name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                           absu.rsplit("/", 1)[-1])[:80]
            s3.put_object(Bucket=BUCKET, Key=f"{warm_prefix}{name}.gz",
                           Body=gzip.compress(body),
                           ContentType="application/octet-stream",
                           ContentEncoding="gzip")
            banked += 1
        time.sleep(0.2)
    rep.kv(check=f"{tag}_files_banked", value=banked)
    if not banked:
        rep.warn(f"{tag}: nothing validated as machine data -- exact "
                 "candidates logged above; follow-up works from those, "
                 "not fresh guesses")


def main():
    with report("4752_ofr_nypd_fails_verify_and_hfm_bsrm") as rep:
        rep.heading("ops 4752 -- NYPD fails verified + HFM/BSRM discovery+bank")

        rep.section("A. Khalid's exact linked series")
        m = "NYPD-PD_AFtR_TOT-A"
        d = depth_of(f"data/warm/ofr/series/{m}.json.gz")
        rep.kv(mnemonic=m, **d)
        if d.get("n"):
            rep.ok(f"{m}: ALREADY BANKED -- {d['n']} obs, "
                    f"{d['earliest']} -> {d['latest']}")
        else:
            rep.warn(f"{m}: not readable from warm -- {d}")

        rep.section("B. The whole NYPD fails family")
        st = s3_json_gz("data/warm/ofr/state.json")
        banked = sorted(map(str, st.get("banked") or st.get("done") or []))
        fails = [x for x in banked if x.startswith("NYPD") and
                  re.search(r"FtR|FtD|FAIL", x, re.I)]
        rep.kv(check="nypd_total_banked", value=len(
            [x for x in banked if x.startswith("NYPD")]))
        rep.kv(check="nypd_fails_mnemonics", value=len(fails))
        for x in fails[:40]:
            rep.log(f"  {x}")
        for probe in fails[:2]:
            pd = depth_of(f"data/warm/ofr/series/{probe}.json.gz")
            rep.kv(mnemonic=probe, **pd)

        rep.section("C. Live catalog recount")
        r = get(f"{DATA}/v1/metadata/mnemonics")
        n_live = 0
        if r["ok"]:
            try:
                dd = json.loads(r["body"])
                n_live = len(dd) if isinstance(dd, list) else \
                    len(dd.get("mnemonics") or dd.get("results") or [])
            except Exception:
                pass
        rep.kv(check="stfm_live_catalog", value=n_live)
        rep.kv(check="stfm_banked", value=len(banked))

        discover_and_bank(rep, "HFM", f"{FRG}/hedge-fund-monitor/",
                           "data/warm/ofr-hfm/")
        discover_and_bank(rep, "BSRM", f"{FRG}/bank-systemic-risk-monitor/",
                           "data/warm/ofr-bsrm/")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
