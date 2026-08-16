"""
ops/4757 -- newyorkfed.org research + data-hub sweep: the institution's
data beyond the Markets API, banked as chartable files.

Khalid: everything NY Fed the institution publishes, as DATA. The
Markets API (10/10 families) is held; this goes after the research
side and the site's central data pages:

  seeds: Survey of Consumer Expectations, Household Debt & Credit,
  DSGE forecasts, the markets data-hub, and the research data index.

Technique = the proven one (ops 4752/4755): fetch each seed page +
its JS bundles, extract BOTH href attributes and quoted strings ending
in .xlsx/.xls/.csv/.zip/.json, keep first-party newyorkfed.org files
only, bank verbatim (cap 70 x 25MB) to
data/warm/nyfed-research/{seed}/{name} with a manifest mapping every
file to its source URL. Raw files first -- chart engines parse from
the bank, never from the wire. deny-Delete + versioned = permanent.
"""
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
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
TIME_CAP_S = 70 * 60
started = time.time()
EXCLUDE = ("google-analytics", "dap.digitalgov", "youtube", "twitter",
            "facebook", "linkedin", "doubleclick")
SEEDS = {
    "sce": f"{NYF}/microeconomics/sce",
    "hhdc": f"{NYF}/microeconomics/hhdc",
    "dsge": f"{NYF}/research/policy/dsge",
    "datahub": f"{NYF}/markets/data-hub",
    "research-data": f"{NYF}/research/data_indicators",
}

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(26_000_000)
            ct = r.headers.get("Content-Type", "")
            if raw[:2] == b"\x1f\x8b":
                import gzip as _g
                raw = _g.decompress(raw)
            return {"ok": True, "status": r.status, "raw": raw, "ct": ct,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "raw": b"", "body": "", "ct": ""}
    except Exception as e:
        return {"ok": False, "status": None, "raw": b"", "body": "", "ct": "",
                 "error": f"{type(e).__name__}: {str(e)[:110]}"}


def absu(h):
    return h if h.startswith("http") else NYF + (h if h.startswith("/")
                                                   else "/" + h)


def main():
    with report("4757_nyfed_research_data_sweep") as rep:
        rep.heading("ops 4757 -- newyorkfed.org research + data-hub sweep")

        manifest = {"swept_at": datetime.now(timezone.utc).isoformat(),
                     "files": {}}
        grand_banked = 0
        for tag, seed in SEEDS.items():
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap before seed " + tag)
                break
            rep.section(f"seed: {tag}")
            r = get(seed)
            rep.log(f"{seed} -> status={r.get('status')} "
                    f"bytes={len(r.get('body', ''))}"
                    + (f" err={r.get('error')}" if r.get("error") else ""))
            corpus = r.get("body", "")
            if r["ok"]:
                for sm in re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"',
                                       corpus)[:8]:
                    br = get(absu(sm))
                    if br["ok"]:
                        corpus += "\n" + br["body"]
            cands = set()
            for m in re.findall(
                    r'href="([^"]+\.(?:xlsx|xls|csv|zip|json))"', corpus, re.I):
                cands.add(m)
            for m in re.findall(
                    r'["\']((?:https?://[^"\']+|/[A-Za-z0-9_\-/.%]+)\.'
                    r'(?:xlsx|xls|csv|zip|json))["\']', corpus, re.I):
                cands.add(m)
            urls = sorted({absu(c) for c in cands
                            if "newyorkfed.org" in absu(c)
                            and not any(x in absu(c) for x in EXCLUDE)})
            rep.kv(seed=tag, candidates=len(urls))
            for u in urls[:20]:
                rep.log(f"  cand: {u[:115]}")
            n_banked = 0
            for u in urls:
                if n_banked >= 70 or time.time() - started > TIME_CAP_S:
                    break
                fr = get(u)
                raw = fr.get("raw", b"")
                if not (fr["ok"] and len(raw) > 300):
                    rep.log(f"  skip {u[:95]} -> status={fr.get('status')} "
                            f"bytes={len(raw)}")
                    continue
                name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                               u.split("newyorkfed.org", 1)[-1]
                               .strip("/"))[:110]
                key = f"data/warm/nyfed-research/{tag}/{name}"
                s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                               ContentType=fr.get("ct") or
                               "application/octet-stream")
                manifest["files"][f"{tag}/{name}"] = {
                    "source_url": u, "bytes": len(raw)}
                n_banked += 1
                grand_banked += 1
                rep.ok(f"  banked {tag}/{name} ({len(raw)} bytes)")
                time.sleep(0.25)
            rep.kv(seed=tag, banked=n_banked)

        s3.put_object(Bucket=BUCKET,
                       Key="data/warm/nyfed-research/_manifest.json",
                       Body=json.dumps(manifest, indent=1),
                       ContentType="application/json")
        rep.kv(check="total_files_banked", value=grand_banked)
        if grand_banked == 0:
            rep.warn("0 files banked -- every candidate and status is logged "
                     "above; the follow-up works from those exact URLs")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
