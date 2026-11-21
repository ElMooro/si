"""ops 6895 - ACTING: the OFR bank's first refresh since 2026-08-06, then the bank read back against the source.

Dry run first: ops 6894 (20:20 UTC, from the checkout, nothing written) - 442 series to write, 531,285 banked
observations kept, 12 revised, 0 gone, 13,109 new, 13 seconds.

  1. The deployed package must carry this checkout's ofr_refresh.py byte for byte, or nothing is invoked.
  2. One run of justhodl-ofr-stfm (the repair run; its own schedule is once a day at about 02:29 UTC and is unchanged).
  3. Read back: the run record in the state file, every series file by the day of its last write, and - asking the
     source again - the newest observation of every mnemonic in the bank against the source.
The only writes are the engine's own: the series files whose observations changed and its two state files.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-ofr-stfm" / "source"))
LIVE = "justhodl-dashboard-live"
FN = "justhodl-ofr-stfm"
BASE = "https://data.financialresearch.gov/v1"
HF_BASE = "https://data.financialresearch.gov/hf/v1"


def behind(bank, source):
    """[(mnemonic, banked newest, source newest)] where the bank's newest observation is older than the source's."""
    return sorted((m, bank.get(m), s) for m, s in source.items() if s and (bank.get(m) or "") < s)


def selftest():
    assert behind({"A": "2026-11-19", "B": "2026-08-04", "C": None}, {"A": "2026-11-19", "B": "2026-11-19", "C": "2026-11-11", "D": None}) == \
        [("B", "2026-08-04", "2026-11-19"), ("C", None, "2026-11-11")]


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import ofr_refresh as R
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=640))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=48))

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw), o["LastModified"]
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}, None

    def listing(prefix):
        rows, token = [], None
        while True:
            kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            rows += [(o["Key"], o["LastModified"], o["Size"]) for o in page.get("Contents", [])]
            token = page.get("NextContinuationToken")
            if not token:
                return rows

    def fetch(url):
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "JustHodl research (ops read-back)"}), timeout=180) as resp:
            return resp.read()

    with report("ops_6895_ofr_bank_first_refresh") as r:
        r.heading("ops 6895 - OFR bank: first refresh since 2026-08-06, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. What is deployed")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for n in ("ofr_refresh.py", "lambda_function.py"):
            hits = [x for x in z.namelist() if x == n or x.endswith("/" + n)]
            same[n] = bool(hits) and z.read(hits[0]) == (ROOT / "aws/lambdas/justhodl-ofr-stfm/source" / n).read_bytes()
        r.log("%s updated %s | %s" % (FN, (g["Configuration"].get("LastModified") or "")[:19], json.dumps(same)))
        if not all(same.values()):
            r.fail("the deployed package is not this checkout's engine; nothing was invoked")
            sys.exit(1)

        r.section("2. One run")
        before = Counter(w.strftime("%Y-%m-%d") for _, w, _ in listing("data/warm/ofr/series/"))
        r.log("series files before, by day of last write: %s" % dict(before))
        t0 = time.time()
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = res["Payload"].read().decode("utf-8", "replace")
        r.log("invoke: status %s%s in %.0f s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", time.time() - t0))
        try:
            body = json.loads(json.loads(raw)["body"])
        except Exception:  # noqa: BLE001
            body = {}
            r.log("answer: %s" % raw[:500])
        r.log("refresh %s | problems %s | hedge-fund pair %s | tranche pulled %s failed %s" % (
            json.dumps(body.get("refresh")), json.dumps(body.get("refresh_problems")), json.dumps(body.get("hf_refresh")), body.get("pulled_this_run"), body.get("failed")))

        r.section("3. Read back")
        state, when = jget("data/warm/ofr/state.json")
        ref = state.get("refresh") or {}
        r.log("state written %s | status %s | refresh at %s: written %s, unchanged %s, shrank %s, failed %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", state.get("status"), ref.get("at"), ref.get("written"), ref.get("unchanged"), len(ref.get("shrank") or []), len(ref.get("failed") or [])))
        for ds, row in (ref.get("datasets") or {}).items():
            r.log("  dataset %-5s %s" % (ds, json.dumps(row)))
        files = listing("data/warm/ofr/series/")
        after = Counter(w.strftime("%Y-%m-%d") for _, w, _ in files)
        r.log("series files after: %d, %.1f MB | by day of last write: %s" % (len(files), sum(s for *_, s in files) / 1e6, dict(after)))

        source, meta = {}, {}
        for ds in R.DATASETS:
            for m, b in R.series_of(json.loads(R.unzip(fetch("%s/series/dataset?dataset=%s" % (BASE, ds))))).items():
                source[m] = R.newest(b)[0]
                meta[m] = b.get("metadata") or {}

        def banked(m):
            doc, _ = jget("data/warm/ofr/series/%s.json.gz" % m)
            return m, R.newest((doc.get("payload") or {}).get(m) or {})[0] if isinstance(doc, dict) else None

        with ThreadPoolExecutor(max_workers=32) as pool:
            bank = dict(pool.map(banked, sorted(source)))
        late = behind(bank, source)
        r.log("bank against source: %d mnemonics, %d current, %d behind %s" % (len(source), len(source) - len(late), len(late), late[:8]))
        for m in ("FNYR-SOFR-A", "FNYR-TGCR-A", "REPO-TRIV1_AR_OO-P", "REPO-GCF_AR_OO-P", "REPO-DVP_AR_OO-P", "NYPD-PD_AFtD_T-A", "MMF-MMF_RP_TOT-M"):
            r.log("  %-22s bank newest %s | source newest %s" % (m, bank.get(m), source.get(m)))
        hf_ok = True
        for m, b in R.series_of(json.loads(R.unzip(fetch("%s/series/dataset?dataset=ficc" % HF_BASE)))).items():
            doc, w = jget("data/warm/ofr-hfm/series/%s.json.gz" % m)
            got = R.newest(doc.get(m) or {})[0] if isinstance(doc, dict) else None
            hf_ok = hf_ok and got == R.newest(b)[0]
            r.log("  %-28s bank newest %s (written %s) | source newest %s" % (m, got, w.strftime("%Y-%m-%d %H:%M") if w else "-", R.newest(b)[0]))

        r.section("3b. Source series the repo board's inventory does not carry")
        inv, _ = jget("data/repo-master-inventory.json")
        on_board = {e.get("mnemonic") for e in (inv.get("series") or [])}
        sample = next(iter(meta.values()), {})
        r.log("metadata of one series: %s" % json.dumps(sample)[:700])
        for m in sorted(set(source) - on_board):
            d = meta[m].get("description") if isinstance(meta[m].get("description"), dict) else {}
            r.log("  %-30s newest %-10s | %s" % (m, source[m], str(d.get("name") or meta[m].get("series_name") or "")[:120]))

        r.section("4. A second look: what a run would write now")
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"dry_run": True}).encode())
        try:
            dry = json.loads(json.loads(res["Payload"].read())["body"])
        except Exception:  # noqa: BLE001
            dry = {}
        r.log("dry run on the Lambda: would write %s, unchanged %s | hedge-fund pair would write %s" % (
            (dry.get("refresh") or {}).get("written"), (dry.get("refresh") or {}).get("unchanged"), ((dry.get("hf") or {}).get("refresh") or {}).get("written")))
        ok = not late and hf_ok and not (ref.get("failed") or ref.get("shrank")) and (dry.get("refresh") or {}).get("written") == 0
        if ok:
            r.ok("the bank holds the source's newest observation for every mnemonic, and a second run would write nothing")
        else:
            r.fail("the bank is not level with the source: behind %d, hedge-fund pair level %s, dry run %s" % (len(late), hf_ok, json.dumps(dry.get("refresh"))[:200]))
        r.kv(written=ref.get("written"), behind_after=len(late), hf_level=hf_ok, second_run_would_write=(dry.get("refresh") or {}).get("written"))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
