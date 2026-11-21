"""ops 6894 - READ-ONLY dry run of the OFR bank's daily refresh, from this checkout, before it is deployed.

The bank never refetched a banked mnemonic (ops 6893: 442 files last written 2026-08-06, 312 behind the source).  The
engine's new refresh (aws/lambdas/justhodl-ofr-stfm/source/ofr_refresh.py) is run here exactly as the Lambda will run
it - real OFR dataset calls, the live state file - with dry_run on, so no file and no state is written.

On top of the engine's own answer this checks what a refresh must not do: lose or alter history.  For every series it
would write, the banked file is read and compared with the source answer observation by observation:
  kept        banked observations found in the source with the same value
  revised     banked observations whose value the source now gives differently
  gone        banked observations the source no longer carries
  new         source observations the bank does not have
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-ofr-stfm" / "source"))
LIVE = "justhodl-dashboard-live"
BASE = "https://data.financialresearch.gov/v1"
HF_BASE = "https://data.financialresearch.gov/hf/v1"


def compare(banked_rows, source_rows):
    """-> {kept, revised, gone, new} between two [date, value] lists (rows without a value are not observations)."""
    b = {str(d)[:10]: v for d, v in banked_rows if v is not None}
    s = {str(d)[:10]: v for d, v in source_rows if v is not None}
    kept = sum(1 for d, v in b.items() if d in s and s[d] == v)
    revised = sum(1 for d, v in b.items() if d in s and s[d] != v)
    return {"kept": kept, "revised": revised, "gone": sum(1 for d in b if d not in s), "new": sum(1 for d in s if d not in b)}


def selftest():
    assert compare([["2026-01-01", 1.0], ["2026-01-02", 2.0], ["2026-01-03", 3.0], ["2026-01-04", None]],
                   [["2026-01-01", 1.0], ["2026-01-02", 2.5], ["2026-01-05", 5.0]]) == {"kept": 1, "revised": 1, "gone": 1, "new": 1}
    assert compare([], [["2026-01-01", 1.0]]) == {"kept": 0, "revised": 0, "gone": 0, "new": 1}


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import ofr_refresh as R
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=48))
    answers = {}

    def fetch(url):
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "JustHodl research (ops dry run)"}), timeout=180) as resp:
            raw = resp.read()
        answers[url] = raw
        return raw

    def refuse(key, body):
        raise AssertionError("a dry run must not write %s" % key)

    def jget(key):
        try:
            raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw)
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}

    with report("ops_6894_ofr_bank_refresh_dry_run") as r:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        r.heading("ops 6894 - OFR bank refresh, dry run from the checkout (read-only), %s" % now[:16])
        state = jget("data/warm/ofr/state.json")
        hf_state = jget("data/warm/ofr-hfm/state.json")
        before = json.dumps(state, sort_keys=True)

        r.section("1. The engine's own dry run")
        t0 = time.time()
        known = dict(state.get("series") or {})
        run = R.refresh(fetch, refuse, known, BASE, R.DATASETS, now, "data/warm/ofr/series/", dry_run=True, log=r.log)
        hf_run = R.refresh(fetch, refuse, dict(hf_state.get("series") or {}), HF_BASE, R.HF_DATASETS, now, "data/warm/ofr-hfm/series/", layout="raw", dry_run=True, log=r.log)
        secs = time.time() - t0
        for ds, row in list(run["datasets"].items()) + list(hf_run["datasets"].items()):
            r.log("  dataset %-5s %s" % (ds, json.dumps(row)))
        r.log("would write %d, unchanged %d, shrank %d, failed %d | hedge-fund pair: would write %d | %.1f s for %d calls, %.1f MB" % (
            run["written"], run["unchanged"], len(run["shrank"]), len(run["failed"]), hf_run["written"], secs, len(answers), sum(len(R.unzip(a)) for a in answers.values()) / 1e6))
        r.log("newest observation dates across the source: %s" % json.dumps(run["newest"]))
        assert known == dict(state.get("series") or {}), "a dry run changed the known map"
        assert json.dumps(jget("data/warm/ofr/state.json"), sort_keys=True) == before, "the state file changed during the dry run"
        r.ok("the state file is byte-for-byte what it was")

        r.section("2. History: every series it would write, banked file against source answer")
        source = {}
        for url, raw in answers.items():
            if "/hf/" not in url:
                source.update(R.series_of(json.loads(R.unzip(raw))))

        def one(m):
            doc = jget("data/warm/ofr/series/%s.json.gz" % m)
            banked = R.observations((doc.get("payload") or {}).get(m) or {}) if isinstance(doc, dict) else []
            return m, compare(banked, R.observations(source[m])), len(json.dumps(R.series_doc(m, source[m], "u", now)))

        with ThreadPoolExecutor(max_workers=32) as pool:
            rows = list(pool.map(one, run["written_ids"]))
        tot = {k: sum(c[k] for _, c, _ in rows) for k in ("kept", "revised", "gone", "new")}
        r.log("%d series: banked observations kept %d, revised %d, gone %d | new observations %d | %.1f MB of series documents before gzip" % (
            len(rows), tot["kept"], tot["revised"], tot["gone"], tot["new"], sum(n for *_, n in rows) / 1e6))
        losers = sorted(((c["gone"], m, c) for m, c, _ in rows if c["gone"]), reverse=True)
        revisers = sorted(((c["revised"], m, c) for m, c, _ in rows if c["revised"]), reverse=True)
        r.log("series that would lose banked observations: %d%s" % (len(losers), "".join("\n    %s %s" % (m, json.dumps(c)) for _, m, c in losers[:12])))
        r.log("series with revised values: %d%s" % (len(revisers), "".join("\n    %s %s" % (m, json.dumps(c)) for _, m, c in revisers[:12])))
        empty = sorted(m for m, c, _ in rows if c["kept"] + c["revised"] + c["new"] == 0)
        r.log("series with no observation at the source: %d %s" % (len(empty), empty[:10]))
        gone_share = tot["gone"] / max(1, tot["kept"] + tot["revised"] + tot["gone"])
        if gone_share <= 0.001 and not run["failed"] and not any(v.get("error") for v in run["datasets"].values()):
            r.ok("a refresh keeps the banked history (%.4f%% of banked observations absent at the source) and adds %d observations" % (100 * gone_share, tot["new"]))
        else:
            r.fail("history would be lost or a dataset failed: gone share %.4f%%, datasets %s" % (100 * gone_share, {d: v.get("error") for d, v in run["datasets"].items() if v.get("error")}))
        r.kv(would_write=run["written"], unchanged=run["unchanged"], shrank=len(run["shrank"]), new_observations=tot["new"], revised=tot["revised"], gone=tot["gone"],
             hf_would_write=hf_run["written"], seconds=round(secs, 1))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
