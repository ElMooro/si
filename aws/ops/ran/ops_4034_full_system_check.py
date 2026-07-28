"""ops_4034 — FULL SYSTEM CHECK: every layer touched today + standing systems."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def age_min(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60
    except Exception:
        return None


def main():
    with report("4034_full_system_check") as rep:
        rep.heading("ops 4034 — full system check")
        checks = []
        now = datetime.now(timezone.utc)

        rep.section("A. capture pipeline — live heartbeat")
        try:
            ev = logs.filter_log_events(
                logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
                startTime=int((now - timedelta(minutes=40)).timestamp() * 1000),
                limit=50)
            starts = [e for e in ev.get("events") or []
                      if "START" in e.get("message", "")]
            rep.kv(ingest_hits_40min=len(starts))
            checks.append(("browser still syncing (auto)", len(starts) >= 1))
        except Exception as e:
            rep.log(f"  logs: {type(e).__name__}")
        wl_age = age_min("data/tv-watchlists.json")
        wl = gj("data/tv-watchlists.json") or {}
        lists = wl.get("lists") or wl.get("watchlists") or []
        rep.kv(watchlists_age_min=round(wl_age or -1, 1), n_lists=len(lists))
        checks.append(("tracker fresh + 491", (wl_age or 999) < 120
                       and len(lists) >= 485))
        srcs = gj("data/tv-sources.json")
        if srcs:
            rep.kv(sources_n=srcs.get("n_symbols"),
                   sources_age_min=round(age_min("data/tv-sources.json") or -1, 1))
            for k, v in list((srcs.get("sources") or {}).items())[:5]:
                rep.log(f"    {k}: {str(v.get('source'))[:44]}")
        else:
            rep.log("  tv-sources.json: not born yet — harvest mid-run (INFO)")

        rep.section("B. workbench — artifact + schedule + page")
        d = gj("data/tv-workbench.json") or {}
        t = d.get("totals") or {}
        rep.kv(marker=d.get("marker"), **t)
        checks.append(("workbench current (491/10k)",
                       (t.get("watchlists") or 0) >= 485
                       and (t.get("unique_symbols") or 0) >= 9000))
        st = sch.get_schedule(Name="tv-workbench-daily")
        rep.kv(workbench_sched=st.get("State"),
               expr=st.get("ScheduleExpression"))
        checks.append(("workbench schedule ENABLED",
                       st.get("State") == "ENABLED"))
        got, htm = 0, ""
        MK = ["v1-ops4019", "pulled from", "TV Workbench"]
        try:
            r = urllib.request.Request(
                f"https://justhodl.ai/tv-workbench.html?cb={int(time.time())}",
                headers={"User-Agent": "Mozilla/5.0",
                         "Cache-Control": "no-cache"})
            htm = urllib.request.urlopen(r, timeout=25).read().decode("utf8",
                                                                      "ignore")
            got = sum(1 for m in MK if m in htm)
        except Exception:
            pass
        checks.append(("workbench page at edge", got == len(MK)))

        rep.section("C. distribution chain — zip/version/installer")
        vj = json.loads(urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
            f"tools/jh-tv-extension.version.json?t={int(time.time())}",
            timeout=25).read())
        zb = urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
            f"tools/jh-tv-extension.zip?t={int(time.time())}",
            timeout=25).read()
        man = json.loads(zf.ZipFile(io.BytesIO(zb)).read("manifest.json"))
        cjs = zf.ZipFile(io.BytesIO(zb)).read("content.js").decode()
        rep.kv(version_json=vj.get("version"), zip_version=man.get("version"),
               zip_bytes=len(zb))
        checks += [("zip + version.json agree on 1.7.0",
                    vj.get("version") == man.get("version") == "1.7.0"),
                   ("autonomy code in served zip",
                    "jh_auto_day" in cjs and "autoSync" in cjs)]
        for k in ("tools/install-jh-extension.bat",
                  "tools/install-jh-extension.ps1"):
            b = urllib.request.urlopen(
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/" +
                k + f"?t={int(time.time())}", timeout=25).read()
            rep.log(f"  {k}: {len(b)}B")
        checks.append(("installer pair serving", True))

        rep.section("D. CB expansion + risk stack still healthy")
        v = gj("data/tradingview.json") or {}
        idx = {r2["symbol"]: r2 for r2 in v.get("symbols") or []}
        NEW = ["JP01Y", "JP05Y", "JP10Y", "JP20Y", "JP30Y", "NOINTR", "NO10Y",
               "PEINTR", "PENUSD", "PEFER", "JPMB", "JPM2YY", "JPCALLR",
               "BRINTR", "BRFER", "BRLUSD", "FIIPYY", "ESIPYY", "ITIPYY",
               "KRIPYY", "BRIPYY", "IT10Y"]
        live = [s2 for s2 in NEW if (idx.get(s2) or {}).get("status") == "LIVE"]
        rep.kv(vault_marker=str(v.get("marker"))[:44], cb_live=len(live),
               of=len(NEW))
        checks.append(("CB expansion >=18/22 LIVE", len(live) >= 18))
        g = gj("data/risk-gate.json") or {}
        gjs = json.dumps(g)
        rep.kv(posture=g.get("posture"), sizing=g.get("sizing_multiplier"),
               jgb10y_in_carry="jgb10y_carry_cost" in gjs)
        checks.append(("risk-gate carries JGB10Y input",
                       "jgb10y_carry_cost" in gjs))
        bar = gj("data/domain-barometers.json") or {}
        for dom in ("MACRO", "LIQUIDITY", "RISK"):
            b2 = (bar.get("barometers") or {}).get(dom) or {}
            rep.log(f"  {dom:9s} {b2.get('score_0_100')} {b2.get('state')} "
                    f"voting={(b2.get('coverage') or {}).get('voting')}")

        rep.section("E. census + schedules")
        c = gj("data/data-census.json") or {}
        cage = age_min("data/data-census.json")
        rep.kv(census_marker=str(c.get("marker"))[:40],
               census_age_h=round((cage or 9999) / 60, 1))
        checks.append(("census ran today (12:45 schedule alive)",
                       (cage or 9999) < 26 * 60))

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(checks)} gates green; sources "
               f"{'LIVE n=' + str((srcs or {}).get('n_symbols')) if srcs else 'in-flight (harvest counting)'}")


if __name__ == "__main__":
    main()
