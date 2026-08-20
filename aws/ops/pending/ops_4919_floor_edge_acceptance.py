"""ops/4919 -- floor desk edge acceptance: the surface Khalid opens.

Ops 4914-4918 proved the engine (v1.0.3 tape accepted, BTBT firing
SENSELESS_DRAWDOWN at 0.65 crypto/mcap). Every one of them reported
edge=PENDING for floor.html. Root cause found on inspection: those
soft polls used a bare urlopen -- default Python-urllib UA -- against
a Cloudflare-fronted origin, while the proven house pattern (ops 4907)
sends an explicit User-Agent. The page marker was present in the repo
the whole time. This op verifies the real surface properly and hard-
gates it, and ships two page-layer fixes:

  * marker bumped to floor-audit-v1.0.3 so a PASS proves THIS deploy
    is at the edge, not a cached v1.0.0 artifact;
  * quarantine strip on the desk -- fund wrappers / suspect inputs /
    broker sheets are now visible WITH their reason instead of
    silently vanishing from the alert board (same honesty rule that
    makes JOIN_BROKEN visible);
  * nav pin /floor.html -> Risk & Crisis (unpinned, "Asset-Floor
    Auditor" hits the System & Meta "audit" keyword and files a risk
    desk under settings/legal -- verified against the live rules).

Gates:
  G1 floor.html at the edge: 200 + marker floor-audit-v1.0.3
  G2 /data/floor-audit.json at the edge (worker proxy path the
     browser actually takes) == the S3 object, v1.0.3
  G3 /nav-manifest.json at the edge lists /floor.html under
     Risk & Crisis
  G4 scheduler ENABLED, cron(35 21 ? * MON-FRI *), correct target
  G5 lambda config sane (timeout/memory/key present)
  G6 tape freshness: version 1.0.3, today's history snapshot on disk
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-floor-audit"
OUT_KEY = "data/floor-audit.json"
HIST_PREFIX = "data/floor-audit/history/"
MARKER = "floor-audit-v1.0.3"
UA = "JustHodl ops4919 edge-acceptance"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def edge(url, cap=200000):
    """House pattern (ops 4907): explicit UA or Cloudflare answers a
    challenge instead of the artifact."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.status, resp.read(cap).decode("utf-8",
                                                      "replace")
    except Exception as e:  # noqa: BLE001
        return "%s:%s" % (type(e).__name__, str(e)[:70]), ""


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main():
    with report("4919-floor-edge-acceptance") as r:
        r.heading("ops 4919 -- floor desk edge acceptance")
        t0 = datetime.now(timezone.utc)

        # G1 -- the page itself (pages.yml is publishing this same push)
        r.section("G1 page at edge")
        st, html, tries = None, "", 0
        for tries in range(1, 25):  # up to ~12 min
            st, html = edge("https://justhodl.ai/floor.html?cb=%d"
                            % time.time())
            if st == 200 and MARKER in html:
                break
            time.sleep(30)
        if not (st == 200 and MARKER in html):
            r.log("FAIL G1: status=%s bytes=%d marker=%s stale_v100=%s"
                  % (st, len(html), MARKER in html,
                     "floor-audit-v1.0.0" in html))
            st2, apex = edge("https://justhodl.ai/?cb=%d" % time.time())
            r.kv(apex_status=st2, apex_bytes=len(apex),
                 diagnosis=("site down" if st2 != 200 else
                            "site up -- pages artifact missing "
                            "floor.html or still publishing"))
            sys.exit(1)
        r.kv(g1="PASS", status=st, bytes=len(html), marker=MARKER,
             attempts=tries,
             quarantine_strip=('id="quarantine"' in html))

        # G2 -- the feed, through the worker proxy the browser uses
        r.section("G2 feed at edge")
        st, body = edge("https://justhodl.ai/data/floor-audit.json"
                        "?cb=%d" % time.time(), cap=4000000)
        if st != 200:
            r.log("FAIL G2: feed status=%s (worker proxy "
                  "justhodl-data-proxy -> S3)" % st)
            sys.exit(1)
        try:
            feed = json.loads(body)
        except Exception as e:  # noqa: BLE001
            r.log("FAIL G2: feed not JSON at edge: %s" % e)
            sys.exit(1)
        disk = s3_json(OUT_KEY) or {}
        if feed.get("version") != "1.0.3" or \
                feed.get("as_of") != disk.get("as_of"):
            r.log("FAIL G2: edge v=%s as_of=%s vs S3 v=%s as_of=%s"
                  % (feed.get("version"), feed.get("as_of"),
                     disk.get("version"), disk.get("as_of")))
            sys.exit(1)
        r.kv(g2="PASS", version=feed["version"], as_of=feed["as_of"],
             universe=feed.get("universe_n"),
             alerts=len(feed.get("alerts") or []),
             wrappers=len(feed.get("fund_wrappers") or []),
             suspects=len(feed.get("suspect_inputs") or []),
             brokers=len(feed.get("broker_sheets") or []))
        for a in (feed.get("alerts") or [])[:8]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"])

        # G3 -- reachable from the drawer, filed where it belongs
        r.section("G3 nav placement")
        st, navb = edge("https://justhodl.ai/nav-manifest.json?cb=%d"
                        % time.time(), cap=400000)
        placed = None
        if st == 200:
            try:
                nav = json.loads(navb)
                for c in nav.get("categories") or []:
                    for pg in c.get("pages") or []:
                        if pg.get("href") == "/floor.html":
                            placed = (c["name"], pg.get("title"))
            except Exception as e:  # noqa: BLE001
                r.log("nav parse: %s" % e)
        if not placed or placed[0] != "Risk & Crisis":
            r.log("FAIL G3: nav status=%s placement=%s (want Risk & "
                  "Crisis)" % (st, placed))
            sys.exit(1)
        r.kv(g3="PASS", category=placed[0], title=placed[1])

        # G4 -- it keeps itself current without anyone touching it
        r.section("G4 schedule")
        sd = sch.get_schedule(Name=FN + "-daily")
        tgt = sd["Target"]["Arn"].rsplit(":", 1)[-1]
        if sd["State"] != "ENABLED" or tgt != FN or \
                sd["ScheduleExpression"] != "cron(35 21 ? * MON-FRI *)":
            r.log("FAIL G4: state=%s target=%s expr=%s"
                  % (sd["State"], tgt, sd["ScheduleExpression"]))
            sys.exit(1)
        r.kv(g4="PASS", state=sd["State"],
             expr=sd["ScheduleExpression"], target=tgt,
             tz=sd.get("ScheduleExpressionTimezone", "UTC"))

        # G5 -- runtime shape
        r.section("G5 lambda config")
        cfg = lam.get_function_configuration(FunctionName=FN)
        envk = (cfg.get("Environment") or {}).get("Variables") or {}
        if cfg["Timeout"] < 300 or cfg["MemorySize"] < 512 or \
                not envk.get("POLYGON_API_KEY"):
            r.log("FAIL G5: timeout=%s mem=%s key=%s"
                  % (cfg["Timeout"], cfg["MemorySize"],
                     bool(envk.get("POLYGON_API_KEY"))))
            sys.exit(1)
        r.kv(g5="PASS", timeout=cfg["Timeout"],
             memory=cfg["MemorySize"], runtime=cfg["Runtime"],
             last_modified=cfg["LastModified"],
             code_bytes=cfg["CodeSize"])

        # G6 -- freshness + history
        r.section("G6 tape freshness")
        day = (disk.get("as_of") or "")[:10]
        snap = s3_json(HIST_PREFIX + day + ".json")
        age_h = (t0 - datetime.fromisoformat(
            disk["as_of"])).total_seconds() / 3600.0
        if not snap or age_h > 24:
            r.log("FAIL G6: snapshot=%s age_h=%.1f"
                  % (bool(snap), age_h))
            sys.exit(1)
        r.kv(g6="PASS", snapshot=HIST_PREFIX + day + ".json",
             age_hours=round(age_h, 2),
             next_run="today 21:35 UTC (M-F)")

        rep = {"op": 4919, "edge": "LIVE", "marker": MARKER,
               "page": "https://justhodl.ai/floor.html",
               "feed": "https://justhodl.ai/data/floor-audit.json",
               "nav": {"category": placed[0], "title": placed[1]},
               "version": feed["version"], "as_of": feed["as_of"],
               "alerts": feed.get("alerts"),
               "quarantine": {
                   "fund_wrappers": feed.get("fund_wrappers"),
                   "suspect_inputs": feed.get("suspect_inputs"),
                   "broker_sheets": feed.get("broker_sheets")},
               "schedule": sd["ScheduleExpression"]}
        (ROOT / "aws" / "ops" / "reports" / "4919.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN ACCEPTED",
             duration_s=int((datetime.now(timezone.utc)
                             - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
