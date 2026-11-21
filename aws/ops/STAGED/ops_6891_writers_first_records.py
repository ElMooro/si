"""ops 6891 - READ-ONLY, re-runnable: what the changed writers did on their first runs, read from the bucket itself.

Three writers were changed on 2026-11-21 so that a file whose content did not change is not written again:
  justhodl-repo            data/repo-history/<series>.json           next run 21:52 UTC
  justhodl-polygon-bank    data/warm/polygon-bank/...  (v1.2.1)      next run 22:41 UTC
  justhodl-bank-histories  data/warm/history-book/snapshots/latest/  (v1.3.0, one document per symbol), nightly 03:05 UTC

An engine's own counters say what it believes; the listing says what happened.  For each prefix this lists every object
and counts them by the UTC day (and hour, for the newest day) of their last write: a writer that rewrites everything
shows one block at its last run, a writer that skips unchanged files shows old dates surviving a new run.

Also read: which of the bond desk's regions keep its world score "UNAVAILABLE" (ops 6890 restored the flows only).
Nothing is written, nothing is invoked.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
SECRETISH = re.compile(r"(?i)\b(api_?key|apikey|token|secret|password|signature|sig|key)=([^&\s\"']+)")
LONGTOKEN = re.compile(r"\b[A-Za-z0-9_\-]{36,}\b")
PREFIXES = (("justhodl-repo", "data/repo-history/"),
            ("justhodl-polygon-bank", "data/warm/polygon-bank/"),
            ("justhodl-bank-histories", "data/warm/history-book/snapshots/latest/"))
COUNTERS = re.compile(r"(?i)(written|unchanged|skipp|puts?\b|put_|version|generated|finished|started|as_of|failed|errors?\b|documents|symbols)")


def mask(text, n=200):
    """A line fit for a public report: no address, no credential-looking value, no long opaque token."""
    t = EMAIL.sub("[address]", str(text))
    t = SECRETISH.sub(lambda m: m.group(1) + "=[...]", t)
    return LONGTOKEN.sub("[...]", t)[:n]


def scalars(doc, depth=0, prefix="", deep=2):
    out = []
    if not isinstance(doc, dict):
        return out
    for k, v in doc.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out.append("%s%s=%s" % (prefix, k, mask(v, 70)))
        elif isinstance(v, list):
            out.append("%s%s=[%d]" % (prefix, k, len(v)))
        elif isinstance(v, dict):
            if depth < deep and len(v) <= 16:
                out += scalars(v, depth + 1, prefix + str(k) + ".", deep)
            else:
                out.append("%s%s={%d}" % (prefix, k, len(v)))
    return out


def counters(doc):
    """The scalar leaves whose name is a counter, a version or a time - what a run record says about its writes."""
    return [s for s in scalars(doc) if COUNTERS.search(s.split("=", 1)[0])]


def by_write_time(rows):
    """[(last_modified, size)] -> {total, bytes, days: [(day, n)], newest_day_hours: [(hour, n)]}, newest first."""
    days, hours, size = Counter(), Counter(), 0
    for when, n in rows:
        days[when.strftime("%Y-%m-%d")] += 1
        size += n
    newest = max(days) if days else None
    for when, _ in rows:
        if when.strftime("%Y-%m-%d") == newest:
            hours[when.strftime("%H:00")] += 1
    return {"total": len(rows), "bytes": size, "days": sorted(days.items(), reverse=True), "newest_day": newest, "newest_day_hours": sorted(hours.items())}


def region_gate(row):
    """Why one region does or does not count toward the bond desk's world score (required_region_composite's own test)."""
    score = row.get("score")
    reasons = []
    if not row.get("fresh"):
        reasons.append("not fresh")
    if type(score) not in (int, float):
        reasons.append("no score")
    if row.get("calls_eligible") is False:
        reasons.append("calls_eligible false")
    return reasons


def selftest():
    t = lambda d, h: datetime(2026, 11, d, h, 5, tzinfo=timezone.utc)   # noqa: E731
    got = by_write_time([(t(21, 21), 10), (t(21, 22), 10), (t(20, 21), 5), (t(21, 21), 1)])
    assert got["total"] == 4 and got["bytes"] == 26 and got["days"] == [("2026-11-21", 3), ("2026-11-20", 1)], got
    assert got["newest_day"] == "2026-11-21" and got["newest_day_hours"] == [("21:00", 2), ("22:00", 1)], got
    assert by_write_time([]) == {"total": 0, "bytes": 0, "days": [], "newest_day": None, "newest_day_hours": []}
    assert region_gate({"fresh": True, "score": 51.2}) == [] and region_gate({"fresh": False, "score": None, "calls_eligible": False}) == ["not fresh", "no score", "calls_eligible false"]
    assert region_gate({"fresh": True, "score": True}) == ["no score"]
    assert counters({"counts": {"history_written": 3, "history_unchanged": 90, "series": 93}, "generated_at": "x", "note": "who a.b@c.org"}) == \
        ["counts.history_written=3", "counts.history_unchanged=90", "generated_at=x"]
    assert "a.b@c.org" not in mask("who a.b@c.org") and "[...]" in mask("apikey=abc123")


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}))

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz"):
                import gzip
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
            rows += [(o["LastModified"], o["Size"]) for o in page.get("Contents", [])]
            token = page.get("NextContinuationToken")
            if not token:
                return rows

    with report("ops_6891_writers_first_records") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6891 - the changed writers' first records (read-only), %s" % now.isoformat(timespec="minutes"))
        summary = {}

        r.section("1. Files by the time of their last write")
        for fn, prefix in PREFIXES:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                deployed = (cfg.get("LastModified") or "")[:19]
            except Exception as e:  # noqa: BLE001
                deployed = type(e).__name__
            w = by_write_time(listing(prefix))
            r.log("%s -> %s : %d files, %.1f MB | package deployed %s" % (fn, prefix, w["total"], w["bytes"] / 1e6, deployed))
            r.log("  by day of last write: %s%s" % (", ".join("%s %d" % d for d in w["days"][:8]), " ... (%d days)" % len(w["days"]) if len(w["days"]) > 8 else ""))
            r.log("  newest day %s by hour: %s" % (w["newest_day"], ", ".join("%s %d" % h for h in w["newest_day_hours"])))
            today = dict(w["days"]).get(now.strftime("%Y-%m-%d"), 0)
            summary[fn] = {"files": w["total"], "written_today": today, "kept_from_before_today": w["total"] - today}

        r.section("2. What each engine recorded")
        doc, when = jget("data/repo.json")
        r.log("data/repo.json written %s | %s" % (when.strftime("%Y-%m-%d %H:%M") if when else "-", "; ".join(counters(doc))[:700] or "no counters"))
        counts = doc.get("counts") or {}
        summary["repo_counters"] = {"history_written": counts.get("history_written"), "history_unchanged": counts.get("history_unchanged")}
        for label, key in (("polygon-bank state", "data/warm/polygon-bank/_state/state.json"), ("polygon-bank manifest", "data/warm/polygon-bank/manifest.json"),
                           ("history-book state", "data/warm/history-book/_state/state.json"), ("history-book manifest", "data/warm/history-book/manifest.json")):
            doc, when = jget(key)
            r.log("%s (%s) written %s | %s" % (label, key, when.strftime("%Y-%m-%d %H:%M") if when else "-", "; ".join(counters(doc))[:900] or "; ".join(scalars(doc, deep=0))[:500]))

        r.section("3. Bond desk: the regions behind its world score")
        desk, when = jget("data/bond-desk.json")
        r.log("data/bond-desk.json written %s | world_anxiety %s | regime %s | flow_status %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", desk.get("world_anxiety"), desk.get("regime"),
            ((((desk.get("regions") or {}).get("us") or {}).get("flows") or {}).get("flow_status"))))
        held = []
        for code, row in (desk.get("regions") or {}).items():
            if not isinstance(row, dict):
                continue
            why = region_gate(row)
            if why:
                held.append(code)
            extra = {k: row.get(k) for k in ("as_of", "status", "reason", "missing", "stale", "age_hours", "source_as_of", "unavailable") if row.get(k) not in (None, "", [], {})}
            r.log("  %-8s score %-6s fresh %-5s calls_eligible %-5s -> %s %s" % (code, row.get("score"), row.get("fresh"), row.get("calls_eligible"), "counts" if not why else "HOLDS THE WORLD SCORE: " + ", ".join(why), mask(json.dumps(extra, default=str), 260) if extra else ""))
        summary["bond_desk_regions_holding_world_score"] = held
        if held:
            r.warn("the bond desk's world score is withheld by %d region(s): %s - its own required-input rule; not changed here" % (len(held), ", ".join(held)))
        else:
            r.ok("every region counts toward the world score")
        r.kv(**{k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in summary.items()})
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
