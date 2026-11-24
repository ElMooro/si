"""ops 6937 - FIRST LIGHT of the Risk Map engine 2.0.0: the deployed function, one dry run inside Lambda, one publication.

Khalid 2026-11-24: "improve this engine so that it covers all asset classes ... make sure whats booming is actually
booming and whats destroyed is actually destroyed".

  1. Guard: the deployed package's files are this checkout's, byte for byte; memory, timeout and the once-a-day rule.
     If not, nothing is invoked.
  2. One invoke with {"dry_run": true}: the engine builds the whole map inside Lambda (its own role, its own S3
     reads) and writes nothing. If that fails, nothing is published.
  3. One invoke with {}: the engine publishes data/regime-map.json and its snapshot. This is a one-off run outside
     the engine's 21:00 UTC schedule, disclosed to Khalid; the schedule itself is untouched and runs as usual.
  4. Read back, and check the published document with arithmetic written here: every change is the close over its
     anchor, every state is the rule's, both lists hold only what is in that state, everything the first engine
     published is there for the same 38 instruments, and the document is served on the site's own path.

Safe to run again: each run is one more dry run and one more publication of the same kind the schedule makes.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import hashlib
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws" / "lambdas" / "justhodl-regime-map" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops")]
FN = "justhodl-regime-map"
LIVE = "justhodl-dashboard-live"
OUT_KEY, PREV_KEY = "data/regime-map.json", "data/regime-map-prev.json"
FILES = ("lambda_function.py", "risk_math.py", "risk_universe.py", "leaders_tape.py")
SERVED = "https://justhodl.ai/data/regime-map.json"
LEGACY_KEYS = ["booming", "destroyed", "elapsed_s", "engine", "generated_at", "n_instruments", "n_transitions", "ok", "prev_regime", "ranked", "regime",
               "regime_changed", "sleeves", "thesis", "transitions", "version", "windows"]
CLASSES = ["us", "world", "bonds", "fx", "cmdty", "crypto", "realestate", "vol", "alts"]


def months_back(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    for day in range(d.day, 0, -1):
        try:
            return date(y, m, day)
        except ValueError:
            continue


def window_start(last_iso, window):
    last = date.fromisoformat(last_iso)
    if window == "w":
        return (last - timedelta(days=7)).isoformat()
    if window == "ytd":
        return date(last.year - 1, 12, 31).isoformat()
    return months_back(last, {"m": 1, "q": 3, "h": 6, "y": 12}[window]).isoformat()


def rule_state(score, r1m, r3m, v50, v200):
    if score >= 45:
        return "BOOMING" if (r1m > 0 and r3m > 0 and max(r1m, r3m) >= 5 and v50 > 0 and v200 > 0) else "RISK-ON"
    if score >= 15:
        return "RISK-ON"
    if score > -15:
        return "NEUTRAL"
    if score > -45:
        return "RISK-OFF"
    return "DESTROYED" if (r1m < 0 and r3m < 0 and min(r1m, r3m) <= -5 and v50 < 0 and v200 < 0) else "RISK-OFF"


def problems(doc):
    """Everything wrong with a published document, by this op's own arithmetic. Pure."""
    bad = []
    for k in LEGACY_KEYS:
        if k not in doc:
            bad.append("the first engine's key %r is missing" % k)
    if doc.get("version") != "2.0.0" or doc.get("contract") != "risk-map.v2" or doc.get("ok") is not True:
        bad.append("version / contract: %s %s" % (doc.get("version"), doc.get("contract")))
    m = doc.get("map") or {}
    tiles = m.get("tiles") or []
    by = {t["t"]: t for t in tiles}
    if len(by) != len(tiles) or m.get("n") != len(tiles):
        bad.append("an instrument appears twice, or the count is not the tiles'")
    if [c["id"] for c in m.get("classes") or []] != CLASSES:
        bad.append("asset classes: %s" % [c["id"] for c in m.get("classes") or []])
    for c in m.get("classes") or []:
        if not any(t["cls"] == c["id"] for t in tiles):
            bad.append("no instrument in the asset class %s" % c["name"])
    for t in tiles:
        for k, (ad, ac, _) in t["a"].items():
            if abs((t["c"] / ac - 1) * 100 - t["r"][k]) > 0.011:
                bad.append("%s %s: published %s is not the close over its anchor" % (t["t"], k, t["r"][k]))
            if k == "d":
                if ad != t["pd"]:
                    bad.append("%s: the day's anchor is not the bar before" % t["t"])
            else:
                want = window_start(t["d"], k)
                if not (ad <= want and (date.fromisoformat(want) - date.fromisoformat(ad)).days <= 10):
                    bad.append("%s %s: anchor %s for a window that starts %s" % (t["t"], k, ad, want))
        edge = min(abs(t["r"]["m"]), abs(t["r"]["q"]), abs(t["v50"]), abs(t["v200"])) < 0.006
        if not t.get("jump") and not edge and rule_state(t["score"], t["r"]["m"], t["r"]["q"], t["v50"], t["v200"]) != t["state"]:
            bad.append("%s: state %s, the rule gives %s" % (t["t"], t["state"], rule_state(t["score"], t["r"]["m"], t["r"]["q"], t["v50"], t["v200"])))
        if t["state"] == "BOOMING" and not (t["score"] >= 45 and t["r"]["m"] > 0 and t["r"]["q"] > 0 and max(t["r"]["m"], t["r"]["q"]) >= 5 and t["v50"] > 0 and t["v200"] > 0):
            bad.append("%s is called BOOMING against its record" % t["t"])
        if t["state"] == "DESTROYED" and not (t["score"] <= -45 and t["r"]["m"] < 0 and t["r"]["q"] < 0 and min(t["r"]["m"], t["r"]["q"]) <= -5 and t["v50"] < 0 and t["v200"] < 0):
            bad.append("%s is called DESTROYED against its record" % t["t"])
    if set(m.get("booming") or []) != {t["t"] for t in tiles if t["state"] == "BOOMING"} or set(m.get("destroyed") or []) != {t["t"] for t in tiles if t["state"] == "DESTROYED"}:
        bad.append("a list is not exactly the instruments in its state")
    if not all(x["state"] == "BOOMING" for x in doc.get("booming") or []) or not all(x["state"] == "DESTROYED" for x in doc.get("destroyed") or []):
        bad.append("a first-map list holds an instrument in another state")
    ranked = doc.get("ranked") or []
    if len(ranked) < 26 or doc.get("n_instruments") != len(ranked) or sum(len(v) for v in (doc.get("sleeves") or {}).values()) != len(ranked):
        bad.append("the first map's instruments: %d" % len(ranked))
    for x in ranked:
        t = by.get(x["ticker"])
        if not t or t["score"] != x["risk_on"] or t["state"] != x["state"]:
            bad.append("%s: the first map's row and the tile disagree" % x["ticker"])
    eq = [x["risk_on"] for s, rows in (doc.get("sleeves") or {}).items() if s.startswith("Equity") for x in rows]
    if eq and (doc["regime"]["equity_avg"] != round(sum(eq) / len(eq), 1) or doc["regime"]["equity_breadth_pct"] != round(100 * sum(1 for v in eq if v > 0) / len(eq))):
        bad.append("the regime's equity average or breadth is not its sleeves'")
    return bad


def selftest():
    assert window_start("2026-11-24", "q") == "2026-08-24" and window_start("2026-03-31", "m") == "2026-02-28" and window_start("2026-01-05", "ytd") == "2025-12-31"
    assert rule_state(46, 3.3, 1.0, 2, 14) == "RISK-ON" and rule_state(50, 12.8, 9.4, 7, 9) == "BOOMING" and rule_state(-46, -3.0, -11.8, -1, -1) == "DESTROYED" and rule_state(-60, 1, -30, -1, -1) == "RISK-OFF"
    tile = {"t": "X", "cls": "us", "state": "BOOMING", "score": 50, "c": 110.0, "d": "2026-11-24", "pd": "2026-11-23", "v50": 3.0, "v200": 9.0,
            "r": {"d": 10.0, "m": 10.0, "q": 10.0}, "a": {"d": ["2026-11-23", 100.0, 1], "m": ["2026-10-23", 100.0, 22], "q": ["2026-08-24", 100.0, 65]}}
    doc = dict({k: None for k in LEGACY_KEYS}, version="2.0.0", contract="risk-map.v2", ok=True, booming=[], destroyed=[], ranked=[], sleeves={}, n_instruments=0,
               regime={"equity_avg": 0, "equity_breadth_pct": 0}, map={"n": 1, "tiles": [tile], "classes": [{"id": c, "name": c} for c in CLASSES], "booming": ["X"], "destroyed": []})
    got = problems(doc)
    assert not any("X" in g for g in got) and any("no instrument in the asset class" in g for g in got) and any("first map's instruments" in g for g in got), got
    lie = json.loads(json.dumps(doc))
    lie["map"]["tiles"][0]["r"]["q"] = 1.0
    assert any("not the close over its anchor" in g for g in problems(lie)) and any("BOOMING against" in g or "the rule gives" in g for g in problems(lie))


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=320, retries={"max_attempts": 0}))
    ev = boto3.client("events", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    with report("ops_6937_risk_map_first_light") as r:
        r.heading("ops 6937 - Risk Map 2.0.0: first light of the deployed engine, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard: the deployed function is this checkout's")
        g = lam.get_function(FunctionName=FN)
        cfg = g["Configuration"]
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in FILES:
            hits = [x for x in z.namelist() if x == name]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        r.log("%s updated %s | memory %s MB, timeout %s s | files as in this checkout: %s" % (FN, (cfg.get("LastModified") or "")[:19], cfg.get("MemorySize"), cfg.get("Timeout"), same))
        if not all(same.values()):
            r.fail("the deployed package is not this checkout's; nothing was invoked")
            sys.exit(1)
        try:
            rule = ev.describe_rule(Name="justhodl-regime-map-daily")
            r.log("schedule: %s %s %s" % (rule.get("Name"), rule.get("ScheduleExpression"), rule.get("State")))
            if rule.get("ScheduleExpression") != "cron(0 21 * * ? *)" or rule.get("State") != "ENABLED":
                fails.append("the engine's daily rule is not cron(0 21 * * ? *) ENABLED")
        except Exception as e:  # noqa: BLE001
            r.log("schedule: the rule could not be read (%s)" % type(e).__name__)
        raw0, when0 = get(OUT_KEY)
        was = json.loads(raw0)
        r.log("before: %s version %s, %d bytes, generated %s, regime %s, %d instruments" % (OUT_KEY, was.get("version"), len(raw0), was.get("generated_at"), (was.get("regime") or {}).get("label"), was.get("n_instruments")))
        prev0 = hashlib.sha256(get(PREV_KEY)[0]).hexdigest()

        r.section("2. One dry run inside Lambda (nothing written)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"dry_run": True}).encode())
        dry = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps({k: (v if not isinstance(v, list) else len(v)) for k, v in dry.items()})[:500]))
        if resp.get("FunctionError") or not dry.get("dry_run") or (dry.get("tiles") or 0) < 240:
            r.fail("the dry run inside Lambda failed or measured too few instruments; nothing was published: %s" % json.dumps(dry)[:600])
            sys.exit(1)
        if hashlib.sha256(get(OUT_KEY)[0]).hexdigest() != hashlib.sha256(raw0).hexdigest() or hashlib.sha256(get(PREV_KEY)[0]).hexdigest() != prev0:
            r.fail("the dry run wrote something")
            sys.exit(1)
        r.log("the feed and the snapshot are byte for byte what they were: the dry run wrote nothing; left out: %s" % (dry.get("left_out") or "none"))

        r.section("3. One publication (a one-off run outside the 21:00 UTC schedule)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        took = time.time() - t0
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), took, json.dumps(out)[:400]))
        if resp.get("FunctionError") or out.get("statusCode") != 200:
            r.fail("the engine did not publish: %s" % json.dumps(out)[:600])
            sys.exit(1)

        r.section("4. Read back")
        raw, when = get(OUT_KEY)
        doc = json.loads(raw)
        m = doc["map"]
        r.log("%s: %d bytes written %s | %s %s | generated %s | built in %s s" % (OUT_KEY, len(raw), when.strftime("%H:%M:%S"), doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("elapsed_s")))
        r.log("%d of %d instruments: %s | dates %s" % (m["n"], m["n_universe"], m["counts"], doc["as_of"]))
        r.log("regime %s (was %s under 1.0.0): %s" % (doc["regime"]["label"], (was.get("regime") or {}).get("label"), {k: v for k, v in doc["regime"].items() if k not in ("label", "summary")}))
        r.log("cross-asset %s: %s" % (doc["cross_asset"]["label"], doc["cross_asset"]["summary"]))
        r.log("booming (%d): %s" % (len(m["booming"]), ", ".join(m["booming"])))
        r.log("destroyed (%d): %s" % (len(m["destroyed"]), ", ".join(m["destroyed"])))
        r.log("held back from booming / destroyed: %s" % [(t["t"], t["held"]) for t in m["tiles"] if t.get("held")])
        r.log("left out: %s | notes: %s" % (m["left_out"] or "none", doc.get("notes") or "none"))
        r.log("turns since the run before: %d on the first map, %d on the whole map (the snapshot before this run held only the first map's 38)" % (doc["n_transitions"], m["n_transitions"]))
        for c in m["classes"]:
            st = m["stats"]["classes"][c["id"]]
            r.log("    %-14s %3d | score %6s | rising %3s%% | %s" % (c["name"], st["n"], st["score"], st["rising_pct"], " ".join("%s %d" % (k.lower(), v) for k, v in st["states"].items() if v)))
        if when <= when0:
            fails.append("the feed was not rewritten")
        found = problems(doc)
        r.log("this op's own checks on the published document: %d problems %s" % (len(found), found[:6]))
        fails += found
        if m["n"] < 0.9 * m["n_universe"] or set(m["counts"]) != {"fund", "coin", "fx", "fut", "idx"} or min(m["counts"].values()) < 3:
            fails.append("too few instruments, or a kind is missing: %s" % m["counts"])
        snap = json.loads(get(PREV_KEY)[0])
        if snap.get("label") != doc["regime"]["label"] or len(snap.get("tiles") or {}) != m["n"] or len(snap.get("states") or {}) != len(doc["ranked"]):
            fails.append("the snapshot for the next run is not this publication's")
        try:
            req = urllib.request.Request(SERVED + "?t=%d" % time.time(), headers={"User-Agent": "jh-ops-probe"})
            with urllib.request.urlopen(req, timeout=60) as resp2:
                served = json.loads(resp2.read())
            r.log("served at %s: version %s, generated %s" % (SERVED, served.get("version"), served.get("generated_at")))
            if served.get("generated_at") != doc.get("generated_at"):
                fails.append("the site's path does not serve this publication yet")
        except Exception as e:  # noqa: BLE001
            fails.append("the site's path could not be read: %s" % type(e).__name__)

        r.kv(tiles=m["n"], universe=m["n_universe"], booming=len(m["booming"]), destroyed=len(m["destroyed"]), regime=doc["regime"]["label"], cross_asset=doc["cross_asset"]["label"],
             seconds=round(took, 1), failed_checks=len(fails))
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("FIRST LIGHT: the deployed engine published %d instruments in nine asset classes in %.1f s; %d booming, %d destroyed, each by the rule" % (m["n"], took, len(m["booming"]), len(m["destroyed"])))


if __name__ == "__main__":
    main()
