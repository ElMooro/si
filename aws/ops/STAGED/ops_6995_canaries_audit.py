"""ops 6995 - read-only audit of the canaries engine (justhodl-canary-warroom, canaries.html).

Khalid, 2026-11-27 11:06 ET: "next improve the engine : canaries https://justhodl.ai/canaries.html and add an icon
for it in chart.html also test it against historical data to calibrate it".

What this reads, and nothing else:
  1. the war room's document, its day-by-day history, the weights and the replay it reads;
  2. each of the twelve feeds it aggregates (when written, by whom, what a row looks like);
  3. both functions' schedules, runs and errors, and the weights engine's last log lines (masked);
  4. how far back the warehouse can take the series a test against history would need.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6995_canaries_audit"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
FEEDS = ["canary-grid", "crisis-canaries", "leading-markets", "dollar-radar", "vol-radar", "ciss-stress", "factor-regime",
         "cftc-all-cache", "global-stress", "plumbing-stress", "eurodollar-plumbing", "alert-sentinel"]
WRITERS = {"canary-grid": "justhodl-canary-grid", "crisis-canaries": "justhodl-crisis-canaries", "leading-markets": "justhodl-leading-markets",
           "dollar-radar": "justhodl-dollar-radar", "vol-radar": "justhodl-vol-radar", "ciss-stress": "justhodl-ciss-stress",
           "factor-regime": "justhodl-factor-regime", "cftc-all-cache": "cftc-futures-positioning-agent", "global-stress": "justhodl-global-stress",
           "plumbing-stress": "justhodl-plumbing-aggregator", "eurodollar-plumbing": "justhodl-eurodollar-plumbing", "alert-sentinel": "justhodl-alert-sentinel"}
PROBE = ["fred:SP500", "fred:VIXCLS", "fred:BAMLH0A0HYM2", "fred:BAMLC0A0CM", "fred:T10Y2Y", "fred:T10Y3M", "fred:ICSA", "fred:IC4WSA", "fred:NFCI",
         "fred:STLFSI4", "fred:DGS10", "fred:DTWEXBGS", "fred:TEDRATE", "fred:SOFR", "fred:WRESBAL", "fred:RRPONTSYD", "fred:PERMIT", "fred:UNRATE",
         "fred:SAHMREALTIME", "fred:INDPRO", "fred:NASDAQCOM", "fred:WILL5000IND", "fred:DJIA", "fred:USREC"]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***"),
         (re.compile(r"(token|secret|password|authorization)([\"'=:\s]+)[^\s\"',}]+", re.I), r"\1\2***")]


def clean(text):
    """Every line this op prints passes here: no key, no address, no token."""
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def cut(v, depth=0):
    """A document's shape without its bulk: long lists of numbers or pairs are counted and sampled."""
    if isinstance(v, dict):
        if depth > 7:
            return {"__dict__": len(v)}
        return {str(k)[:80]: cut(x, depth + 1) for k, x in list(v.items())[:400]}
    if isinstance(v, list):
        rows_of_dicts = v and all(isinstance(x, dict) for x in v[:5])
        if rows_of_dicts and len(v) <= 400:
            return [cut(x, depth + 1) for x in v]
        if len(v) > 40 or (rows_of_dicts and len(v) > 400):
            return {"__list__": len(v), "first": [cut(x, depth + 1) for x in v[:2]], "last": [cut(x, depth + 1) for x in v[-2:]]}
        return [cut(x, depth + 1) for x in v]
    if isinstance(v, str):
        return v[:260]
    return v


def main():
    import boto3
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    ev = boto3.client("events", region_name=REGION)
    now = datetime.now(timezone.utc)

    def kept(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"], o["ContentLength"]

    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def fixture(name, obj):
            text = json.dumps(obj, separators=(",", ":"), default=str)
            for pat, rep in _MASK[:2]:        # a fixture is public too: no address, no key inside a kept URL
                text = pat.sub(rep, text)
            raw = text.encode()
            b64 = base64.b64encode(gzip.compress(raw, 9)).decode()
            r.log("<!-- FIXTURE %s %d bytes gzip+base64" % (name, len(raw)))
            for i in range(0, len(b64), 3000):
                r.log(b64[i:i + 3000])
            r.log("FIXTURE-END -->")

        r.heading("6995 - the canaries engine as it stands (read-only), %s" % now.isoformat(timespec="minutes"))

        # ── 1. the war room's own documents ─────────────────────────────────────────────────────────────────────
        r.section("1. The war room's document")
        doc = None
        try:
            doc, w, n = kept("data/canary-warroom.json")
            say("written %s (%.1f h ago), %d bytes, top keys %s" % (w.isoformat(timespec="minutes"), (now - w).total_seconds() / 3600, n, sorted(doc)))
            say("master %s" % json.dumps(doc.get("master")))
            b = doc.get("barometer") or {}
            v = b.get("views") or {}
            say("barometer per canary %s (%s votes) | per mechanism %s (%s mechanisms) | earned %s (weights of %s, %s learned)" % (
                (v.get("per_canary") or {}).get("score"), (v.get("per_canary") or {}).get("n_votes"), (v.get("per_mechanism") or {}).get("score"),
                (v.get("per_mechanism") or {}).get("n_mechanisms"), (v.get("earned") or {}).get("score"), (v.get("earned") or {}).get("weights_asof"),
                (v.get("earned") or {}).get("n_learned")))
            say("velocity %s" % json.dumps(b.get("velocity")))
            say("breadth and intensity %s" % json.dumps(b.get("breadth_intensity")))
            say("conviction leads %s" % json.dumps(b.get("conviction_leads")))
            say("top_picks %s | divergences %s | bus_canaries %s | playbook rows %s" % (json.dumps(doc.get("top_picks")), json.dumps(doc.get("divergences")),
                                                                                       json.dumps(doc.get("bus_canaries")), len(doc.get("brain_playbook") or [])))
            cans = doc.get("all_canaries") or []
            by = {}
            for c in cans:
                by.setdefault(c.get("mechanism"), []).append(c)
            mv = (v.get("per_mechanism") or {}).get("by_mechanism") or {}
            for card in doc.get("mechanisms") or []:
                k = card.get("key")
                rows = by.get(k) or []
                st = sorted(x.get("stress") for x in rows if isinstance(x.get("stress"), (int, float)))
                distinct = sorted(set(st))
                say("  %-16s card score %s band %s | rows %d firing %d | feed as of %s, %s h old, stale %s%s%s | view %s | stress values: %d distinct%s" % (
                    k, card.get("score"), card.get("band"), len(rows), sum(1 for x in rows if x.get("firing")), str(card.get("feed_as_of"))[:19], card.get("feed_age_h"),
                    card.get("stale"), " | UNAVAILABLE" if card.get("unavailable") else "", (" | ERROR " + str(card.get("error"))) if card.get("error") else "",
                    json.dumps({kk: (mv.get(k) or {}).get(kk) for kk in ("score", "weight", "weight_status", "firing_pct", "velocity_21d")}), len(distinct),
                    (" " + json.dumps(distinct)) if len(distinct) <= 8 else " min %s median %s max %s" % (st[0], st[len(st) // 2], st[-1])))
            say("canaries in all: %d, firing %d, with no stress %d, lead_months values %s" % (
                len(cans), sum(1 for c in cans if c.get("firing")), sum(1 for c in cans if c.get("stress") is None),
                json.dumps(sorted(set(str(c.get("lead_months")) for c in cans)))))
            fixture("canary-warroom", {k: doc[k] for k in doc if k != "brain_playbook"})
        except Exception as e:  # noqa: BLE001
            say("war room document not read: %s" % str(e)[:200])

        r.section("2. Its history, its weights, its replay")
        try:
            hist, w, n = kept("data/warroom-history.json")
            days = [h.get("d") for h in hist]
            say("history: %d days %s to %s, written %s, %d bytes; fields %s" % (len(hist), days[0], days[-1], w.isoformat(timespec="minutes"), n, sorted(hist[-1])))
            gaps = []
            for a, b2 in zip(days, days[1:]):
                d = (datetime.strptime(b2, "%Y-%m-%d") - datetime.strptime(a, "%Y-%m-%d")).days
                if d > 1:
                    gaps.append((a, b2, d))
            say("gaps of more than a day: %d %s" % (len(gaps), json.dumps(gaps[:20])))
            for f in ("pc", "pm", "ew", "votes", "nfire"):
                vals = [h.get(f) for h in hist if isinstance(h.get(f), (int, float))]
                if vals:
                    say("  %-5s %d readings, lowest %s, highest %s, first %s, last %s" % (f, len(vals), min(vals), max(vals), vals[0], vals[-1]))
            fixture("warroom-history", hist)
        except Exception as e:  # noqa: BLE001
            say("history not read: %s" % str(e)[:200])
        try:
            wj, w, n = kept("data/warroom-weights.json")
            say("weights: written %s (%.0f days ago), schema %s, %s learned, took %s s" % (w.isoformat(timespec="minutes"), (now - w).total_seconds() / 86400, wj.get("schema"),
                                                                                         wj.get("n_learned"), wj.get("duration_s")))
            for k, m in (wj.get("mechanisms") or {}).items():
                say("  %-16s %s weight %s | hits %s of %s (%s), lead %s months, false alarms %s, best horizon %s, span %s | %s" % (
                    k, m.get("status"), m.get("weight"), m.get("hits"), m.get("n_crises_covered"), m.get("hit_rate"), m.get("mean_lead_months"), m.get("false_alarm_rate"),
                    m.get("best_horizon_months"), m.get("span"), str(m.get("note"))[:110]))
            fixture("warroom-weights", wj)
        except Exception as e:  # noqa: BLE001
            say("weights not read: %s" % str(e)[:200])
        try:
            rp, w, n = kept("data/warroom-replay.json")
            mo = rp.get("months") or []
            say("replay: written %s, %d months %s to %s" % (w.isoformat(timespec="minutes"), len(mo), mo[0] if mo else None, mo[-1] if mo else None))
            fixture("warroom-replay", rp)
        except Exception as e:  # noqa: BLE001
            say("replay not read: %s" % str(e)[:200])

        # ── 2. the twelve feeds ─────────────────────────────────────────────────────────────────────────────────
        r.section("3. The twelve feeds")
        shapes = {}
        for f in FEEDS:
            try:
                d, w, n = kept("data/%s.json" % f)
                age = (now - w).total_seconds() / 3600
                asof = d.get("generated_at") or d.get("as_of") or d.get("timestamp") or d.get("updated_at") if isinstance(d, dict) else None
                say("%-20s written %s (%.1f h ago) %9d bytes | own stamp %s | top keys %s" % (f, w.isoformat(timespec="minutes"), age, n, str(asof)[:25],
                                                                                         (sorted(d)[:40] if isinstance(d, dict) else "list of %d" % len(d))))
                shapes[f] = cut(d)
            except Exception as e:  # noqa: BLE001
                say("%-20s not read: %s" % (f, str(e)[:160]))
        fixture("feed-shapes", shapes)
        for extra in ("plumbing-history", "indicator-bus", "crisis-canaries-history", "canary-grid-history", "vol-radar-history", "global-stress-history"):
            try:
                h = s3.head_object(Bucket=LIVE, Key="data/%s.json" % extra)
                say("also kept: data/%s.json %d bytes, written %s" % (extra, h["ContentLength"], h["LastModified"].isoformat(timespec="minutes")))
            except Exception:  # noqa: BLE001
                say("not kept: data/%s.json" % extra)
        try:
            ph, w, n = kept("data/plumbing-history.json")
            inds = ph.get("indicators")
            items = list(inds.items()) if isinstance(inds, dict) else [(x.get("id"), x) for x in (inds or [])]
            say("plumbing history: %d indicators; spans: %s" % (len(items), json.dumps({str(i): [len(x.get("history") or []), str(((x.get("history") or [[None]])[0] or [None])[0])[:10],
                                                                                              str(((x.get("history") or [[None]])[-1] or [None])[0])[:10]]
                                                                                    for i, x in items if isinstance(x, dict)})[:3500]))
        except Exception as e:  # noqa: BLE001
            say("plumbing history not read: %s" % str(e)[:160])

        # ── 3. the functions ────────────────────────────────────────────────────────────────────────────────────
        r.section("4. The functions: settings, schedules, runs")
        for fn in ["justhodl-canary-warroom", "justhodl-warroom-weights"] + sorted(set(WRITERS.values())):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                q = [{"Id": tag, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m, "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 86400, "Stat": stat}}
                     for m, stat, tag in (("Invocations", "Sum", "i"), ("Errors", "Sum", "e"), ("Duration", "Average", "a"))]
                got = cw.get_metric_data(MetricDataQueries=q, StartTime=now - timedelta(days=40), EndTime=now)["MetricDataResults"]
                g = {x["Id"]: (x.get("Values") or [], x.get("Timestamps") or []) for x in got}
                last = max(g["i"][1]).strftime("%m-%d") if g["i"][1] else "never in 40 days"
                rules = []
                try:
                    for nm in ev.list_rule_names_by_target(TargetArn=c["FunctionArn"]).get("RuleNames") or []:
                        d = ev.describe_rule(Name=nm)
                        rules.append("%s %s %s" % (nm, d.get("ScheduleExpression"), d.get("State")))
                except Exception as e:  # noqa: BLE001
                    rules.append("rules not read: %s" % str(e)[:60])
                say("%-34s %s MB / %s s, modified %s | 40 days: %d runs on %d days, %d errors, %.1f s a run, last day run %s | %s" % (
                    fn, c.get("MemorySize"), c.get("Timeout"), str(c.get("LastModified"))[:16], int(sum(g["i"][0])), len(g["i"][0]), int(sum(g["e"][0])),
                    (sum(g["a"][0]) / len(g["a"][0]) / 1000.0) if g["a"][0] else 0.0, last, "; ".join(rules) or "no rule (see the scheduler manifest)"))
            except Exception as e:  # noqa: BLE001
                say("%-34s not read: %s" % (fn, str(e)[:140]))
        for fn in ("justhodl-warroom-weights", "justhodl-canary-warroom"):
            try:
                streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=3).get("logStreams") or []
                say("%s: newest log streams %s" % (fn, json.dumps([[s.get("logStreamName", "")[:32], datetime.fromtimestamp((s.get("lastEventTimestamp") or 0) / 1000, timezone.utc).isoformat(timespec="minutes")] for s in streams])))
                for s in streams[:2]:
                    evs = logs.get_log_events(logGroupName="/aws/lambda/" + fn, logStreamName=s["logStreamName"], limit=60, startFromHead=False).get("events") or []
                    for e in evs[-40:]:
                        msg = e.get("message", "").strip()
                        if msg and not msg.startswith(("START", "END", "INIT_START")):
                            say("    %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M"), msg[:230]))
            except Exception as e:  # noqa: BLE001
                say("%s: logs not read: %s" % (fn, str(e)[:160]))

        # ── 4. how far back the warehouse goes ──────────────────────────────────────────────────────────────────
        r.section("5. How far back the warehouse takes the series a test needs (the site's own resolver, one request at a time)")
        import time
        for sid in PROBE:
            try:
                req = urllib.request.Request(RESOLVER + urllib.parse.quote(sid, safe=""), headers={"User-Agent": "JustHodl-Ops/6995"})
                with urllib.request.urlopen(req, timeout=40) as resp:
                    p = json.loads(resp.read().decode("utf-8", "replace"))
                obs = p.get("obs") if isinstance(p, dict) else None
                if obs:
                    first, last = obs[0], obs[-1]
                    say("%-22s %6d observations, %s to %s | keys %s" % (sid, len(obs), str(first[0] if isinstance(first, list) else first.get("date"))[:10],
                                                                    str(last[0] if isinstance(last, list) else last.get("date"))[:10], sorted(k for k in p if k != "obs")[:10]))
                else:
                    say("%-22s no observations | %s" % (sid, json.dumps(p)[:160]))
            except Exception as e:  # noqa: BLE001
                say("%-22s not answered: %s" % (sid, str(e)[:120]))
            time.sleep(0.3)
        for key in ("data/warm/polygon-eod/SPY.json.gz", "data/warm/polygon-full/daily/SPY.json.gz", "data/warm/tv-bars/universe/US__SPY.json.gz",
                    "data/warm/tv-bars/universe/SP__SPX.json.gz", "data/warm/tv-bars/universe/TVC__SPX.json.gz", "data/warm/fmp/historical-price-eod/SPY.json.gz"):
            try:
                o = s3.get_object(Bucket=LIVE, Key=key)
                raw = gzip.decompress(o["Body"].read())
                d = json.loads(raw)
                say("%s: %d bytes unzipped, written %s, shape %s" % (key, len(raw), o["LastModified"].isoformat(timespec="minutes"), json.dumps(cut(d))[:420]))
            except Exception as e:  # noqa: BLE001
                say("%s: %s" % (key, str(e)[:90]))
        for pre in ("data/warm/polygon-eod/", "data/warm/polygon-full/", "data/warm/tv-bars/universe/", "data/warm/official-yields/", "data/warm/"):
            try:
                got = s3.list_objects_v2(Bucket=LIVE, Prefix=pre, Delimiter="/", MaxKeys=60)
                say("%s -> folders %s | files %s" % (pre, [p["Prefix"][len(pre):] for p in got.get("CommonPrefixes") or []][:40], [o["Key"][len(pre):] for o in got.get("Contents") or []][:12]))
            except Exception as e:  # noqa: BLE001
                say("%s: %s" % (pre, str(e)[:90]))
        r.ok("audit read; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
