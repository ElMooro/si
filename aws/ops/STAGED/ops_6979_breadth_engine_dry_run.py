"""ops 6979 - dry run of the breadth engine 1.1 on the runner. Nothing is written, no Lambda is invoked, no alert is sent.

Khalid 2026-11-26: "breadth engine need improvement and better iux". The audit found three things:
  * the engine asked the provider for every session although the house bank keeps each one
    (data/warm/polygon-full/grouped/YYYY/YYYY-MM-DD.json.gz);
  * its "advance / decline" ratio is open-to-close (an issue rising above its own open), not the day-over-day measure
    Zweig defined, and the document did not say so;
  * it kept only the ratio: no counts, no up-volume, nothing on the day-over-day basis.

Version 1.1 reads the bank first, keeps 130 sessions with each session's counts on BOTH bases, adds a day-over-day
reading, a McClellan oscillator and a preliminary reading of today's forming session - all beside the state machine,
which still reads the last 60 sessions of the open-to-close ratio with the same thresholds.

This op runs the engine's own handler from the repository with {"dry_run": true} against the live bucket, with the S3
client's writes captured, the state store guarded and the alert captured, and checks that
  1. nothing was written;
  2. every ratio already kept is unchanged and the bank's counts reproduce it;
  3. the state is what the same rule gives on the kept ratios;
  4. the new readings are there and sane.
It also measures how the two bases differ over the 130 sessions, for Khalid's decision. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6979_breadth_engine_dry_run"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
SRC = ROOT / "aws" / "lambdas" / "justhodl-breadth-thrust" / "source"


class Capture:
    """Stands in for the S3 client: reads pass through, writes are kept here and never sent."""
    def __init__(self, real):
        self.real, self.puts, self.gets = real, {}, 0

    def put_object(self, **kw):
        self.puts[kw.get("Key")] = True
        return {}

    def delete_object(self, **kw):
        self.puts["DELETE " + str(kw.get("Key"))] = True
        return {}

    def get_object(self, **kw):
        self.gets += 1
        return self.real.get_object(**kw)

    def __getattr__(self, name):
        return getattr(self.real, name)


class StateGuard:
    """Stands in for the SSM client: the state is read, never written."""
    def __init__(self, real):
        self.real, self.puts = real, []

    def put_parameter(self, **kw):
        self.puts.append(kw.get("Name"))
        return {}

    def __getattr__(self, name):
        return getattr(self.real, name)


def corr(a, b):
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    sa = sum((x - ma) ** 2 for x in a) ** 0.5
    sb = sum((y - mb) ** 2 for y in b) ** 0.5
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (sa * sb) if sa and sb else None


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120))
    now = datetime.now(timezone.utc)
    fails = []

    def get(key):
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())

    with report(NAME) as r:
        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)

        r.heading("6979 - breadth engine 1.1, dry run on the runner (nothing written), %s" % now.isoformat(timespec="minutes"))
        live = get("data/breadth-thrust.json")
        kept = sorted(get("data/breadth-history.json")["history"], key=lambda x: x["date"])
        r.log("live document: version %s, state %s, written %s | cache %d sessions %s to %s" % (
            live.get("version"), live.get("state"), live.get("as_of"), len(kept), kept[0]["date"], kept[-1]["date"]))

        sys.path.insert(0, str(SRC))
        spec = importlib.util.spec_from_file_location("eng_breadth", str(SRC / "lambda_function.py"))
        L = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(L)
        import breadth_math as BM
        cap, guard, alerts = Capture(L.s3), StateGuard(L.ssm), []
        L.s3, L.ssm = cap, guard
        L.send_telegram = lambda msg: alerts.append(msg)

        r.section("1. The run")
        t0 = time.time()
        out = L.lambda_handler({"dry_run": True}, None)
        took = time.time() - t0
        body = json.loads(out["body"])
        rep, hist = body["report"], body["history"]
        rows = hist["rows"]
        bank = rep["current_readings"]["bank"]
        r.log("took %.1f s on the runner | %d objects read | bank: %s" % (took, cap.gets, json.dumps(bank)))
        r.log("cache would hold %d sessions %s to %s (%d before); %d from the bank, %d fetched from the provider" % (
            hist["n"], hist["first"], hist["last"], hist["n_before"], bank["sessions_added"], rep["current_readings"]["newly_fetched_this_run"]))
        ck(out.get("dry_run") is True and not cap.puts and not guard.puts and not alerts, "nothing was written: no object, no state, no alert (%s %s %d)" % (list(cap.puts), guard.puts, len(alerts)))
        ck(bank["error"] is None and bank["left_for_next_run"] == 0 and bank["files_read"] <= 140, "the bank was read in one run (%d files, none left over)" % bank["files_read"])
        ck(took < 150, "well inside the engine's 300-second limit on the runner (%.1f s)" % took)

        r.section("2. What was kept stays as it was")
        by = {x["date"]: x for x in rows}
        same = [d for d in (x["date"] for x in kept) if d in by and by[d]["ratio"] == next(k["ratio"] for k in kept if k["date"] == d)]
        ck(len(same) == len(kept), "every one of the %d kept ratios is unchanged (%d equal)" % (len(kept), len(same)))
        ck(bank["max_ratio_gap"] is None or bank["max_ratio_gap"] <= 0.002, "the bank's counts reproduce the kept ratios (largest gap %s; none means no kept session needed its counts)" % bank["max_ratio_gap"])
        dates = [x["date"] for x in rows]
        ck(dates == sorted(set(dates)) and 61 <= len(rows) <= 130, "sessions are in order, none twice (%d)" % len(rows))
        ck(hist["with_counts"] == len(rows) and hist["with_day_over_day"] >= len(rows) - 1, "each session has its counts (%d) and its day-over-day reading (%d)" % (hist["with_counts"], hist["with_day_over_day"]))
        bad = [x["date"] for x in rows if x.get("source") == "bank" and x["adv"] + x["dec"] != x["n_stocks"]]
        ck(not bad, "for a session read from the bank, rising + falling equals the issues counted (%s)" % (bad[:3] or "all"))
        aside = max((x.get("set_aside_dod") or 0) / float(x["n_stocks"]) for x in rows)
        fresh = max(((x.get("new_dod") or 0) / float(x["n_stocks"]), x["date"]) for x in rows)
        ck(aside < 0.01, "likely splits set aside are under 1%% of the issues counted on every session (most %.2f%%)" % (100 * aside))
        ck(fresh[0] < 0.05, "on every session nearly every issue had a prior close to be compared with (most without one: %.2f%% on %s)" % (100 * fresh[0], fresh[1]))
        inside = [x["date"] for x in rows if kept[0]["date"] <= x["date"] <= kept[-1]["date"]]
        ck(inside == [k["date"] for k in kept], "no session is slipped in between the kept ones (%d in that range, %d kept)" % (len(inside), len(kept)))
        ck(all(BM.is_weekday(x["date"]) and x["n_stocks"] >= BM.MIN_SESSION_ISSUES for x in rows), "every kept session is a weekday with a full count of issues (fewest %d)" % min(x["n_stocks"] for x in rows))
        r.log("the bank, not taken on trust: not sessions %s | thin files %s | remembered %s" % (json.dumps(bank.get("not_sessions")), json.dumps(bank.get("thin")), json.dumps(body["history"].get("not_sessions"))))
        r.log("sessions marked thin (left out of the day-over-day reading and McClellan): %s" % json.dumps([[x["date"], x["n_stocks"]] for x in rows if x.get("thin")]))

        r.section("3. The state machine")
        z = BM.zweig_reading([x["ratio"] for x in rows[-60:]], L.ZWEIG_LOW, L.ZWEIG_HIGH, L.ZWEIG_WINDOW)
        cr = rep["current_readings"]
        r.log("state %s (live document: %s) | 10-session average %s, window low %s, high %s | sessions read %s" % (
            rep["state"], live.get("state"), cr["zweig_10d_ema"], cr["zweig_window_min"], cr["zweig_window_max"], cr["zweig_sessions_read"]))
        newer_n = sum(1 for x in rows if x["date"] > kept[-1]["date"])
        ck([x["date"] for x in rows[-60:]] == ([k["date"] for k in kept] + [x["date"] for x in rows if x["date"] > kept[-1]["date"]])[-60:], "the 60 sessions the state reads are the kept ones and the %d newer" % newer_n)
        ck(cr["zweig_10d_ema"] == z["current_ema"] and cr["zweig_window_min"] == z["min_in_window"] and cr["zweig_sessions_read"] == 60,
           "the average is the engine's rule on the last 60 kept ratios (%s)" % z["current_ema"])
        ck(rep["state"] in (z["state"], "COOLDOWN") and (L.ZWEIG_LOW, L.ZWEIG_HIGH, L.ZWEIG_WINDOW) == (0.40, 0.615, 10), "the state follows that reading with the thresholds untouched (%s)" % z["state"])
        old = BM.zweig_reading([x["ratio"] for x in kept], L.ZWEIG_LOW, L.ZWEIG_HIGH, L.ZWEIG_WINDOW)
        ck(old["current_ema"] == live["current_readings"]["zweig_10d_ema"], "the shared arithmetic reproduces the live document's average on the live cache (%s)" % old["current_ema"])
        newer = [x for x in rows if x["date"] > kept[-1]["date"]]
        r.log("sessions newer than the live cache: %s" % json.dumps([[x["date"], x["ratio"], x.get("source")] for x in newer]))
        fe, lf = rep["forward_expectations"], live["forward_expectations"]
        ck(all(fe[k]["n"] == lf[k]["n"] and abs((fe[k]["return_pct"] or 0) - (lf[k]["return_pct"] or 0)) < 0.06 for k in ("1m", "3m", "6m", "12m")), "the record after past firings is the live document's (12 months: %s, n %s)" % (fe["12m"]["return_pct"], fe["12m"]["n"]))

        r.section("4. The new readings")
        r.log("the last settled session: " + json.dumps(rep["breadth_today"]))
        dd, mc, pre = rep["day_over_day"], rep["mcclellan"], rep["preliminary"]
        r.log("day over day: " + json.dumps({k: v for k, v in dd.items() if k not in ("basis", "note")}))
        r.log("McClellan: " + json.dumps({k: v for k, v in mc.items() if k not in ("basis", "series")}) + " | last five: " + json.dumps((mc.get("series") or [])[-5:]))
        r.log("preliminary (forming-bar file): " + json.dumps(pre))
        ck(dd.get("available") and dd.get("would_read") in ("NULL", "ARMED", "FIRED") and 0 < dd["ema_10d"] < 1, "the day-over-day reading is there (average %s, would read %s)" % (dd.get("ema_10d"), dd.get("would_read")))
        ck(mc.get("available") and mc.get("seasoned") and abs(mc["oscillator"]) < 400 and len(mc["series"]) == 60, "the McClellan oscillator is there and seasoned (%s from %s, %s sessions)" % (mc.get("oscillator"), mc.get("previous"), mc.get("n_sessions")))
        ck(pre is not None and pre.get("status") in ("settled", "ahead_of_settled", "older_than_settled"), "the forming-bar file was read (%s, session %s)" % ((pre or {}).get("status"), (pre or {}).get("session")))
        if pre and pre.get("status") == "settled":
            r.log("  the forming-bar file against the settled session %s: ratio %s against %s, gap %s, %s issues against %s" % (
                pre["session"], pre["ratio"], pre["settled_ratio"], pre["gap_to_settled"], pre["n_stocks"], by[pre["session"]]["n_stocks"]))
            r.log("  MEASURE  how far the forming-bar reading is from the settled one: %s (a measurement, not a pass / fail)" % pre["gap_to_settled"])
        ck("OPEN-TO-CLOSE" in rep["breadth_basis"] and rep["version"] == "1.1", "the document says which basis the state follows")

        r.section("5. The two bases, side by side (for the decision)")
        both = [x for x in rows if x.get("ratio_dod") is not None]
        a, b = [x["ratio"] for x in both], [x["ratio_dod"] for x in both]
        gap = [abs(p - q) for p, q in zip(a, b)]
        split = sum(1 for p, q in zip(a, b) if (p > 0.5) != (q > 0.5))
        r.log("%d sessions with both: correlation %.3f | mean gap %.3f, largest %.3f | on %d sessions one says more rose and the other more fell" % (
            len(both), corr(a, b), sum(gap) / len(gap), max(gap), split))
        r.log("means: open-to-close %.4f, day-over-day %.4f | lowest 10-session average: %.4f against %.4f | highest: %.4f against %.4f" % (
            sum(a) / len(a), sum(b) / len(b), min(BM.ema(a, 10)[10:]), min(BM.ema(b, 10)[10:]), max(BM.ema(a, 10)[10:]), max(BM.ema(b, 10)[10:])))
        fired = {"open-to-close": [], "day-over-day": []}
        for name, series in (("open-to-close", a), ("day-over-day", b)):
            for i in range(11, len(series) + 1):
                if BM.zweig_reading(series[:i], L.ZWEIG_LOW, L.ZWEIG_HIGH, L.ZWEIG_WINDOW)["state"] == "FIRED":
                    fired[name].append(both[i - 1]["date"])
        r.log("sessions on which each basis would have read FIRED in this window: " + json.dumps(fired))
        r.log("the five widest gaps: " + json.dumps(sorted(([x["date"], x["ratio"], x["ratio_dod"]] for x in both), key=lambda t: -abs(t[1] - t[2]))[:5]))

        r.section("the dry-run documents (for the page's rehearsal)")
        blob = json.dumps({"report": rep, "history": {"history": rows, "as_of": "dry run " + now.isoformat(timespec="minutes")}}, separators=(",", ":"), default=str)
        r.lines.append("<!-- FIXTURE breadth-dry-run %d bytes" % len(blob))
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FIXTURE-END -->")
        r.kv(failed_checks=len(fails), sessions=len(rows), state=rep["state"], seconds=round(took, 1))
        if fails:
            r.fail("; ".join(fails)[:600])
            sys.exit(1)
        r.ok("breadth engine 1.1 reads the bank, keeps both bases and leaves the state machine as it was")


if __name__ == "__main__":
    main()
