"""Local harness -- justhodl-earnings (mandatory push gate)."""
import json
import sys
import types
from pathlib import Path

STORE, PUTS = {}, []


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": types.SimpleNamespace(
            read=lambda: json.dumps(STORE[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        STORE[Key] = json.loads(Body)
        PUTS.append(Key)


boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3_stub

sys.path.insert(0, str(Path(__file__).resolve().parents[2]
                       / "lambdas" / "justhodl-earnings"
                       / "source"))
import lambda_function as eng  # noqa: E402

import os  # noqa: E402
os.environ["FMP_KEY"] = "test"
os.environ.pop("ANTHROPIC_KEY", None)

PAD = ("alpha " * 3500).strip()
TR_TEXT = (PAD + " Our record backlog gives us multi-year "
           "visibility. We are raising guidance on strong "
           "demand. record backlog again. One headwind noted.")
LOW_TEXT = ("alpha " * 3500).strip() + " Fine quarter."
SHORT_TEXT = "record backlog raising guidance strong demand."

CAL = [
    {"symbol": "AAA", "date": "2026-08-10", "epsActual": 1.2,
     "epsEstimated": 1.0, "revenueActual": 110.0,
     "revenueEstimated": 100.0},
    {"symbol": "AAA", "date": "2026-08-01", "epsActual": 0.5,
     "epsEstimated": 1.0, "revenueActual": None,
     "revenueEstimated": None},
    {"symbol": "BBB", "date": "2026-08-09", "epsActual": 0.9,
     "epsEstimated": 1.0, "revenueActual": None,
     "revenueEstimated": None},
    {"symbol": "CCC", "date": "2026-08-08", "epsActual": 2.0,
     "epsEstimated": 0.001, "revenueActual": None,
     "revenueEstimated": None},
    {"symbol": "DDD", "date": "2026-08-08", "epsActual": -1.0,
     "epsEstimated": 1.0, "revenueActual": 80.0,
     "revenueEstimated": 100.0},
    {"symbol": "RY.TO", "date": "2026-08-08",
     "epsActual": 1.0, "epsEstimated": 0.5,
     "revenueActual": None, "revenueEstimated": None},
    {"symbol": "EEE", "date": "2026-08-07", "epsActual": 1.0,
     "epsEstimated": None, "revenueActual": None,
     "revenueEstimated": None},
]
TRL = [{"symbol": "AAA", "period": "Q2", "fiscalYear": 2026,
        "date": "2026-08-10"},
       {"symbol": "BBB", "period": "Q2", "fiscalYear": 2026,
        "date": "2026-08-09"},
       {"symbol": "DDD", "period": "Q2", "fiscalYear": 2026,
        "date": "2026-08-08"}]


def fake_http(url):
    if "earnings-calendar" in url:
        return CAL
    if "transcript-latest" in url:
        return TRL
    if "transcript-dates" in url:
        return [{"quarter": 2, "fiscalYear": 2026,
                 "date": "2026-08-08"}]
    if "earning-call-transcript?" in url:
        sym = url.split("symbol=")[1].split("&")[0]
        txt = {"AAA": TR_TEXT, "BBB": LOW_TEXT,
               "DDD": SHORT_TEXT}.get(sym, LOW_TEXT)
        return [{"symbol": sym, "period": "Q2",
                 "year": 2026, "date": "2026-08-08",
                 "content": txt}]
    raise AssertionError("unexpected url " + url)


eng.http_json = fake_http
FAILS = []


def chk(name, ok):
    print("  [%s] %s " % ("PASS" if ok else "FAIL", name))
    if not ok:
        FAILS.append(name)


def expected_score(text):
    words = max(1, len(text.split()))
    low = text.lower()
    pw = sum(w * low.count(p) for p, w in eng.POS.items()
             if low.count(p))
    nw = sum(w * low.count(p) for p, w in eng.NEG.items()
             if low.count(p))
    return eng.clip(round(50 + 4 * (pw - nw)
                          * (10000.0 / words), 1), 0, 100), \
        words


def main():
    d = eng.build({})
    rows = {r["t"]: r for r in d["beat_league"]}
    chk("surprise math AAA (eps +20, rev +10 -> score "
        "0.6*20+0.4*50=32.0)",
        rows["AAA"]["eps_surprise_pct"] == 20.0
        and rows["AAA"]["rev_surprise_pct"] == 10.0
        and rows["AAA"]["beat_score"] == 32.0)
    chk("dedupe kept latest AAA date",
        rows["AAA"]["date"] == "2026-08-10")
    chk("tiny denominator CCC excluded, null-est EEE "
        "excluded, foreign RY.TO excluded",
        "CCC" not in rows and "EEE" not in rows
        and "RY.TO" not in rows)
    chk("eps-only stays 0.6-weighted (BBB -10 -> -6.0)",
        rows["BBB"]["beat_score"] == -6.0)
    lg = d["beat_league"]
    chk("rank monotonic desc",
        all(lg[i]["beat_score"] >= lg[i + 1]["beat_score"]
            for i in range(len(lg) - 1))
        and lg[0]["rank"] == 1)
    chk("stats beat rate (1 beat of 3 -> 33.3)",
        d["stats"]["n_reporters"] == 3
        and d["stats"]["beat_rate_pct"] == 33.3)
    picks = {p["t"]: p for p in
             d["growth_calls"]["picks"]}
    exp, words = expected_score(eng.PAD_TEST
                                if hasattr(eng, "PAD_TEST")
                                else TR_TEXT)
    chk("AAA picked; growth_score == independent recompute",
        "AAA" in picks
        and picks["AAA"]["growth_score"] == exp
        and picks["AAA"]["words"] == words)
    chk("evidence carries the phrase, demand_flag off "
        "(no demand-flag phrase planted)",
        any("record backlog" in e.lower()
            for e in picks["AAA"]["evidence"])
        and picks["AAA"]["demand_flag"] is False)
    chk("LOW transcript rejected (<3 phrases), SHORT "
        "rejected (<3000 words)",
        "BBB" not in picks and "DDD" not in picks)
    n = d["stats"]["n_reporters"]
    pct = (1 - (rows["AAA"]["rank"] - 1)
           / max(1, n - 1)) * 100
    chk("pick_score formula",
        picks["AAA"]["pick_score"]
        == round(0.55 * exp + 0.45 * pct, 1))
    chk("ai_mode rules_only tagged",
        "rules_only" in picks["AAA"]["ai_mode"]
        and picks["AAA"]["ai_line"] is None)
    chk("history appended",
        len(STORE[eng.HIST_KEY]["runs"]) == 1
        and STORE[eng.HIST_KEY]["runs"][0]["picks"][0]["t"]
        == "AAA")
    chk("write discipline: only hist+out, out last",
        set(PUTS) == {eng.HIST_KEY, eng.OUT_KEY}
        and PUTS[-1] == eng.OUT_KEY)
    d2 = eng.build({})
    chk("history caps at 260",
        len(STORE[eng.HIST_KEY]["runs"]) == 2
        and d2["status"] == "LIVE")
    if FAILS:
        print("HARNESS FAILED:", FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4881)")


if __name__ == "__main__":
    main()
