"""justhodl-ark-holdings offline tests (v2.1.0): the baseline rule, the complete move lists and the handler end to end
on REAL snapshots - the stored snapshot of 2026-11-16 (holdings dated 2026-11-13) and the holdings the engine fetched
on 2026-11-17 (dated 2026-11-16), with the diff the engine published that morning (72 added to, 1 cut).  S3, the
source and the event bus are in-memory fakes.  No network, no AWS.

Run: python3 aws/lambdas/justhodl-ark-holdings/tests/run_tests.py
"""
from types import SimpleNamespace
import copy
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path[:0] = [SRC]
FX = json.load(open(os.path.join(HERE, "fixtures", "ark_snapshots.json"), encoding="utf-8"))
EVENTS = []


class FakeS3:
    def __init__(self):
        self.store, self.fail = {}, set()

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey %s" % Key)
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, Bucket, Key, Body, **_kw):
        if Key in self.fail:
            raise RuntimeError("write refused")
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode("utf-8")

    def doc(self, key):
        return json.loads(self.store[key]) if key in self.store else None


S3 = FakeS3()
sys.modules["boto3"] = SimpleNamespace(client=lambda *a, **k: S3)
sys.modules["_sentry_lite"] = SimpleNamespace(track_errors=lambda f: f)
sys.modules["system_events"] = SimpleNamespace(publish_many=lambda evs: EVENTS.extend(evs))

import ark_moves  # noqa: E402
import lambda_function as lf  # noqa: E402

BASE, CUR, PUB = FX["baseline"], FX["current_holdings"], FX["published"]


def reset(prev=None, base=None, holdings=None):
    S3.store.clear()
    S3.fail.clear()
    EVENTS.clear()
    if prev is not None:
        S3.store[lf.PREV_KEY] = json.dumps(prev).encode()
    if base is not None:
        S3.store[lf.BASE_KEY] = json.dumps(base).encode()
    lf.fetch_holdings_all_funds = lambda: copy.deepcopy(holdings if holdings is not None else CUR)


def run():
    return json.loads(lf.handler({}, None)["body"]), S3.doc(lf.OUTPUT_KEY)


def keys(rows):
    return sorted((r["fund"], r["ticker"]) for r in rows)


def next_day(holdings, day):
    """The same holdings one publication later: every dated line moves to `day` (ARKW's source date stays stale, as live)."""
    out = copy.deepcopy(holdings)
    newest = ark_moves.holdings_date(holdings)
    for rows in out.values():
        for p in rows:
            if p["date"] == newest:
                p["date"] = day
    return out


def test_dates_and_the_baseline_rule():
    assert ark_moves.holdings_date(CUR) == "2026-11-16" and ark_moves.snapshot_date(BASE) == "2026-11-13"
    assert {p["date"] for p in CUR["ARKW"]} == {"2026-10-06"}, "ARKW is stale at the source in the real snapshot"
    latest, older = {"holdings_date": "2026-11-13", "holdings_by_fund": {"A": [{"ticker": "X"}]}}, {"holdings_date": "2026-11-12", "holdings_by_fund": {"A": [{"ticker": "X"}]}}
    assert ark_moves.choose_baseline("2026-11-16", None, None) == (None, True)
    assert ark_moves.choose_baseline("2026-11-16", latest, None) == (latest, True), "a later holdings date: yesterday's snapshot becomes the baseline"
    assert ark_moves.choose_baseline("2026-11-13", latest, older) == (older, False), "the same holdings again: the retained baseline stands"
    assert ark_moves.choose_baseline("2026-11-13", latest, None) == (latest, False), "nothing retained yet: the previous run, as before"
    assert ark_moves.choose_baseline("2026-11-13", latest, latest) == (latest, False), "a retained baseline that is not older is not used"
    assert ark_moves.choose_baseline("", latest, older) == (latest, True), "no dates from the source: the engine behaves as it always did"
    assert ark_moves.choose_baseline("2026-11-12", latest, older) == (latest, False), "a source that goes backwards never promotes a baseline"
    assert ark_moves.snapshot_map(BASE)[("ARKK", "TSLA")]["company"] == "TESLA INC"


def test_the_real_morning_reproduces_what_the_engine_published_and_now_lists_every_move():
    reset(prev=BASE)                                        # 2026-11-17 06:00 UTC: the stored snapshot is the 11-16 run's
    body, out = run()
    d, pub = out["diff_vs_prev"], PUB["diff_vs_prev"]
    assert body["new_day"] is True and out["holdings_date"] == "2026-11-16" and out["prev_holdings_date"] == "2026-11-13" and out["diff_is_carried"] is False
    assert out["prev_snapshot_at"] == PUB["prev_snapshot_at"] and out["version"] == "2.1.0" and out["schema_version"] == "2.0"
    for k in ("n_new_positions", "n_position_adds", "n_position_trims", "n_closed_positions"):
        assert d[k] == pub[k], (k, d[k], pub[k])
    assert (d["n_position_adds"], d["n_position_trims"], d["n_new_positions"], d["n_closed_positions"]) == (72, 1, 0, 0)
    assert d["highlights"] == pub["highlights"], "the 15-row highlights are byte-for-byte what the engine published"
    assert len(d["all"]["position_adds"]) == 72 and d["all"]["position_adds"][:15] == d["highlights"]["biggest_adds"], "the complete list, the highlights first"
    assert len(d["all"]["position_trims"]) == 1 and d["all"]["new_positions"] == [] and d["all"]["closed_positions"] == [] and d["threshold_pct"] == 1.0
    assert all(r["pct_change"] >= 1.0 for r in d["all"]["position_adds"]) and all(r["pct_change"] <= -1.0 for r in d["all"]["position_trims"])
    assert ark_moves.reconciles(ark_moves.moves_block({"position_adds": d["all"]["position_adds"], "position_trims": d["all"]["position_trims"]}), d)
    # every position is published (the feed stopped at 40 a fund) and the snapshots are in place
    assert sum(len(v) for v in out["holdings_by_fund"].values()) == out["n_positions_total"] == PUB["n_positions_total"] == 212
    assert S3.doc(lf.BASE_KEY)["generated_at"] == BASE["generated_at"], "the snapshot the diff was measured against is retained"
    assert S3.doc(lf.PREV_KEY)["holdings_date"] == "2026-11-16" and S3.doc(lf.PREV_KEY)["holdings_by_fund"] == CUR
    assert [e[0] for e in EVENTS] and all(e[0] == "ark.position_change" for e in EVENTS), "the day's material moves are announced once"


def test_a_second_run_a_weekend_or_a_holiday_keeps_the_last_trading_days_moves():
    reset(prev=BASE)
    _, first = run()
    announced = list(EVENTS)
    for _ in range(3):                                      # 06:00 the next mornings, the source still on the same holdings
        body, again = run()
        assert body["new_day"] is False and again["diff_is_carried"] is True
        assert again["diff_vs_prev"]["all"] == first["diff_vs_prev"]["all"] and again["diff_vs_prev"]["n_position_adds"] == 72
        assert again["prev_holdings_date"] == "2026-11-13" and again["prev_snapshot_at"] == BASE["generated_at"]
        assert S3.doc(lf.BASE_KEY)["generated_at"] == BASE["generated_at"]
    assert EVENTS == announced, "a carried diff is not announced again"


def test_the_night_of_the_deploy_both_ways():
    stored = {"generated_at": PUB["generated_at"], "holdings_by_fund": CUR}        # what sits in S3 tonight: no holdings_date field, no retained baseline
    reset(prev=stored, holdings=CUR)                         # the source has not published a new day yet, nothing retained
    body, out = run()
    assert body["new_day"] is False and out["diff_vs_prev"]["n_position_adds"] == 0, "without a retained baseline the engine behaves as it used to"
    reset(prev=stored, base=BASE, holdings=CUR)              # ... which is why the first-light op seeds the baseline first
    body, out = run()
    assert body["new_day"] is False and out["diff_is_carried"] is True and out["diff_vs_prev"]["n_position_adds"] == 72 and out["prev_holdings_date"] == "2026-11-13"
    assert EVENTS == []
    tomorrow = next_day(CUR, "2026-11-17")
    tomorrow["ARKK"][0]["shares"] = int(tomorrow["ARKK"][0]["shares"] * 1.05)
    gone = tomorrow["ARKQ"].pop()
    tomorrow["ARKX"].append({"fund": "ARKX", "ticker": "NEWCO", "company": "NEW COMPANY INC", "shares": 1000, "market_value": 7_500_000.0, "weight": 0.5, "date": "2026-11-17"})
    reset(prev=stored, base=BASE, holdings=tomorrow)         # the source HAS published the next day
    body, out = run()
    d = out["diff_vs_prev"]
    assert body["new_day"] is True and out["holdings_date"] == "2026-11-17" and out["prev_holdings_date"] == "2026-11-16" and out["diff_is_carried"] is False
    assert keys(d["all"]["position_adds"]) == [("ARKK", tomorrow["ARKK"][0]["ticker"])] and d["all"]["position_adds"][0]["pct_change"] == 5.0
    assert keys(d["all"]["new_positions"]) == [("ARKX", "NEWCO")]
    assert d["all"]["closed_positions"] == [{"fund": "ARKQ", "ticker": gone["ticker"], "company": gone["company"], "prev_shares": gone["shares"], "prev_value": gone["market_value"]}], "a closed line names its company"
    assert S3.doc(lf.BASE_KEY)["generated_at"] == PUB["generated_at"], "yesterday's snapshot became the baseline"
    assert [e[1]["change_type"] for e in EVENTS] == ["new_position"], "the $7.5M new position is announced (the 5% add is under the 10% bar)"


def test_a_failed_baseline_write_leaves_the_newest_snapshot_alone_so_the_day_is_tried_again():
    reset(prev=BASE)
    S3.fail.add(lf.BASE_KEY)
    body, out = run()
    assert body["new_day"] is True and body["baseline_kept"] is False and out["diff_vs_prev"]["n_position_adds"] == 72
    assert S3.doc(lf.PREV_KEY)["generated_at"] == BASE["generated_at"], "the newest snapshot was not replaced"
    S3.fail.clear()
    body, out = run()
    assert body["new_day"] is True and body["baseline_kept"] is True and out["diff_vs_prev"]["n_position_adds"] == 72
    assert S3.doc(lf.PREV_KEY)["holdings_date"] == "2026-11-16" and S3.doc(lf.BASE_KEY)["generated_at"] == BASE["generated_at"]


def test_a_first_ever_run_and_a_dead_source_change_nothing():
    reset()
    body, out = run()
    assert body["new_day"] is True and out["diff_vs_prev"]["n_position_adds"] == 0 and out["prev_snapshot_at"] is None and out["diff_is_carried"] is False
    assert lf.BASE_KEY not in S3.store and S3.doc(lf.PREV_KEY)["holdings_date"] == "2026-11-16"
    reset(prev=BASE, holdings={})
    before = dict(S3.store)
    assert json.loads(lf.handler({}, None)["body"]) == {"ok": False, "reason": "no_data"} and S3.store == before, "no fetch, no write"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print("ok  %s" % t.__name__)
    print("%d tests passed" % len(tests))
