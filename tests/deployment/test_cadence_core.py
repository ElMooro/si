"""cadence_core: every schedule expression is classified exactly (at most once a day or not) and rewritten safely.

Khalid, 2026-10-20: every engine runs at most once every 24 hours.  Pure functions, no AWS.
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
import cadence_core as cc  # noqa: E402

OK = [
    "rate(1 day)", "rate(24 hours)", "rate(1440 minutes)", "rate(2 days)", "rate(7 days)",
    "cron(0 12 * * ? *)", "cron(30 6 ? * * *)", "cron(0 8 ? * MON *)", "cron(0 21 ? * MON-FRI *)",
    "cron(0 8 1 * ? *)", "cron(0 8 L * ? *)", "cron(0 8 15W * ? *)", "cron(15 10 ? * 6L *)", "cron(0 12 ? * 2#1 *)",
    "cron(0 12 ? JAN-MAR MON-FRI *)", "cron(5 4 * * ? 2027)", "at(2026-12-01T00:00:00)",
    "cron(0 0 ? * SUN,SAT *)",
]
VIOLATING = [
    "rate(1 minute)", "rate(5 minutes)", "rate(15 minutes)", "rate(1 hour)", "rate(6 hours)", "rate(23 hours)",
    "rate(1439 minutes)",
    "cron(0/5 * * * ? *)", "cron(*/15 * * * ? *)", "cron(0 * * * ? *)", "cron(0 */6 * * ? *)", "cron(0 0/2 * * ? *)",
    "cron(0 12,18 * * ? *)", "cron(0,30 12 * * ? *)", "cron(30 13-21 ? * MON-FRI *)", "cron(0 9-17 ? * 2-6 *)",
    "cron(5,35 13-21 ? * MON-FRI *)", "cron(0 0,6,12,18 * * ? *)", "cron(20 */3 * * ? *)", "cron(7 * * * ? *)",
]
ONE_PER_DAY_BUT_NOT_EVERY_DAY = {            # fires less than once a day on average, never more than once on a day
    "cron(0 8 ? * MON *)": 1 / 7, "cron(0 21 ? * MON-FRI *)": 5 / 7, "cron(0 8 1 * ? *)": 1 / 30.4,
}


def test_compliant_expressions_fire_at_most_once_on_any_day():
    for e in OK:
        assert not cc.violates(e), e
        assert cc.per_active_day(e) <= 1, e


def test_violating_expressions_are_caught():
    for e in VIOLATING:
        assert cc.violates(e), e


def test_average_firings_per_day_are_sensible():
    for e, want in ONE_PER_DAY_BUT_NOT_EVERY_DAY.items():
        assert abs(cc.fires_per_day(e) - want) < 0.02, (e, cc.fires_per_day(e))
    assert cc.fires_per_day("rate(5 minutes)") == 288.0
    assert cc.fires_per_day("rate(1 hour)") == 24.0
    assert cc.fires_per_day("cron(0/5 * * * ? *)") == 288.0
    assert cc.fires_per_day("at(2026-12-01T00:00:00)") == 0.0


def test_garbage_is_refused_loudly():
    for bad in ("", "   ", None, "rate(0 minutes)", "rate(5 fortnights)", "cron(0 12 * * ?)", "cron(0 25 * * ? *)",
                "cron(61 12 * * ? *)", "cron(0 12 * * ? *) extra", "every day", "cron(0/0 * * * ? *)"):
        try:
            cc.parse(bad)
        except cc.ExpressionError:
            continue
        raise AssertionError("accepted %r" % (bad,))


def test_to_daily_always_produces_a_compliant_expression_and_is_stable():
    for e in VIOLATING:
        out = cc.to_daily(e, "key:" + e, depth=0)
        assert not cc.violates(out), (e, out)
        assert out == cc.to_daily(e, "key:" + e, depth=0), "same key, same answer"
        cc.parse(out)


def test_compliant_expressions_are_returned_unchanged():
    for e in OK:
        assert cc.to_daily(e, "k") == e.strip()


def test_windows_collapse_to_their_last_hour_and_keep_their_day_filters():
    assert cc.to_daily("cron(0 13-21 ? * MON-FRI *)", "k") == "cron(0 21 ? * MON-FRI *)"
    assert cc.to_daily("cron(30 13-21 ? * MON-FRI *)", "k") == "cron(30 21 ? * MON-FRI *)"
    assert cc.to_daily("cron(5,35 13-21 ? * MON-FRI *)", "k") == "cron(5 21 ? * MON-FRI *)"
    assert cc.to_daily("cron(0 0,6,12,18 * * ? *)", "k") == "cron(0 18 * * ? *)"
    assert cc.to_daily("cron(0,30 12 * * ? *)", "k") == "cron(0 12 * * ? *)"


def test_full_day_steps_take_an_hour_from_the_dependency_band_with_a_stable_minute():
    for depth, band in ((0, cc.BANDS[0][1]), (1, cc.BANDS[1][1]), (3, cc.BANDS[2][1]), (12, cc.BANDS[3][1])):
        for e in ("rate(5 minutes)", "cron(0/5 * * * ? *)", "cron(0 */6 * * ? *)", "rate(1 hour)"):
            out = cc.to_daily(e, "engine-" + e, depth)
            m = re.fullmatch(r"cron\((\d+) (\d+) \* \* \? \*\)", out)
            assert m, out
            assert int(m.group(2)) in band, (depth, out)
    minutes = {cc.to_daily("rate(5 minutes)", "engine-%d" % i).split()[0] for i in range(200)}
    assert len(minutes) > 30, "engines must not all start in the same minute"


def test_describe_is_human_readable():
    assert cc.describe("rate(5 minutes)").startswith("288/active day")
    assert cc.describe("cron(0 12 * * ? *)") == "1/day"
    assert cc.describe("cron(0 8 ? * MON *)").startswith("~0.14")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("cadence_core tests passed:", len(tests))
