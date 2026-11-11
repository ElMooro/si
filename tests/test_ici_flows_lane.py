"""ICI browser lane boundaries: strict /flows table reading, desk-feed merge semantics, the native engine fed through
the browser transport, and the runner's redaction. No network, no AWS."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ("aws/shared", "aws/ops/checks", "aws/lambdas/justhodl-ici-flows/source", "tests")]
import ici_flows_lane as lane  # noqa: E402
import ici_store as store  # noqa: E402
from test_ici_native import Memory  # noqa: E402
from test_ici_research_candidate import release  # noqa: E402

NOW = datetime.now(timezone.utc)
ACQUIRED = NOW.replace(microsecond=0).isoformat().replace("+00:00", "Z")
LATEST = date(2026, 10, 28)
VALUES = [-22545, -19489, -3055, -2936, -11068, -7926, -3142, -36549]


def flows_page(latest=LATEST, release_day="November 4, 2026", unit="Millions of dollars", tables=1, labels=None, extra_column=None):
    labels = labels or [label for label, _ in lane.MF_LABELS]
    dates = [(latest - timedelta(days=7 * i)).strftime("%m/%d/%Y").lstrip("0").replace("/0", "/") for i in range(5)]
    header = ["", *dates] + ([extra_column] if extra_column else [])
    rows = []
    for label, base in zip(labels, VALUES):
        cells = [format(Decimal(base) * Decimal(k) / Decimal(10), ",.0f") for k in range(10, 5, -1)] + (["0"] if extra_column else [])
        rows.append("<tr><td>" + label + "</td>" + "".join("<td>" + c + "</td>" for c in cells) + "</tr>")
    table = "<table><thead><tr>" + "".join("<th>" + h + "</th>" for h in header) + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    return ("<html><head><script>akamai noise</script></head><body><p>Washington, DC; " + release_day + "</p><h2>" + unit + "</h2>"
            + table * tables + "</body></html>").encode()


def cell(base, k):
    """the printed cell for week k (10 = latest), as the page prints it: whole dollars (millions), rounded"""
    return float(format(Decimal(base) * Decimal(k) / Decimal(10), ",.0f").replace(",", ""))


def originals():
    out = {"flows": (flows_page(), {"url": lane.FLOWS_URL, "http_status": 200, "acquired_at": ACQUIRED, "response_headers": {}, "transport": "chrome"})}
    for kind in ("combined_flows", "mmf"):
        out[kind] = (release(kind), {"url": lane.URLS[kind], "http_status": 200, "acquired_at": ACQUIRED, "response_headers": {}, "transport": "chrome"})
    return out


class Tests(unittest.TestCase):
    def test_flows_table_reads_every_week_and_refuses_ambiguity(self):
        parsed = lane.parse_mutual_fund_flows(flows_page(), ACQUIRED)
        self.assertEqual(parsed["release_date"], "2026-11-04")
        self.assertEqual(parsed["observation_dates"], ["2026-09-30", "2026-10-07", "2026-10-14", "2026-10-21", "2026-10-28"])
        latest = parsed["weeks"]["2026-10-28"]
        self.assertEqual(latest, dict(zip([s for _, s in lane.MF_LABELS], [float(v) for v in VALUES])))
        self.assertEqual(parsed["weeks"]["2026-09-30"]["total"], cell(-36549, 6))
        for bad in (dict(tables=2), dict(unit="Billions of dollars"), dict(release_day="November 4, 2126"), dict(extra_column="$ Change*"),
                    dict(labels=["Equity", "Domestic", "World", "Hybrid", "Bond", "Taxable", "Municipal", "Total"])):
            with self.assertRaises(ValueError):
                lane.parse_mutual_fund_flows(flows_page(**bad), ACQUIRED)
        with self.assertRaises(ValueError):
            lane.parse_mutual_fund_flows(flows_page().replace(b"Washington, DC; November 4, 2026", b"no release line"), ACQUIRED)

    def test_desk_feed_extends_the_seeded_history_and_fresh_wins(self):
        seed = {"history": [{"week_ending": "2026-09-16", "equity": -28083.0, "total": -36716.0}, {"week_ending": "2026-09-23", "equity": -13484.0, "total": -19668.0},
                            {"week_ending": "2026-09-30", "equity": 1.0, "total": 1.0}],
                "sources": [lane.SEED_SOURCE, "chrome-lane:2026-10-28:flows 5w,combined 5w,mmf 3w"], "notes": ["old"]}
        o = originals()
        flows = lane.parse_mutual_fund_flows(o["flows"][0], ACQUIRED)
        combined = lane.research.parse_release(o["combined_flows"][0], "combined_flows", ACQUIRED)
        mmf = lane.research.parse_release(o["mmf"][0], "mmf", ACQUIRED)
        feed = lane.desk_feed(seed, flows, combined, mmf, ACQUIRED, "123")
        weeks = [r["week_ending"] for r in feed["history"]]
        self.assertEqual(weeks, ["2026-09-16", "2026-09-23", "2026-09-30", "2026-10-07", "2026-10-14", "2026-10-21", "2026-10-28"])
        self.assertEqual(feed["history"][2]["total"], cell(-36549, 6))  # the fresh release replaced the stale 2026-09-30 row
        self.assertEqual(feed["history"][0]["equity"], -28083.0)  # seeded weeks untouched
        self.assertEqual(feed["count"], 7);self.assertEqual(feed["latest_week"]["week_ending"], "2026-10-28");self.assertEqual(feed["latest_week"]["total"], -36549.0)
        self.assertEqual(feed["sources"], [lane.SEED_SOURCE, "chrome-lane:2026-11-04:flows 5w,combined 5w,mmf 3w"])
        self.assertEqual(len(feed["combined_history"]), 5);self.assertEqual(len(feed["money_market_history"]), 3)
        self.assertEqual(feed["latest_week"]["money_market"], feed["money_market_history"][-1]["total"]);self.assertEqual(feed["latest_week"]["money_market_week_ending"], "2026-09-23")
        self.assertEqual(feed["money_market_history"][-1]["total"], 175.0)
        # ETF net issuance = combined - mutual fund flows for the same week, null when a side is missing
        combined_weeks = {r["week_ending"] for r in feed["combined_history"]}
        self.assertTrue(all(r["week_ending"] in combined_weeks for r in feed["etf_net_issuance_history"]))
        self.assertNotIn("errors", feed)
        again = lane.desk_feed(feed, flows, combined, mmf, ACQUIRED, "124")
        self.assertEqual(again["history"], feed["history"]);self.assertEqual(len(again["sources"]), 2)

    def test_native_engine_publishes_through_the_browser_transport_and_the_desk_feed_follows(self):
        mem = Memory()
        mem.data[lane.DESK_KEY] = json.dumps({"history": [{"week_ending": "2026-09-23", "equity": -13484.0, "total": -19668.0}], "sources": [lane.SEED_SOURCE]}).encode()
        o = originals()
        summary = lane.run(mem, "bucket", "777", acquire=lambda kinds, log: o, log=lambda *_: None)
        self.assertTrue(summary["native"]["published"]);self.assertIn(store.CURRENT, mem.data)
        packet = json.loads(mem.data[store.CURRENT]);self.assertEqual(packet["contract"], "ici-research.v1")
        self.assertEqual(packet["mmf"]["total_b"], 175)
        feed = json.loads(mem.data[lane.DESK_KEY])
        self.assertEqual(feed["engine"], lane.ENGINE);self.assertEqual(feed["count"], 6);self.assertEqual(summary["desk"]["new_weeks"], ["2026-09-30", "2026-10-07", "2026-10-14", "2026-10-21", "2026-10-28"])
        self.assertEqual(store.acquire.__name__, "acquire")  # the native transport is restored after the run
        journals = [k for k in mem.data if k.startswith(store.PRIVATE + "requests/")]
        self.assertEqual(len(journals), 1);self.assertEqual(json.loads(mem.data[journals[0]])["status"], "complete")
        # a second run with the same request identity is a duplicate for the native engine; the desk feed still refreshes
        summary2 = lane.run(mem, "bucket", "777", acquire=lambda kinds, log: o, log=lambda *_: None)
        self.assertEqual(summary2["native"]["status"], "duplicate_request");self.assertEqual(summary2["desk"]["new_weeks"], [])

    def test_native_failure_is_reported_and_never_blocks_the_desk_feed(self):
        mem = Memory()
        o = originals()
        o["mmf"] = (b"<html>truncated", o["mmf"][1])
        with self.assertRaises(ValueError):
            lane.run(mem, "bucket", "778", acquire=lambda kinds, log: o, log=lambda *_: None)
        self.assertNotIn(lane.DESK_KEY, mem.data)  # the lane does not write a feed from an unparsed release
        o = originals()
        with patch.object(store, "publish", side_effect=RuntimeError("storage denied")):
            summary = lane.run(mem, "bucket", "779", acquire=lambda kinds, log: o, log=lambda *_: None)
        self.assertEqual(summary["native"], {"published": False, "error_type": "RuntimeError"});self.assertIn(lane.DESK_KEY, mem.data)
        self.assertEqual(store.acquire.__name__, "acquire")

    def test_rendered_document_fallback_and_refusal(self):
        class Response:
            def __init__(self, status, body):
                self.status, self._body, self.headers = status, body, {"content-type": "text/html"}
            def body(self):
                return self._body

        class Page:
            def __init__(self, status, body, rendered, url):
                self.url, self._r, self._rendered = url, Response(status, body), rendered
            def goto(self, url, **kw):
                self.url = url if self.url is None else self.url
                return self._r
            def wait_for_selector(self, *a, **k):
                pass
            def content(self):
                return self._rendered

        def fake_playwright(page):
            class Ctx:
                def new_page(self_):
                    return page
                def close(self_):
                    pass
            class Browser:
                def new_context(self_, **k):
                    return Ctx()
                def close(self_):
                    pass
            class Chromium:
                def launch(self_, **k):
                    return Browser()
            class P:
                chromium = Chromium()
                def __enter__(self_):
                    return self_
                def __exit__(self_, *a):
                    return False
            return P
        shell = b"<html><body><div id=app></div></body></html>"
        page = Page(200, shell, flows_page().decode(), None)
        out = lane.acquire_with_chrome(("flows",), log=lambda *_: None, playwright=fake_playwright(page))
        self.assertEqual(out["flows"][1]["transport"], "chrome-rendered-document")
        self.assertEqual(lane.parse_mutual_fund_flows(out["flows"][0], ACQUIRED)["release_date"], "2026-11-04")
        page = Page(403, b"Access Denied", "Access Denied", None)
        with self.assertRaises(ValueError):
            lane.acquire_with_chrome(("flows",), log=lambda *_: None, playwright=fake_playwright(page))

    def test_runner_prints_only_known_literals(self):
        import run_ici_flows_lane as runner
        known = runner.known_messages()
        self.assertIn("Official release unavailable through the browser", known)
        self.assertIn("Complete uniquely identified classification table required", known)
        self.assertNotIn("storage denied", known)


if __name__ == "__main__":
    unittest.main()
