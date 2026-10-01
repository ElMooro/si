"""Offline synthetic renderer tests; never import the network-enabled engine.

Run: python aws/lambdas/justhodl-financial-secretary/tests/run_tests.py
"""
import ast
import copy
import hashlib
import io
import json
import math
from datetime import datetime, timezone, timedelta
from html import escape
from html.parser import HTMLParser
from pathlib import Path
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "source/lambda_function.py"
TREE = ast.parse(SOURCE.read_text())
HELPERS = {"_display_text", "_display_score", "_crypto_risk_html"}
FUNCTIONS = HELPERS | {"build_email_html", "build_deltas", "fetch_tier2", "fetch_yesterday_snapshot"}
SCOPE = dict(math=math, escape=escape, json=json, datetime=datetime,
             timezone=timezone, timedelta=timedelta, BUCKET="synthetic-only")
# Extract only named functions: no boto3 clients, shim, HTTP or module execution.
exec(compile(ast.Module(body=[n for n in TREE.body if isinstance(n, ast.FunctionDef)
                            and n.name in FUNCTIONS], type_ignores=[]), str(SOURCE), "exec"), SCOPE)


def recommendation(index, action="BUY"):
    return dict(ticker=f"SYN{index:02}", name=f"Synthetic {index}", action=action,
                price=100 + index, upside_pct=12, downside_pct=7,
                risk_reward=1.71, reasons=["Synthetic reason"], score=100-index)


def scan():
    recs = [recommendation(i) for i in range(1, 18)]
    # SYN01 leaves the first 10, remains BUY, and is still in the first 15 BUYs.
    current = recs[1:11] + recs[:1] + recs[11:]
    liquidity = dict(net_liquidity=5000, regime="STABLE")
    risk = dict(composite=40, level="MODERATE", vix=17)
    yesterday = dict(timestamp="2040-01-01 18:00:00 ET", recommendations=recs,
                     liquidity=liquidity, risk=risk)
    return dict(timestamp="2040-01-02 08:00:00 ET", liquidity=liquidity, risk=risk,
                recommendations=current, ai_briefing="Synthetic briefing\nSecond line",
                deltas=SCOPE["build_deltas"](liquidity, risk, current, yesterday),
                data_freshness=dict(stocks="real-time (Polygon)", crypto="real-time (CoinMarketCap)",
                                    fred_latest="2039-12-28"),
                tier2=dict(crypto=dict(btc_dominance=50, mcap_change_24h=0,
                    fear_greed_value=0, risk_score=dict(score=0, regime="LOW", action="ACCUMULATE",
                    signals=["Synthetic signal"]), timestamp="2040-01-02T12:00:00Z")))


class TableParser(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.cells = []
        self.in_cell = False
        self.tags = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        if tag == "td":
            self.in_cell = True
            self.cells.append("")

    def handle_endtag(self, tag):
        if tag == "td":
            self.in_cell = False

    def handle_data(self, data):
        if self.in_cell:
            self.cells[-1] += data


class PresentationTests(unittest.TestCase):
    def test_cohort_exit_keeps_recommendation_and_order(self):
        payload = scan()
        before = copy.deepcopy(payload)
        html = SCOPE["build_email_html"](payload)
        self.assertIn("Left top-10 cohort: <b>SYN01</b>", html)
        self.assertIn("Entered top-10 cohort: <b>SYN11</b>", html)
        self.assertIn("not the first 10 BUYs", html)
        self.assertNotIn("Dropped:", html)
        expected = [r["ticker"] for r in payload["recommendations"] if r["action"] == "BUY"][:15]
        actual = [cell for cell in TableParser(html).cells if cell in expected]
        self.assertEqual(actual, expected)
        self.assertIn("SYN01", actual)
        self.assertEqual(payload, before)

    def test_mixed_actions_do_not_change_cohort_or_buy_table_policy(self):
        payload = scan()
        recs = [recommendation(i, "WATCH" if i <= 10 else "BUY") for i in range(1, 28)]
        payload["recommendations"] = recs
        dl = SCOPE["build_deltas"](payload["liquidity"], payload["risk"], recs,
                                  {"recommendations": [recommendation(1)]})
        self.assertEqual(dl["new_picks"], [])
        self.assertEqual(dl["dropped_picks"], ["SYN01"])
        payload["deltas"] = dl
        cells = TableParser(SCOPE["build_email_html"](payload)).cells
        self.assertEqual([v for v in cells if v.startswith("SYN")], [f"SYN{i:02}" for i in range(11, 26)])

    def test_risk_contract_and_legacy_zero(self):
        render = SCOPE["_crypto_risk_html"]
        self.assertEqual(render(0), "0/100")
        self.assertEqual(render(42.5), "42.5/100")
        html = render(dict(score=0, regime="LOW", action="ACCUMULATE", signals=[]))
        for label in ("0/100", "Source regime: LOW", "Source action: ACCUMULATE", "Source signals: none"):
            self.assertIn(label, html)
        self.assertNotIn("{'score'", html)
        self.assertIn("FEAR/GREED</div><div style=\"font-size:22px;font-weight:700\">0/100", SCOPE["build_email_html"](scan()))

    def test_missing_invalid_and_zero_are_distinct(self):
        render = SCOPE["_crypto_risk_html"]
        for value in (None, {}, {"score": None}):
            self.assertEqual(render(value), "Missing")
        for value in (False, True, "0", "", [], float("nan"), float("inf"), -1, 101):
            self.assertEqual(render(value), "Unavailable")
            self.assertEqual(render({"score": value}), "Unavailable")
        for value in (0, 0.0, 100):
            self.assertNotIn("Missing", render(value))
            self.assertNotIn("Unavailable", render(value))

    def test_structured_risk_never_infers_action_or_serializes_objects(self):
        html = SCOPE["_crypto_risk_html"]({"score": 90, "regime": {"bad": "shape"}, "signals": "bad"})
        self.assertIn("90/100", html)
        self.assertIn("Source regime: Unavailable", html)
        self.assertIn("Source signals: Unavailable", html)
        self.assertNotIn("Source action", html)
        self.assertNotIn("bad", html)

    def test_json_fetch_full_render_score_edge_case_parity(self):
        # Exercise real JSON decoding and fetch_tier2 field mapping, not just
        # the formatter. Only the relevant score text may change in the email.
        # NaN/Infinity are Python decoder extensions; huge integers are valid JSON.
        cases = [(10**400, "Unavailable"), (-10**400, "Unavailable"),
                 (True, "Unavailable"), (False, "Unavailable"), (None, "Missing"),
                 (float("nan"), "Unavailable"), (float("inf"), "Unavailable"),
                 (-float("inf"), "Unavailable"), (-1, "Unavailable"), (101, "Unavailable"),
                 (-0.01, "Unavailable"), (100.01, "Unavailable"),
                 (0, "0/100"), (0.0, "0/100"), (100, "100/100"), (100.0, "100/100"),
                 (42.5, "42.5/100"), ("0", "Unavailable"), ([], "Unavailable")]

        def fetched_scan(field, value):
            source = dict(global_market={"btc_dominance": 50}, fear_greed={"current": 0},
                          risk_score=0, generated_at="2040-01-02T12:00:00Z")
            if field == "structured":
                source["risk_score"] = dict(score=value, regime="Synthetic <regime>",
                                           action="Synthetic <action>", signals=["Synthetic <signal>"])
            elif field == "scalar":
                source["risk_score"] = value
            else:
                source["fear_greed"]["current"] = value
            raw = json.dumps(source).encode()

            class SyntheticStore:
                def get_object(self, *, Bucket, Key):
                    return {"Body": io.BytesIO(raw if Key == "crypto-intel.json" else b'{"data":{}}')}

            SCOPE["s3"] = SyntheticStore()
            try:
                payload = scan()
                payload["tier2"] = SCOPE["fetch_tier2"]()
                return payload
            finally:
                del SCOPE["s3"]

        for field in ("scalar", "structured", "fear_greed"):
            label = "FEAR/GREED" if field == "fear_greed" else "RISK SCORE"
            prefix = f'{label}</div><div style="font-size:22px;font-weight:700">'
            reference = SCOPE["build_email_html"](fetched_scan(field, 0))
            self.assertEqual(reference.count(prefix + "0/100"), 1)
            for value, expected in cases:
                with self.subTest(field=field, value=repr(value)):
                    payload = fetched_scan(field, value)
                    before = json.dumps(payload, sort_keys=True)
                    html = SCOPE["build_email_html"](payload)
                    self.assertEqual(html, reference.replace(prefix + "0/100", prefix + expected, 1))
                    self.assertEqual(json.dumps(payload, sort_keys=True), before)
                    self.assertEqual(sum(cell.startswith("SYN") for cell in TableParser(html).cells), 15)
                    if field == "structured":
                        self.assertIn("Synthetic &lt;regime&gt;", html)
                        self.assertNotIn("<regime>", html)

    def test_source_text_is_escaped(self):
        payload = scan()
        attack = '<img src=x onerror="alert(1)">&\''
        for key in ("timestamp", "ai_briefing"):
            payload[key] = attack
        payload["liquidity"]["regime"] = attack
        payload["risk"]["level"] = attack
        payload["recommendations"][0].update(ticker=attack, name=attack, reasons=[attack])
        payload["deltas"].update(new_picks=[attack], dropped_picks=[attack], yesterday_date=attack, regime_change=attack)
        payload["deltas"]["yesterday_picks_performance"][0]["ticker"] = attack
        payload["data_freshness"] = dict(stocks=attack, crypto=attack, fred_latest=attack)
        payload["tier2"]["crypto"].update(timestamp=attack, fear_greed_label=attack,
            stablecoin_net_signal=attack, risk_score=dict(score=0, regime=attack, action=attack, signals=[attack]),
            top_movers=[dict(symbol=attack, price=1, change_24h=0)])
        payload["tier2"]["options"] = dict(put_call_ratio=1, pc_signal=attack, gamma_regime=attack,
            max_gamma_strike=attack, spy_price=attack, timestamp=attack,
            trading_signals=[dict(type=attack, strength=attack, message=attack)])
        payload["tier2"]["sector_rotation"] = dict(leaders=[dict(ticker=attack, chg=1)])
        html = SCOPE["build_email_html"](payload)
        self.assertNotIn(attack, html)
        self.assertIn(escape(attack, quote=True), html)
        self.assertNotIn("img", TableParser(html).tags)

    def test_clocks_are_separate_and_as_supplied(self):
        html = SCOPE["build_email_html"](scan())
        for text in ("Baseline scan timestamp: 2040-01-01 18:00:00 ET", "yesterday's UTC date",
                     "not necessarily the previous email", "fixed UTC−05:00",
                     "Crypto source generated at (UTC): 2040-01-02T12:00:00Z",
                     "not observation timestamps", "individual series may be older"):
            self.assertIn(text, html)

    def test_missing_baseline_and_crypto_card_gate_are_preserved(self):
        payload = scan()
        payload["deltas"] = SCOPE["build_deltas"]({}, {}, [], None)
        payload["tier2"]["crypto"]["btc_dominance"] = None
        html = SCOPE["build_email_html"](payload)
        self.assertNotIn("Left top-10 cohort:", html)
        self.assertNotIn("CRYPTO INTEL", html)
        self.assertIn("TOP BUY RECOMMENDATIONS", html)

    def test_tier2_producer_fields_pass_through_without_network(self):
        class SyntheticStore:
            def get_object(self, *, Bucket, Key):
                self.last = Key
                payload = ({"data": {}, "timestamp": "2040-01-02T11:00:00Z"} if Key == "flow-data.json" else
                           {"risk_score": {"score": 0, "regime": "LOW", "action": "ACCUMULATE", "signals": []},
                            "fear_greed": {"current": 0}, "generated_at": "2040-01-02T12:00:00Z"})
                return {"Body": io.BytesIO(json.dumps(payload).encode())}
        SCOPE["s3"] = SyntheticStore()
        try:
            tier2 = SCOPE["fetch_tier2"]()
        finally:
            del SCOPE["s3"]
        self.assertEqual(tier2["crypto"]["risk_score"]["score"], 0)
        self.assertEqual(tier2["crypto"]["fear_greed_value"], 0)
        self.assertEqual(tier2["crypto"]["timestamp"], "2040-01-02T12:00:00Z")
        self.assertEqual(tier2["options"]["timestamp"], "2040-01-02T11:00:00Z")

    def test_baseline_uses_utc_date_key_and_last_modified_not_previous_email(self):
        class Clock:
            @staticmethod
            def now(tz):
                return datetime(2040, 1, 3, 1, tzinfo=timezone.utc)

        class SyntheticStore:
            def get_paginator(self, name):
                return self

            def paginate(self, **kwargs):
                return [{"Contents": [
                    {"Key": "data/secretary-history/2040-01-02_0900.json", "LastModified": 3},
                    {"Key": "data/secretary-history/2040-01-02_1800.json", "LastModified": 2},
                    {"Key": "data/secretary-history/2040-01-03_0000.json", "LastModified": 4},
                ]}]

            def get_object(self, *, Bucket, Key):
                self.selected = Key
                return {"Body": io.BytesIO(b'{"timestamp":"2040-01-02 09:00:00 ET"}')}

        store = SyntheticStore()
        SCOPE.update(s3=store, datetime=Clock)
        try:
            result = SCOPE["fetch_yesterday_snapshot"]()
        finally:
            del SCOPE["s3"]
            SCOPE["datetime"] = datetime
        self.assertEqual(store.selected, "data/secretary-history/2040-01-02_0900.json")
        self.assertEqual(result["timestamp"], "2040-01-02 09:00:00 ET")

    def test_nonpresentation_code_is_identical_to_reviewed_base(self):
        # Complete module AST outside the renderer + three pure helpers + html import.
        # Pins producers, ranking, missing defaults, delta/baseline, prompts, delivery,
        # allocation, timestamps and handlers to main fd6d7b6b4 (source 35a7d1af7).
        nodes = [n for n in TREE.body if not (
            isinstance(n, ast.FunctionDef) and n.name in HELPERS | {"build_email_html"}
            or isinstance(n, ast.ImportFrom) and n.module == "html")]
        digest = hashlib.sha256(ast.dump(ast.Module(body=nodes, type_ignores=[])).encode()).hexdigest()
        self.assertEqual(digest, "ea9299e54518d95160c8273153a6487cfb8484012b561776e2386130dc6389ab")


if __name__ == "__main__":
    unittest.main(verbosity=2)
