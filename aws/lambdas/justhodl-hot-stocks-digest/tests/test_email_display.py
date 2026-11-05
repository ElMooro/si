"""Offline HTML-only regressions: AST extraction never imports boto3 or invokes a handler."""
import ast
import hashlib
import json
import math
import pickle
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "source/lambda_function.py"


def renderer(path):
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "render_email_html")
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["render_email_html"]


def packet(value=0, missing=False):
    row = {"ticker": "TEST", "score": 12, "venue_count": 1,
           "why": ["synthetic attention"], "bull_pct": value,
           "analyst": {"why_hot": "synthetic", "bull": "unchanged bull text",
                       "bear": "unchanged bear text", "net": "watch"}}
    if missing:
        del row["bull_pct"]
    return {"generated_at": "2026-10-01T12:30:03+00:00", "market_read": "Synthetic fixture",
            "hot_stocks": [row], "warnings": []}


class EmailDisplay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.render = staticmethod(renderer(SOURCE))
        cls.before = staticmethod(renderer(HERE / "fixtures/renderer_before.py"))

    def test_valid_percentages_preserve_entire_html_and_input(self):
        values = [0, 0.0, -0.0, 1, 75, 100, 100.0, 0.1, 75.25,
                  math.nextafter(0, 1), math.nextafter(100, 0)]
        values += [i / 10 for i in range(1001)]
        for value in values:
            with self.subTest(value=value):
                out = packet(value)
                frozen = pickle.dumps(out)
                self.assertEqual(self.render(out), self.before(out))
                self.assertEqual(pickle.dumps(out), frozen)

    def test_unavailable_changes_only_sentiment_text(self):
        values = [None, True, False, "", "0", "75", [], {}, [0], {"value": 75},
                  float("nan"), float("inf"), -float("inf"), -1, 101,
                  math.nextafter(0, -1), math.nextafter(100, math.inf),
                  10 ** 400, -(10 ** 400), 10 ** 10000, -(10 ** 10000)]
        expected = self.before(packet(0)).replace("bull 0%", "bull unavailable")
        for index, value in enumerate(values):
            with self.subTest(case=index):
                out = packet(value)
                frozen = pickle.dumps(out)
                self.assertEqual(self.render(out), expected)
                self.assertEqual(pickle.dumps(out), frozen)
        self.assertEqual(self.render(packet(missing=True)), expected)

    def test_sentiment_cannot_inject_markup_or_text(self):
        expected = self.before(packet(0)).replace("bull 0%", "bull unavailable")
        for value in ["</span><script>alert(1)</script>", "<img src=x onerror=alert(1)>",
                      "&lt;b&gt;75&lt;/b&gt;", "75%\nNET: BUY", '" onclick="attack()']:
            self.assertEqual(self.render(packet(value)), expected)

    def test_wrong_types_are_never_coerced(self):
        class Hostile:
            def __str__(self):
                raise AssertionError("must not format wrong types")
            def __float__(self):
                raise AssertionError("must not coerce wrong types")
        class HostileInt(int):
            def __str__(self):
                raise AssertionError("must not format numeric subclasses")
        for value in [Hostile(), HostileInt(75)]:
            self.assertIn("bull unavailable</span>", self.render(packet(value)))

    def test_twelve_rows_preserve_zero_valid_neighbors_and_net(self):
        out = packet()
        out["hot_stocks"] = [packet(value)["hot_stocks"][0] for value in [None] * 10 + [0, 75]]
        html = self.render(out)
        self.assertEqual(html.count("bull unavailable</span>"), 10)
        self.assertEqual(html.count("bull 0%</span>"), 1)
        self.assertEqual(html.count("bull 75%</span>"), 1)
        self.assertEqual(html.count("NET: watch"), 12)
        self.assertEqual(html.count("<tr>"), 12)

    def test_all_nonrenderer_functions_and_module_statements_unchanged(self):
        baseline = json.loads((HERE / "fixtures/nonrenderer_fingerprints.json").read_text())
        tree = ast.parse(SOURCE.read_text())
        digest = lambda node: hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
        functions = {n.name: digest(n) for n in tree.body
                     if isinstance(n, ast.FunctionDef) and n.name != "render_email_html"}
        self.assertEqual(functions, baseline["functions"])
        tree.body = [n for n in tree.body
                     if not (isinstance(n, ast.FunctionDef) and n.name == "render_email_html")]
        self.assertEqual(digest(tree), baseline["module_without_renderer"])


if __name__ == "__main__":
    unittest.main()
