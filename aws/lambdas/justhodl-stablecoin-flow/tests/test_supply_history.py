"""supply_history(): DefiLlama's daily chart rows become one row per UTC day with total / minted / bridged and the
per-coin circulating supply, null where a coin has no observation; an incomplete history is refused. Offline."""
from pathlib import Path
from unittest.mock import patch
import importlib.util
import sys
import types
import unittest

SRC = Path(__file__).resolve().parents[1] / "source" / "lambda_function.py"


def load():
    # the module builds AWS clients and resolves the Telegram token at import: stub both, no network, no AWS
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda *a, **k: types.SimpleNamespace(put_object=lambda **k: None, get_parameter=lambda **k: {"Parameter": {"Value": "{}"}}, put_parameter=lambda **k: None)
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = Exception
    botocore = types.ModuleType("botocore")
    botocore.exceptions = exceptions
    secret = types.ModuleType("managed_secret")
    secret.managed_secret = lambda *a, **k: ""
    with patch.dict(sys.modules, {"boto3": fake_boto3, "botocore": botocore, "botocore.exceptions": exceptions, "managed_secret": secret}):
        spec = importlib.util.spec_from_file_location("stablecoin_flow_under_test", SRC)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


def chart(days, start=1511913600, value=1e9, step=1e7, minted=True):
    rows = []
    for i in range(days):
        row = {"date": str(start + 86400 * i), "totalCirculating": {"peggedUSD": value + step * i}}
        if minted:
            row["totalMintedUSD"] = {"peggedUSD": value * 1.5 + step * i}
            row["totalBridgedToUSD"] = {"peggedUSD": 1e6}
        rows.append(row)
    return rows


class Tests(unittest.TestCase):
    def test_rows_align_by_utc_day_and_coins_may_start_later(self):
        mod = load()
        answers = {mod.DEFILLAMA_CHART_ALL: chart(1200), mod.DEFILLAMA_CHART_ALL + "?stablecoin=1": chart(1200, value=5e8),
                   mod.DEFILLAMA_CHART_ALL + "?stablecoin=2": chart(300, start=1511913600 + 86400 * 900, value=2e8, minted=False)}
        with patch.object(mod, "http_json", side_effect=lambda url, timeout=20: answers[url]):
            doc = mod.supply_history()
        self.assertEqual(doc["count"], 1200)
        self.assertEqual(doc["first_date"], "2017-11-29")
        self.assertEqual(doc["series"][0], {"date": "2017-11-29", "total": 1e9, "minted": 1.5e9, "bridged": 1e6, "usdt": 5e8, "usdc": None})
        self.assertEqual(doc["series"][900]["usdc"], 2e8)
        self.assertEqual(doc["latest"]["date"], doc["series"][-1]["date"])
        self.assertEqual(doc["units"], "USD")
        self.assertEqual(set(doc["definitions"]), {"total", "minted", "bridged", "usdt", "usdc"})

    def test_incomplete_history_is_refused_and_publication_is_fail_soft(self):
        mod = load()
        with patch.object(mod, "http_json", side_effect=lambda url, timeout=20: chart(5)):
            with self.assertRaises(ValueError):
                mod.supply_history()
            self.assertEqual(mod.publish_supply_history(), {"ok": False, "error_type": "ValueError"})


if __name__ == "__main__":
    unittest.main()
