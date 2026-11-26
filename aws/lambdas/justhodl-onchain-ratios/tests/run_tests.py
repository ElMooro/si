"""The on-chain engine's join with the kept CryptoQuant document (audit of 2026-11-26). No network: S3 is a stub."""
import io
import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "shared"))
sys.modules.setdefault("boto3", types.SimpleNamespace(client=lambda *a, **k: None))
import lambda_function as L  # noqa: E402


class S3:
    def __init__(self, doc):
        self.doc = doc

    def get_object(self, Bucket, Key):
        if self.doc is None:
            raise RuntimeError("NoSuchKey")
        assert Key == "data/cryptoquant-onchain.json"
        return {"Body": io.BytesIO(json.dumps(self.doc).encode())}


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


KEPT = {"status": "EOD", "generated_at": "2026-11-24T21:05:07+00:00", "composite_onchain_risk_z": 0.07, "metrics": {
    "btc_mvrv": {"label": "MVRV Ratio", "value": 1.13, "category": "market_indicator", "pctl_1y": 4, "as_of": "2026-11-24"},
    "btc_whale_ratio": {"label": "Exchange Whale Ratio", "value": 0.41, "category": "flow_indicator"},
    "eth_exchange_reserve": {"label": "ETH Exchange Reserve", "value": 15900000.0, "unit": "ETH", "category": "eth", "pctl_1y": 12, "z365": -1.4, "wow": -0.6, "as_of": "2026-11-24", "cond_stats": {"x": 1}},
    "eth_mvrv": {"label": "ETH MVRV", "value": 0.92, "category": "eth", "as_of": "2026-11-24"},
    "broken": "not a metric"}}


def blank():
    return {"btc": {"mvrv": 1.12}, "eth": {"errors": ["coinmetrics_eth: HTTPError"], "extreme_signals": []}}


def main():
    doc = L._cq_join(blank(), S3(KEPT), "b")
    cq = doc["cryptoquant"]
    check(cq["btc_mvrv"]["value"] == 1.13 and cq["btc_whale_ratio"]["value"] == 0.41 and cq["btc_sopr"] is None, "the Bitcoin core set is joined from an end-of-day document; a metric the document lacks is null, not invented")
    check(cq["status"] == "EOD" and cq["generated_at"].startswith("2026-11-24") and cq["n_metrics_kept"] == 5 and cq["composite_onchain_risk_z"] == 0.07, "labelled with the kept document's own status and date")
    eth = doc["eth"]["cryptoquant"]
    check(sorted(eth) == ["eth_exchange_reserve", "eth_mvrv"], "Ethereum takes exactly the kept document's Ethereum metrics")
    check(eth["eth_exchange_reserve"] == {"label": "ETH Exchange Reserve", "value": 15900000.0, "unit": "ETH", "as_of": "2026-11-24", "pctl_1y": 12, "z365": -1.4, "wow": -0.6}, "value, unit, date and ranks; nothing else")
    check("2 Ethereum metrics" in doc["eth"]["cryptoquant_note"] and "EOD" in doc["eth"]["cryptoquant_note"], "and says where they came from")
    check(doc["eth"]["errors"] == ["coinmetrics_eth: HTTPError"], "the refused request stays on the record")
    live = L._cq_join(blank(), S3(dict(KEPT, status="LIVE")), "b")
    check(live["cryptoquant"]["status"] == "LIVE" and "cryptoquant" in live["eth"], "a live document joins as before")
    for empty in (None, {"status": "EOD", "metrics": {}}, {"status": "EOD"}):
        d = L._cq_join(blank(), S3(empty), "b")
        check("cryptoquant" not in d and "cryptoquant" not in d["eth"], "no kept document or no metrics: nothing is joined and the run goes on")
    print("OK on-chain join")


if __name__ == "__main__":
    main()
