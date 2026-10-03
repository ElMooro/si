"""Ops 1129b: Write extra-providers.json to S3."""
from __future__ import annotations
import json
import boto3

PROVIDERS = {
    "usaspending": {
        "name": "USASpending — federal contracts",
        "api": "api.usaspending.gov/api/v2",
        "engines": ["justhodl-usaspending"],
        "prefixes": [],
        "hot": ["data/usaspending-contracts.json"],
        "label": "Federal contract awards mapped to tickers",
        "cadence": "DAILY",
    },
    "prediction-markets": {
        "name": "Prediction Markets — Kalshi/Polymarket",
        "api": "api.elections.kalshi.com + gamma-api.polymarket.com",
        "engines": ["justhodl-prediction-markets"],
        "prefixes": [],
        "hot": ["data/prediction-markets.json"],
        "label": "Market-implied odds: Fed, elections, recession",
        "cadence": "HOURLY",
    },
    "biotech-catalysts": {
        "name": "Biotech Catalysts — clinicaltrials.gov",
        "api": "clinicaltrials.gov/api/v2",
        "engines": ["justhodl-biotech-catalysts"],
        "prefixes": [],
        "hot": ["data/biotech-catalysts.json"],
        "label": "Trial phase transitions + PDUFA watch by ticker",
        "cadence": "DAILY",
    },
    "activist-radar": {
        "name": "Activist Radar — 13D/13G",
        "api": "data.sec.gov/submissions",
        "engines": ["justhodl-activist-radar"],
        "prefixes": [],
        "hot": ["data/activist-radar.json"],
        "label": "Beneficial ownership: activist accumulation events",
        "cadence": "DAILY",
    },
    "ipo-calendar": {
        "name": "IPO Calendar — S-1 filings",
        "api": "data.sec.gov/submissions",
        "engines": ["justhodl-ipo-calendar"],
        "prefixes": [],
        "hot": ["data/ipo-calendar.json"],
        "label": "Filed/priced/withdrawn IPOs from EDGAR S-1s",
        "cadence": "DAILY",
    },
    "sentiment-surveys": {
        "name": "Sentiment Surveys — AAII/NAAIM",
        "api": "aaii.com + naaim.org",
        "engines": ["justhodl-sentiment-surveys"],
        "prefixes": [],
        "hot": ["data/sentiment-surveys.json"],
        "label": "AAII bull/bear + NAAIM exposure (contrarian gauges)",
        "cadence": "WEEKLY",
    },
    "ici-flows": {
        "name": "ICI — mutual fund flows",
        "api": "ici.org",
        "engines": ["justhodl-ici-flows"],
        "prefixes": [],
        "hot": ["data/ici-fund-flows.json"],
        "label": "Weekly mutual fund/ETF flow estimates",
        "cadence": "WEEKLY",
    },
    "uspto-patents": {
        "name": "USPTO — patents",
        "api": "api.uspto.gov/patentsview",
        "engines": ["justhodl-uspto-patents"],
        "prefixes": [],
        "hot": ["data/uspto-patents.json"],
        "label": "Corporate patent filings mapped to tickers",
        "cadence": "WEEKLY",
    },
    "regsho": {
        "name": "Reg SHO — threshold list",
        "api": "nasdaqtrader.com",
        "engines": ["justhodl-regsho"],
        "prefixes": [],
        "hot": ["data/regsho-threshold.json"],
        "label": "Reg SHO threshold securities (squeeze candidates)",
        "cadence": "DAILY",
    },
    "retail-attention": {
        "name": "Retail Attention — Wikipedia pageviews",
        "api": "wikimedia.org/api/rest_v1",
        "engines": ["justhodl-retail-attention"],
        "prefixes": [],
        "hot": ["data/retail-attention.json"],
        "label": "Wikipedia attention scores by ticker",
        "cadence": "DAILY",
    },
    "earnings-transcripts": {
        "name": "Earnings Transcripts",
        "api": "(free transcript sources)",
        "engines": ["justhodl-earnings-transcripts"],
        "prefixes": [],
        "hot": ["data/earnings-transcripts.json"],
        "label": "Earnings call transcripts for S&P 500",
        "cadence": "EARNINGS",
    },
}

def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    body = json.dumps(PROVIDERS, indent=1).encode()
    s3.put_object(Bucket="justhodl-dashboard-live",
                  Key="data/providers/extra-providers.json",
                  Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=300")
    print(json.dumps({"written": len(PROVIDERS)}))

if __name__ == "__main__":
    main()
