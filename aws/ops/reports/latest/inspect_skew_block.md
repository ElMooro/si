# vix_complex

**Status:** success  
**Duration:** 0.1s  
**Finished:** 2026-05-04T17:11:13+00:00  

## Log
- `17:11:13` {
  "vix": {
    "current": 16.99,
    "date": "2026-05-01",
    "prev_close": 16.89,
    "change": 0.1,
    "change_pct": 0.59,
    "sma_10": 18.385,
    "sma_20": 19.2315,
    "sma_50": 22.2448,
    "percentile_1y": 46.2,
    "zscore": -0.38,
    "high_52w": 31.05,
    "low_52w": 13.47,
    "history_30d": [
      {
        "date": "2026-05-01",
        "value": 16.99
      },
      {
        "date": "2026-04-30",
        "value": 16.89
      },
      {
        "date": "2026-04-29",
        "value": 18.81
      },
      {
        "date": "2026-04-28",
        "value": 17.83
      },
      {
        "date": "2026-04-27",
        "value": 18.02
      },
      {
        "date": "2026-04-24",
        "value": 18.71
      },
      {
        "date": "2026-04-23",
        "value": 19.31
      },
      {
        "date": "2026-04-22",
        "value": 18.92
      },
      {
        "date": "2026-04-21",
        "value": 19.5
      },
      {
        "date": "2026-04-20",
        "value": 18.87
      },
      {
        "date": "2026-04-17",
        "value": 17.48
      },
      {
        "date": "2026-04-16",
        "value": 17.94
      },
      {
        "date": "2026-04-15",
        "val
# skew

- `17:11:13` {}
# put_call

- `17:11:13` {
  "total_put_call_ratio": 0.164,
  "total_call_volume": 888281,
  "total_put_volume": 146042,
  "total_call_premium": 312078273.0,
  "total_put_premium": 22732350.0,
  "net_premium": 289345923.0,
  "options_flow": [
    {
      "ticker": "NVDA",
      "stock_price": 198.45,
      "call_volume": 227195,
      "put_volume": 131570,
      "call_premium": 18181459.0,
      "put_premium": 21089451.0,
      "net_premium": -2907992.0,
      "pc_ratio": 0.579,
      "sentiment": "BULLISH",
      "contracts_analyzed": 15
    },
    {
      "ticker": "TSLA",
      "stock_price": 390.82,
      "call_volume": 352968,
      "put_volume": 0,
      "call_premium": 172339999.0,
      "put_premium": 0,
      "net_premium": 172339999.0,
      "pc_ratio": 0.0,
      "sentiment": "BULLISH",
      "contracts_analyzed": 15
    },
    {
      "ticker": "AAPL",
      "stock_price": 280.14,
      "call_volume": 188547,
      "put_volume": 13155,
      "call_premium": 17835848.0,
      "put_premium": 319314.0,
      "net_premium": 17516534.0,
      "pc_ratio": 0.07,
      "sentiment": "BULLISH",
      "contracts_analyzed": 15
    },
    {
      "ticker": "MSFT",
      "stock_price": 414.44,
      "call_vo
# gamma_exposure

- `17:11:13` {
  "total_gex": 132258.0,
  "call_gex": 132258.0,
  "put_gex": 0,
  "spy_price": 720.65,
  "strike_levels": [
    {
      "strike": 705,
      "gex": 21391.0
    },
    {
      "strike": 712,
      "gex": 18343.0
    },
    {
      "strike": 700,
      "gex": 15464.0
    },
    {
      "strike": 703,
      "gex": 14471.0
    },
    {
      "strike": 710,
      "gex": 12349.0
    },
    {
      "strike": 695,
      "gex": 8579.0
    },
    {
      "strike": 714,
      "gex": 8074.0
    },
    {
      "strike": 707,
      "gex": 7672.0
    },
    {
      "strike": 713,
      "gex": 7093.0
    },
    {
      "strike": 709,
      "gex": 3664.0
    },
    {
      "strike": 701,
      "gex": 3626.0
    },
    {
      "strike": 708,
      "gex": 2626.0
    },
    {
      "strike": 706,
      "gex": 2354.0
    },
    {
      "strike": 699,
      "gex": 1438.0
    },
    {
      "strike": 704,
      "gex": 1223.0
    },
    {
      "strike": 697,
      "gex": 1075.0
    },
    {
      "strike": 690,
      "gex": 822.0
    },
    {
      "strike": 696,
      "gex": 770.0
    },
    {
      "strike": 670,
      "gex": 239.0
    },
    {
      "strike": 680,
      "gex": 126.0
    }
  ],
  "m
# sentiment

- `17:11:13` {
  "vix_sentiment": {
    "score": 53.8,
    "label": "Neutral",
    "vix": 16.99
  },
  "credit_sentiment": {
    "score": 80,
    "ted_spread": 0.09,
    "label": "Greed"
  },
  "curve_sentiment": {
    "score": 60,
    "spread": 0.51,
    "label": "Normal"
  },
  "hy_sentiment": {
    "score": 80,
    "spread": 2.77,
    "label": "Calm"
  },
  "breadth_sentiment": {
    "score": 48.8,
    "label": "Neutral"
  },
  "momentum_sentiment": {
    "score": 70.2,
    "rsi": 70.2,
    "label": "Overbought"
  },
  "news_sentiment": {
    "score": 87.6,
    "avg_raw": 0.1878,
    "article_count": 50,
    "bullish_count": 33,
    "bearish_count": 6,
    "neutral_count": 11,
    "label": "Bullish",
    "top_articles": [
      {
        "title": "Why Gold Won\u2019t Save You in a Real Crisis (But Stocks Will)",
        "source": "24/7 Wall St.",
        "sentiment_score": 0.303424,
        "sentiment_label": "Somewhat-Bullish",
        "time": "20260503T210839",
        "url": "https://247wallst.com/investing/2026/05/03/why-gold-wont-save-you-in-a-real-crisis-but-stocks-will/"
      },
      {
        "title": "SPY ETF Gains 0.3%",
        "source": "Moomoo",
        "sentiment_score": 0.36
# trading_signals

- `17:11:13` [
  {
    "type": "CONTRARIAN_SELL",
    "strength": "MODERATE",
    "source": "PUT_CALL",
    "message": "P/C 0.16 - extreme call buying",
    "confidence": 70
  },
  {
    "type": "RISK_OFF",
    "strength": "MODERATE",
    "source": "FUND_FLOWS",
    "message": "$8374M rotating to safe havens",
    "confidence": 60
  }
]
# market_internals

- `17:11:13` {
  "fed_funds_rate": {
    "date": "2026-04-01",
    "value": 3.64
  },
  "treasury_10y": {
    "date": "2026-04-30",
    "value": 4.4
  },
  "treasury_2y": {
    "date": "2026-04-30",
    "value": 3.88
  },
  "ted_spread": {
    "date": "2022-01-21",
    "value": 0.09
  },
  "hy_spread": {
    "date": "2026-05-01",
    "value": 2.77
  },
  "ig_spread": {
    "date": "2026-05-01",
    "value": 0.81
  },
  "yield_curve_10y2y": {
    "date": "2026-05-01",
    "value": 0.51
  },
  "yield_curve_10y3m": {
    "date": "2026-05-01",
    "value": 0.71
  },
  "breakeven_5y": {
    "date": "2026-05-01",
    "value": 2.69
  },
  "breakeven_10y": {
    "date": "2026-05-01",
    "value": 2.48
  }
}
