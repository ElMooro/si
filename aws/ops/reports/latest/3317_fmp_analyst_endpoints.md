## KEY

**Status:** failure  
**Duration:** 0.5s  
**Finished:** 2026-07-15T01:49:02+00:00  

## Error

```
SystemExit: 1
```

## Data

| RESULT | grades_consensus_AAPL | grades_news | key_fp | key_source | price_target_consensus_AAPL | price_target_news |
|---|---|---|---|---|---|---|
|  |  |  | {'len': 32, 'suffix': 'S8xb'} | justhodl-analyst-consensus.FMP_KEY |  |  |
|  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - symbol'} |  |  |  |  |
|  |  |  |  |  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - symbol'} |
|  | {'http': 200, 'n': 1, 'fields': ['symbol', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell', 'consensus'], 'sample': {'symbol': 'AAPL', 'strongBuy': 1, 'buy': 69, 'hold': 34, 'sell': 7, 'strongSell': 0, 'consensus': 'Buy'}} |  |  |  |  |  |
|  |  |  |  |  | {'http': 200, 'n': 1, 'fields': ['symbol', 'targetHigh', 'targetLow', 'targetConsensus', 'targetMedian'], 'sample': {'symbol': 'AAPL', 'targetHigh': 400, 'targetLow': 250, 'targetConsensus': 324.29, 'targetMedian': 325}} |  |
| FMP_FEEDS_EMPTY |  |  |  |  |  |  |

## Log
## GRADES-NEWS (upgrade/downgrade feed)

## PRICE-TARGET-NEWS (PT revisions)

## GRADES-CONSENSUS (roll-up sanity, AAPL)

## PRICE-TARGET-CONSENSUS (sanity, AAPL)

## VERDICT

- `01:49:02` ✗ Core FMP analyst feeds not returning data — inspect rows.
