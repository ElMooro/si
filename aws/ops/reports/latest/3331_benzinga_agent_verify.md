## ENSURE FMP_KEY

**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-07-15T02:50:00+00:00  

## Data

| RESULT | attempt_1 | counts | fmp_suffix | had_fmp | sample_econ | sample_news | sample_rating | source | ts |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | S8xb | False |  |  |  |  |  |
|  | {'fn_error': None, 'source': 'FMP /stable (Benzinga retired 2026-07-15)', 'counts': {'ratings': 40, 'earnings': 30, 'economics': 20, 'news': 15, 'dividends': 20}} |  |  |  |  |  |  |  |  |
|  |  | {'ratings': 40, 'earnings': 30, 'economics': 20, 'news': 15, 'dividends': 20} |  |  | {'date': '2026-07-15 11:00:00', 'event_name': 'MBA Mortgage Market Index (Jul/10)', 'event': 'MBA Mortgage Market Index (Jul/10)', 'name': 'MBA Mortgage Market Index (Jul/10)', 'actual': '-', 'consensus': '-', 'prior': 266.3, 'impact': 'Low'} | {'title': 'Blockbuster Stock Sales Are Threatening to Overwhelm the Bull Market', 'author': 'WSJ', 'created': '2026-07-14 21:00', 'published': '2026-07-14 21:00', 'url': 'https://www.wsj.com/finance/stocks/blockbuster-stock-sales-are-threatening-to-overwhelm-the-bull-market-0ef50ef7'} | {'date': '2026-07-15', 'ticker': 'EVO', 'symbol': 'EVO', 'analyst': 'Cowen & Co.', 'analyst_name': 'Cowen & Co.', 'action_company': 'Downgrade', 'rating_current': 'Hold', 'pt_current': '-', 'adjusted_pt_current': '-'} | FMP /stable (Benzinga retired 2026-07-15) | 2026-07-15T02:50:00.588968+00:00 |
| FIXED |  |  |  |  |  |  |  |  |  |

## Log
- `02:49:59` ✅ FMP_KEY set, dead BENZINGA_API_KEY removed
## INVOKE (await v2.0)

## VERIFY SECTIONS

## VERDICT

- `02:50:00` ✅ benzinga.html LIVE on FMP — ratings + earnings + econ + news + dividends populate. Dead Benzinga key retired.
