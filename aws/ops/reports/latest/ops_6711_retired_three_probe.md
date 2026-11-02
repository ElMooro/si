# ops 6711 - the three Oct 20 switch-offs: where they write, what fires them, did they work (read-only)

**Status:** success  
**Duration:** 5.1s  
**Finished:** 2026-11-02T16:11:35+00:00  

## Log
## A. engine, destination, output age

- `16:11:31` macro-financial-intelligence: role MacroFinancialIntelligenceRole | env names ['FRED_API_KEY', 'S3_BUCKET'] | S3_BUCKET=macro-financial-intelligence | handler lambda_function.lambda_handler | 512 MB / 900 s
- `16:11:31`    bucket macro-financial-intelligence exists: True
- `16:11:31`    s3://macro-financial-intelligence/latest_brief.json: 159 B, written 2026-10-16T19:51 (16.8 d ago)
- `16:11:31` ofrapi: role ofr-lambda-role | env names ['S3_BUCKET'] | S3_BUCKET=macro-data-lake | handler ofr_lambda_complete.lambda_handler | 512 MB / 120 s
- `16:11:32`    bucket macro-data-lake exists: True
- `16:11:32`    s3://macro-data-lake/scraped_data/ofr/latest.json: 20858888 B, written 2026-05-28T14:00 (158.1 d ago)
- `16:11:32`    scraped_data/ofr/: 305 objects, newest [('scraped_data/ofr/ofr_data_20260527_140033.json', '2026-05-27T14:00'), ('scraped_data/ofr/ofr_data_20260528_140033.json', '2026-05-28T14:00'), ('scraped_data/ofr/latest.json', '2026-05-28T14:00')]
- `16:11:32` treasury-api: role lambda-execution-role | env names ['S3_BUCKET', 'TREASURY_KEY'] | S3_BUCKET=openbb-lambda-data | handler lambda_function.lambda_handler | 512 MB / 60 s
- `16:11:33`    bucket openbb-lambda-data exists: True
- `16:11:33`    s3://openbb-lambda-data/treasury_historical_comprehensive.json: 1682886 B, written 2026-05-28T10:00 (158.3 d ago)
## B. triggers: the rules that name them, the live fan-out manifest

- `16:11:33` rule macro-financial-daily-execution [DISABLED] rate(1 day) | targets [('macro-financial-intelligence', False)]
- `16:11:33` rule ofr-daily-collection: ResourceNotFoundException
- `16:11:33` rule ofr-weekly-report: ResourceNotFoundException
- `16:11:34` rule treasury-auto-update: ResourceNotFoundException
- `16:11:34` live fan-out: macro-financial-intelligence in ticks [] | assignment None
- `16:11:34` live fan-out: ofrapi in ticks [] | assignment None
- `16:11:34` live fan-out: treasury-api in ticks [] | assignment None
- `16:11:34` live fan-out: treasury-auto-updater in ticks [] | assignment None
- `16:11:34` live fan-out retired blocks: {'retired_2026_10_20': ['ecb-data-daily-updater', 'ofrapi', 'ultimate-multi-agent'], 'retired_2026_10_26': ['treasury-auto-updater']}
## C. API Gateway routes that integrate them

- `16:11:34` api 6nl5fzfus7 (OFR-Financial-API, created 2025-08-12): 6 resources; routes to the three: ['POST /ofr/collect -> ofrapi', 'GET /ofr/stats -> ofrapi', 'ANY /ofr -> ofrapi', 'GET /ofr/search -> ofrapi', 'ANY /ofr/{proxy+} -> ofrapi']
- `16:11:35` api i1hgpjotq7 (TreasuryMarketAPI, created 2025-08-13): 21 resources; routes to the three: ['GET /treasury/crisis -> treasury-api', 'GET /indicators/all -> treasury-api', 'GET /summary -> treasury-api', 'GET /dashboard/complete -> treasury-api', 'GET /stress/score -> treasury-api', 'GET /rates/current -> treasury-api', 'ANY /treasury -> treasury-api', 'GET /liquidity/metrics -> treasury-api', 'ANY / -> treasury-api', 'GET /crisis/indicators -> treasury-api', 'GET /alerts/test -> treasury-api', 'GET /treasury/alerts -> treasury-api']
- `16:11:35` api klehdyiwrl: NotFoundException
## D. did they work before the hold?  errors Sep 15 - Oct 17

- `16:11:35` macro-financial-intelligence: 0 error-looking lines
- `16:11:35`    last lines before the hold: []
- `16:11:35` ofrapi: 0 error-looking lines
- `16:11:35`    last lines before the hold: []
- `16:11:35` treasury-api: 0 error-looking lines
- `16:11:35`    last lines before the hold: []
- `16:11:35` ✅ ops 6711 complete (read-only)
