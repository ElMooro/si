# 6985 - does our FINRA access reach a bond-by-bond dataset? (read-only probe), 2026-11-26T20:48+00:00

**Status:** success  
**Duration:** 71.3s  
**Finished:** 2026-11-26T20:49:26+00:00  

## Data

| datasets | per_security | rows_returned | token |
|---|---|---|---|
| 46 | 3 | 3 | True |

## Log
## 1. How we are signed in

- `20:48:15` credentials in the parameter store: {"client id": "kept", "client secret": "kept", "cached token": "kept"}
- `20:48:15` this run: signed in with client credentials (a token came back; not shown)
## 2. FINRA's own list of datasets

- `20:48:16` GET /datasets without a token: HTTP 401, { "statusCode" : 401, "statusDescription" : "Failed to authenticate, check your authentication token", "message": "Unauthorized", "requestId" : "7db9df95-5a3b-4d1e-ac46-0dc9bd68dbac", "resource" : "/d
- `20:48:22` GET /datasets with the token: HTTP 200, 168 datasets
- `20:48:22`   one entry looks like: {"group": "FIRM", "name": "4530FILINGS", "description": "4530 Filing Data", "supportedFormats": ["text/plain", "application/json"], "supportsRecordOffset": true, "supportsRecordLimit": true, "supportsQuery": true, "supportsGetById": false, "supportedMethods": ["GET", "POST"], "status": "Active", "dataVersion": 1}
- `20:48:22`   groups: {"FIFC": 4, "FINRA": 14, "FIRM": 14, "FIXEDINCOMEMARKET": 46, "OTCMARKET": 40, "REGISTRATION": 36, "REPORTCARD": 14}
- `20:48:22` fixed-income datasets listed by FINRA (46): AGENCYCMOPRICING, AGENCYCMOPRICINGMOCK, AGENCYMARKETBREADTH, AGENCYMARKETBREADTHMOCK, AGENCYMARKETSENTIMENT, AGENCYMARKETSENTIMENTMOCK, AGENCYMBSARMHYBRIDPRICING, AGENCYMBSARMHYBRIDPRICINGMOCK, AGENCYMBSPRICING, AGENCYMBSPRICINGMOCK, AGENCYMBSTRADINGACTIVITY, AGENCYMBSTRADINGACTIVITYMOCK, AGENCYTBAPRICING, AGENCYTBAPRICINGMOCK, COLLATERALIZEDOBLIGATIONPRICING, COLLATERALIZEDOBLIGATIONPRICINGMOCK, CORPORATE144AMARKETBREADTH, CORPORATE144AMARKETBREADTHMOCK, CORPORATE144AMARKETSENTIMENT, CORPORATE144AMARKETSENTIMENTMOCK, CORPORATEMARKETBREADTH, CORPORATEMARKETBREADTHMOCK, CORPORATEMARKETSENTIMENT, CORPORATEMARKETSENTIMENTMOCK, CORPORATESANDAGENCIESCAPPEDVOLUME, CORPORATESANDAGENCIESCAPPEDVOLUMEMOCK, DAILYCMBSPRICING, DAILYCMBSPRICINGMOCK, NONAGENCYCMOABSPRICING, NONAGENCYCMOABSPRICINGMOCK, NONAGENCYCMOVINTAGEPRICING, NONAGENCYCMOVINTAGEPRICINGMOCK, SECURITIZEDPRODUCTERRATA, SECURITIZEDPRODUCTERRATAMOCK, SECURITIZEDPRODUCTSCAPPEDVOLUME, SECURITIZEDPRODUCTSCAPPEDVOLUMEMOCK, SECURITIZEDPRODUCTTRADINGACTIVITY, SECURITIZEDPRODUCTTRADINGACTIVITYMOCK, TRACEMONTHLYVOLUME, TRACEMONTHLYVOLUMEMOCK, TREASURYDAILYAGGREGATES, TREASURYDAILYAGGREGATESMOCK, TREASURYMONTHLYAGGREGATES, TREASURYMONTHLYAGGREGATESMOCK, WEEKLYCMBSPRICING, WEEKLYCMBSPRICINGMOCK
## 3. What each fixed-income dataset carries

- `20:48:27` AGENCYCMOPRICING                               HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"f811e204-3c20-49a2-99f3-2f147be3aeb2","message":"FIXED
- `20:48:28` AGENCYMARKETBREADTH                            HTTP 200 |  9 fields | per-security: none | trade: none | Trace Agency Market Breadth
- `20:48:28` AGENCYMARKETSENTIMENT                          HTTP 200 |  6 fields | per-security: none | trade: none | Trace Agency Market Sentiment
- `20:48:29` AGENCYMBSARMHYBRIDPRICING                      HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"5b91507e-b3a9-48ab-853a-99feaba84b3e","message":"FIXED
- `20:48:29` AGENCYMBSPRICING                               HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"8c6e4ee7-6bfe-49b8-b5ac-8cc94b343444","message":"FIXED
- `20:48:30` AGENCYMBSTRADINGACTIVITY                       HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"d39d2eb6-35b3-4baa-bc02-14f6e31e69e6","message":"FIXED
- `20:48:30` AGENCYTBAPRICING                               HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"603bb776-54fd-4b00-9681-222317e3d31c","message":"FIXED
- `20:48:30` COLLATERALIZEDOBLIGATIONPRICING                HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"24f5b9f4-3e72-466a-946c-f2c4ff0957d2","message":"FIXED
- `20:48:31` CORPORATE144AMARKETBREADTH                     HTTP 200 |  9 fields | per-security: none | trade: none | Trace Corporate 144A Market Breadth
- `20:48:31` CORPORATE144AMARKETSENTIMENT                   HTTP 200 |  6 fields | per-security: none | trade: none | Trace Corporate 144A Market Sentiment
- `20:48:32` CORPORATEMARKETBREADTH                         HTTP 200 |  9 fields | per-security: none | trade: none | Trace Corporate Market Breadth
- `20:48:32` CORPORATEMARKETSENTIMENT                       HTTP 200 |  6 fields | per-security: none | trade: none | Trace Corporate Market Sentiment
- `20:48:33` CORPORATESANDAGENCIESCAPPEDVOLUME              HTTP 200 | 47 fields | per-security: ['parMaturityLessThan5YearsQuantity', 'tradeMaturityLessThan5YearsCount', 'parMaturityGreaterThan5YearsLessThan10YearsQuantity', 'tradeMaturityGreaterThan5YearsLessThan10YearsCount', 'parMaturityGreaterThan10YearsLessThan25YearsQuantity', 'tradeMaturityGreaterThan10YearsLessThan25YearsCount', 'parMaturityGreaterThan25YearsQuantity', 'tradeMaturityGreaterThan25YearsCount'] | trade: ['totalVolumeQuantity', 'parLessThan5MillionQuantity', 'parGreaterThan5MillionLessThan10MillionQuantity', 'parGreaterThan10MillionLessThan25MillionQuantity', 'parGreaterThan25MillionQuantity', 'parMaturityLessThan5YearsQuantity'] | Trace Corporates and Agencies Capped Volume
- `20:48:33` DAILYCMBSPRICING                               HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"49587395-6172-4687-b19a-d73191960d05","message":"FIXED
- `20:48:34` NONAGENCYCMOABSPRICING                         HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"6afc3839-7c35-4a29-a0eb-57c405c0d975","message":"FIXED
- `20:48:34` NONAGENCYCMOVINTAGEPRICING                     HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"79ca38b6-0791-4b47-9912-93d265e45e55","message":"FIXED
- `20:48:35` SECURITIZEDPRODUCTERRATA                       HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"ca7cb00f-445a-4a46-916d-1063bace2a5b","message":"FIXED
- `20:48:35` SECURITIZEDPRODUCTSCAPPEDVOLUME                HTTP 200 |  6 fields | per-security: none | trade: none | Trace Securitized Products Capped Volume
- `20:48:35` SECURITIZEDPRODUCTTRADINGACTIVITY              HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"d87c61f2-1746-411e-84be-b4587ca2c45d","message":"FIXED
- `20:48:36` TRACEMONTHLYVOLUME                             HTTP 200 | 20 fields | per-security: none | trade: ['ATSVolumeQuantity', 'dealerVolumeQuantity', 'customerVolumeQuantity', 'totalVolumeQuantity', 'ATSAverageDailyVolumeQuantity', 'dealerAverageDailyVolumeQuantity'] | TRACE Monthly Volume
- `20:48:36` TREASURYDAILYAGGREGATES                        HTTP 200 |  9 fields | per-security: ['yearsToMaturity'] | trade: ['volumeWeightedAveragePrice'] | Treasury Daily Aggregates
- `20:48:37` TREASURYMONTHLYAGGREGATES                      HTTP 200 |  8 fields | per-security: ['yearsToMaturity'] | trade: none | Treasury Monthly Aggregates
- `20:48:37` WEEKLYCMBSPRICING                              HTTP 400 |  0 fields | per-security: none | trade: none |  {"statusCode":400,"statusDescription":"Bad Request","requestId":"c3a0ce09-6ea0-4555-8012-024cebfa4616","message":"FIXED
## 4. Asking for rows from each dataset that carries a per-security field

- `20:48:38` CORPORATESANDAGENCIESCAPPEDVOLUME: HTTP 200, 3 rows | columns: parMaturityGreaterThan25YearsQuantity, customerBuyParGreaterThan5YearsLessThan10YearsQuantity, tradeLessThan5MillionCount, interdealerParGreaterThan10yCountearsLessThan25YearsQuantity, interdealerTradeMaturityGreaterThan25YearsCount, tradeMaturityGreaterThan10YearsLessThan25YearsCount, interdealerParLessThan5yearsQuantity, interdealerTradeMaturityGreaterThan5yearsLessThan10YearsCount, customerBuyTradeMaturityLessThan5YearsCount, parGreaterThan10MillionLessThan25MillionQuantity, parMaturityGreaterThan10YearsLessThan25YearsQuantity, customerSellTradeMaturityGreaterThan25YearsCount, parMaturityGreaterThan5YearsLessThan10YearsQuantity, customerSellTradeMaturityGreaterThan5YearsLessThan10YearsCount, tradeGreaterThan25MillionCount, interdealerParGreaterThan25YearsQuantity, parLessThan5MillionQuantity, interdealerMaturityTradeLessThan5yearsCount, customerSellParGreaterThan10YearsLessThan25YearsQuantity, tradeReportDate, tradeGreaterThan5MillionLessThan10MillionCount, customerBuyTradeMaturityGreaterThan10YearsLessThan25YearsCount, customerBuyParGreaterThan25YearsQuantity, interdealerParGreaterThan5yearsLessThan10YearsQuantity, customerSellTradeMaturityGreaterThan10YearsLessThan25YearsCount, tradeYear, customerSellParGreaterThan25YearsQuantity, tradeGreaterThan10MillionLessThan25MillionCount, tradeMaturityLessThan5YearsCount, customerBuyParLessThan5YearsQuantity, parGreaterThan25MillionQuantity, tradeMonth, customerSellParLessThan5YearsQuantity, tradeMaturityGreaterThan25YearsCount, 144AFlag, parMaturityLessThan5YearsQuantity, customerBuyTradeMaturityGreaterThan5YearsLessThan10YearsCount, parGreaterThan5MillionLessThan10MillionQuantity, customerSellParGreaterThan5YearsLessThan10YearsQuantity, customerBuyTradeMaturityGreaterThan25YearsCount
- `20:48:38`   one row (identifiers held back): {"parMaturityGreaterThan25YearsQuantity": 30247406000, "customerBuyParGreaterThan5YearsLessThan10YearsQuantity": 17387890964.11, "tradeLessThan5MillionCount": 0, "interdealerParGreaterThan10yCountearsLessThan25YearsQuantity": 2405846000, "interdealerTradeMaturityGreaterThan25YearsCount": 302, "tradeMaturityGreaterThan10YearsLessThan25YearsCount": 2661, "interdealerParLessThan5yearsQuantity": 7771103000, "interdealerTradeMaturityGreaterThan5yearsLessThan10YearsCount": 521, "customerBuyTradeMaturityLessThan5YearsCount": 2141, "parGreaterThan10MillionLessThan25MillionQuantity": 87268409718, "parMaturityGreaterThan10YearsLessThan25YearsQuantity": 29137383854.9, "customerSellTradeMaturityGreaterT
- `20:48:38` TREASURYDAILYAGGREGATES: HTTP 200, 3 rows | columns: yearsToMaturity, dealerCustomerVolume, atsInterdealerCount, tradeDate, dealerCustomerCount, volumeWeightedAveragePrice, benchmark, atsInterdealerVolume, productCategory
- `20:48:38`   one row (identifiers held back): {"yearsToMaturity": null, "dealerCustomerVolume": 121.7, "atsInterdealerCount": 36873, "tradeDate": "2023-12-14", "dealerCustomerCount": 48268, "volumeWeightedAveragePrice": null, "benchmark": null, "atsInterdealerVolume": 66.1, "productCategory": "Bills"}
- `20:48:39` TREASURYMONTHLYAGGREGATES: HTTP 200, 3 rows | columns: beginningOfTheMonthDate, yearsToMaturity, dealerCustomerVolume, atsInterdealerCount, dealerCustomerCount, benchmark, atsInterdealerVolume, productCategory
- `20:48:39`   one row (identifiers held back): {"beginningOfTheMonthDate": "2024-01-01", "yearsToMaturity": null, "dealerCustomerVolume": 2362.1, "atsInterdealerCount": 753390, "dealerCustomerCount": 1043856, "benchmark": null, "atsInterdealerVolume": 1308.3, "productCategory": "Bills"}
## 5. Plausible names asked directly

- `20:48:44` trace                                      metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"b931a47f-f215-4656-bd90-22712851e337","message":"Unable to find specified me
- `20:48:49` traceCorporate                             metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"46735c85-f2ab-4434-8d04-f63bc1857672","message":"Unable to find specified me
- `20:48:53` corporateTrades                            metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"bc80e929-fbad-4e87-97ca-294f63089fcc","message":"Unable to find specified me
- `20:48:54` corporateAndAgencyTradeActivity            metadata HTTP 200 | rows HTTP 403: {"statusCode":403,"statusDescription":"Forbidden","requestId":"9dd176aa-54c0-4aa2-8523-57bf3488464c","message":"(our client id, held back) does not have permission to
- `20:48:58` corporatesAndAgenciesTradeActivity         metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"daee102e-6beb-4482-a820-df32f43a3bc9","message":"Unable to find specified me
- `20:49:02` corporate144ATradeActivity                 metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"9b3bee1d-8bb9-41dc-8ef4-627db8e0f506","message":"Unable to find specified me
- `20:49:07` corporateBondTradeHistory                  metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"5d8e6c12-63a8-4627-976d-2e41a67052cb","message":"Unable to find specified me
- `20:49:12` tradeHistory                               metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"c7cea44a-a918-40e8-83a6-7acd5c7bc5f8","message":"Unable to find specified me
- `20:49:12` corporateAndAgencySecurities               metadata HTTP 200 | rows HTTP 403: {"statusCode":403,"statusDescription":"Forbidden","requestId":"33c46270-a217-45de-8a6a-ad49091ae1f5","message":"(our client id, held back) does not have permission to
- `20:49:17` bondSecurities                             metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"0b03d5cd-c537-40fb-8638-b610a2fd464e","message":"Unable to find specified me
- `20:49:21` corporateDebtLastSale                      metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"09977458-7244-4b3d-8158-00baea1c5785","message":"Unable to find specified me
- `20:49:26` corporate144ALastSale                      metadata HTTP 404 |  {"statusCode":404,"statusDescription":"Not Found","requestId":"c8de1d1f-3ece-4f3b-9f03-881131ceb766","message":"Unable to find specified me
## Answer

- `20:49:26` signed in: signed in with client credentials (a token came back; not shown)
- `20:49:26` fixed-income datasets seen: 46 | carrying a per-security field: ['CORPORATESANDAGENCIESCAPPEDVOLUME', 'TREASURYDAILYAGGREGATES', 'TREASURYMONTHLYAGGREGATES'] | bond-by-bond rows actually returned from: ['CORPORATESANDAGENCIESCAPPEDVOLUME', 'TREASURYDAILYAGGREGATES', 'TREASURYMONTHLYAGGREGATES']
- `20:49:26` ✅ probe complete
