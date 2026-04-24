# Test replacement price endpoints

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-04-24T23:15:31+00:00  

## Log
## Ticker SPY

- `23:15:29`   Polygon /prev:      status=200 ✓
- `23:15:29`     body: {"ticker":"SPY","queryCount":1,"resultsCount":1,"adjusted":true,"results":[{"T":"SPY","v":5.617399e+07,"vw":708.4177,"o":709.5,"c":708.45,"h":712.3598,"l":702.2803,"t":1776974400000,"n":735243}],"status":"OK","request_id":"b6b4036ed94d9da4761fd02aa0e
- `23:15:29`   Polygon /range/1/day: status=200 ✓
- `23:15:29`     body: {"ticker":"SPY","queryCount":6,"resultsCount":6,"adjusted":true,"results":[{"v":7.0661926525697e+07,"vw":708.8462,"o":706.14,"c":710.14,"h":712.39,"l":705.76,"t":1776398400000,"n":796355},{"v":4.3531639102279e+07,"vw":708.4344,"o":708.78,"c":708.72,"
- `23:15:30`   FMP v3 /quote:       status=403 ✗
- `23:15:30`   FMP /stable/quote:   status=200 ✓
- `23:15:30`     body: [
  {
    "symbol": "SPY",
    "name": "State Street SPDR S&P 500 ETF Trust",
    "price": 713.94,
    "changePercentage": 0.77493,
    "change": 5.49,
    "volume": 44463821,
    "dayLow": 709.01,
    "dayHigh": 714.47,
    "yearHigh": 714.47,
    "
## Ticker AAPL

- `23:15:30`   Polygon /prev:      status=200 ✓
- `23:15:30`     body: {"ticker":"AAPL","queryCount":1,"resultsCount":1,"adjusted":true,"results":[{"T":"AAPL","v":3.3399639e+07,"vw":273.5556,"o":275.045,"c":273.43,"h":275.77,"l":271.65,"t":1776974400000,"n":551288}],"status":"OK","request_id":"62a9c9ae4f27d0c5ab6547960f
- `23:15:30`   Polygon /range/1/day: status=200 ✓
- `23:15:30`     body: {"ticker":"AAPL","queryCount":6,"resultsCount":6,"adjusted":true,"results":[{"v":6.1436228189065e+07,"vw":269.8735,"o":266.96,"c":270.23,"h":272.3,"l":266.72,"t":1776398400000,"n":723488},{"v":3.6582599341082e+07,"vw":272.6888,"o":270.33,"c":273.05,"
- `23:15:30`   FMP v3 /quote:       status=403 ✗
- `23:15:30`   FMP /stable/quote:   status=200 ✓
- `23:15:30`     body: [
  {
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "price": 271.06,
    "changePercentage": -0.86677,
    "change": -2.37,
    "volume": 38033227,
    "dayLow": 269.67,
    "dayHigh": 273.06,
    "yearHigh": 288.62,
    "yearLow": 193.25,
    
## Ticker GLD

- `23:15:30`   Polygon /prev:      status=200 ✓
- `23:15:30`     body: {"ticker":"GLD","queryCount":1,"resultsCount":1,"adjusted":true,"results":[{"T":"GLD","v":4.976399e+06,"vw":432.6214,"o":433.96,"c":431.04,"h":435.2913,"l":428.22,"t":1776974400000,"n":158328}],"status":"OK","request_id":"59a6756a3a60f9a9567cc4205971
- `23:15:30`   Polygon /range/1/day: status=200 ✓
- `23:15:30`     body: {"ticker":"GLD","queryCount":6,"resultsCount":6,"adjusted":true,"results":[{"v":9.711965300417e+06,"vw":446.6099,"o":445.65,"c":445.93,"h":448.7,"l":445.32,"t":1776398400000,"n":225356},{"v":8.471565783876e+06,"vw":441.8385,"o":443.13,"c":442.09,"h":
- `23:15:30`   FMP v3 /quote:       status=403 ✗
- `23:15:31`   FMP /stable/quote:   status=200 ✓
- `23:15:31`     body: [
  {
    "symbol": "GLD",
    "name": "SPDR Gold Shares",
    "price": 433.25,
    "changePercentage": 0.51271,
    "change": 2.21,
    "volume": 5880085,
    "dayLow": 430.6501,
    "dayHigh": 435.35,
    "yearHigh": 509.7,
    "yearLow": 291.78,
 
## Crypto fallback: CoinGecko

- `23:15:31`   CoinGecko BTC-USD: status=200 ✓
- `23:15:31`     body: {"bitcoin":{"usd":77405}}
- `23:15:31`   CoinGecko ETH-USD: status=200 ✓
- `23:15:31`     body: {"ethereum":{"usd":2315.05}}
- `23:15:31` Done
