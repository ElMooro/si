# a. openapi catalog

**Status:** success  
**Duration:** 18.6s  
**Finished:** 2026-08-17T23:29:10+00:00  

## Log
- `23:28:51` ✅   /openapi/ HTTP 200 bytes=2677
- `23:28:54` ✅   /openapi/v1/swagger.json HTTP 200 bytes=23902
- `23:28:57` ✅   /openapi/swagger.json HTTP 200 bytes=476923
# b. candidates

- `23:29:00` ✅   tpex_3insti_trading_vol HTTP 200 bytes=23902 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, use
- `23:29:01` ✅   tpex_3insti_summary HTTP 200 bytes=1014 head=[
{"Date":"1150817","Investor":"外資及陸資合計","PurchaseAmount":"49780578913","SaleAmount":"54439527047","Net":"-4658948134"},
{"Date":"1150817","Investor":"　外資及陸資(不含自營商)","PurchaseAmoun
- `23:29:02` ✅   tpex_3insti_trading_summary HTTP 200 bytes=23902 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, use
- `23:29:04` ✅   tpex_qfii_trading_vol HTTP 200 bytes=23902 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, use
- `23:29:06` ✅   tpex_3insti_daily_summary HTTP 200 bytes=23902 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, use
# c. zh aggregate value endpoint

- `23:29:07` ✅   /www/zh-tw/insti/3insti?type=Daily&response=js HTTP 200 bytes=10892 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>404 - 證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, user-scalable=no"
- `23:29:09` ✅   /web/stock/3insti/3insti_summary/3itrade_sum_r HTTP 200 bytes=10892 head=<!DOCTYPE html><html lang="zh-Hant-tw"><head><title>404 - 證券櫃檯買賣中心</title><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1, user-scalable=no"
# verdict

- `23:29:10` ✅ value-endpoint evidence above (7 answers) -- wire binds only a NT$-denominated field, else shares w/ unit
