# 1. CBC BPP2Q01en recursive shape walk

**Status:** success  
**Duration:** 15.3s  
**Finished:** 2026-08-17T17:33:50+00:00  

## Log
- `17:33:37` ✅ HTTP 200 bytes=577401
- `17:33:37`   meta: {"title": "Balance of Payments By Period", "sender": "Central Bank of the Republic of China (Taiwan)", "prepared": "2026/8/18 上午 01:33:36", "filename": "BPP2Q01en.px", "links": "https://cpx.cbc.gov.tw/api/DataAPI/Get?FileName=BPP2Q01en", "note": "The symbol '─' for an amount denotes the figure is not available or less than a half unit.", "refperiod": "Please refer to <a href='https://eng.stat.gov.tw/News_NoticeCalendar_EN.aspx?n=4011&Dept=A59000000N'>“Advance release calendar”</a>.", "last_updat...
- `17:33:37`   data type=dict len=2
- `17:33:37` ✅   portfolio/liabilit string hits: 41
- `17:33:37`    @data/structure/Table1/96/data -> Primary income-Investment income-Portfolio investment income-Net Value
- `17:33:37`    @data/structure/Table1/97/data -> Primary income-Investment income-Portfolio investment income-Credit
- `17:33:37`    @data/structure/Table1/98/data -> Primary income-Investment income-Portfolio investment income-Debit
- `17:33:37`    @data/structure/Table1/99/data -> Primary income-Investment income-Portfolio investment income-Inv. income on equity and invest. fund shares-Net Value
- `17:33:37`    @data/structure/Table1/100/data -> Primary income-Investment income-Portfolio investment income-Inv. income on equity and invest. fund shares-Credit
- `17:33:37`    @data/structure/Table1/101/data -> Primary income-Investment income-Portfolio investment income-Inv. income on equity and invest. fund shares-Debit
- `17:33:37`    @data/structure/Table1/102/data -> Primary income-Investment income-Portfolio investment income-Interest-Net Value
- `17:33:37`    @data/structure/Table1/103/data -> Primary income-Investment income-Portfolio investment income-Interest-Credit
- `17:33:37`    @data/structure/Table1/104/data -> Primary income-Investment income-Portfolio investment income-Interest-Debit
- `17:33:37`    @data/structure/Table1/143/data -> Financial account(exc. Reserve assets)-Liabilities
- `17:33:37`    @data/structure/Table1/146/data -> Direct investment-Liabilities
- `17:33:37`    @data/structure/Table1/149/data -> Direct investment- Equity and investment fund shares -Liabilities
- `17:33:37`    @data/structure/Table1/152/data -> Direct investment- Equity and investment fund shares-Equity other than reinvestment of earnings-Liabilities
- `17:33:37`    @data/structure/Table1/155/data -> Direct investment- Equity and investment fund shares-Reinvestment of earnings-Liabilities
- `17:33:37`    @data/structure/Table1/158/data -> Direct investment-Debt instruments-Liabilities
- `17:33:37`    @data/structure/Table1/159/data -> Portfolio investment-Balance
- `17:33:37`   FULL ROW data[structure]: {"Table1": [{"data": "Current account-Net Value"}, {"data": "Current account-Credit"}, {"data": "Current account-Debit"}, {"data": "Goods-Net Value"}, {"data": "Goods-Credit"}, {"data": "Goods-Debit"}, {"data": "Goods-General merchandise-Net Value"}, {"data": "Goods-General merchandise-Credit"}, {"data": "Goods-General merchandise-Debit"}, {"data": "Goods-Net exports of goods under merchanting-Net Value"}, {"data": "Goods-Net exports of goods under merchanting-Credit"}, {"data": "Goods-Net expor...
# 2. TWSE rwd daily foreign-flow endpoints

- `17:33:39` ✅   rwd/en/fund/BFI82U?response=json               HTTP 200 keys=['data', 'date', 'fields', 'hints', 'notes', 'params', 'stat', 'title'] stat=OK
- `17:33:39`     fields: ["Item", "Total Buy", "Total Sell", "Difference"]
- `17:33:39`     rows=6 row0: ["Dealers (Proprietary)", "11,405,300,339", "7,865,150,499", "3,540,149,840"]
- `17:33:39`     date=20260817 title=2026/08/17 Trading Value of Foreign & Other Investors
- `17:33:40` ✅   rwd/en/fund/BFI82U?dayDate=20260814&type=day   HTTP 200 keys=['data', 'date', 'fields', 'hints', 'notes', 'params', 'stat', 'title'] stat=OK
- `17:33:40`     fields: ["Item", "Total Buy", "Total Sell", "Difference"]
- `17:33:40`     rows=6 row0: ["Dealers (Proprietary)", "11,160,429,833", "13,896,418,209", "-2,735,988,376"]
- `17:33:40`     date=20260814 title=2026/08/14 Trading Value of Foreign & Other Investors
- `17:33:42` ✅   rwd/en/fund/TWT38U?response=json               HTTP 200 keys=['data', 'date', 'fields', 'groups', 'notes', 'stat', 'title', 'total'] stat=OK
- `17:33:42`     fields: ["", "Security Code", "Total Buy", "Total Sell", "Difference", "Total Buy", "Total Sell", "Difference", "Total Buy", "Total Sell", "Difference"]
- `17:33:42`     rows=1357 row0: [" ", "00403A", "68,861,020", "14,041,150", "54,819,870", "0", "0", "0", "68,861,020", "14,041,150", "54,819,870"]
- `17:33:42`     date=20260817 title=2026/08/17 Trading Volume of Foreign Investors include Mainland Area Investors (...
- `17:33:44` ✅   en/fund/BFI82U?response=json                   HTTP 200 keys=['data', 'date', 'fields', 'hints', 'notes', 'params', 'stat', 'title'] stat=OK
- `17:33:44`     fields: ["Item", "Total Buy", "Total Sell", "Difference"]
- `17:33:44`     rows=6 row0: ["Dealers (Proprietary)", "11,405,300,339", "7,865,150,499", "3,540,149,840"]
- `17:33:44`     date=20260817 title=2026/08/17 Trading Value of Foreign & Other Investors
# 3. IMF BOP dataflow + dimensions

- `17:33:45` ✅ dataflows total=222
- `17:33:45` ✅ BOP-ish dataflows: 6
- `17:33:45`   BOP_2026_JAN_VINTAGE (IMF.STA v1.0.0) 'Balance of Payments (BOP) 2026 January'
- `17:33:45`   BOP (IMF.STA v21.0.0) 'Balance of Payments (BOP)'
- `17:33:45`   BOP_2026_FEB_VINTAGE (IMF.STA v1.0.0) 'Balance of Payments (BOP) 2026 February'
- `17:33:45`   BOP_2026_MAY_VINTAGE (IMF.STA v1.0.0) 'Balance of Payments (BOP) 2026 May'
- `17:33:45`   BOP_2026_APR_VINTAGE (IMF.STA v1.0.0) 'Balance of Payments (BOP) 2026 April'
- `17:33:45`   BOP_AGG (IMF.STA v9.0.1) 'Balance of Payments and International Investment Position Statistics (BOP/IIP), World and ...'
- `17:33:45`   DSD_BOP_2026_JAN_VINTAGE try -> HTTP 204 json=False
- `17:33:50`   datastructure/all -> HTTP 200 bytes=2141208
# 4. verdict

- `17:33:50` ✅ probes answered: ['cbc', 'imf', 'twse'] -- wire only what is proven
