# a. CBC values container + period axis

**Status:** failure  
**Duration:** 3.9s  
**Finished:** 2026-08-17T17:37:30+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:37:28` ✅ data keys: ['dataSets', 'structure']
- `17:37:28` structure keys: ['Table1']
- `17:37:28`   struct.Table1 len=402 head=[{"data": "Current account-Net Value"}, {"data": "Current account-Credit"}, {"data": "Current account-Debit"}] periodish=0
- `17:37:28` values key 'dataSets' type=list len=169
- `17:37:28`   v[0]: ["1984Q1", "1713.000", "7787.000", "6074.000", "2028.000", "6752.000", "4724.000", "2028.000", "6752.000", "4724.000", "-", "-", "-", "-", "-", "-", "-509.000", "601.000", "1110.000", "-", "-", "-", "-8.000", "-", "8.000", "-214.000", "177.000", "391.000", "-1...
- `17:37:28`   v[1]: ["1984Q2", "1928.000", "9223.000", "7295.000", "2643.000", "8171.000", "5528.000", "2643.000", "8171.000", "5528.000", "-", "-", "-", "-", "-", "-", "-698.000", "628.000", "1326.000", "-", "-", "-", "...
- `17:37:28`   v[-1]: ["2026Q1", "62529.000", "215349.000", "152820.000", "58005.000", "180677.000", "122672.000", "54322.000", "175224.000", "120902.000", "4995.000", "4995.000", "-", "-1312.000", "458.000", "1770.000", "...
- `17:37:28` Portfolio block labels 159..182:
- `17:37:28`   [159] Portfolio investment-Balance
- `17:37:28`   [160] Portfolio investment-Assets
- `17:37:28`   [161] Portfolio investment-Liabilities
- `17:37:28`   [162] Portfolio investment-Equity and investment fund shares-Balance
- `17:37:28`   [163] Portfolio investment-Equity and investment fund shares-Assets
- `17:37:28`   [164] Portfolio investment-Equity and investment fund shares-Liabilities
- `17:37:28`   [165] Portfolio investment-Equity and investment fund shares-Central bank-Balance
- `17:37:28`   [166] Portfolio investment-Equity and investment fund shares-Central bank-Assets
- `17:37:28`   [167] Portfolio investment-Equity and investment fund shares-Central bank-Liabilities
- `17:37:28`   [168] Portfolio investment-Equity and investment fund shares-Deposit-taking corporations, exc. the CBC-Balance
- `17:37:28`   [169] Portfolio investment-Equity and investment fund shares-Deposit-taking corporations, exc. the CBC-Assets
- `17:37:28`   [170] Portfolio investment-Equity and investment fund shares-Deposit-taking corporations, exc. the CBC-Liabilities
- `17:37:28`   [171] Portfolio investment-Equity and investment fund shares-General government-Balance
- `17:37:28`   [172] Portfolio investment-Equity and investment fund shares-General government-Assets
- `17:37:28`   [173] Portfolio investment-Equity and investment fund shares-General government-Liabilities
- `17:37:28`   [174] Portfolio investment-Equity and investment fund shares-Other sectors-Balance
- `17:37:28`   [175] Portfolio investment-Equity and investment fund shares-Other sectors-Assets
- `17:37:28`   [176] Portfolio investment-Equity and investment fund shares-Other sectors-Liabilities
- `17:37:28`   [177] Portfolio investment-Equity and investment fund shares--Other sectors-Other financial corporations-Balance
- `17:37:28`   [178] Portfolio investment-Equity and investment fund shares--Other sectors-Other financial corporations-Assets
- `17:37:28`   [179] Portfolio investment-Equity and investment fund shares--Other sectors-Other financial corporations-Liabilities
- `17:37:28`   [180] Debt securities-Balance
- `17:37:28`   [181] Debt securities-Assets
- `17:37:28`   [182] Debt securities-Liabilities
# b. TWSE BFI82U all rows verbatim

- `17:37:29`   ["Dealers (Proprietary)", "11,405,300,339", "7,865,150,499", "3,540,149,840"]
- `17:37:29`   ["Dealers (Hedge)", "35,764,664,555", "37,899,511,479", "-2,134,846,924"]
- `17:37:29`   ["Securities Investment Trust Companies", "26,085,051,545", "43,861,902,730", "-17,776,851,185"]
- `17:37:29`   ["Foreign Investors include Mainland Area Investors(Foreign Dealers excluded)", "369,663,152,041", "324,215,867,015", "45,447,285,026"]
- `17:37:29`   ["Foreign Dealers", "0", "0", "0"]
- `17:37:29`   ["Total", "442,918,168,480", "413,842,431,723", "29,075,736,757"]
- `17:37:29`   notes: ["Dealers mean Dealers’ proprietary account.", "Securities Investment Trust Companies mean domestic mutual funds managed by Securities Investment Trust Companies.", "Foreign Investors are defined by Regulations Governing Investment in Secur...
# c. IMF DSD for BOP (IMF.STA v21)

- `17:37:30`   datastructure/IMF.STA/DSD_BOP -> HTTP 200 json=True bytes=15036
- `17:37:30` ✅   DIMENSION ORDER: ['COUNTRY', 'BOP_ACCOUNTING_ENTRY', 'INDICATOR', 'UNIT', 'FREQUENCY']
# verdict

- `17:37:30` ✗ unresolved: {'periods': False} -- cannot wire v1.1 blind
