# ops 3952 — manuals via pypdf -> getDataLayer spec -> loans code

**Status:** success  
**Duration:** 6.1s  
**Finished:** 2026-07-27T02:46:55+00:00  

## Log
- `02:46:52`   en manual: 857117b -> 39158 chars, 27 pages
- `02:46:54`   jp manual: 1035777b -> 18658 chars, 22 pages
## getDataLayer spec from the manual

- `02:46:54`   ── ctx ──
 ¶ Specified  ¶ Conditions  ¶ Notes  ¶ /getDataCode  ¶ Time-Series  ¶ Data  ¶ Series Code -  ¶ /getDataLayer Layer  ¶ information  ¶ Layer information arrange the data series  ¶ for individual databases (DBs) in a  ¶ hierarchical tree structure.  ¶ /getMetadata Metadata DB Name  ¶ Metadata refers to information about the  ¶ attributes of time-series data, such as series  ¶ codes and series names. This Metadata can  ¶ be utilized to create parameters for  ¶ '/getDataCode' and '/getDataLayer.'  ¶   ¶ 2. How to Request  ¶ Users of the API can specify their various conditions for the desired data as parameters in  ¶ the request URL. The parameters may include the output file format (JSON or CSV) and  ¶ language (Japanese or English).  ¶   ¶ Example URL  
- `02:46:54`   ── ctx ──
. This Metadata can  ¶ be utilized to create parameters for  ¶ '/getDataCode' and '/getDataLayer.'  ¶   ¶ 2. How to Request  ¶ Users of the API can specify their various conditions for the desired data as parameters in  ¶ the request URL. The parameters may include the output file format (JSON or CSV) and  ¶ language (Japanese or English).  ¶   ¶ Example URL  ¶ https://www.stat-search.boj.or.jp/api/v1/getDataCode?format=json&lang=en&db=CO ¶ &startDate=202401&endDate=202504&code=TK99F1000601GCQ01000,TK99F20 ¶ 00601GCQ01000  ¶     ¶   ¶ 3  ¶   ¶ The API has a compression feature that compresses HTTP responses in gzip  format to  ¶ reduce data transmission size. When making requests using tools or programs, setting the  ¶ "Accept-Encoding: gzip" header 
- `02:46:54`   ── ctx ──
tics  ¶   ¶ 4. Timing for Data Availability via API  ¶ The APIs, '/getDataCode' and '/getDataLayer', provide access to time-series data equivalent  ¶ to that which can be retrieved through the search screen(a). Additionally, the metadata files  ¶ accessible via '/getMetadata' are updated regularly on a daily basis.  ¶   ¶ (a) On the BOJ Time-Series Data Search, time-series data are made available at around 8:50 a.m.  ¶ The schedule is subject to change. For the release schedule of individual statistics, please refer  ¶ to the "Schedule for Releases of Statistical Data." (b)  ¶ (b) https://www.boj.or.jp/en/statistics/outline/index.htm  ¶   ¶ 5. API Request URL Tool  ¶ When creating request URLs for each API, it is recommended that users refer to this 
- `02:46:54`   ── ctx ──
taCode    ¶ https://www.stat -search.boj.or.jp/api/ v1/getDataCode? <Parameter>  ¶ /getDataLayer    ¶ https://www.stat -search.boj.or.jp/api/ v1/getDataLayer? <Parameter>  ¶ /getMetadata    ¶ https://www.stat -search.boj.or.jp/api/ v1/getMetadata? <Parameter>  ¶   ¶ 2. Parameter Specification  ¶ When sending a request , each API requires parameters to be specified, as described in  ¶ "<Parameter>" in "II.1. Structure of the Request URL."  ¶ To specify a parameter, combine the Parameter Name and its value using the equals "=," in  ¶ the format "Parameter Name=value." If multiple parameters need to be specified, connect  ¶ each parameter using the ampersand "&," as in "Parameter Name =value&Parameter  ¶ Name=value&...." For detailed information on Para
## param names + db table + loans mentions

- `02:46:54`   param-ish tokens: ['code', 'endDate', 'format', 'frequency', 'lang', 'layer', 'startDate', 'startPosition', 'uency']
- `02:46:54`   loan ctx: … Database Name DB  Name Database Name  Interest Rates on Deposits and Loans   IR01 The Basic Discount Rates and Basic  Loan Rates (Previously Indicated as  "Official Discount Rates")  IR03 Average Int…
- `02:46:54`   loan ctx: …ates on Deposits and Loans   IR01 The Basic Discount Rates and Basic  Loan Rates (Previously Indicated as  "Official Discount Rates")  IR03 Average Interest Rates on Time  Deposits by Term   IR02 Aver…
- `02:46:54`   loan ctx: …utions by Type of  Deposit  IR04 Average Contract Inter est Rates on  Loans and Discounts  Financial Markets   FM01 Uncollateralized Overnight Call Rate  (average) (Updated every business  day)  FM06 …
- `02:46:54`   loan ctx: …Settlement  Systems  PS02 Basic Figures on Fails  Money, Deposits and Loans   MD01 Monetary Base MD11 Deposits, Vault Cash, and Loans and  Bills Discounted    MD02 Money Stock    MD03 Monetary Survey …
- `02:46:54`   loan ctx: …eposits and Loans   MD01 Monetary Base MD11 Deposits, Vault Cash, and Loans and  Bills Discounted    MD02 Money Stock    MD03 Monetary Survey  MD12 Deposits, Vault Cash, and Loans and  Bills Discounte…
- `02:46:54`   loan ctx: …2 Money Stock    MD03 Monetary Survey  MD12 Deposits, Vault Cash, and Loans and  Bills Discounted by Prefecture  (Domestically Licensed Banks) MD04 (Reference) Changes in Money Stock  (M2+CDs) and Cre…
- `02:46:54`   loan ctx: …e Deposits: Amounts Outstanding  and New Deposits by Maturity    LA01 Loans and Bills Discounted by Sector    MD07 Reserves  LA02 Loans and Discounts by the Bank of  Japan    MD08 BOJ Current Account …
- `02:46:54`   loan ctx: …y    LA01 Loans and Bills Discounted by Sector    MD07 Reserves  LA02 Loans and Discounts by the Bank of  Japan    MD08 BOJ Current Account Balances by  Sector   LA03 Outstanding of Loans (Others)   M…
- `02:46:54`   db= values in text: ['CO', 'FM01']
## call getDataLayer with learned params + drill

- `02:46:55`   still no 200 — manual ctx above should now show the exact required params for the next pass
- `02:46:55` ✅ MANUAL READ COMPLETE
