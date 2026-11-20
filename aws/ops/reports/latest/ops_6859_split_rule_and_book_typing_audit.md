# ops 6859 - the split rule and the factor book's typing, before the fix 2026-11-20T15:33+00:00 (read-only)

**Status:** success  
**Duration:** 385.6s  
**Finished:** 2026-11-20T15:39:33+00:00  

## Data

| book_hits | campaign_scan_tickers | leaders_tickers | strength_tickers |
|---|---|---|---|
| 56 | 15 | 0 | 0 |

## Log
## 1. the factor book: what other_issues.py would move out of common stock

- `15:33:09` book rows 27123, common stock by its flags 5333; would be moved out: 56 ({"is_debt_or_structured": 32, "is_preferred": 20, "is_warrant_unit": 4}); Polygon reference rows 13263
- `15:33:09`   ADAMG   -> is_debt_or_structured  a note by its own name                                         | book: Adamas Trust, Inc. - 9.125% Senior Notes Due 2030 | NASDAQ | type CS | $vol 518475 | mcap 471M | Polygon: Adamas Trust, Inc. 9.125% Senior Notes Due 2030
- `15:33:09`   ADAMH   -> is_debt_or_structured  a note or bond of ADAM (NASDAQ fifth letter H, under a tenth o | book: Adamas Trust, Inc. | NASDAQ | type CS | $vol 709233 | mcap 470M | Polygon: Adamas Trust, Inc. 9.875% Senior Notes Due 2030
- `15:33:09`   ADAMI   -> is_debt_or_structured  a note by its own name                                         | book: Adamas Trust Inc. 9.125% Sr. Notes due 2029 | NASDAQ | type CS | $vol 238013 | mcap 473M | Polygon: Adamas Trust, Inc. 9.125% Senior Notes Due 2029
- `15:33:09`   ADAMK   -> is_debt_or_structured  a note by its own name                                         | book: Adamas Trust, Inc. 9.600% Senior Notes due 2031 | NASDAQ | type CS | $vol 708554 | mcap None | Polygon: Adamas Trust, Inc. 9.600% Senior Notes Due 2031
- `15:33:09`   ADAMO   -> is_debt_or_structured  a note by its own name                                         | book: Adamas Trust, Inc. 9.250% Senior Notes Due 2031 | NASDAQ | type CS | $vol 613699 | mcap 688M | Polygon: Adamas Trust, Inc. 9.250% Senior Notes Due 2031
- `15:33:09`   AGNCL   -> is_debt_or_structured  a note or bond of AGNC (NASDAQ fifth letter L, under a tenth o | book: AGNC Investment Corp. | NASDAQ | type CS | $vol 429718 | mcap 10483M | Polygon: AGNC Investment Corp. Depositary Shares Each Representing a 1/1,000th 
- `15:33:09`   ATLCL   -> is_debt_or_structured  a note by its own name                                         | book: Atlanticus Holdings Corporation 6.125% Senior Notes due 2026 | NASDAQ | type CS | $vol 327726 | mcap 1212M | Polygon: Atlanticus Holdings Corporation 6.125% Senior Notes due 2026
- `15:33:09`   ATLCZ   -> is_debt_or_structured  a note by its own name                                         | book: Atlanticus Holdings Corporation 9.25% Senior Notes due 2029 | NASDAQ | type CS | $vol 525947 | mcap 1218M | Polygon: Atlanticus Holdings Corporation 9.25% Senior Notes due 2029
- `15:33:09`   BHFAL   -> is_debt_or_structured  a note or bond of BHF (NASDAQ fifth letter L, under a tenth of | book: Brighthouse Financial, Inc. | NASDAQ | type CS | $vol 546817 | mcap 2524M | Polygon: Brighthouse Financial, Inc. 6.25% Junior Subordinated Debentures due 2
- `15:33:09`   CGABL   -> is_debt_or_structured  a note by its own name                                         | book: The Carlyle Group Inc. 4.625% Subordinated Notes due 2061 | NASDAQ | type CS | $vol 490527 | mcap 5094M | Polygon: The Carlyle Group Inc. 4.625% Subordinated Notes due 2061
- `15:33:09`   CTGG    -> is_debt_or_structured  a note by its own name                                         | book: Qwest Corporation 6.500% Senior Notes due 2051 | NYSE | type CS | $vol 1116643 | mcap 243M | Polygon: Qwest Corporation 6.500% Senior Notes due 2051
- `15:33:09`   CTHH    -> is_debt_or_structured  a note by its own name                                         | book: Qwest Corporation 6.750% Senior Notes due 2052 | NYSE | type CS | $vol 514468 | mcap 188M | Polygon: Qwest Corporation 6.750% Senior Notes due 2052
- `15:33:09`   DHCNI   -> is_debt_or_structured  a note by its own name                                         | book: Diversified Healthcare Trust, 5.625% Senior Notes due 2042 | NASDAQ | type CS | $vol 1201986 | mcap 1798M | Polygon: Diversified Healthcare Trust 5.625% Senior Notes due 2042
- `15:33:09`   EMP     -> is_debt_or_structured  a note by its own name                                         | book: Entergy Mississippi, Inc. 1M BD 66 | NYSE | type CS | $vol 329076 | mcap 156M | Polygon: Entergy Mississippi, LLC First Mortgage Bonds, 4.90% Series due Octobe
- `15:33:09`   HBANL   -> is_debt_or_structured  a note or bond of HBAN (NASDAQ fifth letter L, under a tenth o | book: Huntington Bancshares Incorporated | NASDAQ | type CS | $vol 1036600 | mcap 33935M | Polygon: Huntington Bancshares Incorporated Depositary Shares, Each Representin
- `15:33:09`   HBANZ   -> is_debt_or_structured  a note or bond of HBAN (NASDAQ fifth letter Z, under a tenth o | book: Huntington Bancshares Incorporated | NASDAQ | type CS | $vol 290340 | mcap 31284M | Polygon: Huntington Bancshares Incorporated Depositary Shares, Each Representin
- `15:33:09`   JSM     -> is_debt_or_structured  a note by its own name                                         | book: Navient Corporation SR NT 6% 121543 | NASDAQ | type CS | $vol 484819 | mcap 4207M | Polygon: Navient LLC 6% Senior Notes due December 15, 2043
- `15:33:09`   MBINL   -> is_debt_or_structured  a note or bond of MBIN (NASDAQ fifth letter L, under a tenth o | book: Merchants Bancorp | NASDAQ | type CS | $vol 453883 | mcap 2416M | Polygon: Merchants Bancorp Depositary Shares, Each Representing a 1/40thInteres
- `15:33:09`   METCI   -> is_debt_or_structured  a note or bond of METC (NASDAQ fifth letter I, under a tenth o | book: Ramaco Resources, Inc. | NASDAQ | type CS | $vol 158152 | mcap None | Polygon: Ramaco Resources, Inc. 8.250% Senior Notes due 2030
- `15:33:09`   METCZ   -> is_debt_or_structured  a note or bond of METC (NASDAQ fifth letter Z, under a tenth o | book: Ramaco Resources, Inc. | NASDAQ | type CS | $vol 229350 | mcap 355M | Polygon: Ramaco Resources, Inc. 8.375% Senior Notes due 2029
- `15:33:09`   NEWTG   -> is_debt_or_structured  a note by its own name                                         | book: NewtekOne, Inc. 8.50% Fixed Rate Senior Notes due 2029 | NASDAQ | type CS | $vol 123394 | mcap 303M | Polygon: NewtekOne, Inc. 8.50% Fixed Rate Senior Notes due 2029
- `15:33:09`   NEWTH   -> is_debt_or_structured  a note or bond of NEWT (NASDAQ fifth letter H, under a tenth o | book: NewtekOne, Inc. | NASDAQ | type CS | $vol 182250 | mcap 300M | Polygon: NewtekOne, Inc. 8.625% Fixed Rate Senior Notes due 2029
- `15:33:09`   NEWTI   -> is_debt_or_structured  a note by its own name                                         | book: NewtekOne, Inc. 8.00% Fixed Rate Senior Notes due 2028 | NASDAQ | type CS | $vol 54428 | mcap 303M | Polygon: NewtekOne, Inc. 8.00% Fixed Rate Senior Notes due 2028
- `15:33:09`   NSLRL   -> is_debt_or_structured  a note or bond of NSLR (NASDAQ fifth letter L, under a tenth o | book: Neostellar Capital Corp. | NASDAQ | type CS | $vol None | mcap 667M | Polygon: Neostellar Capital Corp. 6.00% Notes due 2026
- `15:33:09`   OXSQG   -> is_debt_or_structured  a note or bond of OXSQ (NASDAQ fifth letter G, under a tenth o | book: Oxford Square Capital Corp. | NASDAQ | type CS | $vol None | mcap 96M | Polygon: Oxford Square Capital Corp. 5.50% Notes due 2028
- `15:33:09`   OXSQH   -> is_debt_or_structured  a note or bond of OXSQ (NASDAQ fifth letter H, under a tenth o | book: Oxford Square Capital Corp. | NASDAQ | type CS | $vol 96520 | mcap 96M | Polygon: Oxford Square Capital Corp. 7.75% Notes due 2030
- `15:33:09`   PRHIZ   -> is_debt_or_structured  a note by its own name                                         | book: Presurance Holdings, Inc. 9.75% Senior Unsecured Notes due 2028 | NASDAQ | type CS | $vol None | mcap 20M | Polygon: Presurance Holdings, Inc. 9.75% Senior Unsecured Notes due 2028
- `15:33:09`   RILYG   -> is_debt_or_structured  a note by its own name                                         | book: BRC Group Holdings Inc. 5% Sr. Notes due 2026 | NASDAQ | type CS | $vol 388715 | mcap 138M | Polygon: BRC Group Holdings, Inc. 5.00% Senior Notes due 2026
- `15:33:09`   RILYT   -> is_debt_or_structured  a note by its own name                                         | book: B. Riley Financial, Inc. 6.00% Senior Notes Due 2028 | NASDAQ | type CS | $vol 479848 | mcap 138M | Polygon: BRC Group Holdings, Inc. 6.00% Senior Notes Due 2028
- `15:33:09`   RILYZ   -> is_debt_or_structured  a note by its own name                                         | book: BRC Group Holdings Inc. 5.25% Sr. Notes due 2028 | NASDAQ | type CS | $vol 515145 | mcap 138M | Polygon: BRC Group Holdings, Inc. 5.25% Senior Notes due 2028
- `15:33:09`   TPGXL   -> is_debt_or_structured  a note or bond of TPG (NASDAQ fifth letter L, under a tenth of | book: TPG Inc. | NASDAQ | type CS | $vol 670410 | mcap 18500M | Polygon: TPG Operating Group II, L.P. 6.950% Fixed-Rate Junior Subordinated Not
- `15:33:09`   XELLL   -> is_debt_or_structured  a note by its own name                                         | book: Xcel Energy Inc. 6.25% Junior Subordinated Notes, Series due 208 | NASDAQ | type CS | $vol 3165865 | mcap 12507M | Polygon: Xcel Energy Inc. 6.25% Junior Subordinated Notes, Series due 2085
- `15:33:09`   ACGLN   -> is_preferred           a preferred share of ACGL (NASDAQ fifth letter N, under a tent | book: Arch Capital Group Ltd. | NASDAQ | type CS | $vol 1260936 | mcap 35390M | Polygon: Arch Capital Group Ltd. Depositary Shares, each Representing a 1/1,000
- `15:33:09`   AGNCO   -> is_preferred           a preferred share of AGNC (NASDAQ fifth letter O, under a tent | book: AGNC Investment Corp. | NASDAQ | type CS | $vol 1426567 | mcap 10483M | Polygon: AGNC Investment Corp. Depositary Shares, each representing a 1/1,000th
- `15:33:09`   BHFAM   -> is_preferred           a preferred share of BHF (NASDAQ fifth letter M, under a tenth | book: Brighthouse Financial, Inc. | NASDAQ | type CS | $vol 576450 | mcap 2524M | Polygon: Brighthouse Financial, Inc. Depositary shares each representing a 1/1,
- `15:33:09`   BHFAN   -> is_preferred           a preferred share of BHF (NASDAQ fifth letter N, under a tenth | book: Brighthouse Financial, Inc. | NASDAQ | type CS | $vol 715869 | mcap 2524M | Polygon: Brighthouse Financial, Inc. Depositary shares, each representing a 1/1
- `15:33:09`   BHFAO   -> is_preferred           a preferred share of BHF (NASDAQ fifth letter O, under a tenth | book: Brighthouse Financial, Inc. | NASDAQ | type CS | $vol 791149 | mcap 2524M | Polygon: Brighthouse Financial, Inc. Depositary Shares 6.75% Non-Cum Pfd Series
- `15:33:09`   BPYPM   -> is_preferred           a preferred share by its own name                              | book: Brookfield Property Preferred L.P. | NASDAQ | type CS | $vol 281164 | mcap None | Polygon: Brookfield Property Partners L.P. 6.25% Class A Cumulative Redeemable 
- `15:33:09`   BWBBP   -> is_preferred           a preferred share of BWB (NASDAQ fifth letter P, under a tenth | book: Bridgewater Bancshares, Inc. | NASDAQ | type CS | $vol None | mcap 565M | Polygon: Bridgewater Bancshares, Inc. Depositary Shares, Each Representing a 1/
- `15:33:09`   CNOBP   -> is_preferred           a preferred share of CNOB (NASDAQ fifth letter P, under a tent | book: ConnectOne Bancorp, Inc. | NASDAQ | type CS | $vol 168355 | mcap 1629M | Polygon: ConnectOne Bancorp, Inc. Depositary Shares, each representing a 1/40th
- `15:33:09`   FRMEP   -> is_preferred           a preferred share of FRME (NASDAQ fifth letter P, under a tent | book: First Merchants Corporation | NASDAQ | type CS | $vol None | mcap 2517M | Polygon: First Merchants Corporation Depository Shares
- `15:33:09`   FULTP   -> is_preferred           a preferred share of FULT (NASDAQ fifth letter P, under a tent | book: Fulton Financial Corporation | NASDAQ | type CS | $vol 387235 | mcap 4523M | Polygon: Fulton Financial Corporation Depositary Shares, Each Representing a 1/
- `15:33:09`   HBANM   -> is_preferred           a preferred share of HBAN (NASDAQ fifth letter M, under a tent | book: Huntington Bancshares Incorporated | NASDAQ | type CS | $vol 400109 | mcap 31815M | Polygon: Huntington Bancshares Incorporated Depositary Shares each representing
- `15:33:09`   IMPPP   -> is_preferred           a preferred share of IMPP (NASDAQ fifth letter P, under a tent | book: Imperial Petroleum Inc. | NASDAQ | type CS | $vol None | mcap 181M | Polygon: Imperial Petroleum Inc. 8.75% Series A Cumulative Redeemable Perpetual
- `15:33:09`   LANDO   -> is_preferred           a preferred share of LAND (NASDAQ fifth letter O, under a tent | book: Gladstone Land Corporation | NASDAQ | type CS | $vol 285288 | mcap 297M | Polygon: Gladstone Land Corporation 6.00% Series B Cumulative Redeemable Prefer
- `15:33:09`   MBINM   -> is_preferred           a preferred share of MBIN (NASDAQ fifth letter M, under a tent | book: Merchants Bancorp | NASDAQ | type CS | $vol 213634 | mcap 2435M | Polygon: Merchants Bancorp Depositary Shares, Each Representing a 1/40th Intere
- `15:33:09`   MBINN   -> is_preferred           a preferred share of MBIN (NASDAQ fifth letter N, under a tent | book: Merchants Bancorp | NASDAQ | type CS | $vol 214558 | mcap 2447M | Polygon: Merchants Bancorp Depositary Shares Preferred Series C
- `15:33:09`   MCHPP   -> is_preferred           a preferred share of MCHP (NASDAQ fifth letter P, under a tent | book: Microchip Technology Incorporated | NASDAQ | type CS | $vol 19269842 | mcap 29589M | Polygon: Microchip Technology Incorporated Depositary Shares Each Representing 
- `15:33:09`   MNSBP   -> is_preferred           a preferred share of MNSB (NASDAQ fifth letter P, under a tent | book: MainStreet Bancshares, Inc. | NASDAQ | type CS | $vol None | mcap 168M | Polygon: MainStreet Bancshares, Inc. Depositary Shares
- `15:33:09`   ONBPO   -> is_preferred           a preferred share of ONB (NASDAQ fifth letter O, under a tenth | book: Old National Bancorp | NASDAQ | type CS | $vol 171902 | mcap 9060M | Polygon: Old National Bancorp Depositary Shares, Each Representing a 1/40th Int
- `15:33:09`   WAFDP   -> is_preferred           a preferred share of WAFD (NASDAQ fifth letter P, under a tent | book: WaFd, Inc. | NASDAQ | type CS | $vol 786179 | mcap 2057M | Polygon: WaFd, Inc. Depositary Shares
- `15:33:09`   WSBCO   -> is_preferred           a preferred share of WSBC (NASDAQ fifth letter O, under a tent | book: WesBanco, Inc. | NASDAQ | type CS | $vol 184478 | mcap 3705M | Polygon: WesBanco, Inc. Depositary Shares each representing 1/40th interest in 
- `15:33:09`   AUROW   -> is_warrant_unit        a warrant of AUR (NASDAQ fifth letter W, under a tenth of its  | book: Aurora Innovation, Inc. | NASDAQ | type None | $vol None | mcap 7166M | Polygon: None
- `15:33:09`   BEATW   -> is_warrant_unit        a warrant of BEAT (NASDAQ fifth letter W, under a tenth of its | book: HeartBeam, Inc. | NASDAQ | type None | $vol None | mcap 7M | Polygon: None
- `15:33:09`   BTBDW   -> is_warrant_unit        a warrant of BTBD (NASDAQ fifth letter W, under a tenth of its | book: BT Brands, Inc. | NASDAQ | type None | $vol None | mcap 6M | Polygon: None
- `15:33:09`   SWAGW   -> is_warrant_unit        a warrant of SWAG (NASDAQ fifth letter W, under a tenth of its | book: Stran & Company, Inc. | NASDAQ | type None | $vol None | mcap 22M | Polygon: None
- `15:33:09` controls (must stay common stock): GOOGL common | GOOG common | FOXA common | FOX common | NWSA common | NWS common | LBTYA common | LBTYK common | FWONA common | FWONK common | Z common | ZG common | UA common | UAA common | PFBC common | CMCSA common | BATRA common | BATRK common | LILAK common | LILA common | SIRI common
- `15:33:09` common-stock rows the rule leaves alone whose POLYGON name names a note or a preferred: 13
- `15:33:09`   BBD     Polygon: Banco Bradesco S.A. American Depositary Shares (Each representing one Preferred  | book: Banco Bradesco S.A. | $vol 169817940
- `15:33:09`   CIB     Polygon: Grupo Cibest S.A. American Depositary Shares (each representing four (4) Preferr | book: Grupo Cibest S.A. | $vol 28078846
- `15:33:09`   BAK     Polygon: Braskem S.A. American Depositary Shares (Each representing Two Class A  Preferre | book: Braskem S.A. | $vol 3612408
- `15:33:09`   DHCNL   Polygon: Diversified Healthcare Trust 6.25% Senior Notes Due 2046                         | book: Diversified Healthcare Trust | $vol 896961
- `15:33:09`   OXLCI   Polygon: Oxford Lane Capital Corp. 8.75% Notes due 2030                                   | book: Oxford Lane Capital Corp. | $vol 244017
- `15:33:09`   HWCPZ   Polygon: Hancock Whitney Corporation 6.25% Subordinated Notes due 2060                    | book: Hancock Whitney Corporation - 6 | $vol 232389
- `15:33:09`   BPYPN   Polygon: Brookfield Property Partners L.P. 5.750% Class A Cumulative Redeemable Perpetual | book: Brookfield Property Partners L.P. | $vol 171604
- `15:33:09`   GAINI   Polygon: Gladstone Investment Corporation 7.875% Notes due 2030                           | book: Gladstone Investment Corporation | $vol 159738
- `15:33:09`   RWAYL   Polygon: Runway Growth Finance Corp. 7.50% Notes due 2027                                 | book: Runway Growth Finance Corp. - 7 | $vol 157236
- `15:33:09`   BPYPO   Polygon: Brookfield Property Partners L.P. 6.375% Class A Cumulative Redeemable Perpetual | book: Brookfield Property Partners L.P. | $vol 150706
- `15:33:09`   GECCH   Polygon: Great Elm Capital Corp. 8.125% Notes Due 2029                                    | book: Great Elm Capital Corp. - 8.125 | $vol None
- `15:33:09`   INBKZ   Polygon: First Internet Bancorp 6.0% Fixed-to-Floating Rate Subordinated Notes Due 2029   | book: First Internet Bancorp - Fixed- | $vol None
- `15:33:09`   OXLCO   Polygon: Oxford Lane Capital Corp. Preferred Stock Shares, 6.00% Series 2029              | book: Oxford Lane Capital Corp. | $vol None
- `15:33:09` of the 56, 33 trade $250k a day or more (the floor of the Wyckoff scan and the Volatility desk): ACGLN, ADAMG, ADAMH, ADAMK, ADAMO, AGNCL, AGNCO, ATLCL, ATLCZ, BHFAL, BHFAM, BHFAN, BHFAO, BPYPM, CGABL, CTGG, CTHH, DHCNI, EMP, FULTP, HBANL, HBANM, HBANZ, JSM, LANDO, MBINL, MCHPP, RILYG, RILYT, RILYZ, TPGXL, WAFDP, XELLL
## 2. justhodl-leaders: build() with the at-the-open test off, then on

- `15:33:42` justhodl-leaders, test off: built in 34s, version 1.0.1, as of 2026-11-19
- `15:34:02` justhodl-leaders, test on: built in 20s, version 1.0.1, as of 2026-11-19
- `15:34:02` refused as splits: 0 tickers - 
- `15:34:03` justhodl-leaders: 0 leaves differ; 0 tickers touched (0 with 6 or more fields, 0 with 1 to 5); 0 differences outside ticker rows
- `15:34:03`   fields that moved most: 
## 2. justhodl-campaign-scan: build() with the at-the-open test off, then on

- `15:36:27` justhodl-campaign-scan, test off: built in 144s, version 1.0.2, as of 2026-11-19
- `15:38:51` justhodl-campaign-scan, test on: built in 144s, version 1.0.2, as of 2026-11-19
- `15:38:51` refused as splits: 53 - ABLV 2026-10-12 x0.5565 (opened x0.9874); AIOK 2026-10-28 x2.0969 (opened x1.3063); AIXI 2026-04-07 x2.426 (opened x1.3809); AKAN 2026-04-30 x1.8838 (opened x1.2351); AKBA 2022-03-30 x0.3398 (opened x0.957); BMGL 2026-11-10 x0.3574 (opened x0.9963); BMNR 2025-07-03 x2.3077 (opened x0.9791); BNAI 2024-05-31 x2.1085 (opened x0.9922); CIFR 2022-05-10 x0.5325 (opened x0.7678); CLIK 2025-04-16 x0.258 (opened x0.9906); CLRO 2026-07-07 x1.9771 (opened x1.37); CSAI 2025-02-13 x2.1373 (opened x1.0289); DGNX 2025-01-29 x3.3456 (opened x1.0467); DKI 2026-10-09 x0.4592 (opened x0.8314); EDSA 2026-03-06 x1.8061 (opened x1.0083); EHGO 2026-06-30 x0.4869 (opened x0.9738); ELPW 2026-05-12 x0.5335 (opened x0.8046); EMAT 2026-11-10 x0.5596 (opened x0.8532); ENGS 2025-05-05 x0.3359 (opened x0.9967); FLYE 2025-10-31 x0.4715 (opened x0.8224); FMFC 2025-10-30 x0.5526 (opened x0.8651); GMM 2024-03-18 x0.33 (opened x0.9218); HOLO 2024-02-20 x0.2473 (opened x0.7669); IBIO 2025-04-07 x0.4682 (opened x0.9302); INV 2026-10-07 x0.4744 (opened x0.9043); IPDN 2023-02-28 x0.488 (opened x0.9462); IPST 2026-08-18 x2.1326 (opened x1.0095); JDZG 2026-11-06 x0.5324 (opened x0.8612); JYD 2026-05-20 x0.4789 (opened x0.7394); LGHL 2022-09-29 x0.5075 (opened x1.0); LICN 2026-05-20 x0.3852 (opened x0.9717); LUNR 2023-02-23 x0.2476 (opened x1.1831); MI 2024-11-15 x0.3333 (opened x1.0); MWYN 2025-06-06 x0.2896 (opened x0.9939); NCI 2026-09-28 x0.1625 (opened x1.0); ONEG 2025-11-14 x0.501 (opened x0.9756); OPTX 2024-12-20 x1.8041 (opened x0.9588); PASW 2026-11-05 x0.5427 (opened x0.8601); PLRX 2025-03-03 x0.4012 (opened x0.9012); RBNE 2025-06-17 x1.9185 (opened x1.2627); RETO 2025-02-25 x1.8305 (opened x0.7797); RPGL 2026-10-16 x0.5169 (opened x1.006); SDOT 2026-06-17 x1.9815 (opened x1.2687); SOUN 2022-06-06 x0.5164 (opened x1.0013); STFS 2025-08-20 x0.2101 (opened x0.8986); SXTC 2026-10-09 x0.4329 (opened x0.6885); TWG 2024-10-25 x0.04 (opened x0.563); VSA 2026-03-27 x0.5549 (opened x1.3978); WCT 2024-11-14 x0.5253 (opened x0.9937); WETO 2026-08-17 x2.9915 (opened x1.236); WFF 2026-10-13 x0.1227 (opened x0.8623); YMAT 2026-05-19 x0.4894 (opened x1.0142); YMT 2026-05-20 x0.5572 (opened x0.801)
- `15:38:51` justhodl-campaign-scan: 66 leaves differ; 15 tickers touched (2 with 6 or more fields, 13 with 1 to 5); 20 differences outside ticker rows
- `15:38:51`   fields that moved most: decline_pct 9, low 5, off_low_pct 5, px 5, since_pct 5, range_x 4, ago 2, date 2, status 2, prior_low_date 2, bar_pct 1, score 1, [NCI] 1, rev_up_date 1, [YMAT] 1
- `15:38:51`   BMGL     11 fields | [BMGL].ago: 2 -> 30 ; [BMGL].bar_pct: 7.34 -> 9.76 ; [BMGL].date: 2026-11-17 -> 2026-10-08 ; [BMGL].decline_pct: -87.3 -> -75.8 ; [BMGL].low: 0.2475 -> 0.9073 ; [BMGL].off_low_pct: 29.01 -> -64.81 ; [BMGL].px: 0.3245 -> 1.2759
- `15:38:51`   SDOT      9 fields | [SDOT].ago: 106 -> 122 ; [SDOT].date: 2026-06-22 -> 2026-05-28 ; [SDOT].decline_pct: -99.5 -> -99.4 ; [SDOT].low: 4.82 -> 2.63 ; [SDOT].off_low_pct: -21.99 -> 42.97 ; [SDOT].px: 9.25 -> 3.19 ; [SDOT].rev_up_date: 2026-06-23 -> 2026-06-01
- `15:38:51`   DKI       5 fields | [DKI].low: 22.2915 -> 48.5479 ; [DKI].off_low_pct: -95.47 -> -97.92 ; [DKI].px: 27.8644 -> 60.6848 ; [DKI].since_pct: -96.38 -> -98.34 ; [DKI].range_x: 0.3 -> 0.14
- `15:38:51`   PASW      4 fields | [PASW].low: 0.0633 -> 0.1167 ; [PASW].off_low_pct: 1889.39 -> 979.57 ; [PASW].px: 0.0715 -> 0.1318 ; [PASW].since_pct: 1661.1 -> 855.68
- `15:38:51`   SXTC      4 fields | [SXTC].low: 0.7706 -> 1.78 ; [SXTC].off_low_pct: 236.12 -> 45.51 ; [SXTC].px: 0.8355 -> 1.93 ; [SXTC].since_pct: 210.0 -> 34.2
- `15:38:51`   CLRO      3 fields | [CLRO].decline_pct: -91.6 -> -90.2 ; [CLRO].prior_low_date: 2026-09-24 -> <absent> ; [CLRO].range_x: 2.36 -> 2.54
- `15:38:51`   WFF       2 fields | [WFF].decline_pct: -99.8 -> -99.9 ; [WFF].range_x: 0.14 -> 0.02
- `15:38:51`   BMNR      1 fields | [BMNR].decline_pct: -92.6 -> -92.0
- `15:38:51`   EHGO      1 fields | [EHGO].prior_low_date: 2026-06-12 -> 2026-08-19
- `15:38:51`   FLYE      1 fields | [FLYE].decline_pct: -94.1 -> -94.2
- `15:38:51`   LICN      1 fields | [LICN].decline_pct: -84.7 -> -94.0
- `15:38:51`   NCI       1 fields | [NCI]: <absent> -> {"ticker": "NCI", "name": "Neo-Con...
- `15:38:51`   VSA       1 fields | [VSA].decline_pct: -99.8 -> -99.9
- `15:38:51`   YMAT      1 fields | [YMAT]: <absent> -> {"ticker": "YMAT", "name": "J-Star...
- `15:38:51`   YMT       1 fields | [YMT].decline_pct: -99.4 -> -99.6
- `15:38:51`   by_sector.Basic Materials.bottom: 171 -> 172
- `15:38:51`   by_sector.Consumer Cyclical.bottom: 220 -> 221
- `15:38:51`   counts.bottom.n: 3010 -> 3012
- `15:38:51`   counts.bottom.new_5: 251 -> 250
- `15:38:51`   counts.bottom.stock.by_mark.BOTTOM: 930 -> 931
- `15:38:51`   counts.bottom.stock.by_mark.CLIMAX: 1326 -> 1327
- `15:38:51`   counts.bottom.stock.by_status.HELD: 1041 -> 1042
- `15:38:51`   counts.bottom.stock.by_status.UNDERCUT: 395 -> 396
- `15:38:51`   counts.bottom.stock.flagged_jump: 98 -> 100
- `15:38:51`   counts.bottom.stock.mcap: 54375142340018.0 -> 54375151428699.0
- `15:38:51`   counts.bottom.stock.n: 2256 -> 2258
- `15:38:51`   counts.bottom.stock.new_5: 165 -> 164
- `15:38:51`   prints.4.bottom: 4 -> 5
- `15:38:51`   prints.20.bottom: 38 -> 37
- `15:38:51`   prints.74.bottom: 3 -> 4
- `15:38:51`   prints.96.climax: 26 -> 27
- `15:38:51`   prints.116.climax: 659 -> 660
- `15:38:51`   prints.124.climax: 95 -> 94
- `15:38:51`   universe.splits_refused.n: 0 -> 53
- `15:38:51`   universe.splits_refused.sample: list of 0 -> list of 53
## 2. justhodl-strength: build() with the at-the-open test off, then on

- `15:39:14` justhodl-strength, test off: built in 22s, version 1.0.13, as of 2026-11-19
- `15:39:32` justhodl-strength, test on: built in 19s, version 1.0.13, as of 2026-11-19
- `15:39:32` refused as splits: 0 tickers - 
- `15:39:33` justhodl-strength: 0 leaves differ; 0 tickers touched (0 with 6 or more fields, 0 with 1 to 5); 0 differences outside ticker rows
- `15:39:33`   fields that moved most: 
- `15:39:33` ✅ AUDIT: the book's would-be reclassifications are listed; each engine was built with the old and the new split rule and compared. Nothing written
