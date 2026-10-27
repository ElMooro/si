# ops 6694 - live screener universe vs the bank-built universe: the reason for every difference (read-only)

**Status:** success  
**Duration:** 7.3s  
**Finished:** 2026-10-27T18:51:25+00:00  

## Log
- `18:51:18` live: 5230 stocks, generated 2026-10-27T18:06:31+00:00, by source {'screener': 5225, 'curated_seed': 5}
- `18:51:25` profile rows (raw) 93746; after the stream filter 6670 (2026-10-25T23:13:14+00:00); book {"status": "read", "key": "data/warm/factor-book/universe.json.gz", "generated_at": "2026-10-27T02:50:45.023172+00:00", "symbols": 27215}
## live-only: 269

- `18:51:25` by reason: {'book': 252, 'absent from profile-bulk': 5, 'not actively trading': 5, 'market': 5, 'ETF/fund flag': 2}
- `18:51:25`    book drop - type/flags SP ('is_debt_or_structured',): 72 e.g. ['AFGB(American Financial Group, In)', 'AFGC(American Financial Group, In)', 'AFGD(American Financial Group, In)', 'AFGE(American Financial Group, In)', 'AIZN(Assurant, Inc. 5.25% Subordi)', 'AOMN(Angel Oak Mortgage REIT, Inc)', 'AQNB(Algonquin Power & Utilities )', 'ASBA(Associated Banc-Corp)', 'ATHS(Athene Holding Ltd. 7.250% F)', 'BCPC(Balchem Corp)']
- `18:51:25`    book drop - type/flags CS ('is_shell',): 43 e.g. ['AAC(Ares Acquisition Corp. III C)', 'ALIS(Calisa Acquisition Corp)', 'AMAC(AMR Resources Acquisition Co)', 'AMAN(Amanat Acquisition Corp Clas)', 'ARTC(Art Technology Acquisition C)', 'ASTS(AST SpaceMobile, Inc.)', 'ATLQ(JAB Acquisition Corp. I Clas)', 'BLRK(Bluerock Acquisition Corp. C)', 'BREZ(Breeze Acquisition Corp. II)', 'CATL(Catalyst Acquisition Corp.)']
- `18:51:25`    book drop - type/flags WARRANT ('is_warrant_unit',): 30 e.g. ['ALOVW(Aldabra 4 Liquidity Opportun)', 'ANNAW(AleAnna, Inc.)', 'ASPSW(Altisource Portfolio Solutio)', 'ASPSZ(Altisource Portfolio Solutio)', 'BCTXL(BriaCell Therapeutics Corp.)', 'BIVIW(BioVie Inc.)', 'BRTMW(B&R Technology Merger Corp.)', 'CSHRW(CoinShares PLC)', 'FACWW(Factorial Energy Inc.)', 'GFUZW(General Fusion Group Ltd. Wa)']
- `18:51:25`    book drop - type/flags WARRANT ('is_warrant_unit', 'is_shell'): 18 e.g. ['AEAQW(Activate Energy Acquisition )', 'ARCIW(Archimedes Tech SPAC Partner)', 'BIXIW(Bitcoin Infrastructure Acqui)', 'CAQUW(Cambridge Acquisition Corp. )', 'DAAQW(Digital Asset Acquisition Co)', 'FACTW(FACT II Acquisition Corp)', 'HCMAW(HCM III Acquisition Corp.)', 'IACOW(Idea Acquisition Corp. Warra)', 'MESHW(Meshflow Acquisition Corp. W)', 'MEVOW(M Evo Global Acquisition Cor)']
- `18:51:25`    book drop - type/flags ADRC ('is_preferred',): 16 e.g. ['AGMB(AgomAb Therapeutics N.V. Ame)', 'ARM(Arm Holdings plc American De)', 'CHA(Chagee Holdings Limited Amer)', 'FOCL(EDAP TMS S.A. American Depos)', 'HDL(SUPER HI INTERNATIONAL HOLDI)', 'LFS(LEIFRAS Co., Ltd. American D)', 'LOT(Lotus Technology Inc. Americ)', 'LRE(Lead Real Estate Co., Ltd Am)', 'MWC(Micware Co., Ltd. American D)', 'NAMI(Jinxin Technology Holding Co)']
- `18:51:25`    book drop - type/flags PFD ('is_preferred',): 16 e.g. ['BMNP(Bitmine Immersion Technologi)', 'BUSEP(First Busey Corporation)', 'CHAD(DeFi Development Corp.)', 'CHSCL(CHS Inc.)', 'LILAP(Liberty Latin America Ltd. 9)', 'MBNKO(Medallion Bank)', 'SATS(Strive, Inc. 15.00% Series A)', 'SMCIP(Super Micro Computer, Inc.)', 'STRC(Strategy Inc)', 'STRD(Strategy Inc)']
- `18:51:25`    book drop - type/flags UNIT ('is_warrant_unit',): 13 e.g. ['CCXIU(Churchill Capital Corp XI Un)', 'ESBA(Empire State Realty OP, L.P.)', 'FISK(Empire State Realty OP, L.P.)', 'GHI(Greystone Housing Impact Inv)', 'LTGRU(Long Table Growth Corp. Unit)', 'MNR(Mach Natural Resources LP)', 'NCOOU(Southern Cross Acquisition I)', 'NHIVU(NewHold Investment Corp IV)', 'OGCP(Empire State Realty OP, L.P.)', 'OZ(Belpointe PREP, LLC)']
- `18:51:25`    book drop - type/flags CS ('is_warrant_unit',): 10 e.g. ['CRWS(Crown Crafts, Inc.)', 'FLWS(1-800-FLOWERS.COM, Inc.)', 'GWW(W.W. Grainger, Inc.)', 'NWS(News Corporation)', 'NWSA(News Corporation)', 'POWW(Outdoor Holding Company)', 'WS(Worthington Steel, Inc.)', 'WW(WW International, Inc.)', 'WWW(Wolverine World Wide, Inc.)', 'ZWS(Zurn Elkay Water Solutions C)']
- `18:51:25`    book drop - type/flags RIGHT ('is_warrant_unit', 'is_shell'): 7 e.g. ['AXINR(Axiom Intelligence Acquisiti)', 'HCACR(Hall Chadwick Acquisition Co)', 'IGACR(Invest Green Acquisition Cor)', 'INACR(Indigo Acquisition Corp.)', 'QSEAR(Quartzsea Acquisition Corpor)', 'RFAIR(RF Acquisition Corp II Right)', 'SCIIR(SC II Acquisition Corp.)']
- `18:51:25`    book drop - type/flags UNIT ('is_warrant_unit', 'is_shell'): 7 e.g. ['FTHAU(Forefront Tech Holdings Acqu)', 'IPXGU(Inflection Point Acquisition)', 'KEYYU(Keystone Acquisition Corp. U)', 'OSPRU(Osprey Acquisition Corp. III)', 'PECEU(Peace Acquisition Corp Units)', 'RNAQU(Rainier Acquisition Corp.)', 'XTERU(Karman Line Acquisition Corp)']
- `18:51:25`    book drop - type/flags RIGHT ('is_warrant_unit',): 6 e.g. ['DMIIR(Drugs Made In America Acquis)', 'GIXXR(GigCapital9 Corp. Rights)', 'HCICR(Hennessy Capital Investment )', 'JONER(Jones Ventures INTL Acquisit)', 'TRGSR(TRG Latin America Acquisitio)', 'TWLVR(Twelve Seas Investment Compa)']
- `18:51:25`    book drop - type/flags CS ('is_preferred',): 5 e.g. ['NEWTO(NewtekOne, Inc. 8.5 % Notes )', 'RWAYI(Runway Growth Finance Corp. )', 'TMUSI(T-Mobile USA Inc 5.5 % Notes)', 'TMUSL(T-Mobile US, Inc. 6.25 % Not)', 'TMUSZ(T-Mobile US, Inc. 5.5 % Note)']
- `18:51:25`    book drop - type/flags SP ('is_preferred', 'is_debt_or_structured'): 4 e.g. ['BEPH(Brookfield BRP Holdings (Can)', 'BIPH(Brookfield Infrastructure Fi)', 'SCCD(Sachem Capital Corp. 6.00% N)', 'SOJF(Southern Company 6.5 % Notes)']
- `18:51:25`    book drop - type/flags FUND ('is_fund',): 3 e.g. ['DHC(Diversified Healthcare Trust)', 'ILPT(Industrial Logistics Propert)', 'NXDT(NexPoint Diversified Real Es)']
- `18:51:25`    book drop - type/flags SP ('is_warrant_unit', 'is_debt_or_structured'): 1 e.g. ['DUKU(Duke Energy Corporation Unit)']
- `18:51:25`    book drop - type/flags None ('is_preferred',): 1 e.g. ['ETI-P(Entergy Texas, Inc.)']
- `18:51:25`    absent from profile-bulk: e.g. ['AETR(Westport Fuel Systems Inc)', 'BACCR(Blue Acquisition Corp.)', 'FCDH(VS Media Holdings Limited Cl)', 'GUNN(Gunnison Copper Corp. Spon A)', 'PTIV(Pineapple Financial, Inc.)']
- `18:51:25`    not actively trading: e.g. ['BPMC(Blueprint Medicines Corporat)', 'EXAS(Exact Sciences Corporation)', 'INFN(Infinera Corporation)', 'PSTG(Pure Storage Inc.)', 'QRVO(Qorvo, Inc.)']
- `18:51:25`    ETF/fund flag: e.g. ['GPMT(Granite Point Mortgage Trust)', 'OBTC(Osprey Bitcoin Trust)']
- `18:51:25`    market: e.g. ['NUWE(Nuwellis, Inc.)', 'PPCB(Propanc Biopharma, Inc.)', 'SBFM(Sunshine Biopharma, Inc.)', 'TWG(Top Wealth Group Holding Lim)', 'VIVK(Vivakor, Inc.)']
## bank-only: 111

- `18:51:25`    type CS: 99 e.g. ['ACA(Arcosa, Inc., Construction Materials)', 'ACGLN(Arch Capital Group Ltd., Insurance - Diversified)', 'ADAMG(Adamas Trust, Inc. - 9.125% , REIT - Mortgage)', 'ADAMH(Adamas Trust, Inc., REIT - Mortgage)', 'ADAMI(Adamas Trust Inc. 9.125% Sr., REIT - Mortgage)', 'ADAMO(Adamas Trust, Inc. 9.250% Se, REIT - Mortgage)', 'AGM-A(Federal Agricultural Mortgag, Financial - Credit Services)', 'AGNCL(AGNC Investment Corp., REIT - Mortgage)']
- `18:51:25`    type None: 8 e.g. ['ASTLW(Algoma Steel Group Inc., Steel)', 'BZH(Beazer Homes USA, Inc., Residential Construction)', 'GETY(Getty Images Holdings, Inc., Internet Content & Information)', 'ILLR(Triller Group Inc., Software - Application)', 'ILLRW(Triller Group Inc., Software - Application)', 'PERF(Perfect Corp., Software - Application)', 'SEATW(Vivid Seats Inc., Internet Content & Information)', 'TCBIO(Texas Capital Bancshares, In, Banks - Regional)']
- `18:51:25`    type ADRC: 4 e.g. ['AKO-B(Embotelladora Andina S.A., Beverages - Non-Alcoholic)', 'LAWR(Robot Consulting Co., Ltd. A, Information Technology Services)', 'PHOS(First Phosphate Corp. Sponso, Other Precious Metals)', 'PSNYW(Polestar Automotive Holding , Auto - Manufacturers)']
## live universe composition by Polygon security type (the screener path today)

- `18:51:25` live by type: {'CS': 4681, 'ADRC': 358, 'SP': 77, 'WARRANT': 48, 'UNIT': 20, 'PFD': 16, 'untyped': 14, 'RIGHT': 13, 'FUND': 3}
- `18:51:25`    SP in the live universe: 77 e.g. ['TBB mcap 140.4B', 'SOMN mcap 105.9B', 'SOJE mcap 105.4B', 'ELC mcap 45.4B', 'PFH mcap 39.6B', 'DUKU mcap 39.0B', 'PPLC mcap 35.3B', 'DTW mcap 27.3B']
- `18:51:25`    PFD in the live universe: 16 e.g. ['SMCIP mcap 39.3B', 'STRC mcap 32.9B', 'STRF mcap 30.0B', 'STRK mcap 20.8B', 'BMNP mcap 12.8B', 'TPC mcap 4.2B', 'LILAP mcap 3.0B', 'BUSEP mcap 2.1B']
- `18:51:25`    FUND in the live universe: 3 e.g. ['DHC mcap 1.9B', 'ILPT mcap 0.5B', 'NXDT mcap 0.3B']
- `18:51:25`    UNIT in the live universe: 20 e.g. ['MNR mcap 1.8B', 'OGCP mcap 1.4B', 'ESBA mcap 1.4B', 'FISK mcap 1.4B', 'TXO mcap 0.8B', 'CCXIU mcap 0.4B', 'OSPRU mcap 0.3B', 'KEYYU mcap 0.3B']
- `18:51:25`    WARRANT in the live universe: 48 e.g. ['XRXDW mcap 0.2B', 'SCPQW mcap 0.1B', 'OPENW mcap 0.1B', 'OPENZ mcap 0.1B', 'OPENL mcap 0.1B', 'NIOBW mcap 0.1B', 'HQWWW mcap 0.1B', 'SBCWW mcap 0.0B']
- `18:51:25`    RIGHT in the live universe: 13 e.g. ['JONER mcap 0.0B', 'GIXXR mcap 0.0B', 'HCACR mcap 0.0B', 'DMIIR mcap 0.0B', 'AXINR mcap 0.0B', 'RFAIR mcap 0.0B', 'SCIIR mcap 0.0B', 'IGACR mcap 0.0B']
- `18:51:25` ✅ ops 6694 complete (read-only)
