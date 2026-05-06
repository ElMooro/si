
# 1) Test Atom feed with start/count parameters

- `09:33:34`     SC 13D  → 1 entries
- `09:33:34`     SC 13D &start=0&count=40 → 1 entries
- `09:33:34`     SC 13D &start=0&count=100 → 1 entries
- `09:33:34`     SC 13D &start=40&count=40 → 0 entries

# 2) Test EDGAR full-text search API (json)

- `09:33:37`     SC 13D 7d: 0 hits
- `09:33:38`     SC 13D/A 7d: 0 hits
- `09:33:38`     SC 13G 7d: 0 hits
- `09:33:39`     SC 13G/A 7d: 0 hits

# 3) Test EDGAR full-text search WITHOUT date filter

- `09:33:41`     SC 13D (no date): 100 hits, total=10000
- `09:33:41`       None | AGL Private Credit Income Fund  (CIK 0002011498) | filed=2024-12-17
- `09:33:41`       None | FIRST NATIONAL CORP /VA/  (FXNC)  (CIK 0000719402) | filed=2024-12-17
- `09:33:41`       None | Arcus Biosciences, Inc.  (RCUS)  (CIK 0001724521) | filed=2024-12-17
- `09:33:43`     SC 13G (no date): 100 hits, total=10000
- `09:33:43`       None | Montrose Environmental Group, Inc.  (MEG)  (CIK 00 | filed=2024-12-17
- `09:33:43`       None | PALVELLA THERAPEUTICS, INC.  (PIRS)  (CIK 00015836 | filed=2024-12-17
- `09:33:43`       None | Candel Therapeutics, Inc.  (CADL)  (CIK 0001841387 | filed=2024-12-17

# 4) Test CIK → ticker mapping (reuse existing logic)

- `09:33:43`     loaded 10376 entries from company_tickers.json
- `09:33:43`     GNK found: {'cik_str': 1326200, 'ticker': 'GNK', 'title': 'GENCO SHIPPING & TRADING LTD'}

# 5) Lookup GENCO SHIPPING by CIK in EDGAR

- `09:33:43`     EDGAR atom for GNK: SC14D9C  - Written communication relating to third party tender offer