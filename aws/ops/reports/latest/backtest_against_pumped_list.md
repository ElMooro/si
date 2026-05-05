
# 0) Setup — load all hunter outputs

- `23:38:53`     ✓ universe: loaded 104515 bytes
- `23:38:53`     ✓ nobrainers: loaded 456897 bytes
- `23:38:53`     ✓ insiders: loaded 43345 bytes
- `23:38:53`     ✓ smart_money: loaded 152894 bytes
- `23:38:53`     ✓ deep_value: loaded 59581 bytes
- `23:38:53`     ✓ eps_velocity: loaded 144390 bytes
- `23:38:54`     ✓ themes: loaded 57869 bytes
- `23:38:54`     ✓ supply: loaded 68210 bytes
- `23:38:54`     ✓ compound: loaded 3839 bytes

# 1) PER-NAME analysis — would the system have flagged it?

- `23:38:54`     Sym        Pump  Univ NB     Insid  13F    DV     EPS        Compound
- `23:38:54`     ------ -------- ----- ------ ------ ------ ------ ---------- --------
- `23:38:54`     AXTI      +464% ❌     -      -      -      -      -              -       
- `23:38:54`     LWLG      +408% ❌     -      -      -      -      -              -       
- `23:38:54`     AAOI      +353% ❌     -      -      -      -      -              -       
- `23:38:54`     AEHR      +277% ❌     -      -      -      -      -              -       
- `23:38:54`     SNDK      +139% ❌     -      -      -      -      -              -       
- `23:38:54`     ICHR      +138% ❌     -      -      -      -      -              -       
- `23:38:54`     MRVL      +130% ❌     -      -      -      -      -              -       
- `23:38:54`     INTC      +122% ✓     -      -      -      -      top 70         -       
- `23:38:54`     VIAV      +119% ❌     -      -      -      -      -              -       
- `23:38:54`     LITE      +116% ❌     -      -      -      -      -              -       
- `23:38:54`     CRDO      +101% ❌     -      -      -      -      -              -       
- `23:38:54`     MU          +0% ❌     -      -      -      -      -              -       

# 2) DIAGNOSIS

- `23:38:54`     Total in pump list: 12
- `23:38:54`     Captured by ANY hunter: 1/12
- `23:38:54`     Capture rate: 8%
- `23:38:54`   
- `23:38:54`     ── caught ──
- `23:38:54`       ✓ INTC: +122%
- `23:38:54`   
- `23:38:54`     ── MISSED ── (the painful list)
- `23:38:54`       ❌ AXTI (+464%): Indium phosphide / GaAs substrates for AI optical/RF — supply-constrained niche
- `23:38:54`       ❌ LWLG (+408%): Polymer-based electro-optic photonics — research stage, AI optical interconnect
- `23:38:54`       ❌ AAOI (+353%): Optical transceivers — AI data-center bandwidth supply
- `23:38:54`       ❌ AEHR (+277%): Burn-in test equipment for SiC/AI chips — small mcap, supply-tight niche
- `23:38:54`       ❌ SNDK (+139%): Memory storage — AI structural re-rating play
- `23:38:54`       ❌ ICHR (+138%): Critical fluid delivery for semi/memory fab — picks-and-shovels
- `23:38:54`       ❌ MRVL (+130%): Optical DSP, custom AI silicon
- `23:38:54`       ❌ VIAV (+119%): Optical test equipment — AI optical infrastructure
- `23:38:54`       ❌ LITE (+116%): Lumentum — AI optical lasers
- `23:38:54`       ❌ CRDO (+101%): AEC cables for AI data centers
- `23:38:54`       ❌ MU (+0%): Memory cycle / AI re-rating (mentioned in thesis)

# 3) ROOT-CAUSE ANALYSIS — why we missed them

- `23:38:54`     TYPES of misses:
- `23:38:54`   
- `23:38:54`     A) Sub-$1B microcaps not in universe
- `23:38:54`        Count: 11
- `23:38:54`        • AXTI (+464%) — Indium phosphide / GaAs substrates for AI optical/RF — supply-constrained niche
- `23:38:54`        • LWLG (+408%) — Polymer-based electro-optic photonics — research stage, AI optical interconnect
- `23:38:54`        • AAOI (+353%) — Optical transceivers — AI data-center bandwidth supply
- `23:38:54`        • AEHR (+277%) — Burn-in test equipment for SiC/AI chips — small mcap, supply-tight niche
- `23:38:54`        • SNDK (+139%) — Memory storage — AI structural re-rating play
- `23:38:54`        • ICHR (+138%) — Critical fluid delivery for semi/memory fab — picks-and-shovels
- `23:38:54`        • MRVL (+130%) — Optical DSP, custom AI silicon
- `23:38:54`        • VIAV (+119%) — Optical test equipment — AI optical infrastructure
- `23:38:54`        • LITE (+116%) — Lumentum — AI optical lasers
- `23:38:54`        • CRDO (+101%) — AEC cables for AI data centers
- `23:38:54`        • MU (+0%) — Memory cycle / AI re-rating (mentioned in thesis)
- `23:38:54`   
- `23:38:54`     B) In universe but no hunter flagged it
- `23:38:54`        Count: 0

# 4) THE STRUCTURAL GAPS

- `23:38:54`     Gap #1: SMALL-CAP COVERAGE
- `23:38:54`       Universe v2 has 336 stocks, mostly large/mid-cap (mcap >= $300M).
- `23:38:54`       Names like AXTI, LWLG, AEHR were sub-$500M when the move started.
- `23:38:54`       Current universe MIN_MCAP threshold filters out exactly this kind of name.
- `23:38:54`   
- `23:38:54`     Gap #2: NO 'EARLY MOMENTUM' SIGNAL
- `23:38:54`       All 5 systems are FUNDAMENTAL/POSITIONING-based:
- `23:38:54`         • Nobrainer = theme + supply + valuation
- `23:38:54`         • Insider = SEC Form 4 buys
- `23:38:54`         • Smart Money = 13F (lagging 45 days)
- `23:38:54`         • Deep Value = Ben Graham balance sheet
- `23:38:54`         • EPS Velocity = analyst revisions
- `23:38:54`       NONE of these detect:
- `23:38:54`         • Early price-volume breakouts
- `23:38:54`         • Unusual options flow
- `23:38:54`         • Short-interest squeezes
- `23:38:54`         • Rising relative strength
- `23:38:54`         • Newly added to AI/semi ETFs (passive flows)
- `23:38:54`       These technical/flow signals would have caught LWLG/AAOI/AXTI weeks earlier.
- `23:38:54`   
- `23:38:54`     Gap #3: NO THEME-EXPANSION DETECTION
- `23:38:54`       Theme detector has 79 themes via ETFs. But 'AI memory re-rating' or
- `23:38:54`       'AI optical interconnect supply chain' are SUB-themes inside SOXX/SMH/AIQ
- `23:38:54`       that don't have their own ETF. Need a sub-theme detector that watches
- `23:38:54`       correlated price moves WITHIN a parent theme to catch sub-clusters early.
- `23:38:54`   
- `23:38:54`     Gap #4: NO SOCIAL/NARRATIVE SIGNAL
- `23:38:54`       Many of these moved on Twitter narrative + retail attention before
- `23:38:54`       fundamentals confirmed. We have NO sentiment/social signal layer.
- `23:38:54`       A 'narrative momentum' signal (mentions/searches accelerating) would
- `23:38:54`       have caught AAOI, AEHR, AXTI 4-8 weeks earlier.
- `23:38:54`   
- `23:38:54`     Gap #5: SUPPLY INFLECTION COVERAGE TOO COARSE
- `23:38:54`       Our supply scanner has 22 hard-data signals (MEMORY, LITHIUM, RARE_EARTH...)
- `23:38:54`       But it doesn't have:
- `23:38:54`         • OPTICAL_TRANSCEIVERS (AAOI, LITE, COHR pure-plays)
- `23:38:54`         • TEST_EQUIPMENT_SiC (AEHR, ICHR pure-plays)
- `23:38:54`         • COMPOUND_SEMICONDUCTORS (AXTI, SK Hynix)
- `23:38:54`         • DRAM_CYCLE (MU, SNDK pure-plays)