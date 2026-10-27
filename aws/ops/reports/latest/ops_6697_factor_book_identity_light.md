# ops 6697 - bank-factors v1.0.7: a Polygon common type outweighs the symbol/name heuristics (kick + identity check)

**Status:** success  
**Duration:** 362.9s  
**Finished:** 2026-10-27T19:24:03+00:00  

## Log
- `19:18:00` deployed LastModified 2026-10-27T19:16:31.000+0000, 3008 MB / 900 s
- `19:18:01` manifest before: version 1.0.6, updated 2026-10-27T02:50:45.023172+00:00, common_stocks 5320, universe 27215
- `19:18:01` kicked (async) at 2026-10-27T19:18:01
- `19:24:02` manifest after: version 1.0.7, updated 2026-10-27T19:18:02.021249+00:00, common_stocks 5356 (was 5320), universe 27215
- `19:24:02`    GWW     is_common True (want True) | type CS | flags [] | W.W. Grainger, Inc.
- `19:24:02`    NWSA    is_common True (want True) | type CS | flags [] | News Corporation
- `19:24:02`    NWS     is_common True (want True) | type CS | flags [] | News Corporation
- `19:24:02`    FLWS    is_common True (want True) | type CS | flags [] | 1-800-FLOWERS.COM, Inc.
- `19:24:02`    WWW     is_common True (want True) | type CS | flags [] | Wolverine World Wide, Inc.
- `19:24:02`    ARM     is_common True (want True) | type ADRC | flags [] | Arm Holdings plc American Depositary Shares
- `19:24:02`    PONY    is_common True (want True) | type ADRC | flags [] | Pony AI Inc. American Depositary Shares
- `19:24:02`    PFBC    is_common True (want True) | type CS | flags [] | Preferred Bank
- `19:24:02`    ASTS    is_common True (want True) | type CS | flags [] | AST SpaceMobile, Inc.
- `19:24:02`    EXR     is_common True (want True) | type CS | flags [] | Extra Space Storage Inc.
- `19:24:02`    MDA     is_common True (want True) | type CS | flags [] | MDA Space Ltd
- `19:24:03`    AAPL    is_common True (want True) | type CS | flags [] | Apple Inc.
- `19:24:03`    TMUSL   is_common False (want False) | type CS | flags ['is_preferred'] | T-Mobile US, Inc. 6.25 % Notes -01.09.69
- `19:24:03`    AFGB    is_common False (want False) | type SP | flags ['is_debt_or_structured'] | American Financial Group, Inc.
- `19:24:03`    TBB     is_common False (want False) | type SP | flags ['is_debt_or_structured'] | AT&T Inc. 5.35% GLB NTS 66
- `19:24:03`    BAC-PL  is_common False (want False) | type PFD | flags ['is_preferred'] | Bank of America Corporation
- `19:24:03`    SOMN    is_common False (want False) | type SP | flags ['is_debt_or_structured'] | The Southern Company
- `19:24:03`    STRC    is_common False (want False) | type PFD | flags ['is_preferred'] | Strategy Inc
- `19:24:03` ✅ identity check: 18 names, wrong: []
- `19:24:03`    REPORT RequestId: 28155808-c694-4dcd-b56e-69151bef6e92 Duration: 351791.85 ms Billed Duration: 352352 ms Memory Size: 3008 MB Max Memory Used: 1258 MB Init Duration: 559.50 ms 
XRAY TraceId: 1-6ae0f8e9-1dc09d6c7019ffac4c512a2b SegmentId: 3f73c72245dc1422 Sampled: true
- `19:24:03` ✅ ops 6697 complete
