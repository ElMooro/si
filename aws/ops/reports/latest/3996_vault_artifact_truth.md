# ops 3996 — vault artifact truth + deployed census walk dump

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-07-28T03:06:38+00:00  

## Data

| deployed_marker_line | expected_paths_if_walked_fully | generated_at | last_modified | len_symbols | marker | n_live | n_symbols_field | vault_engine_marker | version |
|---|---|---|---|---|---|---|---|---|---|
|  |  | 2026-07-27T11:35:10.898408+00:00 |  | 561 | tradingview-vault v3.8 ops3974 asia-wire+jpexpyy-fix | 456 | 561 |  | 3.2 |
|  | 561 |  |  |  |  |  |  |  |  |
| MARKER = "data-census v2.1 ops3993 full-lists" |  |  |  |  |  |  |  |  |  |
|  |  |  | 2026-07-27T06:13:53.000+0000 |  |  |  |  | MARKER = "tradingview-vault v3.8 ops3974 asia-wire+jpexpyy-fix" |  |

## Log
## A. the live vault artifact itself

- `03:06:37`   contains EUBUND: True
- `03:06:37`   contains JP02Y: True
- `03:06:37`   contains NO03Y: True
- `03:06:37`   contains PETOT: True
- `03:06:37`   contains JPLG: True
- `03:06:37`   contains US10Y: True
- `03:06:37`   contains USM0: True
- `03:06:37`   contains XAUUSD: True
- `03:06:37`   first 6: ['UNTAGGED', 'DXY', 'FEDFUNDS', 'SPX', 'MOVE', 'CL1!']
- `03:06:37`   last 6:  ['VFMO', 'VICI', 'VPL', 'VUSB', 'VWO', 'XLRE']
- `03:06:37`   numeric fields on row0: 1 (['n_notes'])
## B. deployed census walk() — the list branch, verbatim

- `03:06:37`   elif isinstance(o, list) and o:
          if isinstance(o[0], dict):
              idf = next((f for f in ID_FIELDS if isinstance(o[0].get(f), str)), None)
              if idf:
                  for el in o[:list_cap]:
                      tag = str(el.get(idf, "?"))[:24] if isinstance(el, dict) else "?"
                      walk(el, f"{pre}[{tag}]", depth + 1, out, cap, list_cap)
                      if len(out) >= MAX_PATHS_PER:
                          return out
                  return out
          walk(o[0], f"{pre}[0]", depth + 1, out, cap, list_cap)
      elif isinstance(o, (int, float)) and not isinstance(o, bool):
          out.append({"p": pre, "v": float(o), "leaf": pre.split(".")[-1], "name": Non
- `03:06:37`   CALL SITE: pl = walk(doc, cap=(12000 if key in PRIORITY else MAX_PATHS_PER),
                        list_cap=(700 if key in PRIORITY else MAX_LIST_SAMPLE))
              # v2.1: MAX_LIST_SAMPLE=40 was the REAL binding constraint all
              # along — symbols[0..39] walked, JPLG/JP02Y/NO03Y/PETOT/EUBUND
        
## C. vault ENGINE deployed marker (is v3.8 live?)

- `03:06:38` ✅ PROBE DONE — A decides shrunken-artifact vs census bug; B settles the deployed-code question; C dates the vault code
