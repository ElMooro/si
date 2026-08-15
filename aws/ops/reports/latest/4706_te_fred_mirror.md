# ops 4706 — TE's dedicated /fred/historical/ mirror endpoint

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-08-15T17:01:32+00:00  

## Error

```
SystemExit: 1
```

## Log
- `17:01:31`   stored key length=31 (never printed; testing whether it's already key:secret formatted)
## 1. THE EXACT test — Khalid's symbol, existing key

- `17:01:31`   status=200 bytes=2
- `17:01:31`   parsed 0 rows, dates None -> None
## verdict 1

- `17:01:31` Did not confirm on the first shape — see raw response above for the real reason before abandoning (auth error vs symbol error vs format error all look different)
## 2. If it worked — does the pattern generalize? Test 2 more: a core-27 series and a still-gap series

## verdict

- `17:01:32` ✗ existing key did not unlock the /fred/historical/ endpoint on this pass — raw response above shows exactly why
