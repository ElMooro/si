# ops 3850 — global-recession as CONTEXT (prove it moves nothing)

**Status:** failure  
**Duration:** 257.9s  
**Finished:** 2026-07-25T03:59:14+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Snapshot scoring BEFORE

- `03:54:56` ✅   37 scores · overweight=['XLV', 'XLF', 'RSP', 'IWD', 'IWM', 'SPY', 'XLK']
## 2. Deploy + zip-settle

- `03:54:57`   zip: 95594 bytes
## 1. Lambda

- `03:54:57`   Lambda exists — updating
- `03:55:00` ✅   ✓ updated justhodl-rotation-dashboard
- `03:55:10` ✅   settled after 10s
## 3. Invoke

- `03:55:14` ✅   invoked clean
## 4. THE NEGATIVE GATE — nothing about scoring may change

- `03:55:14` ✅   no confluence score moved moved=[]
- `03:55:14` ✅   overweight list identical ['XLV', 'XLF', 'RSP', 'IWD', 'IWM', 'SPY', 'XLK'] -> ['XLV', 'XLF', 'RSP', 'IWD', 'IWM', 'SPY', 'XLK']
- `03:55:14` ✅   eligible count identical 20 -> 20
## 5. The context block itself

- `03:55:14` ✅   context block present 
- `03:55:14` ✅   probability carried = 32.0%
- `03:55:14` ✅   applied_to_score is FALSE 
- `03:55:14` ✅   why-not-applied disclosed 
- `03:55:14` ✅   coverage verdict carried = PARTIAL — only 4 of 33 countries have an independent check; 
- `03:55:14`     confirmed=4 divergent=5 unconfirmed_share=27.1% us_probit=27.0%
## 6. Page renders it

- `03:59:14` ✗   page marker 'v3-ops3850' never reached the edge
