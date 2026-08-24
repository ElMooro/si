## G-1 markers-in-checkout

**Status:** failure  
**Duration:** 965.5s  
**Finished:** 2026-08-24T03:28:43+00:00  

## Error

```
SystemExit: 1
```

## Log
- `03:12:37`   ok justhodl-dol-full        'v1.0.0 ops4966'
- `03:12:37`   ok justhodl-src-mirror      'bsrm-truth ops 4966'
- `03:12:37`   ok justhodl-provider-catalog 'dol-note-v2'
## G0 settle x3

- `03:12:38`   justhodl-dol-full settled (0s)
- `03:13:29`   justhodl-src-mirror settled (51s)
- `03:13:29`   justhodl-provider-catalog settled (0s)
- `03:13:29` G0 PASS
## G0b dol schedule rate(6 hours)

- `03:13:30` G0b created
## G1 dol-full run

- `03:13:55`   t+  25s files=None
- `03:14:20`   t+  50s files=70
- `03:14:45`   t+  75s files=70
- `03:15:11`   t+ 100s files=70
- `03:15:36`   t+ 126s files=70
- `03:16:01`   t+ 151s files=70
- `03:16:26`   t+ 176s files=70
- `03:16:51`   t+ 201s files=70
- `03:17:17`   t+ 226s files=70
- `03:17:42`   t+ 252s files=70
- `03:18:07`   t+ 277s files=70
- `03:18:32`   t+ 302s files=70
- `03:18:57`   t+ 327s files=70
- `03:19:23`   t+ 352s files=70
- `03:19:48`   t+ 378s files=70
- `03:20:13`   t+ 403s files=70
- `03:20:38`   t+ 428s files=70
- `03:21:03`   t+ 453s files=70
- `03:21:29`   t+ 478s files=70
- `03:21:54`   t+ 504s files=70
- `03:22:19`   t+ 529s files=70
- `03:22:44`   t+ 554s files=70
- `03:22:44` G1 PASS files=70 mb=160.6 universe=70 failures=0
- `03:22:44`   ar539 head: b'"st","rptdate","c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11","c12","c'
## G2 bsrm truth verified

- `03:23:10`   t+  25s orphans awaiting v1.2 write
- `03:23:35`   t+  50s orphans awaiting v1.2 write
- `03:24:00`   t+  75s orphans awaiting v1.2 write
- `03:24:26`   t+ 100s orphans awaiting v1.2 write
- `03:24:51`   t+ 125s orphans awaiting v1.2 write
- `03:25:16`   t+ 151s orphans awaiting v1.2 write
- `03:25:41` G2 FAIL dupnote=True closed=['ofr-bsrm-series'] phase2=['nyfed-haircuts-series'] hfm=99.0h
## G3 cards (post-mark)

- `03:28:43`   dol : FULL ETA DataDownloads corpus (dol-full v1): 70 report csvs · 160.6MB · 0 fresh / 70 unchanged · self-extending harvest
- `03:28:43`   bsrm: src-mirror since ops 4913 (workbooks conditional-ETag, FULL) · bsrm-truth ops 4966: series/ = flagged duplicate of ofr-hfm (4752 bug, 4753 note) -- no transform
- `03:28:43` G3 PASS
- `03:28:43` ops 4966 RED: G2
