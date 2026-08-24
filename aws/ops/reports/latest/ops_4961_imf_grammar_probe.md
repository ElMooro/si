## P0 drain status (bls-full / worldbank-full)

**Status:** failure  
**Duration:** 20.2s  
**Finished:** 2026-08-24T01:59:23+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4961_imf_grammar_probe.py", line 186, in <module>
    R.kv(legacy_flows=n_leg, new_ids=n_new,
                      ^^^^^
NameError: name 'n_leg' is not defined. Did you mean: 'n_new'?

```

## Log
- `01:59:02`   bls-full phase=COMPLETE files/banked=1659 gb=40.06 q=0 fail=3 as_of=2026-08-24T01:23:21
- `01:59:03`   worldbank-full phase=DRAIN files/banked=1796 gb=0.02 q=27366 fail=0 as_of=2026-08-24T01:52:28
## P1 dataflow catalogs (both generations)

- `01:59:03`   legacy host: DNS DEAD (registry fossil, recorded)
- `01:59:03`   2.1 dataflow XML: 200 bytes=446083 flows=222 sample=['QGDP_WCA_2026_FEB_VINTAGE', 'ANEA_2026_APR_VINTAGE', 'ANEA_2026_FEB_VINTAGE', 'IRFCL', 'GDD', 'QGFS_2026_JAN_VINTAGE', 'FA_2026_MAY_VINTAGE', 'LS_2026_FEB_VINTAGE']
- `01:59:03`   resolved flagships: {'BOP': 'BOP', 'IFS': None, 'DOT': None, 'CPI': 'CPI_WCA_2026_MAY_VINTAGE'}
## P2 full-pull ladder (capture refusals verbatim)

- `01:59:04`   BOP(BOP).full_default  200 bytes=3000000 trunc=True <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData x
- `01:59:06`   BOP(BOP).full_csv      200 bytes=3000000 trunc=True <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData x
- `01:59:09`   CPI(CPI_WCA_2026_MAY_VINTAGE).full_default 200 bytes=274909 trunc=False <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData x
- `01:59:10`   CPI(CPI_WCA_2026_MAY_VINTAGE).full_csv 200 bytes=274909 trunc=False <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData x
## P3 slice grammar (freq=A on IFS)

- `01:59:12`   dim_A                  200 bytes=2752 <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData xmlns:ss="h
- `01:59:12`   dim_A_dots             400 bytes=None {"status":400,"code":40000,"message":"key A.......... has more than expected 5 d
- `01:59:22`   lastN                  200 bytes=3000000 <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData xmlns:ss="h
- `01:59:23` artifact data/warm/imf-full/_probe/probe.json (ok_any=True)
