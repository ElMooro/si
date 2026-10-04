# Retained static producer sources

These complete historical sources support the offline page output contracts.
Keep them outside `aws/ops/ran` and `aws/ops/reports`, which the existing
retention workflow archives by age. The `.py.txt` suffix keeps the files inert;
the contract builder parses their Python source without importing or running it.
They are excluded from the public site artifact with the rest of `scripts/`.

`ops_4281_alpha_atlas.py.txt` is the exact 9,618-byte
`aws/ops/ran/ops_4281_alpha_atlas.py` from commit
`77ce5dad1887b0a36b1244ecd35d0a1d4531eb92`, Git blob
`54c67fee8ab8b602dae81dcdf11e8a01f753e5c4`, SHA-256
`522046df7f369156caa8344b51a446df29e72c91623e655d64e029bc4b87a6b3`.
It proves the existing write argument for `data/alpha-atlas.json`, not current
data availability, freshness or execution. Do not execute this historical
operation or copy it into a pending lane to validate a Pages build.

The regression in `tests/deployment/test_static_producer_retention.py` runs the
existing retention archive/removal commands in a temporary Git repository,
checks that old ops files are removed while the retained source survives, and
compiles the same static output contract before and after the sweep. Missing,
malformed, wrong-output and private-output source proofs still fail.
