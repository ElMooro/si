"""Reviewed cohort compiler and upstream issuer compiler identities."""
QUALIFIED={'bond_flow.py': '3d58999c5e8d52209ddaeba3d7bc420ab70ec824ec68ac4f4d012bdc481ef0ed', 'verify_bond_flow.py': 'fab00fd2d52877977761655d4a241f2067c752e792f5ec2dcbc0b54ace10d7f1'}
# 2026-11-21: re-qualified against justhodl-etf-true-flows 3.1.0 (ops 6889, read-only on the live publication: the cohort compiler accepts it and the independent
# check passes - 11 cohorts, 33 windows, 138 members, 66 rational sums). 3.0.1 / 3.1.0 changed etf_native and etf_research and added etf_issuers to the publication's
# compiler inventory; with the old four pins every run since 2026-11-11 ended 'withheld_ValueError'.
UPSTREAM={'etf_issuers': '96a1ad4eed71d1bd3ab447718a159208086a79157501692ac63b97554a948bcf', 'etf_native': 'c2a17295b8bfc4ec1c1339b0fdb35d0a728e26244635521c5ab9a25555bac2dc', 'etf_research': '24187530a946bd5b043365288c1108a14d86dec4c37c9797b945dfbbce2e9c76', 'etf_universe': '7b8da9b7d0a627fd5fd43f96ef77c1ecc3d5064fc8ad937171ac4d5e5b41ad63', 'evidence_store': '3baf3781f7450aafd6ab5ba96418752ebdeee3f154c62aefa31d72717479c976'}
