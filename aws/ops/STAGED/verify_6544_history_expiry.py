"""Read-only readback with exact public-source scope matches; never PUTs.

Candidates are public literals from ops5027/5024/5164/5166/5306 and the
2026-04-25 archive lifecycle source. A candidate is not evidence of installation.
Only an exact live match is reported; every other identifier stays WITHHELD.
"""
import sys
import ops_6544_disable_6541_history_expiry as containment

PUBLIC_SOURCE_PREFIXES = frozenset({
    'data/providers/', 'backup/', 'live/', 'ai/models/repacked/', 'archive/'})
PUBLIC_SOURCE_RULE_IDS = frozenset({
    'ops5027-purge-dead-versions-providers', 'ops5164-purge-dead-versions-data',
    'ops5166-backup-30d', 'expire-access-logs-30d', 'expire-repacked-models'})


def main():
    containment.PUBLIC_PREFIXES = containment.PUBLIC_PREFIXES | PUBLIC_SOURCE_PREFIXES
    containment.PUBLIC_RULE_IDS = containment.PUBLIC_RULE_IDS | PUBLIC_SOURCE_RULE_IDS
    return containment.main(verify_only=True)

if __name__ == '__main__':
    sys.exit(main())
