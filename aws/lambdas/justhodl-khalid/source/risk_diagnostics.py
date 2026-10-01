"""Bounded presentation projection; never policy or authority validation."""
from datetime import datetime
import math

SCHEMA = 'khalid-risk-diagnostics.v1'
SOURCES = {
    'risk_gate': ('risk-gate-research.v1', ['composite', 'sizing_multiplier'], 'Risk Gate withholds unqualified regime and sizing authority.'),
    'bond_warroom': (None, ['eurodollar_shortage.score'], 'The bond desk explicitly withholds offshore-dollar shortage classifier authority.'),
    'eurodollar_stress': ('eurodollar-native-research.v1', ['composite_score'], 'Eurodollar research withholds unvalidated stress scores and thresholds.'),
    'credit_composite': ('credit-composite-abstention.v1', ['composite', 'composite_score'], 'Credit donors have no qualified composite vote or sizing authority.'),
}
SUFFIX = ' Dated observations do not replace the required numeric authority. The existing critical-input rejection remains in force.'


def clock(value):
    if not isinstance(value, str) or not 10 <= len(value) <= 40:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return value if stamp.tzinfo is not None else None
    except ValueError:
        return None


def project(artifact):
    artifact = artifact if isinstance(artifact, dict) else {}
    generated = clock(artifact.get('generated_at'))
    expires = clock(artifact.get('expires_at'))
    health = artifact.get('source_health')
    valid = (artifact.get('engine') == 'justhodl-khalid-risk' and artifact.get('schema_version') == '1.0.0'
             and generated is not None and expires is not None and isinstance(health, list) and len(health) <= 64)
    result = {'schema_version': SCHEMA, 'artifact': 'data/khalid-risk.json',
              'generated_at': generated, 'expires_at': expires, 'rows': []}
    for name, (contract, paths, explanation) in SOURCES.items():
        out = {'source_id': name, 'status': 'UNAVAILABLE', 'authority_diagnostic': None,
               'source_as_of': None, 'age_h': None, 'max_age_h': None}
        matches = [h for h in health if isinstance(h, dict) and h.get('name') == name] if valid else []
        if len(matches) == 1:
            h = matches[0]
            expected = {'schema_version': 'withheld-authority-diagnostic.v1', 'code': 'PRODUCER_AUTHORITY_WITHHELD',
                        'source_id': name, 'producer_contract': contract, 'unavailable_authority_paths': paths,
                        'explanation': explanation + SUFFIX, 'effect': 'EXPLANATION_ONLY'}
            numbers = [h.get('age_h'), h.get('max_age_h')]
            if (h.get('status') == 'INVALID' and h.get('critical') is True
                    and h.get('producer') == 'justhodl-' + name.replace('_', '-')
                    and h.get('key') == 'data/' + name.replace('_', '-') + '.json'
                    and h.get('authority_diagnostic') == expected and clock(h.get('as_of')) is not None
                    # Bound before float coercion: JSON integers can exceed float range.
                    and all(type(n) in (int, float) and 0 <= n <= 2**53 - 1 and math.isfinite(n) for n in numbers)
                    and numbers[1] > 0):
                out.update(status='INVALID', authority_diagnostic=expected, source_as_of=h['as_of'],
                           age_h=h['age_h'], max_age_h=h['max_age_h'])
        result['rows'].append(out)
    return result
