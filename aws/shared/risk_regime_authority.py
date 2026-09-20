"""Prevent descriptive Risk Regime measurements from becoming trading votes."""
import math
from datetime import datetime, timezone

# No Risk Regime forecasting model has passed independent qualification yet.
# Future promotion requires a reviewed model/scorecard identity in this code,
# never merely a self-declared flag in a producer packet.
QUALIFIED_MODELS = {}


def qualified_score(packet, permission='calls_eligible', now=None):
    if not isinstance(packet, dict) or permission not in ('calls_eligible', 'sizing_eligible'): return None
    if packet.get('contract') == 'risk-regime-research.v1': return None
    quality = packet.get('quality'); qualification = packet.get('decision_qualification')
    if (packet.get(permission) is not True or not isinstance(quality, dict) or quality.get('status') != 'fresh'
            or not isinstance(qualification, dict) or qualification.get('status') != 'qualified'
            or not qualification.get('scorecard_manifest_key')): return None
    identity = qualification.get('model_id')
    if not isinstance(identity, str) or identity not in QUALIFIED_MODELS or qualification.get('scorecard_sha256') != QUALIFIED_MODELS[identity]: return None
    value = packet.get('risk_regime_score')
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not -100 <= value <= 100:
        return None
    try:
        at = datetime.fromisoformat(packet['generated_at'].replace('Z', '+00:00'))
        age = ((now or datetime.now(timezone.utc))-at).total_seconds()
        if at.tzinfo is None or not 0 <= age <= 48*3600: return None
    except (ValueError, TypeError, KeyError, AttributeError): return None
    return value


def decision_view(packet, permission='calls_eligible'):
    """A denied legacy packet must not survive through label/narrative fallbacks."""
    value = qualified_score(packet, permission)
    if value is not None: return packet
    return {'risk_regime_score': None, 'risk_regime': 'UNQUALIFIED', 'regime': 'UNQUALIFIED',
        'posture': {'beta_tilt': None, 'size_mult': None, 'hedge': None}, 'tells': [],
        'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'generated_at': packet.get('generated_at') if isinstance(packet, dict) else None,
        'exclusion_reason': 'Risk Regime is descriptive research; no qualified decision or sizing score.'}
