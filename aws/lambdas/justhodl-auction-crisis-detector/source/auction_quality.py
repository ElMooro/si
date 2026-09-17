"""Dated auction measurements; publication alone cannot make an old score current."""
from datetime import datetime, timezone


def stamp_quality(report, now=None):
    now = now or datetime.now(timezone.utc)
    day = (report.get('freshness') or {}).get('latest_auction_date')
    status = 'unavailable'
    if day:
        try:
            age = (now.date()-datetime.fromisoformat(day[:10]).date()).days
            status = 'invalid' if age < 0 else 'stale' if age > 7 else 'fresh'
        except (ValueError, TypeError):
            status = 'invalid'
    if not report.get('recent_auctions'):
        status = 'unavailable'
    report['quality'] = {'observation_date': day, 'publication_date': report.get('generated_at'),
                         'frequency':'daily', 'freshness_basis':'observation', 'status':status,
                         'missing':[] if status=='fresh' else ['current_auction_results']}
    report['call'] = None
    if status != 'fresh':
        report.update(composite_score=None, regime='UNAVAILABLE', interpretation='Current auction observations unavailable; no current stress assessment.')
        for row in (report.get('tail_risk') or {}).values():
            row.update(probability=None, heuristic_score=None, status='unavailable')
        report['triggers'] = []
    return report
