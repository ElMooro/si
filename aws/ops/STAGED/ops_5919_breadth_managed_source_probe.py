"""Preserve public-market rolling state and inspect three native grouped-day responses."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import Counter
import gzip, io, json, math, sys, urllib.request, urllib.error
import boto3
from ops_5917_market_cycle_source_preflight import bounded, preserve, BUCKET, ROOT
sys.path.insert(0, str(ROOT/'aws/shared'))
from massive import get_massive_key, MASSIVE_BASE
from ops_report import report

DATES = ('2026-09-16', '2026-09-17', '2026-09-18')


def finite(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def main():
    with report('ops_5919_breadth_managed_source_probe') as r:
        s3 = boto3.client('s3', region_name='us-east-1')
        lam = boto3.client('lambda', region_name='us-east-1')
        r.kv(engine_invocations=0, private_account_reads=0, notifications_sent=0,
             portfolio_writes=0, paid_ai_calls=0, provider_requests_maximum=len(DATES))
        # This is the all-market provider cache written by the reviewed breadth producer,
        # not a private watchlist, portfolio, Brain or account object.
        raw = bounded(s3.get_object(Bucket=BUCKET, Key='data/market-internals-state.json.gz')['Body'])
        saved = preserve(s3, raw)
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            decoded = stream.read(96*1024*1024+1)
        assert len(decoded) <= 96*1024*1024
        state = json.loads(decoded); days = state.get('days') or []
        counts = Counter(len(v) for v in (state.get('closes') or {}).values() if isinstance(v, list))
        r.kv(protected_market_cache=saved, retained_state={'processed_dates': len(days),
             'first_date': min(days) if days else None, 'last_date': max(days) if days else None,
             'requested_days_marked_processed': {day: day in days for day in DATES},
             'tail_lengths': dict(sorted(counts.items())), 'ticker_tails': sum(counts.values()),
             'previous_close_count': len(state.get('prev_close') or {})})
        cfg = lam.get_function_configuration(FunctionName='justhodl-market-internals')
        r.kv(legacy_environment_key_configured=bool((cfg.get('Environment') or {}).get('Variables', {}).get('POLYGON_KEY')))
        credential = get_massive_key()
        assert credential, 'Existing managed Massive credential is not configured'
        for day in DATES:
            safe_url = MASSIVE_BASE+'/v2/aggs/grouped/locale/us/market/stocks/'+day+'?adjusted=true&include_otc=false'
            req = urllib.request.Request(safe_url, headers={'Authorization': 'Bearer '+credential,
                'User-Agent': 'JustHodl-research-audit/1.0 (+https://justhodl.ai)'})
            try:
                # Refuse redirects so authorization cannot leave this provider host.
                class NoRedirect(urllib.request.HTTPRedirectHandler):
                    def redirect_request(self, *args, **kwargs): return None
                with urllib.request.build_opener(NoRedirect()).open(req, timeout=35) as response:
                    body = bounded(response)
            except urllib.error.HTTPError as exc:
                r.kv(grouped_day={'requested_date': day, 'safe_source_url': safe_url, 'http_status': exc.code,
                                 'empty_session_not_inferred': True})
                continue
            except Exception as exc:
                r.kv(grouped_day={'requested_date': day, 'error_type': type(exc).__name__,
                                 'empty_session_not_inferred': True})
                continue
            assert credential.encode() not in body, 'Provider response reflected authentication material'
            original = preserve(s3, body); doc = json.loads(body); rows = doc.get('results') or []
            assert isinstance(rows, list)
            symbols = Counter(row.get('T') for row in rows if isinstance(row, dict))
            dates = Counter(); invalid = 0; otc = 0; priced = 0
            for row in rows:
                if not isinstance(row, dict): invalid += 1; continue
                valid = (isinstance(row.get('T'), str) and bool(row['T']) and
                    finite(row.get('c')) and row['c'] > 0 and finite(row.get('v')) and row['v'] >= 0 and
                    finite(row.get('t')))
                if not valid: invalid += 1; continue
                dates[datetime.fromtimestamp(row['t']/1000, ZoneInfo('America/New_York')).date().isoformat()] += 1
                otc += row.get('otc') is True
                priced += row['c'] >= 1 and row['v'] >= 50000
            r.kv(grouped_day={'requested_date': day, 'safe_source_url': safe_url,
                'acquired_at': datetime.now(timezone.utc).isoformat(), 'protected_original': original,
                'provider_status': doc.get('status'), 'adjusted': doc.get('adjusted'),
                'query_count': doc.get('queryCount'), 'results_count': doc.get('resultsCount'),
                'rows': len(rows), 'duplicate_symbols': sum(n-1 for n in symbols.values() if n>1),
                'invalid_rows': invalid, 'native_session_dates_et': dict(dates), 'explicit_otc_rows': otc,
                'legacy_price_volume_filter_rows': priced, 'pagination_present': bool(doc.get('next_url'))})
        del credential, cfg
        r.kv(next_work='Use native session identity and original adjusted responses; rebuild metric-specific populations and complete windows. Failed requests remain retryable and never become holidays. No current breadth score or cycle call is qualified by this probe.')


if __name__ == '__main__':
    try: main()
    except Exception:
        print('Breadth source probe failed; inspect its committed report before retrying. No producer was invoked.')
        sys.exit(1)
