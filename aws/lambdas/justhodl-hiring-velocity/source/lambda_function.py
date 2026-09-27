"""Preserved v1 methodology below; active v1.1 emits dated research observations.

The unvalidated leading-growth/score claims in this historical description are
not the active measurement contract. See hiring_observations.py and the final
lambda_handler. The full unmodified predecessor is retained as an audit fixture.

justhodl-hiring-velocity — headcount-inflection leading-growth detector.

A microcap that is aggressively growing headcount is scaling its business
BEFORE the revenue line fully shows it. Hiring velocity is one of the
cleanest leading indicators of a small-company revenue inflection, and —
unlike live job-board scraping — quarterly employee counts are reliably
available from FMP, so this runs on data we already pay for.

PER STOCK it measures:
  • Headcount trend       — YoY and 2yr CAGR of full-time employees
  • Headcount acceleration — is the GROWTH RATE itself rising? (2nd derivative)
  • Revenue per employee   — productivity, and whether it is improving
                             (scaling efficiently) or falling (scaling sloppily)
  • Expansion inflection   — headcount turning up after a flat/declining stretch

SCORE 0-100 (expansion_score):
  rewards: strong + accelerating headcount growth, improving revenue/employee
  penalises: shrinking headcount, collapsing productivity (hiring without output)

The strongest setups: a small-cap hiring 25%+ YoY, with that pace ACCELERATING,
while revenue-per-employee holds or rises. That is a company outgrowing its
own reported numbers — exactly the profile that precedes a revenue inflection.

OUTPUT: data/hiring-velocity.json — ranked expansion-inflection candidates,
cross-tagged with bagger-engine score where available.

HARD GATE: cap_bucket in {nano, micro, small, mid} (same multibagger universe).
Schedule: weekly cron(30 12 ? * SUN *) — headcount data updates with filings.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/hiring-velocity.json"
UNIVERSE_KEY = "data/universe.json"
BAGGER_KEY = "data/bagger-engine.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
CAP_BUCKETS = {"nano", "micro", "small", "mid"}

s3 = boto3.client("s3", region_name="us-east-1")


def _get_json(url, timeout=12, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-Hiring/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.4 * (i + 1))
    return None


def fmp(path, symbol, limit=None, extra=""):
    url = f"https://financialmodelingprep.com/stable/{path}?symbol={symbol}&apikey={FMP_KEY}"
    if limit:
        url += f"&limit={limit}"
    if extra:
        url += extra
    return _get_json(url)


def fetch_employee_history(symbol):
    """Try the documented endpoint names in order; degrade gracefully.
    Returns a list of {date/period, employeeCount} newest-first, or []."""
    for path in ("historical-employee-count", "employee-count"):
        d = fmp(path, symbol, limit=16)
        if isinstance(d, list) and d:
            rows = []
            for r in d:
                cnt = (r.get("employeeCount") or r.get("fullTimeEmployees")
                       or r.get("employees"))
                dt = (r.get("periodOfReport") or r.get("date")
                      or r.get("filingDate") or r.get("acceptedDate"))
                if cnt:
                    try:
                        rows.append({"date": str(dt)[:10], "count": int(cnt)})
                    except Exception:
                        pass
            if rows:
                # ensure newest-first
                rows.sort(key=lambda x: x["date"], reverse=True)
                return rows
    return []


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def cagr(first, last, years):
    try:
        if first is None or last is None or years <= 0 or first <= 0 or last <= 0:
            return None
        return (last / first) ** (1.0 / years) - 1.0
    except Exception:
        return None


def analyze(stock):
    sym = stock.get("symbol")
    if not sym:
        return None

    emp = fetch_employee_history(sym)
    if len(emp) < 3:
        return None  # need history to measure velocity

    counts = [e["count"] for e in emp]   # newest-first
    latest = counts[0]
    if latest <= 0:
        return None

    # YoY: emp data is typically annual for older filings; treat index 1 as ~1yr
    prior_1 = counts[1] if len(counts) > 1 else None
    prior_2 = counts[2] if len(counts) > 2 else None
    oldest = counts[-1]
    n_periods = len(counts) - 1

    yoy = None
    if prior_1 and prior_1 > 0:
        yoy = (latest - prior_1) / prior_1
    prev_yoy = None
    if prior_1 and prior_2 and prior_2 > 0:
        prev_yoy = (prior_1 - prior_2) / prior_2
    multi_cagr = cagr(oldest, latest, n_periods)

    # acceleration: current YoY vs prior YoY
    accel = None
    if yoy is not None and prev_yoy is not None:
        accel = yoy - prev_yoy

    # revenue per employee + trend
    income = fmp("income-statement", sym, limit=3) or []
    rev_per_emp = None
    rev_per_emp_trend = None
    if income and counts:
        rev0 = income[0].get("revenue")
        if rev0 and latest:
            rev_per_emp = rev0 / latest
        if len(income) > 1 and len(counts) > 1:
            rev1 = income[1].get("revenue")
            if rev1 and prior_1 and prior_1 > 0 and rev_per_emp:
                rpe_prior = rev1 / prior_1
                if rpe_prior > 0:
                    rev_per_emp_trend = (rev_per_emp - rpe_prior) / rpe_prior

    # ── scoring ──
    score = 50.0
    notes = []

    if yoy is not None:
        yp = yoy * 100
        if yp >= 40:
            score += 26; notes.append(f"headcount +{yp:.0f}% YoY — aggressive scaling")
        elif yp >= 20:
            score += 18; notes.append(f"headcount +{yp:.0f}% YoY — strong hiring")
        elif yp >= 8:
            score += 9; notes.append(f"headcount +{yp:.0f}% YoY — steady hiring")
        elif yp >= 0:
            score += 0; notes.append(f"headcount roughly flat ({yp:+.0f}% YoY)")
        else:
            score -= 18; notes.append(f"headcount SHRINKING ({yp:.0f}% YoY)")

    inflection = False
    if accel is not None:
        ap = accel * 100
        if ap >= 10:
            score += 16; notes.append(f"hiring pace ACCELERATING (+{ap:.0f}pp)")
            if prev_yoy is not None and prev_yoy <= 0.03 and (yoy or 0) > 0.10:
                inflection = True
                score += 8
        elif ap <= -10:
            score -= 12; notes.append(f"hiring pace decelerating ({ap:.0f}pp)")

    if rev_per_emp_trend is not None:
        rt = rev_per_emp_trend * 100
        if rt >= 5:
            score += 12; notes.append(f"revenue/employee +{rt:.0f}% — scaling efficiently")
        elif rt <= -10:
            score -= 14; notes.append(f"revenue/employee {rt:.0f}% — hiring outpacing output")
        else:
            notes.append("revenue/employee stable")

    if multi_cagr is not None and multi_cagr > 0.15:
        score += 6; notes.append(f"{multi_cagr*100:.0f}% multi-year headcount CAGR")

    score = clamp(score)

    if inflection:
        state = "EXPANSION_INFLECTION"
    elif score >= 78:
        state = "AGGRESSIVE_EXPANSION"
    elif score >= 62:
        state = "STEADY_EXPANSION"
    elif score >= 45:
        state = "FLAT"
    else:
        state = "CONTRACTING"

    return {
        "symbol": sym,
        "name": stock.get("name"),
        "sector": stock.get("sector"),
        "cap_bucket": stock.get("cap_bucket"),
        "market_cap": stock.get("market_cap"),
        "expansion_score": round(score, 1),
        "state": state,
        "headcount_latest": latest,
        "headcount_yoy_pct": round(yoy * 100, 1) if yoy is not None else None,
        "headcount_accel_pp": round(accel * 100, 1) if accel is not None else None,
        "headcount_multiyr_cagr_pct": round(multi_cagr * 100, 1) if multi_cagr is not None else None,
        "revenue_per_employee": round(rev_per_emp) if rev_per_emp else None,
        "revenue_per_employee_trend_pct": round(rev_per_emp_trend * 100, 1) if rev_per_emp_trend is not None else None,
        "inflection": inflection,
        "notes": notes,
        "history": emp[:8],
    }


def _legacy_lambda_handler(event, context):
    t0 = time.time()
    print(f"[hiring-velocity] starting {datetime.now(timezone.utc).isoformat()}")
    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=UNIVERSE_KEY)
        universe = json.loads(obj["Body"].read()).get("stocks", [])
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"universe load: {e}"})}

    candidates = [s for s in universe if s.get("cap_bucket") in CAP_BUCKETS]
    limit = int(event.get("limit", 0)) if isinstance(event, dict) else 0
    if limit:
        candidates = candidates[:limit]
    print(f"[hiring-velocity] scanning {len(candidates)} stocks")

    # bagger scores for cross-tagging
    bagger_scores = {}
    try:
        b = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=BAGGER_KEY)["Body"].read())
        for r in b.get("top_100", []):
            bagger_scores[r.get("symbol")] = r.get("bagger_score")
    except Exception:
        pass

    results = []
    errors = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(analyze, s): s for s in candidates}
        done = 0
        for f in as_completed(futs):
            done += 1
            try:
                r = f.result()
                if r:
                    r["bagger_score"] = bagger_scores.get(r["symbol"])
                    results.append(r)
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[hiring-velocity] err: {e}")
            if done % 300 == 0:
                print(f"[hiring-velocity] {done}/{len(candidates)} "
                      f"scored={len(results)} t={time.time()-t0:.0f}s")

    results.sort(key=lambda x: -x["expansion_score"])
    for i, r in enumerate(results):
        r["rank"] = i + 1

    inflections = [r for r in results if r["inflection"]]
    aggressive = [r for r in results if r["state"] == "AGGRESSIVE_EXPANSION"]
    # highest conviction: hiring inflection AND a strong bagger score
    double_confirmed = [r for r in results
                        if r["expansion_score"] >= 70
                        and (r.get("bagger_score") or 0) >= 70]

    out = {
        "schema_version": "1.0",
        "method": "hiring_velocity_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_scanned": len(candidates),
        "n_scored": len(results),
        "n_errors": errors,
        "counts": {
            "expansion_inflection": len(inflections),
            "aggressive_expansion": len(aggressive),
            "double_confirmed_with_bagger": len(double_confirmed),
        },
        "top_50": results[:50],
        "expansion_inflections": inflections[:30],
        "double_confirmed": sorted(double_confirmed,
                                    key=lambda r: -(r["expansion_score"]
                                                    + (r.get("bagger_score") or 0)))[:25],
        "methodology": (
            "Headcount inflection as a leading revenue indicator. Rewards strong + "
            "accelerating headcount growth with improving revenue/employee. "
            "double_confirmed = hiring expansion AND high bagger-engine score — "
            "the highest-conviction multibagger setups."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=3600")

    if double_confirmed or inflections:
        picks = (out["double_confirmed"][:5] or inflections[:5])
        lines = [f"- <b>{r['symbol']}</b> {(r['name'] or '')[:22]} — "
                 f"headcount {r.get('headcount_yoy_pct')}% YoY, exp score {r['expansion_score']}"
                 + (f", bagger {r['bagger_score']}" if r.get("bagger_score") else "")
                 for r in picks]
        maybe_telegram("[hiring] <b>EXPANSION INFLECTIONS</b> — hiring ahead of revenue:\n"
                        + "\n".join(lines))

    print(f"[hiring-velocity] done {out['elapsed_s']}s scored={len(results)} "
          f"inflections={len(inflections)} double_confirmed={len(double_confirmed)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_scored": len(results), "counts": out["counts"],
        "top_5": [r["symbol"] for r in results[:5]]})}


# Active reported-workforce research path. Complete predecessor retained above.
import hashlib
import re
from urllib.parse import quote_plus
from hiring_observations import CONTRACT, strict, clock, envelope, original, employee_rows, dossier


def _hiring_fetch(symbol, endpoint, limit):
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,19}', symbol):
        return {'status': 'invalid_symbol_not_requested', 'endpoint': endpoint}
    if not FMP_KEY:
        return {'status': 'credential_unavailable', 'endpoint': endpoint}
    url = 'https://financialmodelingprep.com/stable/'+endpoint+'?symbol='+quote_plus(symbol)+'&limit='+str(limit)
    # Provider documents header authorization; the secret never enters a logged URL.
    req = request.Request(url, headers={'User-Agent': 'JustHodl-Hiring/1.1', 'apikey': FMP_KEY})
    class NoRedirect(request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    try:
        with request.build_opener(NoRedirect()).open(req, timeout=12) as response:
            raw = response.read(256*1024+1)
        if len(raw) > 256*1024:
            return {'status': 'response_exceeds_bound', 'endpoint': endpoint, 'original_retained': False}
        if FMP_KEY.encode() in raw:
            return {'status': 'credential_echo_withheld', 'endpoint': endpoint, 'original_retained': False}
        return envelope(raw, datetime.now(timezone.utc).isoformat(), endpoint)
    except Exception as exc:
        return {'status': 'rate_limited' if getattr(exc, 'code', None) == 429 else 'unavailable',
                'endpoint': endpoint, 'http_status': getattr(exc, 'code', None), 'error_type': type(exc).__name__}


def _hiring_company(stock, today):
    item = stock if isinstance(stock, dict) else {}; symbol = item.get('symbol'); acquisitions = []
    for endpoint in ('historical-employee-count', 'employee-count'):
        capture = _hiring_fetch(symbol, endpoint, 16); acquisitions.append(capture)
        if capture['status'] in ('rate_limited', 'credential_unavailable', 'invalid_symbol_not_requested'):
            return acquisitions
        # Never blend a received population with another endpoint, even when unqualified.
        parsed = original(capture)
        if isinstance(parsed, list) and parsed:
            employees = employee_rows(symbol, capture, today)
            if sum(r['measurement_status'] == 'reported_count' for r in employees) >= 3:
                acquisitions.append(_hiring_fetch(symbol, 'income-statement', 3))
            else:
                acquisitions.append({'endpoint': 'income-statement', 'status': 'not_requested_insufficient_employee_history'})
            return acquisitions
    acquisitions.append({'endpoint': 'income-statement', 'status': 'not_requested_insufficient_employee_history'})
    return acquisitions


def _hiring_object(key, bound):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    raw = obj['Body'].read(bound+1)
    if len(raw) > bound or obj.get('ContentLength') != len(raw) or not obj.get('ETag'):
        raise ValueError('Whole declared public object required')
    return raw, obj['ETag']


def _hiring_previous():
    try:
        raw, etag = _hiring_object(S3_KEY, 64*1024*1024)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey'):
            return None, None, None
        raise
    parsed = strict(raw)
    if not isinstance(parsed, dict):
        raise ValueError('Previous public packet malformed')
    return parsed, raw, etag


def _hiring_archive(raw):
    key = 'data/hiring-velocity/history/'+hashlib.sha256(raw).hexdigest()+'.json'
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=raw, IfNoneMatch='*',
                      ContentType='application/json', CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    back, _ = _hiring_object(key, len(raw))
    if back != raw:
        raise ValueError('Immutable whole publication readback differs')
    return {'key': key, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def lambda_handler(event=None, context=None):
    started = time.monotonic(); checked = datetime.now(timezone.utc); today = checked.date().isoformat()
    previous, previous_raw, etag = _hiring_previous()
    if previous and previous.get('measurement_contract') == CONTRACT:
        stamp = clock(previous.get('generated_at'))
        if stamp is None or stamp >= checked:
            raise ValueError('Previous publication clock invalid')
    raw, universe_etag = _hiring_object(UNIVERSE_KEY, 8*1024*1024)
    universe_capture = envelope(raw, datetime.now(timezone.utc).isoformat(), UNIVERSE_KEY)
    universe_doc = original(universe_capture)
    if not isinstance(universe_doc, dict) or not isinstance(universe_doc.get('stocks'), list):
        raise ValueError('Complete received universe required; preserve previous publication')
    universe = universe_doc['stocks']; selected = [i for i, row in enumerate(universe)
        if isinstance(row, dict) and row.get('cap_bucket') in CAP_BUCKETS]
    requested_limit = event.get('limit') if isinstance(event, dict) else None
    if requested_limit is not None and (type(requested_limit) is not int or requested_limit < 0):
        raise ValueError('Event limit must be a nonnegative integer')
    if requested_limit:
        selected = selected[:requested_limit]
    # Preserve the existing bagger donor as context; scores never imply confirmation.
    try:
        bagger_raw, bagger_etag = _hiring_object(BAGGER_KEY, 4*1024*1024)
        bagger_capture = envelope(bagger_raw, datetime.now(timezone.utc).isoformat(), BAGGER_KEY)
        bagger_capture['etag'] = bagger_etag
    except Exception as exc:
        bagger_capture = {'endpoint': BAGGER_KEY, 'status': 'unavailable', 'error_type': type(exc).__name__}
    universe_capture['etag'] = universe_etag
    acquisitions = [[{'status': 'not_attempted_runtime_rate_or_size_limit', 'endpoint': 'historical-employee-count'}] for _ in selected]
    workers = max(1, min(MAX_WORKERS, 8)); received_bytes = 0
    def remaining():
        own = 600-(time.monotonic()-started)
        return min(own, context.get_remaining_time_in_millis()/1000) if context and hasattr(context, 'get_remaining_time_in_millis') else own
    # Bounded windows do not enqueue thousands of jobs beyond the Lambda deadline.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0, len(selected), workers):
            if remaining() < 90 or received_bytes >= 8*1024*1024:
                break
            jobs = [(i, pool.submit(_hiring_company, universe[selected[i]], today))
                    for i in range(offset, min(offset+workers, len(selected)))]
            for index, future in jobs:
                acquisitions[index] = future.result()
                received_bytes += sum(a.get('original_bytes', 0) for a in acquisitions[index])
            if any(a.get('status') == 'rate_limited' for i, _ in jobs for a in acquisitions[i]):
                break
    if not any(a.get('status') == 'received' and isinstance(original(a), list)
               for group in acquisitions for a in group):
        raise ValueError('No received employee/income population; preserve previous publication')
    records = []
    for index, (universe_index, captures) in enumerate(zip(selected, acquisitions)):
        record = dossier(universe[universe_index], captures, today)
        record.update(request_index=index, universe_index=universe_index)
        records.append(record)
    selected_set = set(selected)
    prior = _hiring_archive(previous_raw) if previous_raw is not None else None
    out = {'engine': 'justhodl-hiring-velocity', 'version': '1.1.0', 'schema_version': '1.1',
           'method': 'dated_reported_workforce_observations', 'measurement_contract': CONTRACT,
           'generated_at': datetime.now(timezone.utc).isoformat(), 'acquisition_started_at': checked.isoformat(),
           'checked_as_of': today, 'status': 'RESEARCH_ONLY', 'universe_acquisition': universe_capture,
           'bagger_context_acquisition': bagger_capture, 'request_records': records,
           'unselected_universe_indices': [i for i in range(len(universe)) if i not in selected_set],
           'universe_occurrences': len(universe), 'n_selected_occurrences': len(selected),
           'n_scanned': sum(any(a['status'] not in ('not_attempted_runtime_rate_or_size_limit', 'invalid_symbol_not_requested', 'credential_unavailable') for a in group) for group in acquisitions),
           'n_scored': 0, 'n_errors': sum(any(a['status'] in ('unavailable', 'rate_limited', 'invalid_original', 'response_exceeds_bound', 'credential_echo_withheld') for a in group) for group in acquisitions),
           'n_employee_observations': sum(len(r['employee_observations']) for r in records),
           'n_income_observations': sum(len(r['income_observations']) for r in records),
           'event_limit': requested_limit, 'cap_buckets': sorted(CAP_BUCKETS), 'previous_publication': prior,
           'top_50': [], 'expansion_inflections': [], 'double_confirmed': [],
           'counts': {'expansion_inflection': None, 'aggressive_expansion': None, 'double_confirmed_with_bagger': None},
           'call': None, 'calls_eligible': False, 'forecast_qualified': False, 'sizing_eligible': False, 'execution_eligible': False,
           'signals_logged': 0, 'notifications_sent': 0, 'private_state_read_or_written': False,
           'source_documentation': ['https://site.financialmodelingprep.com/developer/docs/stable/historical-employee-count'],
           'caveats': ['Reporting period, filing date, naive acceptance time and UTC receipt clock are separate; no timezone is invented.',
                       'Annual intervals require unique periods, matching issuer/count definition/annual form and 350–380 elapsed days.',
                       'Reported employee change includes unknown acquisition/disposal, geographic and workforce-definition effects; organic hiring is not verified.',
                       'Revenue divided by ending headcount is not revenue per average worker or a verified productivity measure.',
                       'Every selected occurrence remains visible, including unattempted acquisitions; provider history/universe completeness and original release vintages remain unverified.',
                       'The existing bagger context is retained without independent-evidence or investment confirmation claims.'],
           'elapsed_s': round(time.monotonic()-started, 2)}
    publication = json.dumps(out, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    if len(publication) > 64*1024*1024 or remaining() < 20:
        raise ValueError('Whole publication exceeds size/runtime reserve; preserve prior current packet')
    archived = _hiring_archive(publication)
    precondition = {'IfMatch': etag} if etag else {'IfNoneMatch': '*'}
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=publication, ContentType='application/json',
                  CacheControl='public, max-age=3600', **precondition)
    return {'statusCode': 200, 'body': json.dumps({'measurement_contract': CONTRACT,
            'n_selected_occurrences': len(selected), 'n_employee_observations': out['n_employee_observations'], 'archive': archived})}
