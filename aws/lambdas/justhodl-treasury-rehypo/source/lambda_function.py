"""Treasury collateral and funding review; unobservable reuse metrics remain null."""
import gzip
import io
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from macro_donor_inputs import fails_context
from donor_contract import inspect_donor
from managed_secret import managed_secret

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/treasury-rehypo.json"
LONG_KEY = "data/treasury-rehypo-long.json"
ERA_ORDER = ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022",
             "SBN2024"]
FRED_KEY = managed_secret(("FRED_API_KEY", "FRED_KEY"), ("/justhodl/fred/api-key",))
UA = {"User-Agent": "JustHodl-research/1.0 (github.com/ElMooro)"}
s3 = boto3.client("s3", region_name="us-east-1")
VERSION = "2.0"
METHOD = "collateral-measurement.v2"


def http_json(url, timeout=40, gz=False):  # gz kept for signature
    req = urllib.request.Request(url, headers=UA)
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    if gz or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def zscore(series, lookback=104):
    xs = [v for _, v in series[-lookback:] if v is not None and math.isfinite(v)]
    if len(xs) < 26:
        return None, len(xs)
    mu = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs)) or 1e-9
    return round((xs[-1] - mu) / sd, 2), len(xs)


# ── NY Fed FR2004: catalog-driven keyid discovery ─────────────────────
NYFED = "https://markets.newyorkfed.org/api/pd"


def nyfed_catalog():
    doc = http_json(f"{NYFED}/list/timeseries.json")
    rows = (doc.get("pd") or {}).get("timeseries") or doc.get(
        "timeseries") or []
    return [{"keyid": r.get("keyid"),
             "sb": r.get("seriesbreak"),
             "desc": (r.get("description") or "").upper()}
            for r in rows if r.get("keyid")]


def pick(cat, must, forbid=()):
    hits = [c for c in cat
            if all(m in c["desc"] for m in must)
            and not any(f in c["desc"] for f in forbid)]
    return hits


def nyfed_series(keyid, sb, n=140):
    # ops 4304 truth: /get/all/ is an empty stub; each keyid's own
    # seriesbreak (catalog field) is the working segment.
    doc = http_json(f"{NYFED}/get/{sb}/timeseries/{keyid}.json")
    rows = (doc.get("pd") or {}).get("timeseries") or []
    out = []
    for r in rows:
        try:
            out.append((r.get("asofdate"), float(r.get("value"))))
        except Exception:
            continue
    out.sort()
    return out[-n:]


def stitch(per_break):
    """Concatenate era segments; on overlap dates the LATEST break
    wins (breaks are sequential FR2004 format regimes)."""
    acc = {}
    for sb in ERA_ORDER:
        for d, v in per_break.get(sb, []):
            acc[d] = v
    return sorted(acc.items())


def weekly_last(series):
    """Resample any cadence to weekly (ISO year-week, last obs)."""
    acc = {}
    for d, v in series:
        try:
            y, w, _ = datetime.strptime(str(d)[:10],
                                        "%Y-%m-%d").isocalendar()
        except Exception:
            continue
        acc[(y, w)] = (d, v)
    return [acc[k] for k in sorted(acc)]


def rolling_z(series, win=156):
    out = []
    vals = [v for _, v in series]
    for i, (d, v) in enumerate(series):
        lo = max(0, i - win + 1)
        xs = vals[lo:i + 1]
        if len(xs) < 26:
            out.append((d, None))
            continue
        mu = sum(xs) / len(xs)
        sd = math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs)) \
            or 1e-9
        out.append((d, round((v - mu) / sd, 2)))
    return out


def sum_series(list_of_series):
    acc = {}
    for s in list_of_series:
        for d, v in s:
            acc[d] = acc.get(d, 0.0) + v
    return sorted(acc.items())


# ── OFR STFM ──────────────────────────────────────────────────────────
OFR = ("https://data.financialresearch.gov/v1/series/timeseries"
       "?mnemonic={m}")
OFR_CANDIDATES = {
    "gcf_rate": ["REPO-GCF_AR_AG-P", "REPO-GCF_AR_OO-P",
                 "REPO-GCF_AR_TOT-P"],
    "tri_rate": ["REPO-TRI_AR_OO-P", "REPO-TRI_AR_AG-P",
                 "REPO-TRI_AR_TOT-P"],
    "dvp_vol": ["REPO-DVP_TV_OO-P", "REPO-DVP_TV_TOT-P",
                "REPO-DVP_TV_AG-P"],
}
OFR_LIST = ("https://data.financialresearch.gov/v1/metadata/"
            "mnemonics")


def ofr_discover(notes):
    try:
        doc = http_json(OFR_LIST)  # plain JSON; auto-detect only
        rows = doc if isinstance(doc, list) else \
            doc.get("mnemonics") or []
        ms = [str(r if isinstance(r, str) else r.get("mnemonic"))
              for r in rows]
        repo = [m for m in ms if m and m.startswith("REPO-")]
        notes.append(f"ofr catalog: {len(repo)} REPO-* mnemonics; "
                     f"sample {repo[:8]}")
        return repo
    except Exception as e:
        notes.append(f"ofr discover: {str(e)[:60]}")
        return []


def ofr_series(mnemonic, n=180):
    doc = http_json(OFR.format(m=mnemonic))  # auto-detect gzip
    rows = doc if isinstance(doc, list) else doc.get("timeseries") or \
        doc.get("data") or []
    out = []
    for r in rows:
        try:
            if isinstance(r, list) and len(r) >= 2:
                out.append((str(r[0]), float(r[1])))
            elif isinstance(r, dict):
                out.append((str(r.get("date") or r.get("as_of")),
                            float(r.get("value"))))
        except Exception:
            continue
    out.sort()
    return out[-n:]


def fred_series(sid, n=200):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           "&sort_order=asc")
    doc = http_json(url)
    out = []
    for o in doc.get("observations", []):
        try:
            out.append((o["date"], float(o["value"])))
        except Exception:
            continue
    return out[-n:]


def read_object(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    except Exception:
        return {}


def quality(as_of, max_days=7):
    try:
        age = (datetime.now(timezone.utc).date() - datetime.strptime(as_of, '%Y-%m-%d').date()).days
    except (TypeError, ValueError):
        age = None
    return {'status': 'unavailable' if age is None else 'invalid' if age < 0 else 'stale' if age > max_days else 'fresh',
            'observation_date': as_of, 'age_days': age, 'max_age_days': max_days}


def clean_series(rows):
    out = {}
    for d, v in rows:
        try:
            datetime.strptime(d, '%Y-%m-%d')
            v = float(v)
            if math.isfinite(v): out[d] = v
        except (ValueError, TypeError):
            pass
    return sorted(out.items())


def difference(a, b, scale=1):
    a, b = dict(clean_series(a)), dict(clean_series(b))
    return [(d, (a[d]-b[d])*scale) for d in sorted(set(a)&set(b))]


def calendar_delta(rows, days=28):
    from bisect import bisect_right
    from datetime import timedelta
    rows = clean_series(rows)
    dates = [datetime.strptime(d, '%Y-%m-%d').date() for d, _ in rows]
    out = []
    for i, (d, v) in enumerate(rows):
        target = dates[i]-timedelta(days=days)
        j = bisect_right(dates, target)-1
        if j >= 0:
            bd,bv = rows[j]
            # Last business observation on/before the calendar target; no long gap fill.
            if (target-dates[j]).days <= 4:
                out.append((d,v-bv))
    return out


def measured_leg(rows, source, unit, max_days=7, field='latest', contributes=False):
    rows=clean_series(rows)
    q=quality(rows[-1][0] if rows else None,max_days)
    z,n=zscore(rows)
    good=q['status']=='fresh'
    return {'source':source, 'unit':unit, 'as_of':q['observation_date'],
            field:round(rows[-1][1],3) if good and rows else None,
            'z':z if good else None, 'n':n, 'quality':q,
            'score_contribution':.5 if contributes and good and z is not None else 0}


def review_score(legs):
    required=('fails','sofr_iorb')
    ready=all(legs.get(k,{}).get('quality',{}).get('status')=='fresh'
              and legs.get(k,{}).get('z') is not None for k in required)
    if not ready: return None,'UNAVAILABLE'
    value=round(max(0,min(100,50+12.5*sum(legs[k]['z'] for k in required)/2)),1)
    return value, 'ELEVATED' if value>=62 else 'NO_ELEVATED_FLAGS'


def lambda_handler(event=None, context=None):
    t0=time.time()
    legs,notes={},[]
    # The exact same normalized FR2004 object used by the fails and dealer desks.
    fails_doc=read_object('data/settlement-fails.json')
    joined=fails_context(fails_doc)
    treasury=joined.get('treasury') or {}
    fails_valid=(joined['contract']['usable'] and treasury.get('quality',{}).get('status')=='fresh'
                 and treasury.get('unit')=='USD_bn_par')
    fails_rows=clean_series(treasury.get('gross') or []) if fails_valid else []
    legs['fails']=measured_leg(fails_rows,'data/settlement-fails.json → treasury.gross','USD_bn_par',21,contributes=True)
    if not fails_valid:
        legs['fails']['quality']['status']='unavailable'
    if fails_valid:
        legs['fails'].update(ftd_bn=treasury['ftd_bn'],ftr_bn=treasury['ftr_bn'],gross_bn=treasury['gross_bn'],
                             measurement_note=treasury.get('measurement_note'))
    legs['velocity']={'latest':None,'z':None,'n':0,'as_of':None,'quality':{'status':'unavailable'},
                      'source':'Gross reusable collateral inventory and chain length are not observed.',
                      'unit':'ratio','score_contribution':0,
                      'reason':'Gross financing / absolute net positions is not collateral velocity.'}
    legs['specialness']={'latest_bps':None,'z':None,'n':0,'as_of':None,'quality':{'status':'unavailable'},
                         'unit':'basis_points','score_contribution':0,
                         'source':'Issue-matched GC and specific-security repo rates are not available.',
                         'reason':'GCF minus tri-party is a venue spread, not issue specialness.'}
    dealer=read_object('data/nyfed-primary-dealer.json')
    dc=inspect_donor(dealer,'data/nyfed-primary-dealer.json',8*24,
                     observed_paths=('financing.treasury.as_of',),required_paths=('financing.treasury',),
                     max_observation_age_hours=21*24)
    financing=(dealer.get('financing') or {}).get('treasury') or {}
    fin_valid=(dc['usable'] and dealer.get('methodology_version')=='fr2004-measurement.v2'
               and financing.get('quality',{}).get('status')=='fresh')
    financing_context={'contract':dc,'data':financing if fin_valid else None,'score_contribution':0,
                       'interpretation':'Treasury ex-TIPS plus TIPS, same week. Gross two-sided balances are not unique collateral.'}
    # OFR uses different clearing/counterparty universes. Match collateral and
    # tenor mnemonics explicitly; never fall back to a different collateral class.
    for suffix in ('OO-P','AG-P','TOT-P'):
        mg,mt='REPO-GCF_AR_'+suffix,'REPO-TRI_AR_'+suffix
        try:
            gc=difference(ofr_series(mg),ofr_series(mt),100)
            if not gc: continue
            legs['gc_venue_spread']=measured_leg(gc,f'OFR {mg} minus {mt}','basis_points',field='latest_bps')
            legs['gc_venue_spread']['interpretation']='Same mnemonic collateral/tenor category, different market venues; not issue-specific scarcity.'
            break
        except Exception as exc:
            notes.append('OFR matched pair unavailable: '+suffix+' '+type(exc).__name__)
    try:
        dvp=ofr_series('REPO-DVP_TV_OO-P')
        legs['dvp_volume']=measured_leg(dvp,'OFR REPO-DVP_TV_OO-P','USD')
    except Exception as exc:
        notes.append('OFR DVP unavailable: '+type(exc).__name__)
    sp=[]
    try:
        sp=difference(fred_series('SOFR',99999),fred_series('IORB',99999),100)
        legs['sofr_iorb']=measured_leg(sp,'FRED SOFR minus IORB, matched dates','basis_points',field='latest_bps',contributes=True)
    except Exception as exc:
        notes.append('SOFR/IORB unavailable: '+type(exc).__name__)
    try:
        delta=calendar_delta(fred_series('RRPONTSYD',99999))
        legs['rrp_drain_4w']=measured_leg(delta,'FRED RRPONTSYD calendar 28-day change','USD_bn')
        legs['rrp_drain_4w']['interpretation']='Context only. A decline is not automatically less stress; cash destinations and reserves matter.'
    except Exception as exc:
        notes.append('RRP unavailable: '+type(exc).__name__)
    comp,band=review_score(legs)
    required=('fails','sofr_iorb')
    missing=[k for k in required if legs.get(k,{}).get('quality',{}).get('status')!='fresh' or legs.get(k,{}).get('z') is None]
    generated=datetime.now(timezone.utc).isoformat(timespec='seconds')
    out={'engine':'justhodl-treasury-rehypo','version':VERSION,'methodology_version':METHOD,
         'generated_at':generated,'quality':{'status':'fresh' if not missing else 'incomplete','missing':missing},
         'composite':comp,'band':band,'legs':legs,'legs_missing':missing,'picked_keyids':{},
         'treasury_fails':treasury if fails_valid else None,'financing_context':financing_context,
         'execution_eligible':False,'call':None,'calibration_status':'HEURISTIC_REVIEW_ONLY',
         'methodology':'Equal-weight review index of Treasury gross fails and SOFR-IORB z-scores, 104 observations, minimum 26. '
                       'Both current legs required; weekly fails and daily funding dates remain explicit. '
                       'Velocity and specialness unavailable. Venue spread, volume and RRP are diagnostic only. '
                       'This is not a measurement of rehypothecation or a calibrated crisis probability.',
         'notes':notes,'elapsed_s':round(time.time()-t0,1)}
    # Rebuild current-vintage descriptive history, never carry forward the old
    # overlapping-keyid/velocity index. Only exact shared observation dates enter.
    fails_z=dict(rolling_z(fails_rows,104))
    funding_z=dict(rolling_z(sp,104))
    weekly=[]
    for d in sorted(set(fails_z)&set(funding_z)):
        if fails_z[d] is None or funding_z[d] is None: continue
        c=round(max(0,min(100,50+12.5*(fails_z[d]+funding_z[d])/2)),1)
        weekly.append({'d':d,'c':c,'n':2,'z':{'fails':fails_z[d],'sofr_iorb':funding_z[d]}})
    long_doc={'engine':out['engine'],'version':VERSION,'methodology_version':METHOD,'generated_at':generated,
              'actual_start':weekly[0]['d'] if weekly else None,'n_weekly':len(weekly),'weekly':weekly,
              'legs_available':{k:{'from':r[0][0],'to':r[-1][0]} for k,r in (('fails',fails_rows),('sofr_iorb',sp)) if r},
              'era_coverage':{},'note':'Current-vintage history, not point-in-time outcomes. '
              'Complete matched-date fails/funding only. Legacy composite is quarantined; no pre-data backfill.'}
    history=read_object('data/treasury-rehypo-history.json')
    rows=[r for r in history.get('rows',[]) if r.get('methodology_version')==METHOD]
    rows=rows[-364:]+[{'t':generated,'methodology_version':METHOD,'composite':comp,'band':band,
                     'legs_z':{k:legs[k].get('z') for k in required if k in legs}}]
    out['long_history']={'actual_start':long_doc['actual_start'],'n_weekly':len(weekly)}
    # Publish main last. Every consumer checks methodology, so partial auxiliary
    # writes cannot silently join an old index to the new measurement contract.
    for key,doc in ((LONG_KEY,long_doc),('data/treasury-rehypo-history.json',{'methodology_version':METHOD,'rows':rows}),(OUT_KEY,out)):
        s3.put_object(Bucket=BUCKET,Key=key,Body=json.dumps(doc,allow_nan=False,separators=(',',':')).encode(),
                      ContentType='application/json',CacheControl='public, max-age=1800')
    return {'ok':True,'composite':comp,'band':band,'quality':out['quality'],'legs':list(legs)}
