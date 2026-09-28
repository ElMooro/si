"""
justhodl-theme-classifier
══════════════════════════
Auto-discovers ACTIVE THEMES from the momentum-leaders universe.

THE INSIGHT
═══════════
Themes can't be hardcoded — they rotate. AI semis are hot now; 6 months
ago it was obesity pharma; 18 months ago it was nuclear/SMR. A static
list goes stale.

Instead, we derive themes DYNAMICALLY from co-movement:
  1. Take top 30 by momentum_score from momentum-leaders
  2. Fetch industry classification for each (FMP /stable/profile)
  3. Group by industry
  4. An industry is an ACTIVE THEME if ≥3 momentum leaders share it
  5. Label themes by industry name + dominant sub-themes

When the cluster composition shifts (MRVL drops, KLAC enters), themes
update automatically without code changes.

WHY INDUSTRY (not correlation)
══════════════════════════════
Correlation-based clustering needs 90+ days of overlapping price data
for every pair — expensive and noisy. Industry classification from FMP
is one API call per ticker, gives clean, interpretable buckets, and
correlation within a hot industry is almost always high anyway. We get
90% of the value at 5% of the cost.

OUTPUT
══════
data/momentum-themes.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_momentum_leaders": 30,
  "n_active_themes":    4,
  "themes": {
    "Semiconductors": {
      "tickers":         ["NVDA","AMD","AVGO","MRVL","MU","ARM","SNDK"],
      "n_leaders":       7,
      "avg_momentum":    84.3,
      "label":           "AI semis",
      "is_active":       true
    },
    "Software—Application": {...},
    "Pharmaceuticals—Major":  {...},
  },
  "ticker_to_theme": {
    "NVDA": "Semiconductors",
    "AMD":  "Semiconductors",
    ...
  },
  "all_industries_seen": [...]
}

SCHEDULE
════════
cron(0 */6 * * ? *) — every 6 hours. Themes change slowly; no need to
                       re-cluster every minute.
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET   = "justhodl-dashboard-live"
MOMENTUM_KEY = "data/momentum-leaders.json"
PROFILE_CACHE_KEY = "data/_cache/ticker-profiles.json"
OUTPUT_KEY  = "data/momentum-themes.json"
FMP_KEY = None  # Resolved only for original-scope profile requests.

MIN_TICKERS_FOR_THEME = 3   # need ≥3 momentum leaders in an industry to call it a theme
TOP_N_LEADERS          = 30  # how many momentum leaders to classify
PROFILE_CACHE_TTL_DAYS = 7   # industry classifications change slowly

s3 = None  # Legacy writer is preserved but inactive.

# Human-readable theme aliases for common industries
THEME_ALIASES = {
    "Semiconductors":               "AI semis",
    "Software—Application":         "AI software",
    "Software—Infrastructure":      "AI infrastructure",
    "Information Technology Services": "IT services",
    "Computer Hardware":            "Compute hardware",
    "Drug Manufacturers—General":   "Big pharma",
    "Drug Manufacturers—Specialty & Generic": "Specialty pharma",
    "Biotechnology":                "Biotech",
    "Medical Devices":              "Medtech",
    "Healthcare Plans":             "Health insurance",
    "Banks—Diversified":            "Big banks",
    "Banks—Regional":               "Regional banks",
    "Capital Markets":              "Investment banks",
    "Insurance—Diversified":        "Insurance",
    "Asset Management":             "Asset managers",
    "Copper":                       "Copper miners",
    "Gold":                         "Gold miners",
    "Silver":                       "Silver miners",
    "Other Industrial Metals & Mining": "Industrial metals",
    "Steel":                        "Steel",
    "Oil & Gas E&P":                "Oil & gas",
    "Oil & Gas Integrated":         "Oil majors",
    "Oil & Gas Refining & Marketing": "Refiners",
    "Uranium":                      "Uranium",
    "Solar":                        "Solar",
    "Utilities—Renewable":          "Renewables",
    "Utilities—Regulated Electric": "Utilities",
    "Aerospace & Defense":          "Aerospace & defense",
    "Auto Manufacturers":           "Autos",
    "Auto Parts":                   "Auto parts",
    "REIT—Industrial":              "Industrial REITs",
    "REIT—Specialty":               "Specialty REITs",
    "REIT—Office":                  "Office REITs",
    "Entertainment":                "Entertainment / streaming",
    "Internet Retail":              "E-commerce",
    "Internet Content & Information": "Internet platforms",
    "Telecom Services":             "Telecom",
    "Communication Equipment":      "Comms equipment",
    "Restaurants":                  "Restaurants",
    "Apparel Retail":               "Apparel retail",
    "Specialty Retail":             "Specialty retail",
    "Lodging":                      "Hotels / lodging",
    "Travel Services":              "Travel",
    "Airlines":                     "Airlines",
    "Cybersecurity":                "Cybersecurity",
    "Security & Protection Services": "Security",
}


def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def load_profile_cache() -> Dict[str, dict]:
    """Load cached industry classifications (refreshed weekly)."""
    cached = load_s3_json(PROFILE_CACHE_KEY) or {}
    profiles = cached.get("profiles", {})
    cache_ts = cached.get("generated_at", "")
    # Check freshness — if older than 7 days, treat as empty (forces refetch)
    try:
        cache_age_days = (datetime.now(timezone.utc) -
                            datetime.fromisoformat(cache_ts.replace("Z", "+00:00"))).days
        if cache_age_days > PROFILE_CACHE_TTL_DAYS:
            print(f"[cache] {cache_age_days} days old — refreshing")
            return {}
    except Exception:
        return {}
    return profiles


def save_profile_cache(profiles: Dict[str, dict]) -> None:
    body = json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profiles": profiles,
    }, default=str)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=PROFILE_CACHE_KEY, Body=body,
                        ContentType="application/json")
    except Exception as e:
        print(f"[cache-save] {e}")


def fetch_profile(ticker: str) -> Optional[dict]:
    """Fetch sector + industry from FMP /stable/profile."""
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/themes"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        item = data[0] if isinstance(data, list) and data else data
        if not isinstance(item, dict):
            return None
        return {
            "sector":    item.get("sector") or "Unknown",
            "industry":  item.get("industry") or "Unknown",
            "company":   item.get("companyName") or "",
            "market_cap": item.get("marketCap"),
        }
    except Exception as e:
        print(f"[profile] {ticker}: {str(e)[:80]}")
        return None


def derive_theme_label(industry: str, tickers: List[str]) -> str:
    """Map raw industry name to a human-friendly theme label."""
    alias = THEME_ALIASES.get(industry)
    if alias:
        return alias
    # Strip "—" suffixes for cleaner display
    if "—" in industry:
        return industry.split("—")[0].strip()
    return industry


def _legacy_lambda_handler(event, context):
    t0 = time.time()
    print(f"[themes] start {datetime.now(timezone.utc).isoformat()}")

    # Load momentum leaders
    mom_doc = load_s3_json(MOMENTUM_KEY)
    if not mom_doc:
        return _write_error("No momentum-leaders.json available")

    leaders_raw = (mom_doc.get("leaders") or mom_doc.get("all_scored") or [])[:TOP_N_LEADERS]
    if not leaders_raw:
        return _write_error("No leaders in momentum data")

    leader_tickers = [l["ticker"] for l in leaders_raw if l.get("ticker")]
    leader_scores  = {l["ticker"]: l.get("momentum_score", 0) for l in leaders_raw}
    print(f"[themes] {len(leader_tickers)} momentum leaders to classify")

    # Load profile cache + identify which need fetching
    cache = load_profile_cache()
    to_fetch = [t for t in leader_tickers if t not in cache]
    print(f"[themes] cache hit: {len(leader_tickers) - len(to_fetch)} · need fetch: {len(to_fetch)}")

    # Fetch missing profiles in parallel
    if to_fetch:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(fetch_profile, t): t for t in to_fetch}
            for fut in as_completed(futures, timeout=60):
                t = futures[fut]
                try:
                    p = fut.result()
                    if p:
                        cache[t] = p
                except Exception as e:
                    print(f"[fetch] {t}: {e}")
        # Save updated cache
        save_profile_cache(cache)

    # Group by industry
    industry_buckets: Dict[str, List[str]] = {}
    industry_scores: Dict[str, List[float]] = {}
    ticker_to_industry: Dict[str, str] = {}
    all_industries = set()
    unknown_tickers = []

    for t in leader_tickers:
        p = cache.get(t, {})
        ind = p.get("industry", "Unknown")
        all_industries.add(ind)
        if ind == "Unknown":
            unknown_tickers.append(t)
            continue
        industry_buckets.setdefault(ind, []).append(t)
        industry_scores.setdefault(ind, []).append(leader_scores.get(t, 0))
        ticker_to_industry[t] = ind

    # Build themes (industries with ≥3 leaders)
    themes = {}
    ticker_to_theme: Dict[str, str] = {}
    for ind, tickers in industry_buckets.items():
        n = len(tickers)
        is_active = n >= MIN_TICKERS_FOR_THEME
        if not is_active:
            continue  # not a theme yet
        avg_score = sum(industry_scores[ind]) / max(1, n)
        # Sort tickers within theme by momentum_score desc
        sorted_tickers = sorted(tickers, key=lambda t: -leader_scores.get(t, 0))
        label = derive_theme_label(ind, sorted_tickers)
        themes[ind] = {
            "tickers":      sorted_tickers,
            "n_leaders":    n,
            "avg_momentum": round(avg_score, 2),
            "label":        label,
            "is_active":    True,
            "top_ticker":   sorted_tickers[0] if sorted_tickers else None,
            "top_score":    round(leader_scores.get(sorted_tickers[0], 0), 1) if sorted_tickers else 0,
        }
        for t in sorted_tickers:
            ticker_to_theme[t] = ind

    # Sort themes by n_leaders × avg_momentum (strongest themes first)
    sorted_theme_keys = sorted(themes.keys(),
                                key=lambda k: -(themes[k]["n_leaders"] * themes[k]["avg_momentum"]))
    ordered_themes = {k: themes[k] for k in sorted_theme_keys}

    output = {
        "schema_version":     "1.0",
        "producer":           "justhodl-theme-classifier",
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":        round(time.time() - t0, 2),
        "n_momentum_leaders": len(leader_tickers),
        "n_active_themes":    len(themes),
        "n_classified":       len(ticker_to_industry),
        "n_unknown":          len(unknown_tickers),
        "min_tickers_for_theme": MIN_TICKERS_FOR_THEME,
        "themes":             ordered_themes,
        "ticker_to_theme":    ticker_to_theme,
        "unclassified":       sorted(unknown_tickers),
        "all_industries_seen": sorted(all_industries),
        "metadata": {
            "top_n_leaders_used":   TOP_N_LEADERS,
            "cache_ttl_days":       PROFILE_CACHE_TTL_DAYS,
            "n_profiles_fetched":   len(to_fetch),
            "n_profiles_cached":    len(leader_tickers) - len(to_fetch),
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=900")

    summary = {
        "status":          "ok",
        "elapsed_sec":     output["elapsed_sec"],
        "n_classified":    output["n_classified"],
        "n_active_themes": output["n_active_themes"],
        "themes_summary":  [
            f"{themes[k]['label']} ({themes[k]['n_leaders']} names, avg mom {themes[k]['avg_momentum']})"
            for k in sorted_theme_keys[:5]
        ],
    }
    print(f"[themes] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[themes] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}

# Reproducible issuer classifications; no theme, rank or sizing authority.
from pathlib import Path
from threading import Event
from botocore.config import Config
import urllib.error
import context_evidence_store
import managed_secret as managed_secret_module
import profile_observations as observations
from context_evidence_store import ContextStore,encode,code,now


class _ProfileNoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,status,msg,headers,newurl):
        # Credentials must not follow a vendor redirect to another origin.
        raise urllib.error.HTTPError(req.full_url,status,'Provider redirect refused',headers,fp)


def _profile_request(ticker,started,stop):
    requested=now();url=observations.endpoint(ticker)
    attempt={'ticker':ticker,'endpoint':url,'requested_at':requested,'received_at':requested,
             'status':'not_attempted_stop','http_status':None,'content_encoding':'','network_attempted':False,'acquisition':'provider_request'}
    if stop.is_set() or not FMP_KEY:return attempt,None
    if time.monotonic()-started>75:
        attempt['status']='not_attempted_budget';return attempt,None
    request=urllib.request.Request(url,headers={'User-Agent':'JustHodl/issuer-classifications','apikey':FMP_KEY,'Accept-Encoding':'identity'})
    opener=urllib.request.build_opener(_ProfileNoRedirect());response=None
    try:
        attempt['network_attempted']=True
        try:
            response=opener.open(request,timeout=12);status=response.status
        except urllib.error.HTTPError as exc:response=exc;status=exc.code
        attempt['http_status']=int(status)
        if status in (401,403,429):stop.set()
        attempt['content_encoding']=str(response.headers.get('Content-Encoding') or '').strip().lower()
        declared=response.headers.get('Content-Length')
        if declared is not None and (not declared.isdecimal() or int(declared)>observations.MAX_SOURCE_BYTES):
            attempt.update(status='source_limit_exceeded',received_at=now());return attempt,None
        chunks=[];size=0;deadline=min(started+95,time.monotonic()+20)
        # read1 returns after one buffered/socket read. Bound both the aggregate
        # bytes and elapsed transfer time; an idle timeout alone permits dribbling.
        while True:
            if time.monotonic()>deadline:raise ValueError('Provider transfer budget exhausted')
            chunk=response.read1(min(65536,observations.MAX_SOURCE_BYTES+1-size))
            if not chunk:break
            chunks.append(chunk);size+=len(chunk)
            if size>observations.MAX_SOURCE_BYTES:
                attempt.update(status='source_limit_exceeded',received_at=now());return attempt,None
        raw=b''.join(chunks)
        if declared is not None and len(raw)!=int(declared):raise ValueError('Whole declared provider body required')
        attempt.update(status='received' if status==200 else 'http_error',received_at=now())
        return attempt,raw
    except Exception:
        attempt.update(status='transport_unavailable',received_at=now());return attempt,None
    finally:
        if response is not None:response.close()


def _classifier_read_cache(client):
    # Original fixed key only. Retain whole legacy cache; never renew its clock.
    obj=client.get_object(Bucket=S3_BUCKET,Key=observations.CACHE_KEY);stream=obj['Body']
    try:raw=stream.read(context_evidence_store.MAX_BYTES+1)
    finally:stream.close()
    if len(raw)>context_evidence_store.MAX_BYTES or type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):
        raise ValueError('Whole original profile cache required')
    return raw,obj


def lambda_handler(event,context):
    global FMP_KEY
    started=time.monotonic();started_at=now();stop=Event()
    try:
        here=Path(__file__).resolve().parent
        client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=3,read_timeout=8,retries={'max_attempts':0}))
        paths={'lambda_function.py':here/'lambda_function.py','profile_observations.py':here/'profile_observations.py',
            'context_evidence_store.py':Path(context_evidence_store.__file__),'managed_secret.py':Path(managed_secret_module.__file__)}
        store=ContextStore(client,S3_BUCKET,observations.HEAD,observations.INPUTS,observations.PRIVATE,observations.CONTRACT,paths)
        previous_doc=None;previous=None
        try:previous,meta=store.read(observations.HEAD)
        except Exception as exc:
            if code(exc) not in ('NoSuchKey','404'):raise
            etag=None;previous_ref=None
        else:
            etag=meta.get('ETag')
            if not isinstance(etag,str) or not etag:raise ValueError('Prior head identity required')
            previous_ref=store.retain(previous,'outputs')
            try:previous_doc=observations.strict(previous,str(meta.get('ContentEncoding') or '').strip().lower())
            except (ValueError,UnicodeError):pass
        inputs={};sources={};total=0
        for name,key in observations.ALL_INPUTS.items():
            if time.monotonic()-started>75:raise ValueError('Context acquisition budget exhausted')
            requested=now()
            try:raw,meta=_classifier_read_cache(client) if name=='legacy_profile_cache' else store.read(key)
            except Exception:
                inputs[name]={'source_key':key,'status':'source_read_unavailable','requested_at':requested,'received_at':now()};continue
            received=now();total+=len(raw)
            if total>64*1024*1024:raise ValueError('Whole source aggregate exceeds bound')
            ref=store.retain(raw,'sources');sources[ref['key']]=raw
            inputs[name]={'source_key':key,'status':'received','requested_at':requested,'received_at':received,
                'original_ref':ref,'content_encoding':str(meta.get('ContentEncoding') or '').strip().lower()}
        parent_raw=observations.content(inputs['momentum'],sources)
        if parent_raw is None:raise ValueError('Research population unavailable')
        parent=observations.strict(parent_raw,inputs['momentum'].get('content_encoding',''))
        selected=observations.selection(parent,observations.clock(now()))['selected_tickers']
        reusable=observations.reusable(previous_doc,observations.clock(now()));outcomes=[None]*len(selected);missing=[]
        for i,ticker in enumerate(selected):
            candidate=reusable.get(ticker)
            if candidate is not None and time.monotonic()-started<=75:
                try:
                    ref=candidate['original_ref'];raw,_=store.read(ref['key'])
                    if len(raw)!=ref['bytes'] or context_evidence_store.sha(raw)!=ref['sha256']:raise ValueError('Cached body identity differs')
                    if observations.profile(raw,ticker,candidate.get('content_encoding',''))['status']!='reported_classification':raise ValueError('Cached issuer unqualified')
                except Exception:pass
                else:
                    total+=len(raw)
                    if total>64*1024*1024:raise ValueError('Whole source aggregate exceeds bound')
                    sources[ref['key']]=raw;outcomes[i]={**candidate,'network_attempted':False,'acquisition':'retained_profile'};continue
            missing.append((i,ticker))
        if missing:
            FMP_KEY=managed_secret(('FMP_KEY','FMP_API_KEY'),('/justhodl/fmp/api-key',))
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures={executor.submit(_profile_request,ticker,started,stop):i for i,ticker in missing}
                try:
                    for future in as_completed(futures,timeout=max(1,105-(time.monotonic()-started))):
                        if time.monotonic()-started>105:raise ValueError('Whole collection budget exhausted')
                        attempt,raw=future.result();index=futures[future]
                        if raw is not None:
                            total+=len(raw)
                            if total>64*1024*1024:raise ValueError('Whole source aggregate exceeds bound')
                            ref=store.retain(raw,'sources');sources[ref['key']]=raw;attempt['original_ref']=ref
                        outcomes[index]=attempt
                finally:stop.set()
        generated=now();packet=observations.build(inputs,outcomes,sources,generated,previous_doc)
        compilers={name:store.retain(Path(path).read_bytes(),'compilers') for name,path in paths.items()}
        manifest={'contract':observations.CONTRACT,'acquisition_started_at':started_at,'generated_at':generated,
            'input_attempts':inputs,'profile_attempts':outcomes,'previous_publication':previous_ref,'source_files':compilers,
            'stored_source_bytes':total,'controls':{'maximum_tickers':30,'active_requests':4,'legacy_cache_writes':0,
                'per_profile_ttl_seconds':observations.PROFILE_TTL_SECONDS,'per_profile_bytes':observations.MAX_SOURCE_BYTES,
                'per_context_bytes':context_evidence_store.MAX_BYTES,'aggregate_source_bytes':64*1024*1024,
                'acquisition_budget_s':75,'collection_budget_s':105,'publication_budget_s':135,'provider_retries':0,'stop_on_http_status':[401,403,429]}}
        packet['replay']={'input_ref':store.retain(encode(manifest),'inputs'),'source_files':compilers,
            'previous_publication':previous_ref,'originals_public':False}
        packet['acquisition_started_at']=started_at;body=encode(packet);ref=store.retain(body,'outputs')
        if time.monotonic()-started>135:raise ValueError('Publication budget exhausted')
        condition={'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}
        try:client.put_object(Bucket=S3_BUCKET,Key=observations.HEAD,Body=body,ContentType='application/json',CacheControl='max-age=600',**condition)
        except Exception as exc:raise context_evidence_store.PublicationUncertain(observations.HEAD) from exc
        return {'statusCode':200,'body':json.dumps({'status':'research_only','measurement_contract':observations.CONTRACT,
            'generated_at':generated,'output_sha256':ref['sha256'],'call':'WAIT','model_requests':0,'notifications_sent':0})}
    except Exception as exc:
        stop.set();uncertain=isinstance(exc,context_evidence_store.PublicationUncertain)
        return {'statusCode':503,'body':json.dumps({'status':'unavailable','previous_publication_preserved':None if uncertain else True,
            'publication_status':'acknowledgement_unknown' if uncertain else 'head_write_not_attempted','preservation_scope':'this_attempt_only',
            'error':'Whole source acquisition, validation, retention or conditional publication failed','model_requests':0,'notifications_sent':0})}
