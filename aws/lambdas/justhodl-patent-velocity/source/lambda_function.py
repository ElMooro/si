"""justhodl-patent-velocity — USPTO patent grant velocity tracker.

THESIS
══════
Patent grants are a 12-24 month leading indicator for:
  - New product launches (R&D → patent → product)
  - Strategic pivots (patents reveal new technology focus)
  - M&A activity (acquirers often pre-position via patents in target's domain)
  - Sector rotation (sudden patent activity in a category = institutional R&D money)

Top quant funds (Susquehanna, RBC, Two Sigma) use patent analytics as a
discriminator in technology/biotech/industrial momentum strategies. The
data is FREE via USPTO's PatentsView API — no auth needed.

The signal is VELOCITY, not absolute count. Apple/IBM/Samsung always file
thousands of patents; the alpha is in companies whose patent velocity
ACCELERATES vs their own trailing baseline, especially in new technology
categories.

DATA SOURCE
═══════════
  PatentsView API (USPTO public service):
    https://search.patentsview.org/api/v1/patent/
  
  Returns granted patents with:
    - patent_number, patent_date (grant date), patent_title
    - assignees[].assignee_organization (the company)
    - cpc_at_issue[] (Cooperative Patent Classification — tech category)
    - inventors[]
  
  Free, no auth, fair-use rate limits.

SCORING METHODOLOGY
═══════════════════
  Per company:
    recent_90d = grants in last 90 days
    baseline_365d = grants in trailing 90d-365d (annualized to 90d)
    velocity_ratio = recent_90d / baseline_per_90d
  
  Velocity bands → score (0-100):
    velocity ≥5x: 90  (extreme R&D acceleration)
    velocity ≥3x: 70  (strong push)
    velocity ≥2x: 50  (notable acceleration)
    velocity ≥1.5x: 30
    velocity <0.5x: penalize (R&D drying up)
  
  Bonuses (additive):
    +15 if recent_90d ≥ 50 patents (absolute scale matters)
    +10 if NEW CPC categories appear in recent vs baseline (new tech focus)
    +5 if multi-quarter sustained acceleration

OUTPUT
══════
  data/patent-velocity.json
  Emits patent.velocity_spike for ticker w/ ratio ≥3x + ≥20 recent patents.
"""
import json
import os
import time
import urllib.error
import re
import urllib.request
import urllib.parse
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone

import boto3
from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/patent-velocity.json"

HTTP_TIMEOUT = 25
USER_AGENT = "JustHodl-PatentVelocity/1.0 (raafouis@gmail.com)"

# PatentsView migrated 2024-2025 from no-auth Legacy API to PatentSearch API
# which REQUIRES an API key. Free, register at https://patentsview.org/apis/
# (or USPTO ODP successor at data.uspto.gov). Until key is provisioned in SSM,
# the engine will fail gracefully and write a stub indicating registration needed.
PATENTSVIEW_API_KEY = os.environ.get("PATENTSVIEW_API_KEY", "").strip()

# Windows
RECENT_DAYS = int(os.environ.get("RECENT_DAYS", "90"))
BASELINE_DAYS = int(os.environ.get("BASELINE_DAYS", "365"))

# Cap on companies (we curate to high-IP industries)
MAX_COMPANIES = int(os.environ.get("MAX_COMPANIES", "120"))

# Universe — companies where patent activity matters most.
# These are the high-IP filers across tech, biotech, defense, semis.
# Patent activity is most predictive in R&D-driven sectors.
TICKER_TO_ASSIGNEE = {
    # ── Mega cap tech (high baseline filers; velocity changes are signal)
    "AAPL":   ["APPLE INC", "Apple Inc."],
    "MSFT":   ["MICROSOFT CORPORATION", "Microsoft Corp"],
    "GOOGL":  ["GOOGLE LLC", "GOOGLE INC", "Alphabet Inc."],
    "META":   ["META PLATFORMS INC", "Facebook Inc."],
    "AMZN":   ["AMAZON TECHNOLOGIES INC", "Amazon.com Inc."],
    "NVDA":   ["NVIDIA CORPORATION", "Nvidia Corp"],
    "TSLA":   ["TESLA INC"],
    "ORCL":   ["ORACLE INTERNATIONAL CORPORATION"],
    "CRM":    ["SALESFORCE INC", "Salesforce.com Inc."],
    "ADBE":   ["ADOBE INC", "Adobe Systems"],
    # ── Semis / hardware
    "AMD":    ["ADVANCED MICRO DEVICES INC"],
    "INTC":   ["INTEL CORPORATION"],
    "QCOM":   ["QUALCOMM INCORPORATED", "Qualcomm Inc."],
    "AVGO":   ["BROADCOM INC", "Broadcom Corp"],
    "MU":     ["MICRON TECHNOLOGY INC"],
    "TXN":    ["TEXAS INSTRUMENTS INCORPORATED"],
    "AMAT":   ["APPLIED MATERIALS INC"],
    "LRCX":   ["LAM RESEARCH CORPORATION"],
    "KLAC":   ["KLA CORPORATION"],
    "ARM":    ["ARM LIMITED", "ARM Holdings"],
    "TSM":    ["TAIWAN SEMICONDUCTOR MANUFACTURING COMPANY"],
    # ── Cloud / SaaS
    "NOW":    ["SERVICENOW INC"],
    "SNOW":   ["SNOWFLAKE INC"],
    "NET":    ["CLOUDFLARE INC"],
    "DDOG":   ["DATADOG INC"],
    "PLTR":   ["PALANTIR TECHNOLOGIES INC"],
    "PANW":   ["PALO ALTO NETWORKS INC"],
    "FTNT":   ["FORTINET INC"],
    "CRWD":   ["CROWDSTRIKE HOLDINGS INC"],
    "ZS":     ["ZSCALER INC"],
    "S":      ["SENTINELONE INC"],
    # ── Defense / aerospace
    "LMT":    ["LOCKHEED MARTIN CORPORATION"],
    "RTX":    ["RAYTHEON TECHNOLOGIES CORPORATION", "Raytheon Company"],
    "NOC":    ["NORTHROP GRUMMAN CORPORATION", "Northrop Grumman Systems Corporation"],
    "GD":     ["GENERAL DYNAMICS CORPORATION"],
    "BA":     ["THE BOEING COMPANY", "Boeing Company"],
    "HII":    ["HUNTINGTON INGALLS INDUSTRIES"],
    # ── Industrial / robotics
    "GE":     ["GENERAL ELECTRIC COMPANY"],
    "GEV":    ["GE VERNOVA INC", "GE Vernova"],
    "HON":    ["HONEYWELL INTERNATIONAL INC"],
    "CAT":    ["CATERPILLAR INC"],
    "ROK":    ["ROCKWELL AUTOMATION INC"],
    "EMR":    ["EMERSON ELECTRIC CO"],
    # ── Auto / EV
    "GM":     ["GENERAL MOTORS COMPANY", "GM Global Technology Operations"],
    "F":      ["FORD GLOBAL TECHNOLOGIES LLC", "Ford Motor Company"],
    "RIVN":   ["RIVIAN AUTOMOTIVE INC"],
    "LCID":   ["LUCID GROUP INC"],
    # ── Pharma / biotech
    "PFE":    ["PFIZER INC"],
    "MRK":    ["MERCK SHARP & DOHME LLC", "Merck Sharp & Dohme Corp"],
    "JNJ":    ["JANSSEN PHARMACEUTICALS INC", "Johnson & Johnson"],
    "ABBV":   ["ABBVIE INC"],
    "LLY":    ["ELI LILLY AND COMPANY"],
    "BMY":    ["BRISTOL-MYERS SQUIBB COMPANY"],
    "GILD":   ["GILEAD SCIENCES INC"],
    "REGN":   ["REGENERON PHARMACEUTICALS INC"],
    "VRTX":   ["VERTEX PHARMACEUTICALS INC"],
    "MRNA":   ["MODERNA INC", "ModernaTX"],
    "BNTX":   ["BIONTECH SE"],
    "NVAX":   ["NOVAVAX INC"],
    # ── Medical devices
    "MDT":    ["MEDTRONIC INC"],
    "ISRG":   ["INTUITIVE SURGICAL INC"],
    "SYK":    ["STRYKER CORPORATION"],
    "BSX":    ["BOSTON SCIENTIFIC SCIMED INC", "Boston Scientific Corporation"],
    "EW":     ["EDWARDS LIFESCIENCES CORPORATION"],
    # ── Energy / battery / clean
    "PLUG":   ["PLUG POWER INC"],
    "ENPH":   ["ENPHASE ENERGY INC"],
    "FSLR":   ["FIRST SOLAR INC"],
    "QS":     ["QUANTUMSCAPE BATTERY", "QuantumScape Corporation"],
    # ── Quantum / next-gen
    "IBM":    ["INTERNATIONAL BUSINESS MACHINES CORPORATION"],
    "RGTI":   ["RIGETTI COMPUTING INC"],
    "IONQ":   ["IONQ INC"],
    "QBTS":   ["D-WAVE SYSTEMS INC"],
    # ── Networking
    "CSCO":   ["CISCO TECHNOLOGY INC", "Cisco Systems Inc."],
    "JNPR":   ["JUNIPER NETWORKS INC"],
    # ── Storage / mem
    "WDC":    ["WESTERN DIGITAL TECHNOLOGIES INC"],
    "STX":    ["SEAGATE TECHNOLOGY"],
}

# Inverse map for lookup
ASSIGNEE_TO_TICKER = {}
for ticker, assignees in TICKER_TO_ASSIGNEE.items():
    for a in assignees:
        ASSIGNEE_TO_TICKER[a.upper()] = ticker

s3 = boto3.client("s3", region_name=REGION)


def _http_post(url, payload, timeout=HTTP_TIMEOUT, retries=2):
    """PatentsView API uses POST with JSON payload."""
    data = json.dumps(payload).encode("utf-8")
    h = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if PATENTSVIEW_API_KEY:
        h["X-Api-Key"] = PATENTSVIEW_API_KEY
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=h, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            print(f"[patent] HTTP {e.code} from {url[:80]}")
            try:
                err_body = e.read().decode("utf-8")[:300]
                print(f"[patent]   body: {err_body}")
            except Exception:
                pass
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            print(f"[patent] err: {type(e).__name__} {str(e)[:80]}")
            return None
    return None


def fetch_patents_for_assignees(assignee_names: list, days_back: int) -> list:
    """ops 2702: keyless source. Google Patents XHR count queries for US GRANTS
    published in [recent] and [baseline] windows; synthesizes minimal per-patent
    records (date only, no CPC) so the original velocity/scoring pipeline runs
    unchanged. Window consistency makes the ratio robust even if the status
    filter is loosely applied upstream."""
    end = datetime.now(timezone.utc).date()
    mid = end - timedelta(days=RECENT_DAYS)
    start = end - timedelta(days=days_back)
    q = " OR ".join('assignee:"%s"' % a for a in assignee_names[:4])

    def _count(d1, d2):
        inner = ("q=(%s)&country=US&status=GRANT&type=PATENT&after=publication:%s&before=publication:%s"
                 % (q, d1.strftime("%Y%m%d"), d2.strftime("%Y%m%d")))
        url = "https://patents.google.com/xhr/query?url=" + urllib.parse.quote(inner, safe="")
        for attempt in range(2):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                           "Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=12) as r:  # ops 2704: fail fast into backoff; 20s+ stalls burned the run budget
                    body = r.read().decode("utf-8", "ignore")
                mm = re.search(r'"total_num_results"\s*:\s*(\d+)', body)
                return int(mm.group(1)) if mm else 0
            except urllib.error.HTTPError as e:
                if e.code in (429, 503) and attempt < 2:
                    time.sleep(3); continue
                print("[patent] google HTTP %s" % e.code); return None
            except Exception as e:
                if attempt < 2:
                    time.sleep(2); continue
                print("[patent] google err %s" % str(e)[:70]); return None
        return None

    n_recent = _count(mid, end)
    time.sleep(1.0)
    n_base = _count(start, mid)
    time.sleep(1.0)
    if n_recent is None or n_base is None:
        return []   # either window throttled -> skip; cursor retries next cycle
    recs = []
    rd, bd = (mid + timedelta(days=1)).isoformat(), (start + timedelta(days=1)).isoformat()
    for _ in range(int(n_recent or 0)):
        recs.append({"patent_date": rd, "cpc_codes": [], "patent_title": None})
    for _ in range(int(n_base or 0)):
        recs.append({"patent_date": bd, "cpc_codes": [], "patent_title": None})
    return recs


def analyze_ticker(ticker: str, assignee_names: list) -> dict:
    """Pull patents for one ticker's assignees + compute velocity."""
    patents = fetch_patents_for_assignees(assignee_names, BASELINE_DAYS)
    if not patents:
        return None
    
    today = datetime.now(timezone.utc).date()
    recent_cutoff = (today - timedelta(days=RECENT_DAYS)).isoformat()
    baseline_cutoff = (today - timedelta(days=BASELINE_DAYS)).isoformat()
    
    recent_patents = []
    baseline_patents = []
    
    recent_cpcs = Counter()
    baseline_cpcs = Counter()
    
    for p in patents:
        try:
            date = (p.get("patent_date") or "")[:10]
            if not date or date < baseline_cutoff:
                continue
            
            # CPC categories (Cooperative Patent Classification)
            cpcs_raw = p.get("cpc_at_issue") or []
            cpc_codes = set()
            for cpc in cpcs_raw[:5]:  # cap per-patent CPCs
                if isinstance(cpc, dict):
                    sec = cpc.get("cpc_section_id") or cpc.get("cpc_section")
                    grp = cpc.get("cpc_group_id") or cpc.get("cpc_group")
                    code = grp or sec
                    if code: cpc_codes.add(code[:8])
            
            patent_summary = {
                "number": p.get("patent_number"),
                "date":   date,
                "title":  (p.get("patent_title") or "")[:140],
                "cpcs":   sorted(cpc_codes)[:3],
            }
            
            if date >= recent_cutoff:
                recent_patents.append(patent_summary)
                for c in cpc_codes: recent_cpcs[c] += 1
            else:
                baseline_patents.append(patent_summary)
                for c in cpc_codes: baseline_cpcs[c] += 1
        except Exception:
            continue
    
    n_recent = len(recent_patents)
    n_baseline = len(baseline_patents)
    
    # Velocity calc
    baseline_per_recent_window = n_baseline * (RECENT_DAYS / (BASELINE_DAYS - RECENT_DAYS))
    velocity = (n_recent / baseline_per_recent_window) if baseline_per_recent_window > 0 else None
    
    # New CPC categories: in recent but not baseline
    recent_only_cpcs = set(recent_cpcs.keys()) - set(baseline_cpcs.keys())
    
    # Score 0-100
    # ops 2704: significance floor — Google's exact-assignee-phrase counts are a
    # CONSISTENT estimator (undercount cancels in the ratio) but tiny samples
    # (GOOGL 4/90d style) make the ratio noise. Below the floor: no banding,
    # flagged low_sample, neutral score.
    low_sample = (n_recent + n_baseline) < 8
    score = 0
    if velocity is not None and not low_sample:
        if velocity >= 5:    score += 90
        elif velocity >= 3:  score += 70
        elif velocity >= 2:  score += 50
        elif velocity >= 1.5: score += 30
        elif velocity >= 0.7: score += 15
        elif velocity < 0.5:  score -= 10
    
    if n_recent >= 50:  score += 15
    elif n_recent >= 20: score += 10
    elif n_recent >= 5:  score += 5
    
    if len(recent_only_cpcs) >= 2:
        score += 10
    
    if low_sample:
        score = min(score, 10)
    score = max(0, min(100, score))
    
    # Thesis
    thesis_bits = []
    if velocity is not None and velocity >= 3:
        thesis_bits.append(f"patent velocity {velocity:.1f}x baseline")
    elif velocity is not None and velocity >= 2:
        thesis_bits.append(f"R&D accelerating ({velocity:.1f}x)")
    if recent_only_cpcs:
        thesis_bits.append(f"NEW tech categories: {', '.join(sorted(recent_only_cpcs)[:3])}")
    if n_recent >= 50:
        thesis_bits.append(f"high absolute scale ({n_recent} patents 90d)")
    thesis = " · ".join(thesis_bits) if thesis_bits else "Stable patent baseline"
    
    return {
        "ticker":              ticker,
        "score":               score,
        "n_recent_patents":    n_recent,
        "n_baseline_patents": n_baseline,
        "velocity_ratio":      round(velocity, 2) if velocity is not None else None,
        "low_sample":          low_sample,
        "top_cpcs_recent":     [{"cpc": c, "count": n}
                                  for c, n in recent_cpcs.most_common(5)],
        "new_cpcs":            sorted(recent_only_cpcs)[:5],
        "sample_recent_patents": recent_patents[:5],
        "thesis":              thesis,
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # Bail early if API key not provisioned. PatentsView migrated to require
    # key in 2024-2025; write a stub output so dashboard shows actionable msg.
    if False:  # ops 2702: keyless Google Patents source — PatentsView key no longer required
        msg = ("PatentsView API key not provisioned. Register at "
                "https://patentsview.org/apis/ (free) → service desk request → "
                "drop key in SSM at /justhodl/patentsview-key + Lambda env "
                "PATENTSVIEW_API_KEY.")
        print(f"[patent] ABORT: {msg}")
        stub = {
            "schema_version": "1.0",
            "method":         "patent_velocity_v1",
            "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status":         "needs_api_key",
            "needs_setup":    msg,
            "register_url":   "https://patentsview.org/apis/",
            "universe_size":  len(TICKER_TO_ASSIGNEE),
            "n_results":      0,
            "highlights":     {"velocity_spikes": [], "new_tech_focus": [], "highest_scale": []},
            "all_results":    [],
        }
        s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY,
                       Body=json.dumps(stub, default=str, separators=(",", ":")).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        return {"statusCode": 200, "body": json.dumps({
            "ok": False, "reason": "needs_api_key",
            "message": msg,
        })}
    
    _lim = int((event or {}).get("limit") or MAX_COMPANIES)
    # ── incremental coverage (ops 2704): cursor rotation + prior-run merge so
    #    throttled partial passes ACCUMULATE to full-universe coverage instead
    #    of overwriting it. Every row carries a per-ticker `asof`.
    _prior = {}
    try:
        _prior = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read().decode("utf-8"))
    except Exception:
        pass
    _prev_rows = {}
    for _r in (_prior.get("all_results") or []):
        if _r.get("ticker"):
            _r.setdefault("asof", _prior.get("generated_at") or "")
            _prev_rows[_r["ticker"]] = _r
    _all = list(TICKER_TO_ASSIGNEE.keys())
    _cur = int(_prior.get("cursor") or 0) % max(1, len(_all))
    _order = _all[_cur:] + _all[:_cur]
    tickers = _order[:_lim]
    print(f"[patent] processing {len(tickers)} tickers w/ patent activity")
    
    results = []
    for i, ticker in enumerate(tickers):
        try:
            r = analyze_ticker(ticker, TICKER_TO_ASSIGNEE[ticker])
            if r:
                r["asof"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                results.append(r)
        except Exception as e:
            print(f"[patent] err on {ticker}: {str(e)[:80]}")
        
        # Pace USPTO requests
        time.sleep(0.8)
        
        if (i + 1) % 20 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            print(f"[patent] processed {i+1}/{len(tickers)}  found {len(results)}  "
                  f"elapsed {elapsed:.0f}s")
        
        # Time budget
        if (datetime.now(timezone.utc) - started).total_seconds() > 640:
            print("[patent] time budget exhausted")
            break
    
    _fresh = {r["ticker"]: r for r in results if r.get("ticker")}
    _cut = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat(timespec="seconds")
    _merged = {t: r for t, r in _prev_rows.items() if (r.get("asof") or "") >= _cut}
    _merged.update(_fresh)
    _next_cursor = (_cur + (i + 1 if tickers else 0)) % max(1, len(_all))
    results = list(_merged.values())
    print("[patent] merge: fresh=%d carried=%d total=%d next_cursor=%d"
          % (len(_fresh), len(results) - len(_fresh), len(results), _next_cursor))
    results.sort(key=lambda r: -r["score"])
    
    # Highlights
    velocity_spikes = [r for r in results
                         if r["velocity_ratio"] is not None
                         and r["velocity_ratio"] >= 2.0
                         and r["n_recent_patents"] >= 5][:20]
    new_tech_focus = [r for r in results if r["new_cpcs"]][:20]
    highest_scale = sorted(results, key=lambda r: -r["n_recent_patents"])[:15]
    
    out = {
        "schema_version":   "1.0",
        "method":           "patent_velocity_v1",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "data_source":      "Google Patents grant-count windows (keyless; ops 2702) — velocity-consistent",
        "windows": {
            "recent_days":   RECENT_DAYS,
            "baseline_days": BASELINE_DAYS,
        },
        
        "universe_size":      len(_all),
        "cursor":             _next_cursor,
        "coverage": {"fresh_this_run": len(_fresh), "total": len(results), "universe": len(_all)},
        "n_results":          len(results),
        "n_velocity_spikes":  len(velocity_spikes),
        "n_new_tech_focus":   len(new_tech_focus),
        
        "highlights": {
            "velocity_spikes": velocity_spikes,
            "new_tech_focus":  new_tech_focus,
            "highest_scale":   highest_scale,
        },
        
        "all_results":        results,
        "top_picks": [{"ticker": r.get("ticker"), "score": r["score"], "direction": "long",
                       "reason": "patent velocity %sx, %s grants/90d" % (r.get("velocity_ratio"), r.get("n_recent_patents"))}
                      for r in velocity_spikes[:15] if r.get("ticker")],
        
        "notes": (
            f"USPTO patent grant velocity over {RECENT_DAYS}d window vs trailing "
            f"baseline. Patent grants are a 12-24 month leading indicator for "
            "new product launches, M&A activity, and strategic pivots. Velocity "
            "≥2x = acceleration; ≥3x = strong R&D push; ≥5x = exceptional. "
            "NEW tech categories (CPC codes appearing in recent but not baseline) "
            "indicate fresh technology focus. Universe curated to ~80 high-IP "
            "companies across tech / biotech / semis / defense / industrial / "
            "EVs / quantum. Keyless Google Patents count windows (ops 2702); CPC new-tech detection dormant pending PatentsView key."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=43200")
    print(f"[patent] wrote {len(body):,}B  results={len(results)}  "
          f"spikes={len(velocity_spikes)}  duration={out['duration_s']}s")
    
    # Emit events
    try:
        from system_events import publish_many
        events_pub = []
        for r in velocity_spikes[:3]:
            if r["velocity_ratio"] >= 3.0 and r["n_recent_patents"] >= 20:
                events_pub.append(("patent.velocity_spike", {
                    "ticker":          r["ticker"],
                    "velocity_ratio":  r["velocity_ratio"],
                    "n_recent":        r["n_recent_patents"],
                    "new_cpcs":        r["new_cpcs"][:3],
                    "score":           r["score"],
                }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[patent] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":               True,
        "n_results":        len(results),
        "n_velocity_spikes": len(velocity_spikes),
        "n_new_tech_focus": len(new_tech_focus),
        "duration_s":       out["duration_s"],
    })}


lambda_handler = handler
