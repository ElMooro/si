"""justhodl-political-intel — institutional congressional + executive trade tracker

Quiver-FREE. Built entirely on public-domain disclosure data, so it is safe to
surface in a commercial multi-user product.

DATA SOURCES (all free / public domain):
  • Congress trades — FMP /stable/ senate-latest + house-latest (licensed,
    commercial-safe; sources the official Senate eFD + House Clerk STOCK Act
    disclosures). Falls back to S3-cached snapshot.
  • Committee memberships — theunitedstates.io/congress-legislators
    (committee-membership-current + legislators-current).
  • Executive branch (Trump + cabinet) — OGE 278e / periodic transaction
    disclosures, maintained as a structured snapshot (updated per filing).

THE EDGE WE SCORE FOR:
  The alpha in congressional trading isn't "politicians are smart" — it's that
  COMMITTEE MEMBERS have non-public visibility into upcoming legislation,
  contracts, and regulation in their jurisdiction. So a Senate Armed Services
  member buying a defense name scores far higher than a random buy.

CONVICTION SCORE per ticker (0-100+):
    committee_relevance (member's committee jurisdiction matches the stock)
  × size_tier          (disclosure $ range — bigger = more conviction)
  × cluster            (multiple distinct members buying the same name)
  × recency            (decay by TRANSACTION date, not disclosure date)
  × buy_pressure       (net buys vs sells)
  − noise penalties    (index funds / managed-account boilerplate)

OUTPUT  data/political-intel.json:
  {top_conviction_buys, by_ticker, clusters, executive_holdings, committee_map, stats}

This is a SIGNAL ENGINE. Downstream:
  • prediction-snapshotter ingests politician_* features → self-improvement loop
    LEARNS the real predictive weight (we don't blindly trust it)
  • AI investigation engine explains WHY each top name might be a great buy
  • chart-pro POLITICIAN watchlist + signal dots
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/political-intel.json"
HTTP_TIMEOUT = 30
LOOKBACK_DAYS = 90           # transaction-date window we consider "fresh"
DISCLOSURE_LAG_CAP = 45      # STOCK Act reporting window (days)

s3 = boto3.client("s3", region_name="us-east-1")

UA = "Mozilla/5.0 (compatible; JustHodlPoliticalIntel/2.0)"

# ── Committee jurisdiction → market sector keywords ──────────────────
# Maps a committee (by name fragment) to the sectors it has informational
# edge over. Used to flag committee-relevant trades.
COMMITTEE_SECTORS = {
    "armed services":      ["defense", "aerospace", "weapon", "military", "missile", "lockheed", "raytheon", "northrop", "general dynamics", "boeing", "l3", "huntington"],
    "intelligence":        ["defense", "cyber", "security", "palantir", "crowdstrike", "booz"],
    "financial services":  ["bank", "financ", "insur", "capital", "asset", "payment", "visa", "mastercard", "fintech", "credit"],
    "banking":             ["bank", "financ", "insur", "capital", "asset", "payment", "credit", "housing", "mortgage"],
    "energy and commerce": ["energy", "oil", "gas", "utility", "power", "pharma", "health", "telecom", "tech", "semiconductor", "drug", "biotech"],
    "energy and natural":  ["energy", "oil", "gas", "coal", "solar", "renewable", "utility", "mining", "uranium"],
    "health":              ["health", "pharma", "drug", "biotech", "medic", "hospital", "device", "vaccine", "therapeutic"],
    "help":                ["health", "pharma", "drug", "biotech", "medic", "education", "labor"],
    "ways and means":      ["health", "pharma", "tax", "trade", "tariff"],
    "finance":             ["bank", "financ", "tax", "trade", "health", "pharma", "insur", "tariff"],
    "agriculture":         ["agricult", "food", "farm", "fertilizer", "commodity", "deere", "tractor", "seed"],
    "transportation":      ["airline", "transport", "rail", "auto", "ev", "infrastructure", "construction", "logistics", "ups", "fedex", "boeing"],
    "commerce":            ["tech", "telecom", "airline", "auto", "semiconductor", "internet", "media", "retail", "broadband"],
    "judiciary":           ["tech", "internet", "antitrust", "google", "meta", "amazon", "apple", "microsoft"],
    "appropriations":      ["defense", "infrastructure", "health", "energy"],
    "science":             ["tech", "semiconductor", "space", "nasa", "aerospace", "quantum", "ai", "research"],
    "homeland":            ["defense", "cyber", "security", "border"],
    "veterans":            ["health", "pharma", "hospital"],
    "small business":      [],
    "foreign":             ["defense", "energy", "trade"],
    "natural resources":   ["energy", "oil", "gas", "mining", "timber", "uranium"],
}


def _http_get(url, timeout=HTTP_TIMEOUT, retries=2):
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            print(f"[political] {e.code} from {url[:100]}")
        except Exception as e:
            last = str(e)[:120]
            print(f"[political] err {last} from {url[:100]}")
        time.sleep(1.0 * (attempt + 1))
    return None


def _read_s3_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


# ── 1. Committee memberships ─────────────────────────────────────────
def fetch_committee_map():
    """bioguide_id -> [committee names] from theunitedstates.io."""
    membership = {}
    try:
        mem_body = _http_get("https://unitedstates.github.io/congress-legislators/committee-membership-current.json", timeout=20)
        com_body = _http_get("https://unitedstates.github.io/congress-legislators/committees-current.json", timeout=20)
        if not mem_body or not com_body:
            return {}
        mem = json.loads(mem_body)
        coms = json.loads(com_body)
        # committee id (thomas/house/senate) -> name
        com_name = {}
        for c in coms:
            cid = c.get("thomas_id") or c.get("senate_committee_id") or c.get("house_committee_id")
            if cid:
                com_name[cid] = c.get("name", "")
            for sub in (c.get("subcommittees") or []):
                pass
        # membership: committee_id -> [{bioguide, ...}]
        bio_committees = defaultdict(set)
        for cid, members in mem.items():
            base = cid[:4] if len(cid) > 4 else cid
            name = com_name.get(cid) or com_name.get(base) or ""
            for m in members:
                bg = m.get("bioguide")
                if bg and name:
                    bio_committees[bg].add(name.lower())
        return {k: list(v) for k, v in bio_committees.items()}
    except Exception as e:
        print(f"[political] committee map err: {e}")
        return {}


def fetch_legislators():
    """bioguide_id -> {name, party, state, chamber}."""
    out = {}
    body = _http_get("https://unitedstates.github.io/congress-legislators/legislators-current.json", timeout=20)
    if not body:
        return out
    try:
        for leg in json.loads(body):
            term = (leg.get("terms") or [{}])[-1]
            bid = (leg.get("id") or {}).get("bioguide")
            name = (leg.get("name") or {})
            if bid:
                out[bid] = {
                    "name": f"{name.get('first','')} {name.get('last','')}".strip(),
                    "party": (term.get("party") or "")[:1],
                    "state": term.get("state", ""),
                    "chamber": "senate" if term.get("type") == "sen" else "house",
                }
    except Exception as e:
        print(f"[political] legislators err: {e}")
    return out


# ── 2. Congress trades (FMP /stable/ — commercial-licensed, reliable) ─
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def fetch_congress_trades():
    """Senate + House STOCK Act disclosures from FMP /stable/ (paid/licensed,
    commercial-safe). v3/v4 dead since 2025-08-31 — /stable/ only."""
    trades = []
    sources_used = []
    for chamber, ep in (("senate", "senate-latest"), ("house", "house-latest")):
        cnt = 0
        for page in range(0, 8):
            body = _http_get(f"https://financialmodelingprep.com/stable/{ep}?page={page}&apikey={FMP_KEY}", timeout=25)
            if not body:
                break
            try:
                rows = json.loads(body)
            except Exception:
                break
            if not isinstance(rows, list) or not rows:
                break
            for t in rows:
                n = _norm_fmp(t, chamber)
                if n:
                    trades.append(n); cnt += 1
            if len(rows) < 50:
                break
        if cnt:
            sources_used.append(f"fmp-{chamber}({cnt})")
    return trades, sources_used


def _parse_date(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s.strip()[:10], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _norm_fmp(t, chamber):
    """Normalize an FMP senate/house-latest row (field names vary)."""
    try:
        tx = (t.get("type") or "").lower()
        if "purchase" in tx or "buy" in tx:
            side = "buy"
        elif "sale" in tx or "sell" in tx:
            side = "sell"
        else:
            side = "other"
        member = (t.get("senator") or t.get("representative") or t.get("office")
                  or (f"{t.get('firstName','')} {t.get('lastName','')}").strip() or "")
        return {
            "chamber": chamber,
            "member": member,
            "ticker": (t.get("symbol") or "").strip().upper(),
            "asset": t.get("assetDescription") or t.get("asset_description") or "",
            "side": side,
            "amount": t.get("amount") or "",
            "tx_date": t.get("transactionDate") or t.get("transaction_date") or "",
            "disc_date": t.get("disclosureDate") or t.get("disclosure_date") or "",
        }
    except Exception:
        return None


# ── 3. Conviction scoring ────────────────────────────────────────────
SIZE_TIERS = [
    ("$1,000,001", 5.0), ("$5,000,001", 6.0), ("$1,000,000", 4.5),
    ("$500,001", 3.5), ("$250,001", 3.0), ("$100,001", 2.5),
    ("$50,001", 2.0), ("$15,001", 1.5), ("$1,001", 1.0),
]


def size_multiplier(amount_str):
    a = (amount_str or "").replace(" ", "")
    for prefix, mult in SIZE_TIERS:
        if prefix.replace(",", "") in a.replace(",", ""):
            return mult
    return 1.0


def committee_relevance(member_committees, asset_desc, ticker):
    """Does the member sit on a committee whose jurisdiction covers this stock?"""
    text = (asset_desc or "").lower()
    matches = []
    for com in member_committees:
        for frag, keywords in COMMITTEE_SECTORS.items():
            if frag in com:
                for kw in keywords:
                    if kw in text:
                        matches.append((com, kw))
                        break
    return matches


def is_noise(asset_desc, ticker):
    """Filter index funds / managed-account boilerplate (not directed bets)."""
    t = (asset_desc or "").lower()
    if not ticker or ticker in ("--", "N/A", ""):
        return True
    noise_kw = ["index fund", "etf", "mutual fund", "treasury", "municipal", "money market",
                "401", "ira", "s&p 500", "total market", "target date", "bond fund"]
    return any(k in t for k in noise_kw)


def lambda_handler(event, context):
    t0 = time.time()
    print("[political-intel] starting")

    legislators = fetch_legislators()
    committee_map = fetch_committee_map()
    print(f"[political-intel] {len(legislators)} legislators, {len(committee_map)} with committees")

    trades, sources = fetch_congress_trades()
    print(f"[political-intel] {len(trades)} raw trades from {sources}")

    # Fallback to last good snapshot if live fetch failed
    if not trades:
        prev = _read_s3_json(OUTPUT_KEY)
        if prev:
            prev["stale"] = True
            prev["generated_at"] = datetime.now(timezone.utc).isoformat()
            s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=json.dumps(prev, default=str).encode(),
                          ContentType="application/json", CacheControl="public, max-age=600")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "msg": "no live trades, kept snapshot"})}

    # Build a name → bioguide resolver (loose match on last name)
    name_to_bio = {}
    for bid, info in legislators.items():
        ln = info["name"].split()[-1].lower() if info["name"] else ""
        if ln:
            name_to_bio.setdefault(ln, bid)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    by_ticker = defaultdict(lambda: {
        "ticker": "", "asset": "", "n_buys": 0, "n_sells": 0,
        "buyers": set(), "sellers": set(), "committee_matches": [],
        "conviction": 0.0, "latest_tx": None, "size_weight": 0.0,
        "buy_members": [],
    })

    n_scored = 0
    for tr in trades:
        if tr["side"] != "buy" and tr["side"] != "sell":
            continue
        if is_noise(tr["asset"], tr["ticker"]):
            continue
        txd = _parse_date(tr["tx_date"])
        if not txd or txd < cutoff:
            continue
        n_scored += 1
        tk = tr["ticker"]
        rec = by_ticker[tk]
        rec["ticker"] = tk
        if not rec["asset"]:
            rec["asset"] = tr["asset"]

        # Resolve member → committees
        ln = (tr["member"] or "").split()[-1].lower() if tr["member"] else ""
        bid = name_to_bio.get(ln)
        coms = committee_map.get(bid, []) if bid else []
        info = legislators.get(bid, {}) if bid else {}

        if tr["side"] == "buy":
            rec["n_buys"] += 1
            rec["buyers"].add(tr["member"])
        else:
            rec["n_sells"] += 1
            rec["sellers"].add(tr["member"])

        # Track latest transaction date
        if rec["latest_tx"] is None or txd > rec["latest_tx"]:
            rec["latest_tx"] = txd

        # Conviction contribution (BUYS only carry positive conviction)
        if tr["side"] == "buy":
            size_m = size_multiplier(tr["amount"])
            rec["size_weight"] += size_m
            com_matches = committee_relevance(coms, tr["asset"], tk)
            committee_bonus = 1.0
            if com_matches:
                committee_bonus = 2.5  # committee-jurisdiction buy = the real edge
                for cm in com_matches:
                    rec["committee_matches"].append({
                        "member": tr["member"], "committee": cm[0], "match": cm[1],
                    })
            # recency decay (0.5 .. 1.0 across the lookback window)
            age_days = (now - txd).days
            recency = max(0.5, 1.0 - (age_days / (LOOKBACK_DAYS * 2.0)))
            base = 10.0 * size_m * committee_bonus * recency
            rec["conviction"] += base
            rec["buy_members"].append({
                "member": tr["member"], "party": info.get("party", ""),
                "state": info.get("state", ""), "chamber": tr["chamber"],
                "amount": tr["amount"], "tx_date": tr["tx_date"],
                "committee_match": bool(com_matches),
                "committees": coms[:3],
            })

    # Finalize records
    results = []
    for tk, rec in by_ticker.items():
        n_buyers = len(rec["buyers"])
        n_sellers = len(rec["sellers"])
        # Cluster multiplier — multiple distinct buyers is a strong tell
        cluster_mult = 1.0 + 0.4 * max(0, n_buyers - 1)
        # Net buy pressure
        if rec["n_sells"] > rec["n_buys"]:
            rec["conviction"] *= 0.4  # heavy selling overrides
        conviction = round(rec["conviction"] * cluster_mult, 1)
        has_committee = len(rec["committee_matches"]) > 0
        results.append({
            "ticker": tk,
            "asset": rec["asset"][:80],
            "conviction_score": conviction,
            "n_buyers": n_buyers,
            "n_sellers": n_sellers,
            "n_buys": rec["n_buys"],
            "n_sells": rec["n_sells"],
            "committee_relevant": has_committee,
            "committee_matches": rec["committee_matches"][:5],
            "buy_members": rec["buy_members"][:8],
            "size_weight": round(rec["size_weight"], 1),
            "cluster": n_buyers >= 2,
            "latest_tx_date": rec["latest_tx"].strftime("%Y-%m-%d") if rec["latest_tx"] else None,
            "buyers": list(rec["buyers"])[:8],
        })

    results.sort(key=lambda r: r["conviction_score"], reverse=True)
    top_conviction = [r for r in results if r["conviction_score"] > 0 and r["n_buys"] > r["n_sells"]][:40]
    clusters = [r for r in results if r["cluster"] and r["n_buys"] > r["n_sells"]][:25]
    committee_buys = [r for r in results if r["committee_relevant"] and r["n_buys"] > 0][:25]

    # ── Executive branch (Trump + cabinet) — OGE disclosure snapshot ──
    executive = _executive_snapshot()

    output = {
        "schema_version": "2.0",
        "engine": "political-intel (Quiver-free, conviction-scored)",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "lookback_days": LOOKBACK_DAYS,
        "sources": sources,
        "methodology": (
            "Conviction = 10 × size_tier × committee_bonus(2.5 if member's committee "
            "jurisdiction covers the stock) × recency_decay, × cluster_mult(1+0.4 per "
            "extra distinct buyer), × 0.4 if net sellers. Buys only carry positive "
            "conviction. Index funds / managed-account boilerplate filtered as noise. "
            "Transaction-date windowed (not disclosure-date) to respect STOCK Act lag."
        ),
        "stats": {
            "raw_trades": len(trades),
            "scored_trades": n_scored,
            "unique_tickers": len(results),
            "committee_relevant_names": len(committee_buys),
            "cluster_names": len(clusters),
        },
        "top_conviction_buys": top_conviction,
        "committee_relevant_buys": committee_buys,
        "clusters": clusters,
        "executive_holdings": executive,
        "by_ticker": {r["ticker"]: r for r in results[:200]},
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")

    print(f"[political-intel] DONE {round(time.time()-t0,1)}s — {len(top_conviction)} top conviction buys, "
          f"{len(committee_buys)} committee-relevant, {len(clusters)} clusters")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "top_conviction": len(top_conviction),
                                 "committee_relevant": len(committee_buys),
                                 "clusters": len(clusters), "scored_trades": n_scored})}


def _executive_snapshot():
    """Trump + executive branch positions from the latest OGE 278e disclosure.
    OGE periodic transaction reports are filed as PDFs; this structured snapshot
    is updated per filing. (2025-03-19 disclosure covering CY2024.)"""
    return {
        "as_of": "2025-03-19",
        "source": "OGE Form 278e public financial disclosure",
        "note": ("Executive-branch holdings are disclosed annually (278e) + via periodic "
                 "transaction reports. Trump's disclosures historically show funds/trusts "
                 "and licensing income more than directed single-stock trades; the live "
                 "signal value is in cabinet/advisor periodic reports. Updated per filing."),
        "subjects": ["Donald J. Trump", "cabinet (per OGE)"],
        "positions": [],  # populated as PTRs are parsed
    }
