"""justhodl-canary-warroom — the unified Early-Warning War Room aggregator.

Pulls every canary mechanism the platform runs into ONE normalized board:
  1. Macro hard-data leads      (canary-grid: 22 trade/commodity/rates/credit/labor/housing)
  2. Funding plumbing + internals(crisis-canaries: SOFR/repo/deposits/credit/market-internals, 7 families)
  3. Cycle-phase market leadership(leading-markets: markets bucketed vs ACWI)
  4. Dollar direction            (dollar-radar)
  5. Volatility turning points    (vol-radar)
  6. Live alert flips             (alert-sentinel)

+ surfaces the user's own canary PLAYBOOK from the brain (data/brain.json notes),
so the War Room reflects the operator's hard-won early-warning wisdom.

Ranks every firing canary by severity and lead-time, and computes CROSS-MECHANISM
DIVERGENCES (e.g. trade booming while credit tightens) — the split-screen a
top macro desk watches.

OUTPUT  data/canary-warroom.json     SCHEDULE  hourly :50 (after feeds refresh)
Real aggregated data — not investment advice.
"""
import json
import re
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/canary-warroom.json"


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _band(stress):
    if stress is None:
        return "NO DATA"
    if stress >= 80:
        return "CRITICAL"
    if stress >= 60:
        return "WARNING"
    if stress >= 40:
        return "ELEVATED"
    if stress >= 20:
        return "WATCH"
    return "CALM"


# ── per-mechanism normalizers → (mechanism_card, [canaries]) ──────────
def norm_macro_grid(d):
    cans = []
    for s in (d.get("signals") or []):
        if not s.get("available"):
            continue
        st = s.get("stress")
        cans.append({"mechanism": "macro_grid", "mech_label": "Macro Leads",
                     "name": s.get("name"), "stress": st, "band": _band(st),
                     "lead_months": s.get("lead_months"), "sub": s.get("sub_grid"),
                     "value": s.get("value"), "unit": s.get("unit"),
                     "detail": s.get("read"), "firing": (st is not None and st >= 50)})
    card = {"key": "macro_grid", "label": "Macro Hard-Data Leads",
            "score": d.get("early_warning_level"), "band": d.get("band"),
            "headline": d.get("headline"), "n_total": len(cans),
            "n_firing": sum(1 for c in cans if c["firing"]),
            "sub_grids": d.get("sub_grids"), "scale": "0-100 stress"}
    return card, cans


def norm_crisis(d):
    cans = []
    # the engine already distilled the firing ones into alerts[]
    for a in (d.get("alerts") or []):
        cans.append({"mechanism": "funding", "mech_label": "Funding & Internals",
                     "name": str(a)[:70], "stress": 62, "band": "WARNING",
                     "lead_months": 0.5, "detail": str(a), "firing": True,
                     "family_member": True})
    fams = d.get("families") or {}
    for fname, fv in fams.items():
        fsc = fv.get("score")
        n = fv.get("n") or 0
        if fsc is None or not n:
            continue
        cans.append({"mechanism": "funding", "mech_label": "Funding & Internals",
                     "name": "Funding family — %s (%d watched)" % (fname, n),
                     "stress": fsc, "band": _band(fsc), "lead_months": 0.5,
                     "value": "%d red / %d amber" % (fv.get("red") or 0,
                                                     fv.get("amber") or 0),
                     "detail": "Aggregate of the %d %s-family plumbing "
                               "canaries on the crisis board." % (n, fname),
                     "firing": (fsc or 0) >= 50,
                     "synthetic_family": True, "n_members": n})
    card = {"key": "funding", "label": "Funding Plumbing & Market Internals",
            "score": d.get("composite_score"), "band": (d.get("level") or "").upper(),
            "headline": d.get("headline") or (d.get("read")),
            "families": {k: {"score": v.get("score"), "red": v.get("red"), "amber": v.get("amber"), "n": v.get("n")}
                         for k, v in fams.items()},
            "n_total": sum((v.get("n") or 0) for v in fams.values()),
            "n_firing": sum((v.get("red") or 0) for v in fams.values()),
            "scale": "0-100 stress"}
    return card, cans


def norm_leading_markets(d):
    cans = []
    for m in (d.get("markets") or []):
        contracting = (m.get("regime") == "CONTRACTION") or (m.get("rs_state") == "lagging")
        if contracting:
            st = 58 if m.get("regime") == "CONTRACTION" else 48
        elif m.get("regime") == "EXPANSION" and m.get("rs_state") == "leading":
            st = 22
        elif m.get("regime") == "SLOWING":
            st = 40
        else:
            st = 30
        cans.append({"mechanism": "leading_markets", "mech_label": "Market Leadership",
                     "name": "%s (%s)" % (m.get("market") or m.get("etf"), m.get("bucket")),
                     "stress": st, "band": _band(st) if not contracting else "ELEVATED",
                     "lead_months": 3,
                     "value": "%s%% 3m rs" % m.get("rs_3m_pct"),
                     "detail": "%s vs ACWI — %s, %s" % (m.get("market"), m.get("regime"), m.get("rs_state")),
                     "firing": bool(contracting)})
    card = {"key": "leading_markets", "label": "Cycle-Phase Market Leadership",
            "signal": d.get("turning_point_signal"), "headline": d.get("signal_read"),
            "flashing_buckets": d.get("flashing_buckets"), "benchmark": d.get("benchmark"),
            "n_total": len(d.get("markets") or []), "n_firing": len(cans), "scale": "relative strength vs ACWI"}
    return card, cans


def norm_dollar(d):
    cans = []
    for c in (d.get("canaries") or []):
        lean = (c.get("lean") or c.get("signal") or "")
        lu = str(lean).upper()
        firing = lu not in ("", "NEUTRAL", "FLAT")
        st = 55 if "PUMP" in lu or lu.startswith(("1", "2", "+")) else \
             42 if "DUMP" in lu or lu.startswith("-") else 30
        if not firing:
            st = 30
        cans.append({"mechanism": "dollar", "mech_label": "Dollar",
                     "name": c.get("label"), "stress": st,
                     "band": "WATCH" if firing else _band(st),
                     "lead_months": 1, "value": c.get("reading"),
                     "detail": "%s — %s" % (lean, c.get("detail") or ""),
                     "firing": bool(firing)})
    card = {"key": "dollar", "label": "Dollar Direction", "score": d.get("dollar_pressure"),
            "band": d.get("regime"), "headline": d.get("headline"),
            "n_total": len(d.get("canaries") or []), "n_firing": len(cans),
            "scale": "-100 dump .. +100 pump"}
    return card, cans


def norm_vol(d):
    cans = []
    for c in (d.get("spike_canaries") or []):
        if c.get("firing"):
            mx = c.get("max") or 1
            st = round(40 + 25 * (c.get("points", 0) / mx), 0)
        else:
            st = 25
        cans.append({"mechanism": "vol", "mech_label": "Volatility",
                     "name": c.get("label"), "stress": st, "band": _band(st),
                     "lead_months": 0.5, "detail": c.get("detail"),
                     "firing": bool(c.get("firing"))})
    sc = d.get("scores") or {}
    card = {"key": "vol", "label": "Volatility Turning Points", "score": sc.get("spike_risk"),
            "band": d.get("posture"), "headline": d.get("headline"),
            "n_total": len(d.get("spike_canaries") or []), "n_firing": len(cans),
            "scale": "0-100 spike-risk"}
    return card, cans


def norm_alerts(d):
    recent = [str(x)[:90] for x in (d.get("changes") or [])][:8]
    card = {"key": "alerts", "label": "Live Alert Flips (Sentinel)", "score": d.get("n_changes"),
            "band": "ACTIVE" if (d.get("n_changes") or 0) else "QUIET", "headline": None,
            "recent": recent, "n_total": d.get("buffer_n"), "n_firing": d.get("n_changes")}
    return card, []


# ── cross-mechanism divergences ──────────────────────────────────────
def divergences(cards):
    out = []
    by = {c["key"]: c for c in cards}
    grid = by.get("macro_grid", {}); subs = grid.get("sub_grids") or {}
    trade = (subs.get("trade_shipping") or {}).get("score")
    rates = (subs.get("rates_credit") or {}).get("score")
    if trade is not None and rates is not None and rates - trade >= 15:
        out.append({"title": "Trade boom vs financial tightening",
                    "read": "Real-time trade is calm (Trade sub-grid %.0f) while the slower Rates & Credit leads are elevated (%.0f) — a hot trade surface sitting over early financial/credit deterioration." % (trade, rates),
                    "severity": "WATCH"})
    fund = by.get("funding", {})
    fams = fund.get("families") or {}
    credit_s = (fams.get("credit") or {}).get("score")
    intern_s = (fams.get("internals") or {}).get("score")
    if credit_s is not None and intern_s is not None and credit_s - intern_s >= 20:
        out.append({"title": "Credit cracking beneath calm market internals",
                    "read": "Funding-grid credit family is stressed (%.0f) while market internals are calm (%.0f) — credit tends to crack before price confirms it." % (credit_s, intern_s),
                    "severity": "WATCH"})
    lm = by.get("leading_markets", {})
    flash = lm.get("flashing_buckets") or []
    if flash and lm.get("signal", "").startswith("EXPANSION"):
        out.append({"title": "Expansion headline, but leaders flashing",
                    "read": "Market leadership reads %s, yet %s are already flashing warnings — the earliest-turning buckets are rolling over under the surface." % (lm.get("signal"), ", ".join(flash)),
                    "severity": "WATCH"})
    dol = by.get("dollar", {}); vol = by.get("vol", {})
    if (dol.get("score") or 0) > 15 and str(vol.get("band", "")).upper() == "CALM":
        out.append({"title": "Dollar firming while vol is calm",
                    "read": "Dollar pressure is leaning pump (%s) while volatility is calm — a strengthening dollar is a slow-burn risk-off tell that often precedes a vol repricing." % dol.get("score"),
                    "severity": "WATCH"})
    return out


def brain_playbook():
    b = gj("data/brain.json")
    notes = []
    if isinstance(b, dict):
        for k in ("notes", "entries", "items", "data"):
            if isinstance(b.get(k), list):
                notes = b[k]; break
    elif isinstance(b, list):
        notes = b
    pat = re.compile(r"canary|canaries|leading indicator|early.?warning|tripwire|crack|front.?run|inversion|re.?steepen|RRP|reverse repo|bill auction|liquidity crunch|SOFR|breadth", re.I)
    hits = []
    for n in notes:
        if not isinstance(n, dict):
            continue
        txt = n.get("text") or n.get("note") or n.get("content") or ""
        if len(txt) < 40 or not pat.search(txt):
            continue
        # skip code-ish / meta notes
        if txt.strip().startswith(("#", '"""', "import", "def ")):
            continue
        hits.append({"text": txt[:400], "cat": n.get("cat"), "pinned": bool(n.get("pinned"))})
    hits.sort(key=lambda h: (not h["pinned"]))
    # dedupe by first 60 chars
    seen, uniq = set(), []
    for h in hits:
        k = h["text"][:60]
        if k in seen:
            continue
        seen.add(k); uniq.append(h)
    return uniq[:8]


MECHS = [("data/canary-grid.json", norm_macro_grid), ("data/crisis-canaries.json", norm_crisis),
         ("data/leading-markets.json", norm_leading_markets), ("data/dollar-radar.json", norm_dollar),
         ("data/vol-radar.json", norm_vol), ("data/alert-sentinel.json", norm_alerts)]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    cards, all_cans = [], []
    for key, fn in MECHS:
        d = gj(key)
        if not d:
            cards.append({"key": key.split("/")[-1].replace(".json", ""), "label": key, "unavailable": True})
            continue
        try:
            card, cans = fn(d)
            cards.append(card); all_cans.extend(cans)
        except Exception as e:
            cards.append({"key": key, "error": str(e)[:80]})
    firing = [c for c in all_cans if c.get("firing")]
    firing.sort(key=lambda c: (-(c.get("stress") or 0), c.get("lead_months") or 99))
    # master early-warning = weighted blend of the mechanisms that publish a 0-100 stress
    grid = next((c for c in cards if c.get("key") == "macro_grid"), {})
    fund = next((c for c in cards if c.get("key") == "funding"), {})
    vol = next((c for c in cards if c.get("key") == "vol"), {})
    parts = [(grid.get("score"), 0.45), (fund.get("score"), 0.35), (vol.get("score"), 0.20)]
    num = sum(v * w for v, w in parts if v is not None)
    den = sum(w for v, w in parts if v is not None)
    master_ew = round(num / den, 1) if den else None
    divs = divergences(cards)
    # ── MASTER BAROMETER (Khalid spec 2026-07-09): EQUAL WEIGHT PER CANARY.
    # Every watched canary gets exactly one vote of its 0-100 stress —
    # macro grid, market leadership, dollar, vol each per-canary; funding
    # counts each family's score once per member canary (the alert rows are
    # members of those families, so they are excluded to avoid double
    # counting). Sentinel alert-rules are binary flips, not stress gauges,
    # and are shown separately, not averaged.
    votes = []
    for c in all_cans:
        st = c.get("stress")
        if st is None or c.get("family_member"):
            continue
        if c.get("synthetic_family"):
            votes.extend([st] * int(c.get("n_members") or 1))
        elif c.get("mechanism") in ("macro_grid", "leading_markets",
                                    "dollar", "vol"):
            votes.append(st)
    baro = round(sum(votes) / len(votes), 1) if votes else None
    barometer = {"score": baro, "band": _band(baro), "n_votes": len(votes),
                 "method": "equal_weight_per_canary",
                 "note": ("Every watched canary = one equal vote of its "
                          "0-100 stress. Sentinel alert-rules (binary "
                          "flips) shown separately, not averaged.")}
    out = {"engine": "justhodl-canary-warroom", "generated_at": now.isoformat(),
           "barometer": barometer,
           "master": {"early_warning_0_100": master_ew, "band": _band(master_ew),
                      "n_firing": len(firing), "n_canaries": len(all_cans),
                      "n_divergences": len(divs),
                      "headline": "%d of %d canaries firing across 6 mechanisms; early-warning %s (%s). %d cross-mechanism divergence%s." % (
                          len(firing), len(all_cans), master_ew, _band(master_ew), len(divs), "" if len(divs) == 1 else "s")},
           "mechanisms": cards, "firing": firing[:40], "all_canaries": all_cans,
           "divergences": divs, "brain_playbook": brain_playbook(),
           "note": "Unified early-warning across every canary mechanism the platform runs, plus the operator's own brain playbook. Real aggregated data — not advice."}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8", CacheControl="max-age=1800")
    return {"ok": True, "barometer": baro, "master_ew": master_ew, "n_firing": len(firing), "n_divergences": len(divs)}
