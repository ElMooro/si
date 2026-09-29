"""Pure auction scoring used by the Treasury desk's historical composite.

The configured auction-desk handler writes its own desk and history artifacts.
The live detector owns the separate auction-crisis output. This module exposes
only the detector-compatible metric extraction and scoring functions; importing
it performs no network, credential, or storage operations.

The parity regression compares this scoring API with the detector source.
"""

NORMAL_2024_BASELINE = {
    "bills_lt_90d": {
        "btc_mean": 2.85,         # bills BTC normal range
        "btc_std": 0.6,
        "low_rate_floor_pct": 4.0, # 4-5% = current Fed-on-hold
        "indirect_share_pct": 35,  # avg indirect on bills
        "pd_share_pct": 50,        # higher PD% normal on bills
        "aah_mean": 50,
    },
    "coupons_gt_3y": {
        "btc_mean": 2.45,
        "btc_std": 0.25,
        "indirect_share_pct": 70,  # foreign CBs heavy in coupons
        "pd_share_pct": 18,        # very low PD share is healthy
        "aah_mean": 75,            # aah varies widely on coupons; high but well-bid
    },
    "tips": {
        "btc_mean": 2.40,
        "btc_std": 0.35,
        "indirect_share_pct": 65,
        "pd_share_pct": 20,
    },
}


def parse_float(v, default=None):
    if v in (None, "", "null"):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def classify_tenor_bucket(record):
    """Returns 'bills_lt_90d', 'bills_gte_90d', 'coupons_lt_3y', 'coupons_gt_3y', 'tips', 'frn'."""
    sec_type = (record.get("security_type") or "").upper()
    sec_term = (record.get("security_term") or "").upper()
    if "TIPS" in sec_type or "TIIN" in sec_type or "TIPS" in sec_term:
        return "tips"
    if "FRN" in sec_type or "FRN" in sec_term:
        return "frn"
    if "BILL" in sec_type:
        # parse tenor from term: e.g. "28-DAY", "13-WEEK", "26-WEEK"
        days = parse_term_to_days(sec_term)
        if days is None:
            return "bills_lt_90d"
        return "bills_lt_90d" if days < 90 else "bills_gte_90d"
    if "NOTE" in sec_type or "BOND" in sec_type:
        days = parse_term_to_days(sec_term)
        if days is None or days >= 365 * 3:
            return "coupons_gt_3y"
        return "coupons_lt_3y"
    return "coupons_gt_3y"


def parse_term_to_days(term):
    """'28-DAY' → 28, '13-WEEK' → 91, '10-YEAR' → 3650."""
    if not term:
        return None
    t = term.upper().strip()
    parts = t.replace("-", " ").split()
    try:
        n = int(parts[0])
    except (ValueError, IndexError):
        return None
    if "DAY" in t:
        return n
    if "WEEK" in t:
        return n * 7
    if "YEAR" in t:
        return n * 365
    if "MONTH" in t:
        return n * 30
    return None


def compute_record_metrics(record):
    """Extract the 6 diagnostic metrics from a fiscaldata auction record.

    Field names from API:
      - bid_to_cover_ratio
      - high_yield (or high_discnt_rate for bills)
      - low_yield (or low_discnt_rate)
      - allocation_pctage  (% allotted at high)
      - primary_dealer_accepted
      - direct_bidder_accepted
      - indirect_bidder_accepted
      - total_accepted
    """
    btc = parse_float(record.get("bid_to_cover_ratio"))

    # Bills use discount rate, coupons use yield
    high_rate = parse_float(record.get("high_yield"))
    if high_rate is None:
        high_rate = parse_float(record.get("high_discnt_rate"))
    low_rate = parse_float(record.get("low_yield"))
    if low_rate is None:
        low_rate = parse_float(record.get("low_discnt_rate"))
    median_rate = parse_float(record.get("median_yield"))
    if median_rate is None:
        median_rate = parse_float(record.get("median_discnt_rate"))

    aah = parse_float(record.get("allocation_pctage"))

    pd = parse_float(record.get("primary_dealer_accepted"), 0) or 0
    direct = parse_float(record.get("direct_bidder_accepted"), 0) or 0
    indirect = parse_float(record.get("indirect_bidder_accepted"), 0) or 0
    total_competitive = pd + direct + indirect
    pd_share = (pd / total_competitive * 100) if total_competitive > 0 else None
    indirect_share = (indirect / total_competitive * 100) if total_competitive > 0 else None
    direct_share = (direct / total_competitive * 100) if total_competitive > 0 else None

    accepted_total = parse_float(record.get("total_accepted"))

    tail_bp = None
    if high_rate is not None and median_rate is not None:
        tail_bp = (high_rate - median_rate) * 100  # basis points

    return {
        "auction_date": record.get("auction_date"),
        "issue_date": record.get("issue_date"),
        "security_type": record.get("security_type"),
        "security_term": record.get("security_term"),
        "cusip": record.get("cusip"),
        "tenor_bucket": classify_tenor_bucket(record),
        "high_rate": high_rate,
        "low_rate": low_rate,
        "median_rate": median_rate,
        "allocated_at_high_pct": aah,
        "btc": btc,
        "primary_dealer_pct": pd_share,
        "indirect_pct": indirect_share,
        "direct_pct": direct_share,
        "tail_bp": tail_bp,
        "accepted_billions": accepted_total / 1e9 if accepted_total else None,
    }


def score_indicators(metrics, fed_funds_rate):
    """Run each metric through the historical-pattern crisis test.

    Returns dict of {indicator_name: score 0-100}.
    """
    bucket = metrics["tenor_bucket"]
    scores = {}

    # ── 1. ZERO-RATE BILL FLOOR ──
    # only diagnostic when Fed has positive policy rate (else it's just at the lower bound)
    if bucket == "bills_lt_90d" and metrics["low_rate"] is not None:
        if fed_funds_rate is not None and fed_funds_rate > 1.0:
            # In a normal-rate environment, floor at 0% = panic
            if metrics["low_rate"] <= 0.001:
                scores["zero_rate_floor"] = 100  # 2008 / 2020 GFC pattern
            elif metrics["low_rate"] < fed_funds_rate * 0.3:
                scores["zero_rate_floor"] = 70   # severe undercut
            elif metrics["low_rate"] < fed_funds_rate * 0.6:
                scores["zero_rate_floor"] = 40
            else:
                scores["zero_rate_floor"] = 0    # normal
        else:
            # Fed already at zero-bound — signal undefined
            scores["zero_rate_floor"] = None

    # ── 2. BID-TO-COVER EXTREMES ──
    if metrics["btc"] is not None:
        if bucket.startswith("bills_"):
            # On bills, EXTREMELY HIGH BTC = panic (2020-Mar-26 had 4.74)
            base = NORMAL_2024_BASELINE["bills_lt_90d"]
            z = (metrics["btc"] - base["btc_mean"]) / base["btc_std"]
            if z >= 3.0:
                scores["btc_extreme"] = 100
            elif z >= 2.0:
                scores["btc_extreme"] = 70
            elif z >= 1.5:
                scores["btc_extreme"] = 45
            elif z <= -1.5:
                scores["btc_extreme"] = 50  # weak demand also stressful
            elif z <= -2.0:
                scores["btc_extreme"] = 80  # failed auction territory
            else:
                scores["btc_extreme"] = 0
        elif bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # On coupons, LOW BTC = failed auction warning
            base = NORMAL_2024_BASELINE.get("coupons_gt_3y" if bucket == "coupons_gt_3y" else "tips" if bucket == "tips" else "coupons_gt_3y")
            z = (metrics["btc"] - base["btc_mean"]) / base["btc_std"]
            if z <= -2.5:
                scores["btc_extreme"] = 100  # near-failure
            elif z <= -1.5:
                scores["btc_extreme"] = 70
            elif z <= -1.0:
                scores["btc_extreme"] = 40
            else:
                scores["btc_extreme"] = 0

    # ── 3. ALLOTTED-AT-HIGH (TAIL SEVERITY) ──
    if metrics["allocated_at_high_pct"] is not None:
        aah = metrics["allocated_at_high_pct"]
        if bucket.startswith("bills_"):
            # On bills during stress, LOW AAH = panic clustering at the low end
            # Only diagnostic when paired with other stress indicators
            if aah < 15:
                scores["tail_stress"] = 60  # extreme clustering — wait for confirmation
            elif aah > 90:
                scores["tail_stress"] = 50  # weak last leg
            else:
                scores["tail_stress"] = 0
        elif bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            if aah > 95:
                scores["tail_stress"] = 75  # near tail-out, dealers absorbed
            elif aah > 90:
                scores["tail_stress"] = 50
            elif aah < 30:
                scores["tail_stress"] = 70  # below-norm = investors stepped back
            else:
                scores["tail_stress"] = 0

    # ── 4. PRIMARY DEALER SHARE ──
    if metrics["primary_dealer_pct"] is not None:
        pd_pct = metrics["primary_dealer_pct"]
        if bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # PD > 35% on coupons = dealer absorption (2008 had 46-70%)
            if pd_pct > 50:
                scores["pd_absorption"] = 100  # GFC-extreme
            elif pd_pct > 35:
                scores["pd_absorption"] = 70
            elif pd_pct > 25:
                scores["pd_absorption"] = 35
            else:
                scores["pd_absorption"] = 0   # healthy
        else:
            # Bills naturally have higher PD shares; only flag if MUCH higher than baseline
            base = NORMAL_2024_BASELINE["bills_lt_90d"]["pd_share_pct"]
            if pd_pct > base + 25:
                scores["pd_absorption"] = 70
            elif pd_pct > base + 15:
                scores["pd_absorption"] = 35
            else:
                scores["pd_absorption"] = 0

    # ── 5. INDIRECT (FOREIGN) SHARE COLLAPSE ──
    if metrics["indirect_pct"] is not None:
        ind_pct = metrics["indirect_pct"]
        if bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # Foreign demand on coupons. <50% = exodus pattern (2008-09-18 had 29.3%)
            if ind_pct < 30:
                scores["indirect_collapse"] = 100
            elif ind_pct < 50:
                scores["indirect_collapse"] = 65
            elif ind_pct < 60:
                scores["indirect_collapse"] = 30
            else:
                scores["indirect_collapse"] = 0
        # Bills indirect varies more; not as diagnostic

    return {k: v for k, v in scores.items() if v is not None}
