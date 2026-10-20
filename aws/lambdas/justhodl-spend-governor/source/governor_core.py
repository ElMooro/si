"""justhodl-spend-governor - pure decision logic (no AWS calls; unit-tested in tests/run_tests.py).

Why a governor and not an AWS Budget Action: AWS has no native hard cap. Budget Actions can only attach
IAM/SCP policies or stop EC2/RDS, cannot stop scheduled or event-driven Lambda invocations, and fire
after the money is spent.  Billing data also lags (Budgets/Cost Explorer refresh ~3x a day), and some
charges continue even when every engine is stopped (storage, data transfer for the site, alarms).

So the governor works on lines computed BELOW the cap:

    reserve   = residual/day x days_left + baseline/day x lag_days + safety
    freeze_at = cap - reserve                    (floored at min_freeze_fraction x cap)
    brake_at  = freeze_at - max(brake_gap_usd, baseline/day x brake_gap_days)

  * BRAKE  (MTD >= brake_at):  shed the most expensive load (top Lambda spenders, SageMaker, EC2/RDS...)
  * FREEZE (MTD >= freeze_at): throttle everything except a small exempt set
  * FUSE   (any time):         a runaway function (guard alarm in ALARM and the function alone burning
                               >= runaway_fn_usd_day) is throttled within the hour, cap or no cap
  * FAST FUSE (v1.1.0):        a sustained runaway alarm (15 min far above normal) reaches the governor
                               through SNS and the hottest sustained burners are throttled in seconds

Once BRAKE/FREEZE is entered it holds until the month rolls over, the cap is RAISED, or an operator
releases it - never because the moving lines drift (that would flap).  FUSE entries are only lifted
by an operator, because the 1st of the month does not fix a runaway.
"""
from datetime import datetime, timedelta, timezone

VERSION = "1.5.0"
STAGES = ("NORMAL", "BRAKE", "FREEZE")
LEVEL = {s: i for i, s in enumerate(STAGES)}

DEFAULTS = {
    "lag_days": 1.0,              # spend already incurred but not yet in billing data
    "safety_usd": 10.0,           # flat margin under the cap
    "brake_gap_usd": 40.0,        # brake at least this far below the freeze line ...
    "brake_gap_days": 2.0,        # ... or this many days of baseline spend, whichever is larger
    "min_freeze_fraction": 0.5,   # the freeze line never drops below 50% of the cap
    "min_brake_fraction": 0.4,
    "hysteresis_usd": 10.0,       # a cap raise must clear the crossed line by this much to release
    "residual_default_usd_day": 3.0,
    "baseline_default_usd_day": 20.0,
    "runaway_fn_usd_day": 20.0,   # a single function burning faster than this during a guard alarm is a runaway
    "runaway_total_factor": 3.0,  # ... and the fleet as a whole must exceed 3x its normal daily spend
    "fast_fuse_min_share": 0.10,  # fast fuse (v1.1.0): a function must carry >= 10% of the fleet's burn ...
    "fast_fuse_max_fns": 5.0,     # ... and at most the 5 hottest are throttled per alarm
    "brake_shed_share": 0.8,      # BRAKE throttles the top Lambda spenders covering 80% of Lambda cost
    "brake_max_functions": 40,
    "brake_min_fn_usd_day": 0.05,
    # v1.2.0 diffuse-storm fuse (Khalid, 2026-10-20): the Oct 12-16 loop was one hot pair, but a storm spread over
    # many functions each under runaway_fn_usd_day was invisible to both fuses.  When the FLEET burns >= this rate
    # (normal Lambda spend is ~$4/day; $75/day = ~$2,300/month from Lambda alone) the hottest functions covering
    # diffuse_share of the burn are throttled, each >= diffuse_min_fn_usd_day, at most diffuse_max_fns.
    "diffuse_fleet_usd_day": 75.0,
    "diffuse_share": 0.6,
    "diffuse_min_fn_usd_day": 2.0,
    "diffuse_max_fns": 30.0,
    "egress_alert_usd_day": 5.0,  # v1.2.0: S3 data-transfer-out above this on a complete day is reported (~55 GB)
    # v1.3.0 engine kill switch (Khalid, 2026-10-20: "any engine or events that cause $50 in a month shuts down
    # automatically and inform me and won't turn on again unless I manually turn it on"): every function's month-to-
    # date Lambda cost is metered from CloudWatch every tick; past engine_cap_usd_month it is held at concurrency 0
    # under ledger stage CAP, which no restore, cap raise or month rollover lifts - only {"action":"release_cap"}.
    "engine_cap_usd_month": 50.0,
    "meter_interval_h": 1.0,
    "meter_lag_min": 3.0,          # Lambda metrics land 1-3 minutes late: the window ends this far in the past
    # v1.4.0 anomaly sentinel (Khalid, 2026-10-20: "detect anomalies and fix them automatically"): each engine is
    # compared with its own last-14-day baseline every metered window.  A window with more invocations than
    # anom_inv_factor x a normal DAY (floor anom_min_inv) or more cost than anom_usd_factor x a normal day (floor
    # anom_min_usd) is a strike; two consecutive strikes (or one window >= anom_hard_usd) hold the engine at
    # concurrency 0 for anom_cooldown_h, then it is released automatically; the third hold in a month is permanent
    # (stage CAP: manual release only).
    "anom_inv_factor": 3.0,
    "anom_min_inv": 200.0,
    "anom_usd_factor": 3.0,
    "anom_min_usd": 1.0,
    "anom_hard_usd": 5.0,
    "anom_cooldown_h": 6.0,
    "anom_max_holds_month": 3.0,
    "anom_error_min": 10.0,        # errors in a window, and ...
    "anom_error_share": 0.5,       # ... errors / invocations at or above this -> an error storm is reported
    "anom_baseline_days": 14.0,
    # v1.5.0 storage fuse (storage_fuse.py): daily growth of the live bucket from the free S3 metrics
    "storage_warn_gb": 150.0,          # one-day growth that is reported (the weekly ~140 GB spike sits just under it)
    "storage_warn_pct": 20.0,
    "storage_objects_warn": 500000.0,
    "storage_trip_gb": 500.0,          # one-day growth that holds the hottest writer (or two warn days in a row)
    "storage_trip_pct": 50.0,
    "storage_objects_trip": 2000000.0,
    "storage_hold_top": 1.0,           # writers held on a trip; a second strike holds 3 and suspends versioning
    "storage_after_hour": 5.0,         # UTC hour after which the previous day's S3 metrics are expected
    "s3_usd_day_alert": 10.0,          # yesterday's S3 service cost (Cost Explorer) that is reported
}

PERSISTENT_USAGE_PATTERNS = (
    # charges that keep accruing with every engine stopped
    "timedstorage", "storage", "hostedzone", "alarmmonitorusage", "metricmonitorusage", "kms-keys",
    "secret", "ebs:volumeusage", "ebs:snapshot", "datatransfer-out", "requests-tier2", "dashboards",
    "studio:volumeusage", "pitrstorage", "storagelens",
)


def as_utc(ts):
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def month_key(now):
    return now.strftime("%Y-%m")


def month_start(now):
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month_start(now):
    return (month_start(now) + timedelta(days=32)).replace(day=1)


def days_left(now):
    return max(0.0, (next_month_start(now) - now).total_seconds() / 86400.0)


def merged_cfg(overrides=None):
    cfg = dict(DEFAULTS)
    for k, v in (overrides or {}).items():
        if k in DEFAULTS and isinstance(v, (int, float)) and not isinstance(v, bool):
            cfg[k] = float(v)
    return cfg


def effective_cap(budget_limit, control, now):
    """The AWS Budget amount is the single source of truth; a per-month override may replace it."""
    ov = ((control or {}).get("cap_overrides") or {}).get(month_key(now))
    if isinstance(ov, dict) and isinstance(ov.get("cap_usd"), (int, float)) and ov["cap_usd"] > 0:
        return float(ov["cap_usd"]), "override:" + month_key(now)
    if budget_limit is None:
        return None, "missing"
    return float(budget_limit), "budget"


def median(values):
    v = sorted(float(x) for x in values)
    if not v:
        return None
    n = len(v)
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2.0


def baseline_from_daily(daily_totals, today, cfg):
    """Robust daily spend: median of the last 14 COMPLETE days (a multi-day spike cannot drag it)."""
    complete = [v for d, v in sorted(daily_totals.items()) if d < today.isoformat()][-14:]
    m = median(complete)
    return (m, "median14") if m is not None else (cfg["baseline_default_usd_day"], "default")


def residual_from_usage(daily_usage, today, cfg):
    """Persistent (non-stoppable) spend per day: MIN over the last 7 complete days, so a transient
    storage balloon that lifecycle is already purging does not set the floor for a whole month."""
    per_day = []
    for d, usage in sorted(daily_usage.items()):
        if d >= today.isoformat():
            continue
        per_day.append(sum(float(v) for k, v in usage.items()
                           if any(p in k.lower() for p in PERSISTENT_USAGE_PATTERNS)))
    per_day = per_day[-7:]
    return (min(per_day), "min7") if per_day else (cfg["residual_default_usd_day"], "default")


def thresholds(cap, mtd, now, baseline, residual, cfg):
    dl = days_left(now)
    lag_buffer = max(baseline, residual) * cfg["lag_days"]
    reserve = residual * dl + lag_buffer + cfg["safety_usd"]
    freeze_floor = cap * cfg["min_freeze_fraction"]
    freeze_at = max(cap - reserve, freeze_floor)
    gap = max(cfg["brake_gap_usd"], baseline * cfg["brake_gap_days"])
    brake_at = min(max(freeze_at - gap, cap * cfg["min_brake_fraction"]), freeze_at)
    projected = mtd + max(baseline, residual) * (dl + cfg["lag_days"])
    frozen_eom = mtd + lag_buffer + residual * dl  # where the month lands if frozen right now
    return {
        "cap": round(cap, 2), "mtd": round(mtd, 2), "days_left": round(dl, 2),
        "baseline_usd_day": round(baseline, 2), "residual_usd_day": round(residual, 2),
        "lag_buffer": round(lag_buffer, 2), "reserve": round(reserve, 2),
        "freeze_at": round(freeze_at, 2), "brake_at": round(brake_at, 2),
        "freeze_line_floored": cap - reserve < freeze_floor,
        "projected_eom": round(projected, 2), "frozen_eom": round(frozen_eom, 2),
        "cap_at_risk_even_frozen": frozen_eom > cap,
    }


def decide(stage, mtd, th, cap_at_stage, cfg, suppressed=False):
    """Return (next_stage, reason).  Escalates immediately; de-escalates only on a cap raise."""
    stage = stage if stage in LEVEL else "NORMAL"
    if suppressed:
        return "NORMAL", "suppressed by operator release"
    target = "FREEZE" if mtd >= th["freeze_at"] else "BRAKE" if mtd >= th["brake_at"] else "NORMAL"
    if LEVEL[target] > LEVEL[stage]:
        line = th["freeze_at"] if target == "FREEZE" else th["brake_at"]
        return target, "MTD $%.2f >= %s line $%.2f (cap $%.2f)" % (mtd, target.lower(), line, th["cap"])
    if LEVEL[target] < LEVEL[stage]:
        cap_then = float(cap_at_stage if cap_at_stage is not None else th["cap"])
        crossed = th["freeze_at"] if stage == "FREEZE" else th["brake_at"]
        if th["cap"] > cap_then and mtd < crossed - cfg["hysteresis_usd"]:
            return target, "cap raised $%.2f -> $%.2f; MTD $%.2f clears the %s line" % (cap_then, th["cap"], mtd, stage.lower())
        return stage, "holding %s until the 1st, a cap raise, or an operator release" % stage
    return stage, "steady"


def rank_brake_targets(costs, exempt, cfg, always=()):
    """costs: {function: usd_per_day}.  Top spenders covering brake_shed_share of the total, plus always-list."""
    total = sum(v for k, v in costs.items() if k not in exempt and v > 0)
    picked, covered = [], 0.0
    for fn, usd in sorted(costs.items(), key=lambda kv: -kv[1]):
        if fn in exempt or usd < cfg["brake_min_fn_usd_day"]:
            continue
        if total and covered / total >= cfg["brake_shed_share"]:
            break
        if len(picked) >= int(cfg["brake_max_functions"]):
            break
        picked.append(fn)
        covered += usd
    for fn in always:
        if fn not in exempt and fn not in picked:
            picked.append(fn)
    return picked


def runaway_functions(costs_per_day, baseline, exempt, cfg):
    """Functions to throttle by the FUSE: each alone >= runaway_fn_usd_day, and the fleet total far above normal."""
    total = sum(costs_per_day.values())
    if total < max(cfg["runaway_fn_usd_day"], baseline * cfg["runaway_total_factor"]):
        return []
    return sorted((fn for fn, usd in costs_per_day.items()
                   if usd >= cfg["runaway_fn_usd_day"] and fn not in exempt),
                  key=lambda fn: -costs_per_day[fn])


def fast_fuse_targets(costs_per_day, buckets, exempt, cfg):
    """Functions the FAST fuse throttles once a sustained runaway alarm fires (v1.1.0).

    The alarm already proves the fleet has been far above normal for 15 minutes, so unlike the hourly fuse there
    is no second strike: a function qualifies when it burned >= runaway_fn_usd_day over the alarm window, carries
    >= fast_fuse_min_share of the fleet's burn, and every 5-minute bucket of the window stayed at that rate (a
    burst that already ended - the 2026-10-10 16:30 fan-out of 654 copies for ~4 minutes - never qualifies)."""
    total = sum(costs_per_day.values())
    floor = cfg["runaway_fn_usd_day"]
    picks = [fn for fn, usd in sorted(costs_per_day.items(), key=lambda kv: -kv[1])
             if fn not in exempt and usd >= floor and usd >= cfg["fast_fuse_min_share"] * total]
    picks = [fn for fn in picks if buckets.get(fn) and min(buckets[fn]) >= floor]
    return picks[:int(cfg["fast_fuse_max_fns"])]


def diffuse_targets(costs_per_day, baseline, exempt, cfg):
    """v1.2.0: functions to throttle when the fleet as a whole is in a storm that no single function dominates.
    Empty unless the fleet burns >= diffuse_fleet_usd_day AND >= runaway_total_factor x normal.  Returns the hottest
    non-exempt functions (each >= diffuse_min_fn_usd_day) until diffuse_share of the fleet burn is covered."""
    total = sum(costs_per_day.values())
    if total < max(cfg["diffuse_fleet_usd_day"], baseline * cfg["runaway_total_factor"]):
        return []
    picked, covered = [], 0.0
    for fn, usd in sorted(costs_per_day.items(), key=lambda kv: (-kv[1], kv[0])):
        if covered >= cfg["diffuse_share"] * total or len(picked) >= int(cfg["diffuse_max_fns"]):
            break
        if fn in exempt or usd < cfg["diffuse_min_fn_usd_day"]:
            continue
        picked.append(fn)
        covered += usd
    return picked


def egress_usd_by_day(daily_usage):
    """{day: USD} of data-transfer-out usage types (site egress from S3 is the one residual that can balloon)."""
    out = {}
    for d, usage in daily_usage.items():
        v = sum(float(x) for k, x in usage.items() if "datatransfer-out" in k.lower())
        if v:
            out[d] = round(v, 4)
    return out


def is_exempt(fn, control, self_name):
    c = control or {}
    if fn == self_name:
        return True
    if fn in set(c.get("exempt_functions") or []):
        return True
    return any(fn.startswith(p) for p in (c.get("exempt_prefixes") or []) if p)


def lambda_cost_usd(duration_ms, invocations, memory_mb, arch):
    """USD of `duration_ms` of execution at `memory_mb` plus `invocations` requests (list prices, no free tier)."""
    price = 0.0000133334 if arch == "arm64" else 0.0000166667
    return (duration_ms / 1000.0) * (memory_mb / 1024.0) * price + invocations * 2e-7


def anomaly_baselines(daily, today, days=14):
    """{fn: (median invocations/day, median usd/day)} over the last `days` complete days of the meter's daily record
    (a day with no row for a function counts as zero: an engine that never runs has a baseline of nothing)."""
    keys = sorted(d for d in daily if d < today)[-int(days):]
    if not keys:
        return {}
    fns = set()
    for d in keys:
        fns |= set(daily[d])
    out = {}
    for fn in fns:
        inv = [float((daily[d].get(fn) or [0, 0])[0]) for d in keys]
        usd = [float((daily[d].get(fn) or [0, 0])[1]) for d in keys]
        out[fn] = (median(inv) or 0.0, median(usd) or 0.0, len(keys))
    return out


def anomalies(window_inv, window_usd, window_err, baselines, exempt, cfg):
    """Engines whose last metered window is far outside their own normal day.  -> [{fn, kind, inv, usd, err, limit}]"""
    out = []
    for fn, inv in window_inv.items():
        if fn in exempt:
            continue
        usd, err = float(window_usd.get(fn, 0.0)), float(window_err.get(fn, 0.0))
        b_inv, b_usd, n = baselines.get(fn) or (0.0, 0.0, 0)
        lenient = 3.0 if n < 3 else 1.0                      # a new engine with no history gets room
        lim_inv = max(cfg["anom_min_inv"] * lenient, cfg["anom_inv_factor"] * b_inv)
        lim_usd = max(cfg["anom_min_usd"] * lenient, cfg["anom_usd_factor"] * b_usd)
        kinds = []
        if usd >= cfg["anom_hard_usd"]:
            kinds.append("hard")
        if inv > lim_inv:
            kinds.append("invocations")
        if usd > lim_usd:
            kinds.append("cost")
        if err >= cfg["anom_error_min"] and inv and err / inv >= cfg["anom_error_share"]:
            kinds.append("errors")
        if kinds:
            out.append({"fn": fn, "kind": "+".join(kinds), "inv": round(inv), "usd": round(usd, 3), "err": round(err),
                        "limit": {"inv": round(lim_inv), "usd": round(lim_usd, 2)}, "baseline_days": n})
    return sorted(out, key=lambda a: -a["usd"])


def cap_breaches(mtd, caps, default_cap, released, exempt):
    """-> [(fn, usd_counted, cap)] for every function whose month-to-date Lambda cost, less what had accrued when an
    operator last released it, reaches its cap.  caps: per-function overrides (0 / None = no cap for that function)."""
    out = []
    for fn, usd in mtd.items():
        if fn in exempt:
            continue
        cap = caps.get(fn, default_cap) if isinstance(caps, dict) else default_cap
        if not cap or cap <= 0:
            continue
        counted = usd - float((released.get(fn) or {}).get("mtd_at_release") or 0.0)
        if counted >= cap:
            out.append((fn, round(counted, 4), float(cap)))
    return sorted(out, key=lambda t: -t[1])


def lambda_cost_per_day(duration_ms, invocations, memory_mb, arch, hours):
    """USD/day implied by `hours` of CloudWatch Duration/Invocations sums."""
    price = 0.0000133334 if arch == "arm64" else 0.0000166667
    usd = (duration_ms / 1000.0) * (memory_mb / 1024.0) * price + invocations * 2e-7
    return usd * (24.0 / hours) if hours else 0.0
