#!/usr/bin/env python3
"""scripts/pipeline_order.py - the day as a pipeline: readers fire after their writers (Khalid, 2026-10-20/21).

At once a day (the cadence floor, ops 6554), WHEN an engine fires relative to the engines whose files it reads decides
whether a page shows today's numbers or yesterday's.  Faults the planner removes:

  race     a reader fires inside its writer's running time (0..margin minutes after it): it reads the previous file
           or a half-written one, and which one changes from day to day
  stale    a reader fires in the hour BEFORE its writer (--stale-min): it consumes a file that is ~24 hours old when
           a shift of minutes would make it minutes old
  a day    (pass >= 2, config/provider-publication.json) a puller runs before its provider publishes for the day: the
  late     provider is a pinned writer firing at its publication time(s); the puller moves to the latest print
           inside its own window + the provider margin
  bursts   (pass >= 2) more than slot_max pullers of one provider start on the same minute (rate limits): the
           excess moves to the next free minute; members of one fan-out tick are packed into late ticks the same way

Rules:
  * readers move LATER only, at most --max-move min (60; --provider-max-move for a day-late fix), inside their own
    session window (post-close 21:00-01:00 UTC, overnight 01:00-09:00, Europe / US pre-market 09:00-14:30, US session
    14:30-21:00) plus --grace-min past the window end; what would need more is left alone and listed
  * fan-out tick members, cadence-floor exempt triggers and non-daily crons are pinned; members of one tick that read
    other members of the same tick (fired in parallel by justhodl-scheduler) move to a LATE tick of that tick
    (`<tick>-late`, `-late2` ...: jhk-tick-* EventBridge Scheduler schedules a quarter hour apart); with provider slot
    caps the members of a tick are packed into late ticks so no late tick starts more than slot_max pullers of one
    provider, writers of in-tick readers first
  * edges inside a strongly connected component (A reads B, B reads A) are feedback with a one-day lag by design
  * margin(writer) = 2 x its mean duration (never more than its timeout; the timeout when no run is on record) + 3 min,
    clamped 5..--margin-cap min, from the ops 6546 account data
  * a moved reader is somebody's writer: the loop runs to a fixed point, and a writer whose own readers could not
    follow (pinned, capped) is kept in place rather than made to feed them yesterday's file - no edge gets worse.
    A day-late fix is the exception: a reader of a puller that caught today's print reads the same print it used to,
    a day later only by file time, so it is not a regression (its own catch-up is a later pass)
  * at most four triggers land on one minute
  * (pass >= 2, config/intraday-lane.json) the intraday lane: one Scheduler schedule in New York time fans the light
    Polygon feeds out every five minutes during market hours; exempt from the floor by name, caps unchanged

Output: config/pipeline-order-plan.json (pass 1) / config/pipeline-order-plan-<N>.json - cadence_apply actions
(kind, name, before, after) + late_ticks + tick_moves + intraday_lane + summary.
  python3 scripts/pipeline_order.py --pass N --write    build pass N and write it (--only-lane: the intraday lane and the
                                                        order rules only, no provider alignment / spreading)
  python3 scripts/pipeline_order.py --check             exit 1 when a committed plan no longer matches the configs (an
                                                        applied plan: the repo must declare its state; an unapplied one
                                                        must rebuild identically)
  python3 scripts/pipeline_order.py --pass N --apply-repo   make the repo say what AWS will say after the apply ops
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import hashlib
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import cadence_plan as cp  # noqa: E402
import provider_map as pm  # noqa: E402

CRON = re.compile(r"^cron\((\d{1,2}) (\d{1,2}) (\*|\?) (\*) (\?|\*|MON-FRI|[A-Z,-]+) (\*)\)$")
WINDOWS = [("post-close", 21 * 60, 25 * 60), ("overnight", 1 * 60, 9 * 60), ("pre-market", 9 * 60, 14 * 60 + 30), ("session", 14 * 60 + 30, 21 * 60)]
SLOT_MAX = 4
SCHEDULER_FN = "justhodl-scheduler"
LATE_STEP_MIN = 15
PROVIDER = "provider:"


def minute_of(expr):
    m = CRON.match(expr or "")
    return int(m.group(2)) * 60 + int(m.group(1)) if m else None


def tz_offset_min(tz_name, when=None):
    """Minutes to ADD to a schedule's local cron minute to get UTC (America/New_York in October: +240). A Scheduler
    schedule carries ScheduleExpressionTimezone; the manifest records it as "timezone". Computed for `when` (now by
    default) - a plan written in EDT is re-checked by --check after the clocks change. 2026-10-21: three New York
    schedules (auction-desk-buyback, bond-warroom-after-auction, master-ranker-weekday-close) were moved by a UTC
    reading of their New York crons; this is the fix."""
    if not tz_name or tz_name == "UTC":
        return 0
    try:
        from zoneinfo import ZoneInfo
        when = when or datetime.now(timezone.utc)
        return -int(when.astimezone(ZoneInfo(tz_name)).utcoffset().total_seconds() // 60)
    except Exception:  # noqa: BLE001
        return 0


def utc_minute(expr, tz_name):
    m = minute_of(expr)
    return None if m is None else (m + tz_offset_min(tz_name)) % 1440


def with_minute(expr, minute):
    m = CRON.match(expr)
    return "cron(%d %d %s %s %s %s)" % (minute % 60, (minute // 60) % 24, m.group(3), m.group(4), m.group(5), m.group(6))


def hhmm(minute):
    return "%02d:%02d" % ((minute // 60) % 24, minute % 60)


def hm_minute(text):
    h, m = text.split(":")
    return int(h) * 60 + int(m)


def window_of(minute):
    m = minute % 1440
    for name, a, b in WINDOWS:
        if a <= m < b or (b > 1440 and m < b - 1440):
            return name, a, b
    return WINDOWS[0]


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else None


def base_tick(name):
    return re.sub(r"-late\d*$", "", name)


def runtimes(repo):
    """{fn: (mean duration s or None, timeout s or None)} from the ops 6546 account snapshot (Duration / Invocations
    over its window; the configured timeout is the hard bound of one run)."""
    path = repo / "aws/ops/reports/latest/ops_6546_cost_audit_data.md"
    if not path.exists():
        return {}
    t = path.read_text(encoding="utf-8")
    b = "".join(re.findall(r"^[A-Za-z0-9+/=]+$", t, re.M))
    d = json.loads(gzip.decompress(base64.b64decode(b)))
    daily, cfg = d.get("lambda_daily") or {}, d.get("lambda") or {}
    out = {}
    for fn, c in cfg.items():
        out[fn] = (None, c.get("timeout"))
    for key, days in daily.items():
        fn, metric = key.rsplit("|", 1)
        if metric != "Duration":
            continue
        inv = sum((daily.get(fn + "|Invocations") or {}).values())
        if inv:
            out[fn] = (sum(days.values()) / inv / 1000.0, out.get(fn, (None, None))[1])
    return out


def margin_for(fn, rt, cap_min):
    """Minutes a reader waits after its writer's start: twice the writer's mean run (never more than its timeout; the
    timeout itself when no run is on record) plus 3 min, clamped 5..cap_min."""
    mean_s, timeout_s = rt.get(fn, (None, None))
    run_s = min(2 * mean_s, timeout_s or 2 * mean_s) if mean_s is not None else (timeout_s or 60.0)
    return int(min(cap_min, max(5, round((run_s + 180) / 60.0))))


def bucket(g, mg, stale_min):
    if g < mg:
        return "race"
    if g >= 1440 - stale_min:
        return "stale"
    return "<1h" if g < 60 else "1-6h" if g < 360 else "6-12h" if g < 720 else "12-22h"


def scc(nodes, inputs):
    """Tarjan over reader<-writer edges; returns {node: component id}."""
    sys.setrecursionlimit(100000)
    idx, low, on, st, comp, counter, cid = {}, {}, set(), [], {}, [0], [0]

    def strong(v):
        idx[v] = low[v] = counter[0]
        counter[0] += 1
        st.append(v)
        on.add(v)
        for w in inputs.get(v, ()):
            if w not in idx:
                strong(w)
                low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], idx[w])
        if low[v] == idx[v]:
            while True:
                w = st.pop()
                on.discard(w)
                comp[w] = cid[0]
                if w == v:
                    break
            cid[0] += 1
    for v in sorted(nodes):
        if v not in idx:
            strong(v)
    return comp


def load_optional(repo, name):
    p = repo / "config" / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def build(repo, margin_cap=30, stale_min=60, grace_min=30, max_move=60, pass_no=1, provider_max_move=None, only_lane=False):
    sched = json.loads((repo / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((repo / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    floor = json.loads((repo / "config/cadence-floor.json").read_text(encoding="utf-8"))
    man = json.loads((repo / "engine-manifest.json").read_text(encoding="utf-8"))
    exempt_names = {x["name"] for x in floor.get("exempt_triggers", [])}
    rt = runtimes(repo)
    pub = load_optional(repo, "provider-publication.json") if pass_no >= 2 and not only_lane else None
    lane_cfg = load_optional(repo, "intraday-lane.json") if pass_no >= 2 else None
    pullers = pm.build(repo) if pub else {}
    slot_max = (pub or {}).get("slot_max") or {}
    prov_mg = int((pub or {}).get("margin_min", 30))
    prov_move = provider_max_move if provider_max_move is not None else int((pub or {}).get("max_move_min", 300))
    step = int((pub or {}).get("tick_step_min", LATE_STEP_MIN))
    lane_tick = (lane_cfg or {}).get("tick")
    # --- triggers and firings ------------------------------------------------------------------------------------
    triggers, fires, tick_time, tick_expr = [], defaultdict(list), {}, {}
    for sec, kind in (("rules", "rule"), ("schedules", "sched")):
        for e in sched.get(sec, []):
            if e.get("state") != "ENABLED":
                continue
            minute = utc_minute(e.get("expr"), e.get("timezone"))          # the plan thinks in UTC; the cron stays in its own zone
            tz_off = tz_offset_min(e.get("timezone"))
            targets = e.get("targets") or []
            fns = sorted({fn_of(t.get("arn")) for t in targets if fn_of(t.get("arn"))})
            if SCHEDULER_FN in fns:
                for t in targets:
                    m = re.search(r'"tick":\s*"([^"]+)"', t.get("input") or "")
                    if m and minute is not None:
                        tick_time[m.group(1)] = minute
                        tick_expr[m.group(1)] = (kind, e["name"], e["expr"])
                continue
            if not fns:
                continue
            pinned = minute is None or e["name"] in exempt_names
            idx = len(triggers)
            triggers.append({"kind": kind, "name": e["name"], "bus": e.get("bus"), "group": e.get("group"), "expr": e["expr"], "state": e["state"],
                             "timezone": e.get("timezone") or "UTC", "tz_off": tz_off,
                             "input": "".join(t.get("input") or "" for t in targets), "functions": fns, "minute": minute, "orig": minute,
                             "pinned": pinned, "why_pinned": "exempt" if e["name"] in exempt_names else ("not a daily cron" if minute is None else "")})
            if minute is not None:
                for fn in fns:
                    fires[fn].append([minute, idx])
    tick_of = {}
    for tick, members in fan.get("ticks", {}).items():
        if tick == lane_tick:
            continue                                                 # the intraday lane is extra freshness, not an ordering edge
        if tick in tick_time:
            for fn in members:
                fires[fn].append([tick_time[tick], "tick:" + tick])
                tick_of[fn] = tick
    # --- dependency edges, feedback (SCC) removed ---------------------------------------------------------------
    writers, reads = defaultdict(set), defaultdict(set)
    for e in man["engines"]:
        for k in e["keys"]:
            writers[k].add(e["engine"])
        for k in e.get("reads", []):
            reads[e["engine"]].add(k)
    inputs = defaultdict(set)
    for r, ks in reads.items():
        for k in ks:
            for w in writers.get(k, ()):
                if w != r and w in fires and r in fires:
                    inputs[r].add(w)
    comp = scc(set(inputs) | {w for ws in inputs.values() for w in ws}, inputs)
    feedback = 0
    for r in list(inputs):
        keep = {w for w in inputs[r] if comp.get(w) != comp.get(r)}
        feedback += len(inputs[r]) - len(keep)
        inputs[r] = keep
    mg_of = {w: margin_for(w, rt, margin_cap) for ws in inputs.values() for w in ws}
    # --- providers as pinned writers (pass >= 2) ----------------------------------------------------------------
    provs_of = {}                                                    # fn -> data providers it pulls (for caps and alignment)
    events = {}
    if pub:
        for p, evs in (pub.get("events") or {}).items():
            events[p] = sorted(hm_minute(x["utc"]) for x in evs)
        for fn, pl in pullers.items():
            dp = [p for p in pm.data_providers(pl) if p in slot_max or p in events]
            if dp and fn in fires:
                provs_of[fn] = dp
                for p in dp:
                    if p in events:
                        node = PROVIDER + p
                        if node not in fires:
                            fires[node] = [[m, "provider"] for m in events[p]]
                            mg_of[node] = prov_mg
                        inputs[fn].add(node)

    def is_prov(w):
        return w.startswith(PROVIDER)

    def edge_stats():
        c, ages = Counter(), []
        for r, ws in inputs.items():
            for w in ws:
                if is_prov(w):
                    continue
                for rm, _ in fires[r]:
                    g = min(((rm - wm) % 1440) for wm, _ in fires[w])
                    c[bucket(g, mg_of[w], stale_min)] += 1
                    ages.append(g)
        ages.sort()
        return {"buckets": dict(c), "edges": len(ages), "mean_input_age_h": round(sum(ages) / 60.0 / len(ages), 2) if ages else None,
                "median_input_age_h": round(ages[len(ages) // 2] / 60.0, 2) if ages else None}

    def edge_buckets():
        out = {}
        for r, ws in inputs.items():
            for w in ws:
                if is_prov(w):
                    continue
                for pos, (rm, _) in enumerate(fires[r]):
                    g = min(((rm - wm) % 1440) for wm, _ in fires[w])
                    out[(r, w, pos)] = (bucket(g, mg_of[w], stale_min), g)
        return out

    def axis(minute, wname, wa):
        return minute + 1440 if wname == "post-close" and minute < wa else minute

    def latest_print_in_window(prov_node, minute_now):
        """The latest publication of a provider inside the window of a puller firing at `minute_now` (on that
        window's axis), or None when no print falls inside it."""
        wname, wa, wb = window_of(minute_now)
        ev = [axis(m, wname, wa) for m, _ in fires[prov_node]]
        inwin = [e for e in ev if wa <= e < wb + grace_min - mg_of[prov_node]]
        return max(inwin) if inwin else None

    def prov_stats():
        """Per provider: pullers that fire after the latest print inside their window (the window of their original
        time) vs before it."""
        out = {}
        for p in events:
            node = PROVIDER + p
            fresh = late = 0
            for fn, ws in inputs.items():
                if node not in ws:
                    continue
                for pos, (rm, _) in enumerate(fires[fn]):
                    om = orig_of.get(fn, [rm])[pos] if pos < len(orig_of.get(fn, [])) else rm
                    lp = latest_print_in_window(node, om)
                    if lp is None:
                        continue
                    wname, wa, _ = window_of(om)
                    (fresh, late) = (fresh + 1, late) if axis(rm, wname, wa) >= lp + mg_of[node] else (fresh, late + 1)
            out[p] = {"after_the_print": fresh, "before_it": late}
        return out

    def burst_stats():
        """Per provider: the busiest minute (pullers starting together)."""
        per = defaultdict(Counter)
        for fn, dp in provs_of.items():
            for rm, _ in fires[fn]:
                for p in dp:
                    per[p][rm % 1440] += 1
        return {p: {"max_per_minute": max(c.values()), "minutes_over_cap": sum(1 for v in c.values() if v > slot_max.get(p, SLOT_MAX))}
                for p, c in per.items() if c}

    orig_of = {fn: [m for m, _ in fires[fn]] for fn in list(fires) if not is_prov(fn)}   # the original minutes (window of record)
    before, baseline = edge_stats(), edge_buckets()           # the original timetable, the yardstick for regressions
    prov_before, burst_before = prov_stats(), burst_stats()
    # --- ticks: dependency levels and provider packing into late ticks --------------------------------------------
    late_ticks, tick_moves = {}, {}
    groups = defaultdict(list)                                     # base tick -> members (its late ticks merged back)
    for tick, members in fan.get("ticks", {}).items():
        if tick in tick_time and tick != lane_tick:
            groups[base_tick(tick)] += list(members)
    for base, members in sorted(groups.items()):
        if base not in tick_time:
            continue
        ms = set(members)
        dep = {r: {w for w in inputs.get(r, ()) if w in ms} for r in members}
        dep = {r: ws for r, ws in dep.items() if ws}
        caps = {p: slot_max[p] for p in slot_max} if pub else {}
        packed = pack_tick(members, dep, provs_of, caps)
        subs = {}
        for fn, sub in sorted(packed.items()):
            name = base if sub == 0 else "%s-late%s" % (base, "" if sub == 1 else str(sub))
            subs.setdefault(name, []).append(fn)
            if tick_of.get(fn) != name:
                tick_moves[fn] = {"from": tick_of.get(fn), "to": name, "reads_in_tick": sorted(dep.get(fn, ())), "minute": (tick_time[base] + sub * step) % 1440}
                for f in fires[fn]:
                    if f[1] == "tick:" + tick_of.get(fn, ""):
                        f[0], f[1] = (tick_time[base] + sub * step) % 1440, "tick:" + name
        for name, fns in subs.items():
            if name == base:
                continue
            sub = int(re.search(r"-late(\d*)$", name).group(1) or 1)
            minute = (tick_time[base] + sub * step) % 1440
            late_ticks[name] = {"from_tick": base, "minute": minute, "kind": "sched", "group": "default", "name": "jhk-tick-" + name,
                                "expr": "cron(%d %d * * ? *)" % (minute % 60, minute // 60), "members": sorted(fns),
                                "exists": name in tick_time, "current_expr": tick_expr.get(name, (None, None, None))[2]}
    # --- the solver: readers move later; a writer whose readers cannot follow stays put ---------------------------
    def trig_provs(idx):
        return {p for fn in triggers[idx]["functions"] for p in provs_of.get(fn, ())}

    frozen, freeze_why, unresolved = set(), {}, []
    for _iteration in range(30):
        for idx, t in enumerate(triggers):
            t["minute"] = t["orig"]
            for fn in t["functions"]:
                for f in fires[fn]:
                    if f[1] == idx:
                        f[0] = t["orig"]
        slots = Counter(t["minute"] for t in triggers if t["minute"] is not None)
        pslots = Counter()
        for idx, t in enumerate(triggers):
            if t["minute"] is not None:
                for p in trig_provs(idx):
                    pslots[(p, t["minute"])] += 1
        for fn, dp in provs_of.items():                              # tick members occupy their minute too (pinned)
            for m, src in fires[fn]:
                if isinstance(src, str) and src.startswith("tick:"):
                    for p in dp:
                        pslots[(p, m)] += 1
        for tick, m in tick_time.items():                        # nothing new lands on a fan-out tick's minute
            if tick != lane_tick:
                slots[m] = max(slots[m], SLOT_MAX)
        capped, drivers, aligned = {}, {}, set()

        def room(idx, minute):
            m = minute % 1440
            return slots[m] < SLOT_MAX and all(pslots[(p, m)] < slot_max[p] for p in trig_provs(idx) if p in slot_max)

        def move(idx, target, why):
            t = triggers[idx]
            while not room(idx, target):
                target += 1
            slots[t["minute"]] -= 1
            for p in trig_provs(idx):
                pslots[(p, t["minute"])] -= 1
            t["minute"] = target % 1440
            slots[t["minute"]] += 1
            for p in trig_provs(idx):
                pslots[(p, t["minute"])] += 1
            for fn in t["functions"]:
                for f in fires[fn]:
                    if f[1] == idx:
                        f[0] = t["minute"]
            drivers[t["name"]] = sorted(set(why) | set(drivers.get(t["name"], [])))

        for _round in range(80):
            changed = 0
            for idx, t in enumerate(triggers):
                if t["pinned"] or idx in frozen:
                    continue
                wname, wa, wb = window_of(t["orig"])
                cur, origin = axis(t["minute"], wname, wa), axis(t["orig"], wname, wa)
                need, blocked = [], []
                for fn in t["functions"]:
                    for w in inputs.get(fn, ()):
                        mg = mg_of[w]
                        if is_prov(w):
                            lp = latest_print_in_window(w, t["orig"])
                            if lp is None or lp + mg <= cur:
                                continue
                            want = lp + mg
                            if want < wb + grace_min and want - origin <= prov_move:
                                need.append((want, w, max(1, (t["minute"] - lp) % 1440), "align"))
                            else:
                                blocked.append((w, (t["minute"] - lp) % 1440, want))
                            continue
                        for wm, _src in fires[w]:
                            g = (t["minute"] - wm) % 1440
                            b = bucket(g, mg, stale_min)
                            if b not in ("race", "stale"):
                                continue
                            want = axis(wm + mg, wname, wa)
                            if want <= cur:
                                want += 1440                        # the writer sits "behind" on this axis: a full lap
                            if b == "race":
                                need.append((want, w, g, "race"))
                            elif want < wb + grace_min and want - origin <= max_move:
                                need.append((want, w, g, "stale"))
                            else:
                                blocked.append((w, g, want))
                if blocked:
                    capped[t["name"]] = {"window": wname, "left_alone": [{"writer": w, "gap_min": g, "would_need": hhmm(want)} for w, g, want in blocked]}
                if not need:
                    continue
                target = max(x[0] for x in need)
                if any(k == "align" for *_, k in need):
                    aligned.add(idx)
                move(idx, target, [(w, g) for _, w, g, _k in need])
                changed += 1
            # bursts: more than slot_max pullers of one provider on the same minute -> the excess moves to the next free minute
            for (p, m), n in sorted(pslots.items()):
                if p not in slot_max or n <= slot_max[p]:
                    continue
                here = sorted(i for i, t in enumerate(triggers) if t["minute"] == m and p in trig_provs(i) and not t["pinned"] and i not in frozen)
                fixed = n - len(here)                              # pinned / frozen pullers that stay on this minute
                for idx in here[max(0, slot_max[p] - fixed):]:
                    t = triggers[idx]
                    wname, wa, wb = window_of(t["orig"])
                    if axis(t["minute"], wname, wa) + 1 >= wb + grace_min or (t["minute"] - t["orig"]) % 1440 >= 30:
                        continue
                    move(idx, axis(t["minute"], wname, wa) + 1, [(PROVIDER + p, 0)])
                    changed += 1
                    if pslots[(p, m)] <= slot_max[p]:
                        break
            if not changed:
                break
        # regressions: a race where there was none, or an input that was fresh (< 12 h old) and now reads yesterday
        now = edge_buckets()
        unresolved, new_freeze = [], set()
        for key, (b, g) in now.items():
            b0, g0 = baseline[key]
            if (b == "race" and b0 != "race") or (b == "stale" and g0 < 720):
                r, w, pos = key
                movers = sorted({f[1] for f in fires[w] if isinstance(f[1], int) and triggers[f[1]]["minute"] != triggers[f[1]]["orig"]})
                if b == "stale" and movers and all(m in aligned for m in movers):
                    continue                                       # the puller caught today's print: same data for this reader
                culprit = None
                for m in movers:
                    if m in frozen or any(g2 < mg_of[w2] for w2, g2 in drivers.get(triggers[m]["name"], []) if w2 in mg_of):
                        continue                                   # a race fix is not undone for a stale reader
                    culprit = m
                if culprit is not None:
                    new_freeze.add(culprit)
                    freeze_why[culprit] = "its reader %s (%s) could not follow" % (r, fires[r][pos][1] if not isinstance(fires[r][pos][1], int) else triggers[fires[r][pos][1]]["name"])
                else:
                    unresolved.append({"reader": r, "writer": w, "was": b0, "now": b, "gap_before_min": g0, "gap_after_min": g})
        if not new_freeze:
            break
        frozen |= new_freeze
    after, prov_after, burst_after = edge_stats(), prov_stats(), burst_stats()
    # --- actions in the cadence_apply format ---------------------------------------------------------------------
    actions = []
    for idx, t in enumerate(triggers):
        if t["minute"] is None or t["minute"] == t["orig"]:
            continue
        wname, wa, wb = window_of(t["orig"])
        why = drivers.get(t["name"], [])
        kinds = sorted({("print" if is_prov(w) and g else "burst" if is_prov(w) else "order") for w, g in why})
        actions.append({"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"), "functions": t["functions"], "op": "retime",
                        "before": {"expr": t["expr"], "state": t["state"], "input_sha": hashlib.sha256(t["input"].encode()).hexdigest()[:16]},
                        "after": {"expr": with_minute(t["expr"], (t["minute"] - t.get("tz_off", 0)) % 1440), "state": t["state"]},
                        "timezone": t.get("timezone", "UTC"),
                        "move_min": (t["minute"] - t["orig"]) % 1440, "window": wname, "crosses_window_end": axis(t["minute"], wname, wa) >= wb,
                        "drivers": [{"writer": w, "gap_before_min": g} for w, g in why], "why": kinds,
                        "reason": "pipeline order: %s -> %s UTC%s (%s), %s" % (
                            hhmm(t["orig"]), hhmm(t["minute"]), (" [%s cron]" % t["timezone"]) if t.get("tz_off") else "", wname,
                            "after " + ", ".join(w.replace(PROVIDER, "") + ("'s print" if is_prov(w) and g else " (burst)" if is_prov(w) else "") for w, g in why) if why else "fires after its writers")})
    for name, lt in late_ticks.items():                           # an existing late tick whose minute changed is a retime
        if lt["exists"] and lt["current_expr"] and lt["current_expr"] != lt["expr"]:
            kind, tname, cur_expr = tick_expr[name]
            actions.append({"kind": kind, "name": tname, "bus": "default" if kind == "rule" else None, "group": "default" if kind == "sched" else None,
                            "functions": [SCHEDULER_FN], "op": "retime", "before": {"expr": cur_expr, "state": "ENABLED", "input_sha": ""},
                            "after": {"expr": lt["expr"], "state": "ENABLED"}, "move_min": (lt["minute"] - minute_of(cur_expr)) % 1440,
                            "window": window_of(lt["minute"])[0], "crosses_window_end": False, "drivers": [], "why": ["tick"],
                            "reason": "late tick %s re-packed: %s -> %s UTC" % (name, hhmm(minute_of(cur_expr)), hhmm(lt["minute"]))})
    actions.sort(key=lambda a: (a["kind"], a["name"]))
    # --- the intraday lane ----------------------------------------------------------------------------------------
    lane = None
    if lane_cfg:
        members = sorted(lane_cfg["members"])
        known = {e["engine"] for e in man["engines"]}
        unknown = [m for m in members if m not in known]
        elsewhere = {m: tick_of[m] for m in members if m in tick_of}
        if unknown or elsewhere:
            raise ValueError("intraday lane: unknown engines %s; members of another tick %s" % (unknown, elsewhere))
        present = sorted(fan.get("ticks", {}).get(lane_cfg["tick"], []))
        lane = dict(lane_cfg, members=members, already_in_manifest=present == members,
                    schedule_in_manifest=any(e["name"] == lane_cfg["schedule"]["name"] for e in sched.get("schedules", [])),
                    exempt_in_floor=lane_cfg["schedule"]["name"] in exempt_names)
    moves = [a["move_min"] for a in actions]
    summary = {"pass": pass_no, "triggers": len(triggers), "movable": sum(1 for t in triggers if not t["pinned"]), "pinned": sum(1 for t in triggers if t["pinned"]),
               "tick_members": sum(len(v) for k, v in fan.get("ticks", {}).items() if k in tick_time and k != lane_tick),
               "edges": before["edges"], "feedback_edges_ignored": feedback, "moved_triggers": len(actions),
               "moves_by_why": dict(Counter(k for a in actions for k in a["why"])),
               "moved_tick_members": len(tick_moves), "late_ticks": len(late_ticks), "new_ticks": sum(1 for lt in late_ticks.values() if not lt["exists"]),
               "left_alone_triggers": len(capped), "left_alone_edges": sum(len(v["left_alone"]) for v in capped.values()),
               "moves_crossing_window_end": sum(1 for a in actions if a["crosses_window_end"]),
               "writers_kept_in_place": len(frozen), "unresolved_regressions": len(unresolved),
               "move_minutes": {"max": max(moves) if moves else 0, "mean": round(sum(moves) / len(moves), 1) if moves else 0,
                                "<=15": sum(1 for m in moves if m <= 15), "16-60": sum(1 for m in moves if 15 < m <= 60), ">60": sum(1 for m in moves if m > 60)},
               "moves_by_window": dict(Counter(a["window"] for a in actions)),
               "before": before, "after": after, "providers_before": prov_before, "providers_after": prov_after,
               "bursts_before": burst_before, "bursts_after": burst_after,
               "intraday_lane_members": len(lane["members"]) if lane else 0,
               "rules": {"stale_min": stale_min, "grace_min": grace_min, "max_move_min": max_move, "provider_max_move_min": prov_move if pub else None,
                         "provider_margin_min": prov_mg if pub else None, "margin_cap_min": margin_cap, "slot_max": SLOT_MAX, "provider_slot_max": slot_max,
                         "late_step_min": step, "only_lane": bool(only_lane)},
               "margin_source": "aws/ops/reports/latest/ops_6546_cost_audit_data.md lambda_daily Duration/Invocations + lambda timeout (%d functions known)" % len(rt)}
    plan = {"version": "pipeline-order.v2" if pass_no >= 2 else "pipeline-order.v1", "pass": pass_no,
            "rule": "readers fire after their writers: no reader inside a writer's margin, none within %d min before it%s" % (
                stale_min, "; pullers after their provider's print, no provider burst over its slot cap" if pub else ""),
            "summary": summary, "actions": actions, "late_ticks": late_ticks, "tick_moves": tick_moves, "left_alone": capped,
            "kept_in_place": {triggers[i]["name"]: freeze_why[i] for i in sorted(frozen)}, "unresolved_regressions": unresolved,
            "intraday_lane": lane, "margins": {w: mg_of[w] for w in sorted(mg_of)}, "fanout_removals": {}}
    return plan, {"triggers": triggers}


def pack_tick(members, dep, provs_of, caps):
    """Sub-tick (0 = the tick itself, 1 = -late, 2 = -late2 ...) for every member: a reader lands after the sub-tick
    of every in-tick writer it reads; no sub-tick holds more than caps[p] pullers of provider p (writers of in-tick
    readers are placed first, then the readers, then the rest)."""
    readers_of = defaultdict(set)
    for r, ws in dep.items():
        for w in ws:
            readers_of[w].add(r)
    level = {m: 0 for m in members}
    for _ in range(6):
        changed = False
        for r, ws in dep.items():
            want = max(level[w] for w in ws) + 1
            if want > level[r] and want < 5:
                level[r], changed = want, True
        if not changed:
            break
    order = sorted(members, key=lambda m: (level[m], 0 if readers_of.get(m) else 1, -len(readers_of.get(m, ())), m))
    used = defaultdict(Counter)                                    # sub -> provider counts
    out = {}
    for m in order:
        lo = max([out[w] for w in dep.get(m, ()) if w in out] + [-1]) + 1
        sub = lo
        while any(used[sub][p] >= caps[p] for p in provs_of.get(m, ()) if p in caps):
            sub += 1
        out[m] = sub
        for p in provs_of.get(m, ()):
            used[sub][p] += 1
    return out


def final_state(sched, actions):
    final = {}
    for sec, kind in (("rules", "rule"), ("schedules", "sched")):
        for e in sched.get(sec, []):
            final[(kind, e["name"])] = (e.get("expr") or "", e.get("state"))
    for a in actions:
        final[(a["kind"], a["name"])] = (a["after"]["expr"], a["after"]["state"])
    return final


def late_tick_entries(sched, plan):
    """The new jhk-tick-* entries for the manifest's `schedules` list: EventBridge Scheduler schedules (the classic
    rule bus is at its cap), one target - justhodl-scheduler with {"tick": "<name>"} - like the jhk-tick-* rules."""
    tick = next(e for e in sched["rules"] if e["name"] == "jhk-tick-daily-morn")
    out = []
    for name, lt in sorted(plan["late_ticks"].items()):
        out.append({"expr": lt["expr"], "group": lt["group"], "kind": "scheduler", "name": lt["name"], "state": "ENABLED", "timezone": "UTC",
                    "targets": [{"arn": tick["targets"][0]["arn"], "input": json.dumps({"tick": name}), "path": None}]})
    lane = plan.get("intraday_lane")
    if lane:
        s = lane["schedule"]
        out.append({"expr": s["expr"], "group": s["group"], "kind": "scheduler", "name": s["name"], "state": "ENABLED", "timezone": s["timezone"],
                    "targets": [{"arn": tick["targets"][0]["arn"], "input": json.dumps({"tick": lane["tick"]}), "path": None}]})
    return out


def apply_fanout(fan, plan):
    """Tick moves and the intraday lane in the fan-out manifest (idempotent).  Returns the number of changes."""
    changed = 0
    for fn, mv in sorted(plan["tick_moves"].items()):
        src = fan["ticks"].get(mv["from"], []) if mv["from"] else []
        dst = fan["ticks"].setdefault(mv["to"], [])
        if fn in src:
            fan["ticks"][mv["from"]] = [x for x in src if x != fn]
            changed += 1
        if fn not in dst:
            dst.append(fn)
            changed += 0 if fn in src else 1
        if fn in fan.get("assignment", {}):
            fan["assignment"][fn]["ticks"] = [mv["to"]]
    lane = plan.get("intraday_lane")
    if lane and sorted(fan["ticks"].get(lane["tick"], [])) != sorted(lane["members"]):
        fan["ticks"][lane["tick"]] = list(lane["members"])
        for fn in lane["members"]:
            rec = fan.setdefault("assignment", {}).setdefault(fn, {"fired_hours": [], "per_day": 0.0, "ticks": []})
            rec["ticks"] = [lane["tick"]]
        fan["intraday_lane_2026_10_21"] = {"why": lane["why"], "schedule": lane["schedule"], "members": list(lane["members"])}
        changed += 1
    if changed and plan["tick_moves"]:
        fan["pipeline_order_2026_10_20"] = {
            "why": "Khalid, 2026-10-20/21: readers fire after their writers; a member of a tick that reads files written by other "
                   "members of the same tick (fired in parallel) runs from a late twin a quarter hour later, and a tick never starts "
                   "more than the provider slot cap of pullers of one provider (config/provider-publication.json).",
            "late_ticks": {k: {"schedule": v["name"], "expr": v["expr"], "from_tick": v["from_tick"]} for k, v in plan["late_ticks"].items()},
            "moved": {fn: mv["to"] for fn, mv in sorted(plan["tick_moves"].items())}}
    return changed


def apply_floor(floor, plan):
    lane = plan.get("intraday_lane")
    if not lane:
        return 0
    s = lane["schedule"]
    if any(e.get("name") == s["name"] for e in floor.get("exempt_triggers", [])):
        return 0
    floor.setdefault("exempt_triggers", []).append({"kind": s["kind"], "name": s["name"], "why": lane["cadence_floor_reason"]})
    return 1


def apply_repo(plan, repo):
    """Repo edits for the apply: engine config crons + schedule-manifest (cadence_plan.apply_repo), the new late-tick
    and lane schedule entries, the fan-out manifest's tick moves and lane, the cadence-floor exemption."""
    configs, decl = cp.load_configs(repo)
    sched_path = repo / "config/schedule-manifest.json"
    sched = json.loads(sched_path.read_text(encoding="utf-8"))
    p = dict(plan, final=final_state(sched, plan["actions"]), exempt=set(), decl=decl, configs=configs, depth={}, tick_removals={})
    stats, notes = cp.apply_repo(p, repo)
    sched = json.loads(sched_path.read_text(encoding="utf-8"))
    have = {e["name"] for e in sched["schedules"]}
    new = [e for e in late_tick_entries(sched, plan) if e["name"] not in have]
    if new:
        sched["schedules"] = sorted(sched["schedules"] + new, key=lambda e: e["name"])
        sched_path.write_text(json.dumps(sched, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    stats["schedule_entries_added"] = len(new)
    fpath = repo / "config/fanout-manifest.json"
    raw = fpath.read_text(encoding="utf-8")
    fan = json.loads(raw)
    if json.dumps(fan, indent=1, ensure_ascii=False) + "\n" != raw:
        raise AssertionError("fanout-manifest.json no longer round-trips; edit it by hand")
    changed = apply_fanout(fan, plan)
    if changed:
        fpath.write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    stats["fanout_changes"] = changed
    floor_path = repo / "config/cadence-floor.json"
    floor = json.loads(floor_path.read_text(encoding="utf-8"))
    if apply_floor(floor, plan):
        floor_path.write_text(json.dumps(floor, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        stats["floor_exemptions_added"] = 1
    return stats, notes


def plan_paths(repo):
    out = [repo / "config/pipeline-order-plan.json"]
    out += sorted((repo / "config").glob("pipeline-order-plan-*.json"), key=lambda p: int(re.search(r"-(\d+)\.json$", p.name).group(1)))
    return [p for p in out if p.exists()]


def check_applied(plans, repo):
    """After --apply-repo the repo declares the applied state of every applied plan in order: each action's trigger
    at the `after` expression of the LAST applied plan that moved it, late ticks present, moved members in their
    ticks, the lane in place."""
    sched = json.loads((repo / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((repo / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    floor = json.loads((repo / "config/cadence-floor.json").read_text(encoding="utf-8"))
    have = {(k, e["name"]): (e.get("expr"), e.get("state")) for sec, k in (("rules", "rule"), ("schedules", "sched")) for e in sched.get(sec, [])}
    expected, bad = {}, []
    for plan in plans:
        for a in plan["actions"]:
            expected[(a["kind"], a["name"])] = (a["after"]["expr"], a["after"]["state"])
        for lt in plan["late_ticks"].values():
            expected[(lt["kind"], lt["name"])] = (lt["expr"], "ENABLED")
        lane = plan.get("intraday_lane")
        if lane:
            s = lane["schedule"]
            expected[(s["kind"], s["name"])] = (s["expr"], "ENABLED")
            if sorted(fan["ticks"].get(lane["tick"], [])) != sorted(lane["members"]):
                bad.append("lane members")
            if not any(e.get("name") == s["name"] for e in floor.get("exempt_triggers", [])):
                bad.append("lane exemption")
    bad += ["%s %s" % k for k, v in expected.items() if have.get(k) != v]
    last_tick = {}
    for plan in plans:
        for fn, mv in plan["tick_moves"].items():
            last_tick[fn] = mv["to"]
    bad += [fn for fn, tick in last_tick.items() if fn not in fan["ticks"].get(tick, [])]
    if bad:
        print("applied plan(s) not reflected by the repo:", bad[:10])
        return False
    print("plan(s) applied; the repo declares every planned trigger at its new time")
    return True


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    def flag(name, default=None):
        return argv[argv.index(name) + 1] if name in argv else default
    paths = plan_paths(ROOT)
    plans = [json.loads(p.read_text(encoding="utf-8")) for p in paths]
    applied = [p for p in plans if p.get("applied")]
    pending = [(path, p) for path, p in zip(paths, plans) if not p.get("applied")]
    if "--check" in argv:
        ok = check_applied(applied, ROOT) if applied else True
        for path, p in pending:
            rules = (p.get("summary") or {}).get("rules") or {}
            rebuilt, _ = build(ROOT, rules.get("margin_cap_min", 30), rules.get("stale_min", 60), rules.get("grace_min", 30), rules.get("max_move_min", 60),
                               p.get("pass", 1), rules.get("provider_max_move_min"), rules.get("only_lane", False))
            same = rebuilt["actions"] == p["actions"] and rebuilt["late_ticks"] == p["late_ticks"] and rebuilt.get("intraday_lane") == p.get("intraday_lane")
            print("%s: %s" % (path.name, "matches the configs" if same else "DIFFERS from the configs - rebuild it"))
            ok = ok and same
        return 0 if ok else 1
    pass_no = int(flag("--pass", max([p.get("pass", 1) for p in plans] + [0]) + (0 if pending else 1)))
    out = ROOT / ("config/pipeline-order-plan.json" if pass_no == 1 else "config/pipeline-order-plan-%d.json" % pass_no)
    committed = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    if "--apply-repo" in argv:
        if not committed or committed.get("applied"):
            print("nothing to apply: %s %s" % (out.name, "already applied" if committed else "missing"))
            return 1
        stats, notes = apply_repo(committed, ROOT)
        committed["applied"] = {"repo_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "edits": stats}
        out.write_text(json.dumps(committed, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        print("repo edits:", json.dumps(stats))
        for n in notes[:40]:
            print("note:", n)
        return 0
    rules = (committed.get("summary") or {}).get("rules") or {}

    def opt(name, key, default):
        return int(argv[argv.index(name) + 1]) if name in argv else rules.get(key, default)
    plan, _ = build(ROOT, opt("--margin-cap", "margin_cap_min", 30), opt("--stale-min", "stale_min", 60), opt("--grace-min", "grace_min", 30),
                    opt("--max-move", "max_move_min", 60), pass_no, opt("--provider-max-move", "provider_max_move_min", None),
                    "--only-lane" in argv or rules.get("only_lane", False))
    print(json.dumps({k: v for k, v in plan["summary"].items()}, indent=1))
    if "--write" in argv:
        if committed.get("applied"):
            print("refusing to overwrite an applied plan: %s" % out.name)
            return 1
        out.write_text(json.dumps(plan, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        print("written:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
