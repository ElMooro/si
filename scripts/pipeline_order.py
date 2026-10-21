#!/usr/bin/env python3
"""scripts/pipeline_order.py - the day as a pipeline: readers fire after their writers (Khalid, 2026-10-20).

At once a day (the cadence floor, ops 6554), WHEN an engine fires relative to the engines whose files it reads decides
whether a page shows today's numbers or yesterday's.  Two things are wrong in the current timetable:

  race   a reader fires inside its writer's running time (0..margin minutes after it): it reads the previous file or a
         half-written one, and which one changes from day to day
  stale  a reader fires in the hour BEFORE its writer (--stale-min): it consumes a file that is ~24 hours old when
         a shift of minutes would make it minutes old

This planner moves readers LATER - never earlier - with these rules:
  * a trigger moves only inside its own session window (post-close 21:00-01:00 UTC, overnight 01:00-09:00, Europe /
    US pre-market 09:00-14:30, US session 14:30-21:00), plus a grace of --grace-min past the window end: a pre-market
    engine stays a pre-market engine, because its time may also encode an external feed (futures, releases) this map
    cannot see; a stale edge whose fix needs more than that is left alone and listed; a race fix may cross
  * fan-out tick members, cadence-floor exempt triggers and non-daily crons are pinned; members of one tick that read
    other members of the same tick (fired in parallel by justhodl-scheduler) move to a LATE tick of that tick
    (`<tick>-late`: a new jhk-tick-* EventBridge Scheduler schedule a quarter hour later), the only new trigger the
    plan creates
  * edges inside a strongly connected component (A reads B, B reads A) are feedback with a one-day lag by design and
    are not ordering constraints
  * margin(writer) = 2 x its mean duration (never more than its timeout; the timeout when no run is on record) + 3 min,
    clamped 5..--margin-cap min, from the ops 6546 account data
  * no trigger moves more than --max-move minutes (60); a move that would need more is left alone and listed
  * a moved reader is somebody's writer: the loop runs to a fixed point, and a writer whose own readers could not
    follow (pinned, capped) is kept in place rather than made to feed them yesterday's file - no edge gets worse
  * at most four triggers land on one minute

Output: config/pipeline-order-plan.json - cadence_apply actions (kind, name, before, after) + late_ticks + summary.
  python3 scripts/pipeline_order.py --write        build the plan and write it
  python3 scripts/pipeline_order.py --check        exit 1 when the committed plan no longer matches the configs (before
                                                   the apply) or the repo no longer declares the applied state (after)
  python3 scripts/pipeline_order.py --apply-repo   make the repo say what AWS will say after the apply ops
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

CRON = re.compile(r"^cron\((\d{1,2}) (\d{1,2}) (\*|\?) (\*) (\?|\*|MON-FRI|[A-Z,-]+) (\*)\)$")
WINDOWS = [("post-close", 21 * 60, 25 * 60), ("overnight", 1 * 60, 9 * 60), ("pre-market", 9 * 60, 14 * 60 + 30), ("session", 14 * 60 + 30, 21 * 60)]
SLOT_MAX = 4
SCHEDULER_FN = "justhodl-scheduler"
LATE_STEP_MIN = 15


def minute_of(expr):
    m = CRON.match(expr or "")
    return int(m.group(2)) * 60 + int(m.group(1)) if m else None


def with_minute(expr, minute):
    m = CRON.match(expr)
    return "cron(%d %d %s %s %s %s)" % (minute % 60, (minute // 60) % 24, m.group(3), m.group(4), m.group(5), m.group(6))


def hhmm(minute):
    return "%02d:%02d" % ((minute // 60) % 24, minute % 60)


def window_of(minute):
    m = minute % 1440
    for name, a, b in WINDOWS:
        if a <= m < b or (b > 1440 and m < b - 1440):
            return name, a, b
    return WINDOWS[0]


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else None


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


def build(repo, margin_cap=30, stale_min=60, grace_min=30, max_move=60):
    sched = json.loads((repo / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((repo / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    floor = json.loads((repo / "config/cadence-floor.json").read_text(encoding="utf-8"))
    man = json.loads((repo / "engine-manifest.json").read_text(encoding="utf-8"))
    exempt_names = {x["name"] for x in floor.get("exempt_triggers", [])}
    rt = runtimes(repo)
    # --- triggers and firings ------------------------------------------------------------------------------------
    triggers, fires, tick_time = [], defaultdict(list), {}
    for sec, kind in (("rules", "rule"), ("schedules", "sched")):
        for e in sched.get(sec, []):
            if e.get("state") != "ENABLED":
                continue
            minute = minute_of(e.get("expr"))
            targets = e.get("targets") or []
            fns = sorted({fn_of(t.get("arn")) for t in targets if fn_of(t.get("arn"))})
            if SCHEDULER_FN in fns:
                for t in targets:
                    m = re.search(r'"tick":\s*"([^"]+)"', t.get("input") or "")
                    if m and minute is not None:
                        tick_time[m.group(1)] = minute
                continue
            if not fns:
                continue
            pinned = minute is None or e["name"] in exempt_names
            idx = len(triggers)
            triggers.append({"kind": kind, "name": e["name"], "bus": e.get("bus"), "group": e.get("group"), "expr": e["expr"], "state": e["state"],
                             "input": "".join(t.get("input") or "" for t in targets), "functions": fns, "minute": minute, "orig": minute,
                             "pinned": pinned, "why_pinned": "exempt" if e["name"] in exempt_names else ("not a daily cron" if minute is None else "")})
            if minute is not None:
                for fn in fns:
                    fires[fn].append([minute, idx])
    for tick, members in fan.get("ticks", {}).items():
        if tick in tick_time:
            for fn in members:
                fires[fn].append([tick_time[tick], "tick:" + tick])
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

    def edge_stats():
        c, ages = Counter(), []
        for r, ws in inputs.items():
            for w in ws:
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
                for pos, (rm, _) in enumerate(fires[r]):
                    g = min(((rm - wm) % 1440) for wm, _ in fires[w])
                    out[(r, w, pos)] = (bucket(g, mg_of[w], stale_min), g)
        return out

    before, baseline = edge_stats(), edge_buckets()           # the original timetable, the yardstick for regressions
    # --- late ticks: members of one tick reading other members of the same tick -----------------------------------
    late_ticks, tick_moves = {}, {}
    for tick, members in fan.get("ticks", {}).items():
        if tick not in tick_time:
            continue
        ms = set(members)
        dep = {r: {w for w in inputs.get(r, ()) if w in ms} for r in members}
        dep = {r: ws for r, ws in dep.items() if ws}
        if not dep:
            continue
        level = {m: 0 for m in members}
        for _ in range(6):
            changed = False
            for r, ws in dep.items():
                want = max(level[w] for w in ws) + 1
                if want > level[r] and want < 4:
                    level[r], changed = want, True
            if not changed:
                break
        step = max(LATE_STEP_MIN, max(mg_of.get(w, 5) for ws in dep.values() for w in ws))
        step = ((step + 4) // 5) * 5
        for r, lv in sorted(level.items()):
            if lv == 0:
                continue
            name = "%s-late%s" % (tick, "" if lv == 1 else str(lv))
            minute = tick_time[tick] + lv * step
            lt = late_ticks.setdefault(name, {"from_tick": tick, "minute": minute % 1440, "kind": "sched", "group": "default", "name": "jhk-tick-" + name,
                                              "expr": "cron(%d %d * * ? *)" % (minute % 60, (minute // 60) % 24), "members": []})
            lt["members"] = sorted(lt["members"] + [r])
            tick_moves[r] = {"from": tick, "to": name, "reads_in_tick": sorted(dep[r]), "minute": minute % 1440}
            for f in fires[r]:
                if f[1] == "tick:" + tick:
                    f[0], f[1] = minute % 1440, "tick:" + name
    # --- the solver: readers move later; a writer whose readers cannot follow stays put ---------------------------
    def axis(minute, wname, wa):
        return minute + 1440 if wname == "post-close" and minute < wa else minute

    frozen, freeze_why, unresolved = set(), {}, []
    for _iteration in range(30):
        for t in triggers:
            t["minute"] = t["orig"]
        for idx, t in enumerate(triggers):
            for fn in t["functions"]:
                for f in fires[fn]:
                    if f[1] == idx:
                        f[0] = t["orig"]
        slots = Counter(t["minute"] for t in triggers if t["minute"] is not None)
        capped, drivers = {}, {}
        for _round in range(60):
            changed = 0
            for idx, t in enumerate(triggers):
                if t["pinned"] or idx in frozen:
                    continue
                wname, wa, wb = window_of(t["orig"])
                cur, origin = axis(t["minute"], wname, wa), axis(t["orig"], wname, wa)
                need_race, need_stale, blocked = [], [], []
                for fn in t["functions"]:
                    for w in inputs.get(fn, ()):
                        mg = mg_of[w]
                        for wm, _src in fires[w]:
                            g = (t["minute"] - wm) % 1440
                            b = bucket(g, mg, stale_min)
                            if b not in ("race", "stale"):
                                continue
                            want = axis(wm + mg, wname, wa)
                            if want <= cur:
                                want += 1440                        # the writer sits "behind" on this axis: a full lap
                            if b == "race":
                                need_race.append((want, w, g))
                            elif want < wb + grace_min and want - origin <= max_move:
                                need_stale.append((want, w, g))
                            else:
                                blocked.append((w, g, want))
                if blocked:
                    capped[t["name"]] = {"window": wname, "left_alone": [{"writer": w, "gap_min": g, "would_need": hhmm(want)} for w, g, want in blocked]}
                if not need_race and not need_stale:
                    continue
                target = max(x[0] for x in need_race + need_stale)
                if need_race and (target >= wb + grace_min or target - origin > max_move) and \
                        not any(x[0] >= wb + grace_min or x[0] - origin > max_move for x in need_race):
                    target = max(x[0] for x in need_race)          # the stale fixes would leave the window: race only
                    for want, w, g in need_stale:
                        if want > target:
                            capped.setdefault(t["name"], {"window": wname, "left_alone": []})["left_alone"].append({"writer": w, "gap_min": g, "would_need": hhmm(want)})
                while slots[target % 1440] >= SLOT_MAX:
                    target += 1
                slots[t["minute"]] -= 1
                t["minute"] = target % 1440
                slots[t["minute"]] += 1
                for fn in t["functions"]:
                    for f in fires[fn]:
                        if f[1] == idx:
                            f[0] = t["minute"]
                drivers[t["name"]] = sorted({(w, g) for _, w, g in need_race + need_stale} | set(drivers.get(t["name"], [])))
                changed += 1
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
                culprit = None
                for m in movers:
                    if m in frozen or any(g2 < mg_of[w2] for w2, g2 in drivers.get(triggers[m]["name"], [])):
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
    after = edge_stats()
    # --- actions in the cadence_apply format ---------------------------------------------------------------------
    actions = []
    for t in triggers:
        if t["minute"] is None or t["minute"] == t["orig"]:
            continue
        wname, wa, wb = window_of(t["orig"])
        actions.append({"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"), "functions": t["functions"], "op": "retime",
                        "before": {"expr": t["expr"], "state": t["state"], "input_sha": hashlib.sha256(t["input"].encode()).hexdigest()[:16]},
                        "after": {"expr": with_minute(t["expr"], t["minute"]), "state": t["state"]},
                        "move_min": (t["minute"] - t["orig"]) % 1440, "window": wname, "crosses_window_end": axis(t["minute"], wname, wa) >= wb,
                        "drivers": [{"writer": w, "gap_before_min": g} for w, g in drivers.get(t["name"], [])],
                        "reason": "pipeline order: %s -> %s UTC (%s), fires after %s" % (
                            hhmm(t["orig"]), hhmm(t["minute"]), wname, ", ".join(w for w, _ in drivers.get(t["name"], [])) or "its writers")})
    actions.sort(key=lambda a: (a["kind"], a["name"]))
    moves = [a["move_min"] for a in actions]
    summary = {"triggers": len(triggers), "movable": sum(1 for t in triggers if not t["pinned"]), "pinned": sum(1 for t in triggers if t["pinned"]),
               "tick_members": sum(len(v) for k, v in fan.get("ticks", {}).items() if k in tick_time),
               "edges": before["edges"], "feedback_edges_ignored": feedback, "moved_triggers": len(actions),
               "moved_tick_members": len(tick_moves), "new_ticks": len(late_ticks), "left_alone_triggers": len(capped),
               "left_alone_edges": sum(len(v["left_alone"]) for v in capped.values()),
               "moves_crossing_window_end": sum(1 for a in actions if a["crosses_window_end"]),
               "writers_kept_in_place": len(frozen), "unresolved_regressions": len(unresolved),
               "move_minutes": {"max": max(moves) if moves else 0, "mean": round(sum(moves) / len(moves), 1) if moves else 0,
                                "<=15": sum(1 for m in moves if m <= 15), "16-60": sum(1 for m in moves if 15 < m <= 60), ">60": sum(1 for m in moves if m > 60)},
               "moves_by_window": dict(Counter(a["window"] for a in actions)),
               "before": before, "after": after,
               "rules": {"stale_min": stale_min, "grace_min": grace_min, "max_move_min": max_move, "margin_cap_min": margin_cap, "slot_max": SLOT_MAX, "late_step_min": LATE_STEP_MIN},
               "margin_source": "aws/ops/reports/latest/ops_6546_cost_audit_data.md lambda_daily Duration/Invocations + lambda timeout (%d functions known)" % len(rt)}
    plan = {"version": "pipeline-order.v1", "rule": "readers fire after their writers: no reader inside a writer's margin, none within %d min before it" % stale_min,
            "summary": summary, "actions": actions, "late_ticks": late_ticks, "tick_moves": tick_moves, "left_alone": capped,
            "kept_in_place": {triggers[i]["name"]: freeze_why[i] for i in sorted(frozen)}, "unresolved_regressions": unresolved,
            "margins": {w: mg_of[w] for w in sorted(mg_of)}, "fanout_removals": {}}
    return plan, {"triggers": triggers}


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
    return out


def apply_fanout(fan, plan):
    """Move each tick_moves function from its tick to its late tick (idempotent).  Returns the number moved."""
    moved = 0
    for fn, mv in sorted(plan["tick_moves"].items()):
        src = fan["ticks"].get(mv["from"], [])
        dst = fan["ticks"].setdefault(mv["to"], [])
        if fn in src:
            fan["ticks"][mv["from"]] = [x for x in src if x != fn]
            moved += 1
        if fn not in dst:
            dst.append(fn)
        if fn in fan.get("assignment", {}):
            fan["assignment"][fn]["ticks"] = [mv["to"]]
    if moved:
        fan["pipeline_order_2026_10_20"] = {
            "why": "Khalid, 2026-10-20: readers fire after their writers. A member of a tick that reads files written by other "
                   "members of the same tick (fired in parallel) runs from the tick's late twin a quarter hour later.",
            "late_ticks": {k: {"schedule": v["name"], "expr": v["expr"], "from_tick": v["from_tick"]} for k, v in plan["late_ticks"].items()},
            "moved": {fn: mv["to"] for fn, mv in sorted(plan["tick_moves"].items())}}
    return moved


def apply_repo(plan, repo):
    """Repo edits for the apply: engine config crons + schedule-manifest (cadence_plan.apply_repo), the new late-tick
    rule entries in schedule-manifest, and the fanout manifest's tick moves."""
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
    stats["late_tick_schedules_added"] = len(new)
    fpath = repo / "config/fanout-manifest.json"
    raw = fpath.read_text(encoding="utf-8")
    fan = json.loads(raw)
    if json.dumps(fan, indent=1, ensure_ascii=False) + "\n" != raw:
        raise AssertionError("fanout-manifest.json no longer round-trips; edit it by hand")
    moved = apply_fanout(fan, plan)
    if moved:
        fpath.write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    stats["fanout_members_moved"] = moved
    return stats, notes


def check_applied(plan, repo):
    """After --apply-repo the repo declares the applied state: every action's trigger at its `after` expression, the
    late tick schedules present, the moved members in their late ticks."""
    sched = json.loads((repo / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((repo / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    have = {(k, e["name"]): (e.get("expr"), e.get("state")) for sec, k in (("rules", "rule"), ("schedules", "sched")) for e in sched.get(sec, [])}
    bad = [a["name"] for a in plan["actions"] if have.get((a["kind"], a["name"])) != (a["after"]["expr"], a["after"]["state"])]
    bad += [lt["name"] for lt in plan["late_ticks"].values() if have.get((lt["kind"], lt["name"])) != (lt["expr"], "ENABLED")]
    bad += [fn for fn, mv in plan["tick_moves"].items() if fn not in fan["ticks"].get(mv["to"], []) or fn in fan["ticks"].get(mv["from"], [])]
    if bad:
        print("applied plan not reflected by the repo:", bad[:10])
        return False
    print("plan applied; the repo declares every planned trigger at its new time")
    return True


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    out = ROOT / "config/pipeline-order-plan.json"
    committed = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    rules = (committed.get("summary") or {}).get("rules") or {}          # a rerun without flags reproduces the committed plan

    def opt(name, key, default):
        return int(argv[argv.index(name) + 1]) if name in argv else rules.get(key, default)
    if "--apply-repo" in argv:
        stats, notes = apply_repo(committed, ROOT)
        committed["applied"] = {"repo_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "edits": stats}
        out.write_text(json.dumps(committed, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        print("repo edits:", json.dumps(stats))
        for n in notes[:40]:
            print("note:", n)
        return 0
    if "--check" in argv and committed.get("applied"):
        return 0 if check_applied(committed, ROOT) else 1
    plan, _ = build(ROOT, opt("--margin-cap", "margin_cap_min", 30), opt("--stale-min", "stale_min", 60), opt("--grace-min", "grace_min", 30),
                    opt("--max-move", "max_move_min", 60))
    print(json.dumps(plan["summary"], indent=1))
    if "--write" in argv:
        out.write_text(json.dumps(plan, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        print("written:", out)
    if "--check" in argv:
        if committed.get("actions") != plan["actions"] or committed.get("late_ticks") != plan["late_ticks"]:
            print("plan differs from config/pipeline-order-plan.json")
            return 1
        print("plan matches config/pipeline-order-plan.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
