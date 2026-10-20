#!/usr/bin/env python3
"""scripts/cadence_plan.py - plan "every engine runs at most once per 24 hours" (Khalid, 2026-10-20).

Reads a snapshot of every EventBridge rule and Scheduler schedule, the repo's per-engine config.json files, the
fan-out manifest and the static dependency map, and writes:

  config/cadence-plan.json      reviewable list of actions (retime / disable / keep) with before + after, no payloads
  (with --apply-repo)           the matching edits to aws/lambdas/*/config.json, config/schedule-manifest.json and
                                config/fanout-manifest.json, so a later deploy can never put the old cadence back
                                (scripts/check_existing_schedule.py refuses a deploy whose config differs from the
                                live schedule - the repo and AWS must move together)

A "job" is one function + one payload.  Several triggers for the same job (the twin `x-hourly` + `x-daily` rules,
a rule plus a Scheduler schedule, a fan-out tick plus its own rule) run it several times for no benefit: one
survives.  One function with several DIFFERENT payloads (ai-brief-router's five briefing contexts, the SDMX walker's
four agencies) is several jobs; each runs at most once a day.

Run:  python3 scripts/cadence_plan.py --snapshot <triggers.json> [--apply-repo]
The snapshot is the list written by aws/ops/STAGED/ops_6549_cadence_snapshot.py (kind, name, state, expr, fn, arn,
bus/group, input_sha, input_keys); raw payloads are never written to the repo (they can hold secrets).
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
from collections import defaultdict, Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
import cadence_core as cc  # noqa: E402

IGNORED_INPUT_KEYS = {"source", "src", "ops", "reason", "detail-type", "trigger"}
FANOUT_RANK = {  # slower tick first: a function in several ticks keeps the slowest one
    "monthly": 0, "weekly-sun": 1,
    "daily-06utc": 2, "daily-07utc": 2, "daily-08utc": 2, "daily-morn": 2, "daily-15utc": 2, "daily-16utc": 2,
    "daily-17utc": 2, "daily-eve": 2, "every_6h": 3, "4hourly": 4, "every_3h": 5, "hourly": 6, "30min": 7,
    "15min": 8, "5min": 9,
}


# ------------------------------------------------------------------------------------------------ helpers --
def input_key(raw):
    """Canonical job identity of a target payload ('' / '{}' / a bare source marker are the default job)."""
    if raw is None:
        return "{}"
    s = raw.strip() if isinstance(raw, str) else json.dumps(raw)
    if s in ("", "{}", "null"):
        return "{}"
    try:
        d = json.loads(s)
    except ValueError:
        return "raw:" + hashlib.sha256(s.encode()).hexdigest()[:12]
    if isinstance(d, dict):
        d = {k: v for k, v in d.items() if k not in IGNORED_INPUT_KEYS}
        if not d:
            return "{}"
    return json.dumps(d, sort_keys=True, separators=(",", ":"))


def hashed_input_key(raw):
    """input_key() with the canonical payload replaced by its hash: what a snapshot stores (payloads can hold secrets
    and this repository is public)."""
    k = input_key(raw)
    return k if k == "{}" else "sha:" + hashlib.sha256(k.encode()).hexdigest()[:20]


def job_id(fn, ikey):
    return "%s#%s" % (fn, "default" if ikey == "{}" else hashlib.sha256(ikey.encode()).hexdigest()[:10])


def dependency_depth(repo):
    """Longest chain of scheduled producers feeding each engine (static code references; cycles collapse)."""
    path = repo / "docs/audit/dependency-map.json"
    if not path.exists():
        return {}
    d = json.loads(path.read_text(encoding="utf-8"))
    engines, writers = d.get("engines", {}), d.get("writers", {})
    es, nodes = defaultdict(set), set(engines)
    for fn, rec in engines.items():
        for k in rec.get("reads", []):
            for p in writers.get(k, []):
                if p != fn:
                    es[p].add(fn)
                    nodes.add(p)
    sys.setrecursionlimit(100000)
    idx, low, on, st, comp = {}, {}, set(), [], {}
    counter, cid = [0], [0]

    def strong(v):
        idx[v] = low[v] = counter[0]
        counter[0] += 1
        st.append(v)
        on.add(v)
        for w in es.get(v, ()):
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
    deps = defaultdict(set)
    for p, cs in es.items():
        for c in cs:
            if comp[p] != comp[c]:
                deps[comp[c]].add(comp[p])
    memo = {}

    def depth(c):
        if c in memo:
            return memo[c]
        memo[c] = 0
        memo[c] = 1 + max([depth(p) for p in deps.get(c, ())] or [-1])
        return memo[c]

    return {fn: depth(comp[fn]) for fn in nodes}


def load_configs(repo):
    """-> {fn: (path, config)} and declarations {(kind, name): [(fn, path, locator)]}."""
    configs, decl = {}, defaultdict(list)
    for p in sorted((repo / "aws/lambdas").glob("*/config.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        fn = c.get("function_name") or p.parent.name
        configs[fn] = (p, c)
        s = c.get("schedule")
        if isinstance(s, dict):
            field = "cron" if "cron" in s else "expression" if "expression" in s else None
            if s.get("scheduler_name"):
                decl[("sched", s["scheduler_name"])].append((fn, p, ("schedule", field)))
            elif s.get("rule_name") or s.get("name"):
                decl[("rule", s.get("rule_name") or s.get("name"))].append((fn, p, ("schedule", field)))
        sc = c.get("eventbridge_scheduler")
        if isinstance(sc, dict) and sc.get("schedule_name"):
            decl[("sched", sc["schedule_name"])].append((fn, p, ("eventbridge_scheduler", "cron")))
        rules = c.get("eventbridge_rules")
        if isinstance(rules, list):
            for i, r in enumerate(rules):
                if isinstance(r, dict) and r.get("name"):
                    decl[("rule", r["name"])].append((fn, p, ("eventbridge_rules", i)))
                elif isinstance(r, str):
                    decl[("rule", r)].append((fn, p, ("eventbridge_rules_name", i)))
        # Schedules created by launch ops rather than by the deploy workflow ("the workflow handles only the primary
        # block"): the repo still records them, and a stale record would show the old cadence next to the live one.
        extra = c.get("eventbridge_scheduler_extra")
        if isinstance(extra, dict) and extra.get("schedule_name"):
            decl[("sched", extra["schedule_name"])].append((fn, p, ("eventbridge_scheduler_extra", "cron")))
        extras = c.get("eventbridge_schedulers_extra")
        if isinstance(extras, list):
            for i, e in enumerate(extras):
                if isinstance(e, dict) and e.get("schedule_name"):
                    field = "expression" if "expression" in e else "cron" if "cron" in e else None
                    if field:
                        decl[("sched", e["schedule_name"])].append((fn, p, ("eventbridge_schedulers_extra", i, field)))
        own = c.get("scheduler")
        if isinstance(own, dict) and own.get("name") and own.get("expr"):
            decl[("sched", own["name"])].append((fn, p, ("scheduler", "expr")))
    return configs, decl


# ---------------------------------------------------------------------------------------------------- plan --
def build_plan(snapshot, repo, exempt):
    configs, decl = load_configs(repo)
    depth = dependency_depth(repo)
    fanout = json.loads((repo / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    ticks = fanout.get("ticks", {})
    exempt_names = {(e["kind"], e["name"]) for e in exempt.get("exempt_triggers", [])}

    # one record per (trigger object, target)
    recs = []
    for t in snapshot:
        if t.get("state") != "ENABLED" or not t.get("fn"):
            continue
        key = (t["kind"], t["name"])
        ik = t.get("ikey") or input_key(t.get("input"))
        recs.append({"trigger": t, "key": key, "job": job_id(t["fn"], ik), "fn": t["fn"], "ikey": ik,
                     "declared": any(d[0] == t["fn"] for d in decl.get(key, [])),
                     "pad": cc.per_active_day(t["expr"]), "fpd": cc.fires_per_day(t["expr"])})
    for tick, members in ticks.items():           # fan-out membership is a trigger too (default payload, daily)
        for fn in members:
            recs.append({"trigger": {"kind": "tick", "name": "tick:" + tick, "expr": "", "fn": fn}, "key": ("tick", tick),
                         "job": job_id(fn, "{}"), "fn": fn, "ikey": "{}", "declared": False, "pad": 1, "fpd": 1.0,
                         "tick": tick})

    jobs = defaultdict(list)
    for r in recs:
        jobs[r["job"]].append(r)
    width = defaultdict(set)                      # functions a trigger object starts: a shared rule is the worst keeper
    for r in recs:
        width[r["key"]].add(r["fn"])

    def eff_coverage(r):
        """Share of days the job would run once it complies (a violator keeps its day-of-week filter)."""
        return min(1.0, r["fpd"] / max(1, r["pad"]))

    def rank(r):
        t = r["trigger"]
        compliant_daily = r["pad"] <= 1 and eff_coverage(r) >= 0.7        # a real daily/weekday schedule
        return (0 if r["key"] in exempt_names else 1,
                0 if compliant_daily else 1,
                -round(eff_coverage(r), 4),
                0 if r["declared"] else 1,
                len(width[r["key"]]),
                0 if r["key"][0] != "tick" else 1 + FANOUT_RANK.get(r.get("tick"), 3),
                abs(r["fpd"] - 1.0),
                0 if t["kind"] == "sched" else 1,
                t["name"])

    keepers, losers = {}, defaultdict(list)
    for jid, rs in jobs.items():
        order = sorted(rs, key=rank)
        keepers[jid] = order[0]
        for r in order[1:]:
            losers[jid].append(r)

    # trigger-object level decisions
    obj = {}
    for jid, rs in jobs.items():
        for r in rs:
            if r["key"][0] == "tick":
                continue
            o = obj.setdefault(r["key"], {"trigger": r["trigger"], "keep_for": [], "drop_for": []})
            (o["keep_for"] if keepers[jid] is r else o["drop_for"]).append(jid)

    actions = []
    tick_removals = defaultdict(list)
    for key, o in sorted(obj.items()):
        t = o["trigger"]
        depth_fn = min([depth.get(r["fn"], 0) for rs in jobs.values() for r in rs if r["key"] == key] or [0])
        base = {"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"),
                "functions": sorted({r["fn"] for rs in jobs.values() for r in rs if r["key"] == key}),
                "before": {"expr": t["expr"], "state": t["state"], "input_sha": t.get("input_sha") or
                           hashlib.sha256((t.get("input") or "").encode()).hexdigest()[:16]}}
        if key in exempt_names:
            actions.append(dict(base, op="keep", reason="control plane (config/cadence-floor.json)", after=base["before"]))
            continue
        if o["drop_for"] and not o["keep_for"]:
            twins = sorted({keepers[j]["trigger"]["name"] for j in o["drop_for"]})
            actions.append(dict(base, op="disable", reason="same job already runs from %s" % ", ".join(twins),
                                after={"expr": cc.to_daily(t["expr"], t["name"], depth_fn), "state": "DISABLED"}))
            continue
        new = cc.to_daily(t["expr"], t["name"], depth_fn)
        remove = sorted({j.split("#")[0] for j in o["drop_for"]})
        extra = {"remove_functions": remove} if remove else {}
        if new == t["expr"].strip() and not remove:
            actions.append(dict(base, op="keep", reason="already at most once a day", after=base["before"]))
        else:
            actions.append(dict(base, op="retime" if new != t["expr"].strip() else "keep",
                                reason=("%s -> once a day" % cc.describe(t["expr"])) +
                                       ("; target(s) %s already run from another trigger" % ", ".join(remove) if remove else ""),
                                after={"expr": new, "state": "ENABLED"}, depth=depth_fn, **extra))
    for jid, rs in losers.items():
        for r in rs:
            if r["key"][0] == "tick":
                tick_removals[r["tick"]].append(r["fn"])

    # Switched-off triggers keep their old expression, so one console click (or a script that only flips State) would
    # bring a 5-minute engine back.  They get the compliant expression too, still DISABLED: zero cost, and it keeps the
    # repo's config.json (which must equal live for a deploy to pass scripts/check_existing_schedule.py) truthful.
    planned = {(a["kind"], a["name"]) for a in actions}
    off = {}
    for t in snapshot:
        key = (t["kind"], t["name"])
        if t.get("state") in (None, "ENABLED") or key in planned or key in exempt_names or not t.get("expr"):
            continue
        off.setdefault(key, []).append(t)
    for key, ts in sorted(off.items()):
        t = ts[0]
        try:
            if not cc.violates(t["expr"]):
                continue
        except cc.ExpressionError:
            continue
        fns = sorted({x["fn"] for x in ts if x.get("fn")})
        depth_fn = min([depth.get(f, 0) for f in fns] or [0])
        actions.append({"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"),
                        "functions": fns,
                        "before": {"expr": t["expr"], "state": t["state"], "input_sha": t.get("input_sha") or
                                   hashlib.sha256((t.get("input") or "").encode()).hexdigest()[:16]},
                        "op": "retime", "depth": depth_fn,
                        "reason": "switched off, but %s if it were switched back on -> once a day" % cc.describe(t["expr"]),
                        "after": {"expr": cc.to_daily(t["expr"], t["name"], depth_fn), "state": t["state"]}})
    actions.sort(key=lambda a: (a["kind"], a["name"]))

    # what the account looks like once the plan has run: the repo's declarations and manifest must say exactly this
    final = {(t["kind"], t["name"]): (t.get("expr") or "", t.get("state")) for t in snapshot if t.get("name") and not t.get("err")}
    for a in actions:
        if a["op"] in ("retime", "disable"):
            final[(a["kind"], a["name"])] = (a["after"]["expr"], a["after"]["state"])
    return {"actions": actions, "tick_removals": {k: sorted(set(v)) for k, v in tick_removals.items()},
            "jobs": {j: [r["trigger"]["name"] for r in rs] for j, rs in jobs.items() if len(rs) > 1},
            "keepers": {j: k["trigger"]["name"] for j, k in keepers.items()}, "depth": depth,
            "configs": configs, "decl": decl, "fanout": fanout, "final": final, "exempt": exempt_names}


# ---------------------------------------------------------------------------------------------- repo edits --
def json_spans(text):
    """Character span of every JSON value in `text`, keyed by its path (a tuple of keys / list indexes).
    Lets us replace one string in a file and leave every other byte (indentation, inline arrays, key order,
    escapes) exactly as the author wrote it - 249 of the 818 engine config.json files do not survive a
    json.dumps round trip, and a reformatting diff would hide the real change."""
    n, spans, ws = len(text), {}, " \t\r\n"

    def skip(i):
        while i < n and text[i] in ws:
            i += 1
        return i

    def string_end(i):
        j = i + 1
        while text[j] != '"':
            j += 2 if text[j] == "\\" else 1
        return j + 1

    def value(i, path):
        i = skip(i)
        c = text[i]
        if c == "{":
            j = skip(i + 1)
            if text[j] == "}":
                spans[path] = (i, j + 1)
                return j + 1
            while True:
                j = skip(j)
                ke = string_end(j)
                key = json.loads(text[j:ke])
                j = skip(ke)
                if text[j] != ":":
                    raise ValueError("expected ':' at %d" % j)
                j = skip(value(j + 1, path + (key,)))
                if text[j] == ",":
                    j += 1
                    continue
                if text[j] != "}":
                    raise ValueError("expected '}' at %d" % j)
                spans[path] = (i, j + 1)
                return j + 1
        if c == "[":
            j = skip(i + 1)
            if text[j] == "]":
                spans[path] = (i, j + 1)
                return j + 1
            idx = 0
            while True:
                j = skip(value(j, path + (idx,)))
                idx += 1
                if text[j] == ",":
                    j += 1
                    continue
                if text[j] != "]":
                    raise ValueError("expected ']' at %d" % j)
                spans[path] = (i, j + 1)
                return j + 1
        if c == '"':
            e = string_end(i)
            spans[path] = (i, e)
            return e
        j = i
        while j < n and text[j] not in ",]} \t\r\n":
            j += 1
        spans[path] = (i, j)
        return j

    end = skip(value(0, ()))
    if end != n:
        raise ValueError("trailing characters after the JSON document")
    return spans


class JsonFile:
    """A JSON file edited by exact string replacement.  render() re-parses the result and refuses to return
    anything but the intended change."""

    def __init__(self, path, text=None):
        self.path = Path(path) if path is not None else None
        self.text = text if text is not None else self.path.read_text(encoding="utf-8")
        self.obj = json.loads(self.text)
        self.spans = json_spans(self.text)
        self.sets = {}

    @classmethod
    def from_text(cls, text):
        return cls(None, text)

    def get(self, path, default=None):
        cur = self.obj
        for k in path:
            try:
                cur = cur[k]
            except (KeyError, IndexError, TypeError):
                return default
        return cur

    def set_string(self, path, new):
        cur = self.get(path)
        if not isinstance(cur, str):
            return False
        if cur == new:
            return False
        self.sets[path] = new
        return True

    def render(self):
        text = self.text
        for path in sorted(self.sets, key=lambda p: self.spans[p][0], reverse=True):
            a, b = self.spans[path]
            text = text[:a] + json.dumps(self.sets[path], ensure_ascii=False) + text[b:]
        want = copy.deepcopy(self.obj)
        for path, val in self.sets.items():
            cur = want
            for k in path[:-1]:
                cur = cur[k]
            cur[path[-1]] = val
        if json.loads(text) != want:
            raise AssertionError("edit of %s changed more than intended" % self.path)
        return text

    def save(self):
        if self.sets:
            self.path.write_text(self.render(), encoding="utf-8")
        return len(self.sets)


def config_paths(locator):
    """-> (path to the expression string, path to the state string or None) inside one engine config.json."""
    kind, where = locator[0], locator[1]
    if kind == "schedule":
        return ("schedule", where), None
    if kind == "eventbridge_scheduler":
        return ("eventbridge_scheduler", "cron"), ("eventbridge_scheduler", "state")
    if kind == "eventbridge_rules":
        return ("eventbridge_rules", where, "schedule"), ("eventbridge_rules", where, "state")
    if kind == "eventbridge_scheduler_extra":
        return ("eventbridge_scheduler_extra", "cron"), ("eventbridge_scheduler_extra", "state")
    if kind == "eventbridge_schedulers_extra":
        return ("eventbridge_schedulers_extra", where, locator[2]), ("eventbridge_schedulers_extra", where, "state")
    if kind == "scheduler":
        return ("scheduler", "expr"), None
    return None, None


def apply_repo(plan, repo):
    """Make the repo say what AWS will say.  Edits only the strings that change; returns counters + notes."""
    by_key = {(a["kind"], a["name"]): a for a in plan["actions"]}
    final, exempt = plan.get("final") or {}, plan.get("exempt") or set()
    files, stats, notes = {}, Counter(), []

    def jf(path):
        if path not in files:
            files[path] = JsonFile(path)
        return files[path]

    # 1. engine config.json: schedule / eventbridge_scheduler / eventbridge_rules blocks that name a trigger
    for key, entries in sorted(plan["decl"].items()):
        if key in exempt:                                    # the control plane stays exactly as written
            continue
        a = by_key.get(key)
        for fn, path, locator in entries:
            epath, spath = config_paths(locator)
            if epath is None:
                continue
            f = jf(path)
            cur = f.get(epath)
            if not isinstance(cur, str) or not cur.strip():
                continue
            if a and a["op"] in ("retime", "disable"):
                new = a["after"]["expr"]
            elif cc.violates(cur):                           # declared, not planned, still a violator
                live_expr = (final.get(key) or ("", None))[0]
                if live_expr and not cc.violates(live_expr):
                    new = live_expr                          # the account already runs it once a day: say exactly that
                else:                                        # not in the account at all: a later deploy would create it
                    new = cc.to_daily(cur, key[1], plan["depth"].get(fn, 0))
            else:
                if a and a["before"]["expr"].strip() != cur.strip():
                    notes.append("config differs from live (left as is): %s %s" % (fn, key[1]))
                continue
            if f.set_string(epath, new):
                stats["config_expressions"] += 1
            if a and a["op"] == "disable" and spath is not None and f.get(spath) is not None:
                if f.set_string(spath, "DISABLED"):
                    stats["config_states"] += 1

    # 2. cadence-only string schedules ("schedule": "rate(15 minutes)")
    for fn, (path, cfg) in sorted(plan["configs"].items()):
        s = cfg.get("schedule")
        if isinstance(s, str) and cc.violates(s):
            if jf(path).set_string(("schedule",), cc.to_daily(s, "cfg:" + fn, plan["depth"].get(fn, 0))):
                stats["config_string_schedules"] += 1

    # 3. config/schedule-manifest.json - the declarative copy of live state
    for f in files.values():
        stats["files_written"] += 1 if f.save() else 0
    mpath = repo / "config/schedule-manifest.json"
    new_text, n = apply_schedule_manifest(mpath.read_text(encoding="utf-8"), plan["actions"], final)
    if n:
        mpath.write_text(new_text, encoding="utf-8")
        stats["files_written"] += 1
        stats["manifest_entries"] = n

    # 4. config/fanout-manifest.json - a function belongs to one tick, and to none when it has its own trigger
    fpath = repo / "config/fanout-manifest.json"
    raw = fpath.read_text(encoding="utf-8")
    fan = json.loads(raw)
    if json.dumps(fan, indent=1, ensure_ascii=False) + "\n" != raw:
        raise AssertionError("fanout-manifest.json no longer round-trips; edit it by hand")
    removed_from = apply_fanout(fan, plan["tick_removals"])
    stats["fanout_removed"] = sum(len(v) for v in plan["tick_removals"].values())
    if removed_from:
        fpath.write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        stats["fanout_files_written"] = 1
    stats["notes"] = len(notes)
    return dict(stats), notes


def apply_fanout(fan, tick_removals):
    """Remove each listed function from the named ticks.  A function stays in the slowest tick it was in, and in none
    when its own rule or schedule already runs it.  Mutates `fan`; returns {function: [ticks it left]}."""
    removed_from = defaultdict(list)
    for tick, fns in tick_removals.items():
        drop = set(fns)
        for f in fan["ticks"].get(tick, []):
            if f in drop:
                removed_from[f].append(tick)
        fan["ticks"][tick] = [f for f in fan["ticks"].get(tick, []) if f not in drop]
    left = defaultdict(list)
    for tick, fns in fan["ticks"].items():
        for f in fns:
            left[f].append(tick)
    for f in list(fan.get("assignment", {})):
        if f in removed_from:
            if f in left:
                fan["assignment"][f]["ticks"] = sorted(left[f], key=lambda t: FANOUT_RANK.get(t, 3))
                fan["assignment"][f]["per_day"] = float(len(left[f]))
            else:
                del fan["assignment"][f]
    if removed_from:
        fan.setdefault("cadence_floor_2026_10_20", {
            "why": "Khalid, 2026-10-20: every engine runs at most once per 24 h. A function listed in several ticks "
                   "ran once per tick; it now stays in the slowest tick (or none, when its own rule or schedule "
                   "already runs it daily). To restore a membership, put the name back in ticks.<tick>.",
            "removed": {f: sorted(t) for f, t in sorted(removed_from.items())}})
    return dict(removed_from)


def apply_schedule_manifest(text, actions, final=None):
    """config/schedule-manifest.json text with each retimed / disabled trigger's expr + state replaced, every other
    byte untouched, and the targets a plan removes from a shared rule dropped from that rule's entry.  `final`
    ({(kind, name): (expr, state)} - the account after the plan) also brings entries the plan does not touch in line
    with it: the manifest was written 2026-10-08 and later ops switched some of its rules off.
    Returns (new_text, entries_changed)."""
    f = JsonFile.from_text(text)
    by_key = {(a["kind"], a["name"]): a for a in actions}
    final = final or {}
    changed, drops = 0, []
    for section, kind in (("rules", "rule"), ("schedules", "sched")):
        for i, entry in enumerate(f.obj.get(section, [])):
            key = (kind, entry.get("name"))
            a = by_key.get(key)
            if a and a["op"] in ("retime", "disable"):
                c = f.set_string((section, i, "expr"), a["after"]["expr"])
                c |= f.set_string((section, i, "state"), a["after"]["state"])
                changed += 1 if c else 0
                if a.get("remove_functions"):
                    drops.append((section, i, set(a["remove_functions"])))
            elif key in final and final[key][0] and final[key][1]:
                c = f.set_string((section, i, "expr"), final[key][0])
                c |= f.set_string((section, i, "state"), final[key][1])
                changed += 1 if c else 0
    out = f.render()
    if drops:
        nl = "\n" if out.endswith("\n") else ""
        parsed = json.loads(out)
        style = next((ea for ea in (True, False) if json.dumps(parsed, indent=1, ensure_ascii=ea) + nl == out), None)
        if style is None:
            raise AssertionError("schedule-manifest.json does not round-trip through json.dumps(indent=1); "
                                 "removing a target would reformat it")
        for section, i, fns in drops:
            tgs = parsed[section][i].get("targets") or []
            keep = [t for t in tgs if (re.match(r".*:function:([^:]+)", t.get("arn") or "") or [None, None])[1] not in fns]
            if keep and len(keep) < len(tgs):
                parsed[section][i]["targets"] = keep
                changed += 1
        out = json.dumps(parsed, indent=1, ensure_ascii=style) + nl
    return out, changed


def summarise(plan):
    c = Counter(a["op"] for a in plan["actions"])
    firings_before = sum(cc.fires_per_day(a["before"]["expr"]) for a in plan["actions"] if a["before"]["state"] == "ENABLED")
    firings_after = sum(cc.fires_per_day(a["after"]["expr"]) for a in plan["actions"] if a["after"]["state"] == "ENABLED")
    return {"ops": dict(c), "scheduled_firings_per_day_before": round(firings_before, 1),
            "scheduled_firings_per_day_after": round(firings_after, 1)}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--repo", default=str(ROOT))
    ap.add_argument("--out", default="config/cadence-plan.json")
    ap.add_argument("--apply-repo", action="store_true")
    a = ap.parse_args(argv)
    repo = Path(a.repo)
    snapshot = json.loads(Path(a.snapshot).read_text(encoding="utf-8"))
    exempt = json.loads((repo / "config/cadence-floor.json").read_text(encoding="utf-8"))
    plan = build_plan(snapshot, repo, exempt)
    summary = summarise(plan)
    out = {"version": 1, "rule": exempt.get("rule"), "summary": summary,
           "actions": [x for x in plan["actions"] if x["op"] != "keep"], "fanout_removals": plan["tick_removals"]}
    if a.apply_repo:
        out["repo_edits"], notes = apply_repo(plan, repo)
        for n in notes[:50]:
            print("note:", n)
    (repo / a.out).write_text(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(out["summary"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
