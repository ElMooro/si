"""ops 6928 - the Fed securities held outright desk: can every chart be drawn as percent change from a year ago? (read-only)

Khalid 2026-11-23: "on all the charts on: Fed securities held outright you should also add the option to view the chart
on year to year percentage change. add it to every single chart".

Audit before building.  The 222 series of the desk are read one at a time through /series - the request each card
makes, answered from the warehouse - with a pause between them.  Nothing is written but this report and a pack of
real histories for the pages' tests.  Per series:
  - its rhythm (the median gap between prints), its weekday, the prints since 1996 and before;
  - zeros, readings below zero, and whether it crosses zero;
  - FRED's "percent change from year ago", ((x(t) / x(t - a year)) - 1) * 100, with the year taken as FRED takes it:
    52 prints back for a weekly line (the same weekday 364 days earlier), 12 for a monthly one;
  - how many prints have a year-ago print at all, the range of the result and how far its extremes stand from the rest.
The 52-week base is checked against the Federal Reserve itself: the H.4.1 prints "change from a year ago" lines
(...XCH52NWW) for ten of the levels, and level(t) - level(t - 364 days) has to equal them.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
FROM = "1996-01-01"
PAUSE = 0.25
CATALOG = json.loads(Path(__file__).with_name("ops_6834_desk_catalogs.json").read_text(encoding="utf-8"))["soma"]
PACK_ALWAYS = ("WSHOSHO", "WSHOTSL", "WSHOMCB", "TREAST", "WSHOBL", "WUDSHO", "WUPSHO", "WSHOFADSL", "RESPPALGXCH52NWW", "RESPPALGXCH1NWW")


def http(url, timeout=90):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
        return resp.status, resp.read()


def series(sid):
    last = None
    for attempt in range(3):
        try:
            _, body = http(PROXY + "/series?id=" + urllib.parse.quote("FRED:" + sid))
            doc = json.loads(body)
            obs = []
            for row in doc.get("obs") or doc.get("bars") or []:
                d, v = (row[0], row[1]) if isinstance(row, list) else (row.get("time") or row.get("date"), row.get("close", row.get("value")))
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if d and v == v:
                    obs.append((str(d)[:10], v))
            obs.sort()
            return {"obs": obs, "source": str(doc.get("source") or "")[:60]}
        except Exception as exc:  # noqa: BLE001
            last = exc
            if getattr(exc, "code", None) in (400, 404):
                return {"error": "HTTP %s" % exc.code}
            time.sleep(2 + 3 * attempt)
    return {"error": str(last)[:160]}


def rhythm(obs):
    days = [date.fromisoformat(d).toordinal() for d, _ in obs]
    gaps = sorted(b - a for a, b in zip(days, days[1:]) if b > a)
    return gaps[len(gaps) // 2] if gaps else None


def yoy(obs, gap):
    """FRED's percent change from a year ago.  Short rhythms: the print 364 days earlier (52 weeks, the same weekday);
    monthly and longer: the print of the same month a year earlier.  No figure where that print is missing or zero."""
    out = []
    if gap is not None and gap < 20:
        at = dict(obs)
        for d, v in obs:
            was = at.get((date.fromisoformat(d) - timedelta(days=364)).isoformat())
            if was is not None and was != 0:
                out.append((d, (v / was - 1) * 100, v, was))
    else:
        at = {d[:7]: v for d, v in obs}
        for d, v in obs:
            was = at.get("%04d-%s" % (int(d[:4]) - 1, d[5:7]))
            if was is not None and was != 0:
                out.append((d, (v / was - 1) * 100, v, was))
    return out


def pct(sorted_values, q):
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def f(v, places=1):
    return "-" if v is None else ("%+.*f" % (places, v))


def main():
    now = datetime.now(timezone.utc)
    with report("ops_6928_held_outright_yoy_audit") as r:
        r.heading("ops 6928 - percent change from a year ago on the held outright desk: the audit (read-only), %s" % now.isoformat(timespec="minutes"))
        items = [(ci, cat["cat"], it[0], it[1], (it[3] if len(it) > 3 else "")) for ci, cat in enumerate(CATALOG) for it in cat["items"]]
        r.log("%d series in %d families; %d tagged as lines that are themselves a change" % (len(items), len(CATALOG), sum(1 for i in items if i[4] == "C")))
        got, t0 = {}, time.time()
        for n, (_, _, sid, _, _) in enumerate(items):
            got[sid] = series(sid)
            time.sleep(PAUSE)
        r.log("read %d series one at a time in %.0f s; no answer: %s" % (len(items), time.time() - t0, [s for s, g in got.items() if "error" in g] or "none"))

        classes, facts, pack, fails = {}, {}, {}, []
        for ci, cat, sid, title, tag in items:
            g = got[sid]
            if "error" in g or not g["obs"]:
                classes.setdefault("no answer", []).append(sid)
                continue
            obs = g["obs"]
            gap = rhythm(obs)
            since = [o for o in obs if o[0] >= FROM]
            lead = [o for o in obs if "1994-12-01" <= o[0] < FROM]
            wd = sorted({date.fromisoformat(d).weekday() for d, _ in obs[-260:]})
            zeros = sum(1 for _, v in since if v == 0)
            neg = sum(1 for _, v in since if v < 0)
            pos = sum(1 for _, v in since if v > 0)
            y = [p for p in yoy(obs, gap) if p[0] >= FROM]
            vals = sorted(p[1] for p in y)
            missing = sum(1 for d, _ in since if d >= "1997-01-01") - len([p for p in y if p[0] >= "1997-01-01"])
            if tag == "C":
                cls = "a change line (tagged)"
            elif not since:
                cls = "ended before 1996"
            elif neg and pos:
                cls = "crosses zero (not tagged)"
            elif neg and not pos:
                cls = "below zero throughout"
            elif not y:
                cls = "no year-ago pair"
            elif zeros:
                cls = "level with zeros"
            else:
                cls = "level above zero"
            classes.setdefault(cls, []).append(sid)
            lo, hi = pct(vals, 0.02), pct(vals, 0.98)
            crossings = [y[i][0] for i in range(1, len(y)) if (y[i][1] > 0) != (y[i - 1][1] > 0)]
            facts[sid] = {"cls": cls, "gap": gap, "weekdays": wd, "n": len(obs), "since": len(since), "lead": len(lead), "first": obs[0][0], "last": obs[-1][0],
                          "zeros": zeros, "neg": neg, "yoy_n": len(y), "missing": missing, "yoy_last": y[-1] if y else None,
                          "yoy_min": min(y, key=lambda p: p[1]) if y else None, "yoy_max": max(y, key=lambda p: p[1]) if y else None,
                          "p2": lo, "p98": hi, "crossings": len(crossings), "last_cross": crossings[-1] if crossings else None, "title": title, "family": ci}

        r.section("1. What each line is")
        for cls in sorted(classes, key=lambda c: -len(classes[c])):
            r.log("**%s**: %d  %s" % (cls, len(classes[cls]), " ".join(classes[cls][:14]) + (" ..." if len(classes[cls]) > 14 else "")))
        gaps = {}
        for sid, fct in facts.items():
            gaps.setdefault(fct["gap"], []).append(sid)
        r.log("rhythm (median days between prints): %s" % {k: len(v) for k, v in sorted(gaps.items(), key=lambda kv: (kv[0] is None, kv[0]))})
        odd = {sid: fct["weekdays"] for sid, fct in facts.items() if fct["gap"] == 7 and fct["weekdays"] != [2]}
        r.log("weekly lines not always dated a Wednesday in their last 260 prints: %s" % (odd or "none"))
        r.log("lines with prints in the year before 1996 (needed for a 1996 figure): %s" % ([s for s, fct in facts.items() if fct["lead"]] or "none"))
        miss = {sid: fct["missing"] for sid, fct in facts.items() if fct["missing"] and fct["cls"] in ("level above zero", "level with zeros", "below zero throughout")}
        r.log("level lines where a print since 1997 has no usable year-ago print (missing, or zero): %d of them, e.g. %s" % (len(miss), dict(list(sorted(miss.items(), key=lambda kv: -kv[1]))[:12])))

        r.section("2. The 52-week base against the Federal Reserve's own change-from-a-year-ago lines")
        level_by_title = {t: sid for _, _, sid, t, tag in items if tag != "C"}
        pairs = []
        for _, _, sid, title, tag in items:
            if tag != "C" or "XCH52" not in sid:
                continue
            base = title.replace(" · week-average change from a year ago", " · week average").replace(" · change from a year ago", "")
            if base in level_by_title:
                pairs.append((level_by_title[base], sid))
        r.log("%d pairs found by title" % len(pairs))
        for lvl, chg in pairs:
            if "obs" not in got.get(lvl, {}) or "obs" not in got.get(chg, {}):
                continue
            at, c = dict(got[lvl]["obs"]), got[chg]["obs"]
            same = other = none = 0
            worst = (0, None)
            for d, v in c[-520:]:
                now_v, was = at.get(d), at.get((date.fromisoformat(d) - timedelta(days=364)).isoformat())
                if now_v is None or was is None:
                    none += 1
                    continue
                diff = abs((now_v - was) - v)
                if diff <= 1.5:
                    same += 1
                else:
                    other += 1
                    if diff > worst[0]:
                        worst = (diff, d)
            alt = sum(1 for d, v in c[-520:] if at.get(d) is not None and at.get((date.fromisoformat(d) - timedelta(days=371)).isoformat()) is not None and abs((at[d] - at[(date.fromisoformat(d) - timedelta(days=371)).isoformat()]) - v) <= 1.5)
            r.log("  %-22s minus its print 364 days earlier = %-24s on %d of the last %d weeks (%d differ, worst %s; %d without both prints) | with 371 days instead: %d" % (
                lvl, chg, same, min(520, len(c)), other, "%.0f on %s" % worst if worst[1] else "-", none, alt))
            if same < 0.97 * (same + other):
                fails.append("%s: the 52-week difference equals %s on only %d of %d weeks" % (lvl, chg, same, same + other))

        r.section("3. Percent change from a year ago, line by line")
        fam = None
        for ci, cat, sid, title, tag in items:
            fct = facts.get(sid)
            if ci != fam:
                fam = ci
                r.log("**%s**" % cat)
            if not fct:
                r.log("  %-24s no answer" % sid)
                continue
            if not fct["yoy_n"] or fct["cls"].startswith(("a change", "crosses")):
                r.log("  %-24s %-26s %s to %s, %d prints since 1996 (%d zero, %d below zero)" % (sid, fct["cls"], fct["first"], fct["last"], fct["since"], fct["zeros"], fct["neg"]))
                continue
            yl, ymin, ymax = fct["yoy_last"], fct["yoy_min"], fct["yoy_max"]
            spread = (fct["p98"] - fct["p2"]) or 1.0
            far = max((ymax[1] - fct["p98"]) / spread, (fct["p2"] - ymin[1]) / spread)
            r.log("  %-24s %-22s %4d figures (%3d prints without) | last %s%% on %s (%.0f against %.0f) | low %s%% %s, high %s%% %s | middle 96%%: %s to %s | extremes %.1f spans beyond | crossed zero %d times, last %s" % (
                sid, fct["cls"], fct["yoy_n"], fct["missing"], f(yl[1]), yl[0], yl[2], yl[3], f(ymin[1]), ymin[0][:7], f(ymax[1]), ymax[0][:7], f(fct["p2"]), f(fct["p98"]), far, fct["crossings"], fct["last_cross"] or "-"))

        r.section("4. How far the extremes stand from the rest")
        drawn = [fct for fct in facts.values() if fct["yoy_n"] and not fct["cls"].startswith(("a change", "crosses"))]
        ratio = []
        for fct in drawn:
            spread = (fct["p98"] - fct["p2"]) or 1.0
            ratio.append(max((fct["yoy_max"][1] - fct["p98"]) / spread, (fct["p2"] - fct["yoy_min"][1]) / spread))
        for edge in (0.5, 1, 2, 5, 20):
            r.log("extremes more than %s spans of the middle 96%% beyond it: %d of %d lines" % (edge, sum(1 for x in ratio if x > edge), len(drawn)))
        r.log("largest figure anywhere: %s" % max(((fct["yoy_max"][1], fct["yoy_max"][0], fct["title"]) for fct in drawn), default=None).__repr__())

        # a pack of whole histories (nothing thinned: a year-ago figure needs the print 364 days earlier)
        chosen = list(PACK_ALWAYS)
        for cls, ids in classes.items():
            for sid in ids[:2]:
                if sid not in chosen:
                    chosen.append(sid)
        wild = sorted(drawn, key=lambda fct: -(fct["yoy_max"][1] - fct["p98"]))[:2]
        chosen += [sid for sid, fct in facts.items() if fct in wild and sid not in chosen]
        for sid in chosen:
            if "obs" in got.get(sid, {}):
                pack["FRED:" + sid] = [o for o in got[sid]["obs"] if o[0] >= "1994-06-01"] or got[sid]["obs"][-60:]
        doc = {"generated_at": now.isoformat(timespec="seconds"), "note": "whole histories since mid-1994 as /series answered, for the year-over-year tests", "series": pack,
               "facts": {sid: {k: fct[k] for k in ("cls", "gap", "since", "zeros", "neg", "yoy_n", "yoy_last", "yoy_min", "yoy_max", "p2", "p98", "crossings", "last_cross")} for sid, fct in facts.items()}}
        blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":")).encode("utf-8"), 9)).decode("ascii")
        (ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6928_fixture.md").write_text(blob + "\n")
        r.log("pack: %d whole histories (%s), every line's facts, %d characters" % (len(pack), " ".join(k[5:] for k in pack), len(blob)))

        for line in fails:
            r.warn(line)
        r.kv(series=len(items), answered=len(facts), base_mismatches=len(fails), **{k.replace(" ", "_").replace("(", "").replace(")", ""): len(v) for k, v in classes.items()})


if __name__ == "__main__":
    main()
