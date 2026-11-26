"""ops 6965 - read-only: what in the provider's ETF rows is not money (before the money map is published).

The dry run (ops 6964) agreed with the provider to the dollar, and that is the problem: some of the provider's own
figures are not flows. Seen there: Vanguard funds whose share count rose 4x to 8x in a day with the NAV unchanged
(+$401B that is share splits), a November series whose figure says -85% with the share count unchanged, tickers whose
shares and NAV both changed scale in a day, and three Alger funds whose counts tripled and collapsed in two weeks.

This op measures each kind on the whole warehouse so that the rules are written from the rows, not from a guess:

  1. the warehouse's record of splits (polygon-bank/splits) against the share counts;
  2. the provider's figure against its own share counts, with and without a missing day in between;
  3. days on which shares and NAV both change scale (a different series under the same ticker);
  4. share counts that jump and come back, by how many days later;
  5. the histories of the funds named above.

Nothing is written. Safe to run again.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import math
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
STAGING = ROOT / "aws" / "ops" / "staging" / "moneymap"
sys.path.insert(0, str(STAGING if (STAGING / "flow_map.py").exists() else ROOT / "aws" / "lambdas" / "justhodl-global-flow-desk" / "source"))
NAME = "ops_6965_money_map_data_quality_probe"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
SPLITS = "data/warm/polygon-bank/splits/"


def usd(v):
    if v is None:
        return "n/a"
    a = abs(v)
    s = "-" if v < 0 else "+"
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.1fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0f" % (s, a)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import flow_map as M

    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120, max_pool_connections=48))
    now = datetime.now(timezone.utc)

    def get(key):
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    with report(NAME) as r:
        r.heading("6965 - what in the provider's ETF rows is not money (read-only), %s" % now.isoformat(timespec="minutes"))
        series, profiles, issuer, months = M.collect(s3, LIVE, r.log)
        cal = M.calendar(series)
        pos = {d: i for i, d in enumerate(cal)}
        rows_of = {t: [x for x in rows if x[0] in pos] for t, rows in series.items()}
        r.log("funds %d, reporting days %d (%s..%s)" % (len(rows_of), len(cal), cal[0], cal[-1]))

        def line(rr):
            return " ; ".join("%s sh %.4gM nav %.2f f %s" % (d[2:], (s_ or 0) / 1e6, n_ or 0, usd(f_)) for d, s_, n_, f_ in rr)

        # ------------------------------------------------------------------ 1. the record of splits
        r.section("1. The warehouse's record of splits against the share counts")
        keys = sorted(k["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=SPLITS) for k in page.get("Contents", []))
        r.log("split files %d, %s .. %s" % (len(keys), keys[0].split("/")[-1] if keys else None, keys[-1].split("/")[-1] if keys else None))
        want = [k for k in keys if k.split("/")[-1][:7] >= cal[0][:7]]
        rec = defaultdict(list)
        shape = None
        for k in want:
            try:
                doc = get(k)
            except Exception as exc:  # noqa: BLE001
                r.log("    %s unreadable: %s" % (k, type(exc).__name__))
                continue
            rows = doc if isinstance(doc, list) else (doc.get("results") or doc.get("rows") or []) if isinstance(doc, dict) else []
            if shape is None and rows:
                shape = sorted(rows[0])
            for x in rows:
                if isinstance(x, dict) and x.get("ticker") in rows_of and x.get("split_from") and x.get("split_to"):
                    rec[x["ticker"]].append((str(x.get("execution_date"))[:10], float(x["split_to"]) / float(x["split_from"])))
        r.log("months read %d (%s..%s), row fields %s | ETFs in the map with a recorded split in the kept history: %d funds, %d splits" % (
            len(want), want[0].split("/")[-1] if want else None, want[-1].split("/")[-1] if want else None, shape, len(rec), sum(len(v) for v in rec.values())))
        seen = miss = stag = adj = 0
        notes = []
        for t, evs in sorted(rec.items()):
            rr = rows_of[t]
            for d, c in evs:
                if not (cal[0] < d <= cal[-1]):
                    continue
                best = None
                for i in range(1, len(rr)):
                    if abs((datetime.strptime(rr[i][0], "%Y-%m-%d") - datetime.strptime(d, "%Y-%m-%d")).days) > 6:
                        continue
                    s0, s1, n0, n1 = rr[i - 1][1], rr[i][1], rr[i - 1][2], rr[i][2]
                    if s0 and s1 and n0 and n1 and abs(math.log(s1 / s0 / c)) < 0.2:
                        best = (rr[i][0], s1 / s0, n1 / n0, i)
                        break
                if best is None:
                    miss += 1
                    if len(notes) < 14:
                        near = [x for x in rr if abs((datetime.strptime(x[0], "%Y-%m-%d") - datetime.strptime(d, "%Y-%m-%d")).days) <= 4]
                        notes.append("NOT SEEN %s %s x%.4g | %s" % (t, d, c, line(near[:6])))
                    continue
                seen += 1
                flat = abs(math.log(best[2])) < 0.08
                adj += flat
                stag += (best[0] != d)
                if flat and len(notes) < 40:
                    i = best[3]
                    notes.append("NAV ALREADY ADJUSTED %s recorded %s x%.4g, count moved %s x%.4f nav x%.4f | next nav x%.3f" % (t, d, c, best[0], best[1], best[2], (rr[i + 1][2] / rr[i][2]) if i + 1 < len(rr) and rr[i][2] else float("nan")))
        r.log("recorded splits inside the history: found in the share counts %d (on another day than recorded %d; with the NAV already adjusted %d), not found %d" % (seen, stag, adj, miss))
        for n in notes:
            r.log("    " + n)
        # the engine's own inferred splits against the record
        inferred = []
        for t, rr in rows_of.items():
            for x in M.clean_series(rr):
                if x[4] and x[4].startswith("split"):
                    inferred.append((t, x[0], x[4]))
        conf = sum(1 for t, d, _ in inferred if any(abs((datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(dd, "%Y-%m-%d")).days) <= 6 for dd, _ in rec.get(t, [])))
        r.log("splits the engine infers from opposite moves: %d, of which on record %d | not on record: %s" % (len(inferred), conf, ", ".join(
            "%s %s %s" % (t, d[2:], x.replace("split ", "")) for t, d, x in inferred if not any(abs((datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(dd, "%Y-%m-%d")).days) <= 6 for dd, _ in rec.get(t, [])))[:1500]))
        for t in "VGT VUG VOOG MGK VO PDEC UDEC XBOX SGVA TSPY TDAQ MSTU FSOL OWNB BITQ FETH".split():
            r.log("record for %s: %s" % (t, rec.get(t)))
        # whole-ratio jumps with the NAV flat and no record
        whole = []
        for t, rr in rows_of.items():
            for i in range(1, len(rr)):
                s0, s1, n0, n1 = rr[i - 1][1], rr[i][1], rr[i - 1][2], rr[i][2]
                if not (s0 and s1 and n0 and n1) or s0 * n0 < 5e7:
                    continue
                ratio = s1 / s0
                k = round(ratio) if ratio > 1 else 1.0 / round(1 / ratio) if ratio > 0 else 0
                if k and k != 1 and abs(math.log(ratio / k)) < 0.004 and abs(math.log(n1 / n0)) < 0.06:
                    on = any(abs((datetime.strptime(rr[i][0], "%Y-%m-%d") - datetime.strptime(dd, "%Y-%m-%d")).days) <= 6 for dd, _ in rec.get(t, []))
                    whole.append((abs(s1 - s0) * n0, t, rr[i][0], ratio, on, rr[i + 1][1] / s1 if i + 1 < len(rr) and rr[i + 1][1] else None))
        whole.sort(reverse=True)
        r.log("share counts that move by a whole ratio (within 0.4%%) with the NAV flat, funds of $50M+: %d (on record %d) | dollars: on record %s, not on record %s" % (
            len(whole), sum(1 for x in whole if x[4]), usd(sum(x[0] for x in whole if x[4])), usd(sum(x[0] for x in whole if not x[4]))))
        for v, t, d, ratio, on, nx in whole[:40]:
            r.log("    %-6s %s x%.4f %s %s next-day count x%s" % (t, d, ratio, usd(v), "ON RECORD" if on else "no record", "%.3f" % nx if nx else "n/a"))
        c2 = Counter(round(x[3]) if x[3] > 1 else -round(1 / x[3]) for x in whole if not x[4])
        r.log("    not on record, by ratio (negative = 1-for-n): %s" % dict(c2.most_common(12)))

        # ------------------------------------------------------------------ 2. the provider's figure against its own counts
        r.section("2. The provider's figure against its own share counts")
        agree = dis = gap_agree = gap_dis = nofig = 0
        dis_usd = gap_usd = 0.0
        worst, worst_gap = [], []
        recent = set(cal[-63:])
        for t, rr in rows_of.items():
            for i in range(1, len(rr)):
                d, s1, n1, f = rr[i]
                d0, s0, n0, _ = rr[i - 1]
                if not (s0 and s1 and n0) or d not in recent:
                    continue
                if f is None:
                    nofig += 1
                    continue
                mine = (s1 - s0) * n0
                tol = max(2000.0, 0.003 * abs(mine), 2e-4 * s0 * n0)
                gap = pos[d] - pos[d0] != 1
                ok = abs(f - mine) <= tol
                if gap:
                    gap_agree += ok
                    gap_dis += (not ok)
                    if not ok:
                        gap_usd += abs(f - mine)
                        worst_gap.append((abs(f - mine), t, d, d0, f, mine))
                else:
                    agree += ok
                    dis += (not ok)
                    if not ok:
                        dis_usd += abs(f - mine)
                        worst.append((abs(f - mine), t, d, f, mine, s0, s1, n0))
        r.log("last 63 days | previous row is the previous day: agree %d, disagree %d (%s apart in all) | a day missing in between: agree %d, disagree %d (%s) | no figure %d" % (
            agree, dis, usd(dis_usd), gap_agree, gap_dis, usd(gap_usd), nofig))
        for v, t, d, f, mine, s0, s1, n0 in sorted(worst, reverse=True)[:25]:
            r.log("    no gap  %-6s %s figure %s, counts say %s (shares %.4gM -> %.4gM, nav %.2f)" % (t, d, usd(f), usd(mine), s0 / 1e6, s1 / 1e6, n0))
        for v, t, d, d0, f, mine in sorted(worst_gap, reverse=True)[:25]:
            r.log("    gap     %-6s %s (previous row %s) figure %s, counts say %s" % (t, d, d0, usd(f), usd(mine)))
        by_day = Counter(d for _, t, d, *_ in worst)
        r.log("    no-gap disagreements by day (top): %s" % dict(by_day.most_common(10)))
        missing = Counter()
        for t, rr in rows_of.items():
            have = {x[0] for x in rr}
            if rr and pos[rr[-1][0]] >= len(cal) - 4:
                for d in cal[-21:]:
                    if d not in have and d > rr[0][0]:
                        missing[d] += 1
        r.log("current funds without a row, by day (last 21): %s" % dict(sorted(missing.items())))

        # ------------------------------------------------------------------ 3. both change scale
        r.section("3. Days on which shares and NAV both change scale and it is not a split")
        breaks = []
        for t, rr in rows_of.items():
            for i in range(1, len(rr)):
                s0, s1, n0, n1 = rr[i - 1][1], rr[i][1], rr[i - 1][2], rr[i][2]
                if not (s0 and s1 and n0 and n1 and s0 > 0 and s1 > 0 and n0 > 0 and n1 > 0):
                    continue
                ls, ln = math.log(s1 / s0), math.log(n1 / n0)
                if abs(ln) > 0.35 and not (ls * ln < 0 and abs(ls + ln) < 0.35):
                    nx = rr[i + 1] if i + 1 < len(rr) else None
                    back = bool(nx and nx[2] and abs(math.log(nx[2] / n0)) < 0.12)
                    breaks.append((s0 * n0, t, rr[i][0], s1 / s0, n1 / n0, back, len(rr) - i))
        breaks.sort(reverse=True)
        r.log("NAV moves more than 42%% in a day without a matching share move: %d (the NAV is back the next day in %d)" % (len(breaks), sum(1 for x in breaks if x[5])))
        for a, t, d, rs_, rn, back, left in breaks[:40]:
            r.log("    %-6s %s assets before %s | shares x%.4f nav x%.4f | %s | rows after %d" % (t, d, usd(a), rs_, rn, "back next day" if back else "stays", left))

        # ------------------------------------------------------------------ 4. there and back
        r.section("4. Share counts that jump 40%+ and come back")
        hist = Counter()
        usd_by = defaultdict(float)
        never = 0
        ex = []
        for t, rr in rows_of.items():
            i = 1
            while i < len(rr):
                s0, s1, n0 = rr[i - 1][1], rr[i][1], rr[i - 1][2]
                if s0 and s1 and n0 and abs(math.log(s1 / s0)) > 0.336 and s0 * n0 >= 2e7:
                    jump = s1 - s0
                    hit = None
                    for j in range(i + 1, min(i + 16, len(rr))):
                        if rr[j][1] and abs(rr[j][1] - s0) <= abs(jump) / 3.0:
                            hit = j
                            break
                    if hit:
                        hist[hit - i] += 1
                        usd_by[hit - i] += abs(jump) * n0
                        if abs(jump) * n0 > 2e8 and len(ex) < 400:
                            ex.append((abs(jump) * n0, t, rr[i][0], rr[hit][0], hit - i, s1 / s0))
                        i = hit + 1
                        continue
                    never += 1
                i += 1
        r.log("jumps undone by two thirds or more, by rows later: %s | dollars by rows later: %s | jumps not undone within 15 rows: %d" % (
            dict(sorted(hist.items())), {k: usd(v) for k, v in sorted(usd_by.items())}, never))
        for v, t, d, d2, n, ratio in sorted(ex, reverse=True)[:30]:
            r.log("    %-6s %s -> back by %s (%d rows) count x%.3f, %s each way" % (t, d, d2, n, ratio, usd(v)))

        # ------------------------------------------------------------------ 5. the named funds
        r.section("5. The histories of the funds named above")
        for t, n in (("CNEQ", 70), ("ALAI", 45), ("ATFV", 45), ("ARKQ", 14), ("FSOL", 12), ("PDEC", 14), ("XBOX", 50), ("DNOV", 14), ("VGT", 0), ("BITQ", 0), ("MSTU", 0)):
            rr = rows_of.get(t, [])
            pr = profiles.get(t) or {}
            r.log("%s (%s, inception %s, %d rows): %s" % (t, (pr.get("description") or "")[:40], str(pr.get("inception_date"))[:10], len(rr), line(rr[-n:]) if n else ""))
        for t, d in (("VGT", "2026-04-21"), ("BITQ", "2025-12-09"), ("MSTU", "2026-08-21"), ("FSOL", "2026-11-05"), ("OWNB", "2025-12-10"), ("OUNZ", "2025-12-05")):
            rr = rows_of.get(t, [])
            i = next((j for j, x in enumerate(rr) if x[0] >= d), None)
            if i is not None:
                r.log("%s around %s: %s" % (t, d, line(rr[max(0, i - 3):i + 4])))
        r.kv(funds=len(rows_of), recorded_split_funds=len(rec), whole_ratio_jumps=len(whole), figure_disagreements=dis, gap_disagreements=gap_dis, scale_breaks=len(breaks))
        r.ok("measured; nothing written")


if __name__ == "__main__":
    main()
