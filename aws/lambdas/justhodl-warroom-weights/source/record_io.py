"""record_io - what the canaries' record reads and writes. The arithmetic is canary_record.py; nothing is computed here.

Reads, from the warehouse only:
  * every series the macro grid and the plumbing aggregator turn into a canary - asked of the site's own resolver,
    one request at a time (no provider is called, no key is used);
  * the CISS series and today's percentiles from the CISS engine's document;
  * the S&P 500's daily closes from the bar bank;
  * each engine's own reading today (the grid's, the plumbing aggregator's, the CISS engine's), to check the rebuild.
Writes one document, data/canary-record.json - unless this is a dry run, or the record is too thin to stand.
"""
from datetime import datetime, timezone
import gzip
import json
import os
import time
import urllib.parse
import urllib.request

import canary_record as R

OUT_KEY = "data/canary-record.json"
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
SPX_KEY = "data/warm/tv-bars/universe/I__SPX.json.gz"
PAUSE = 0.25
MIN_TESTED, MIN_BAROMETER_MONTHS, MIN_CLOSES = 100, 400, 20000
HERE = os.path.dirname(os.path.abspath(__file__))


def _ymd(text):
    t = str(text)[:10]
    if len(t) == 7 and t[4] == "-":                 # a monthly row: the month, dated its 28th
        t += "-28"
    if len(t) != 10 or t[4] != "-" or t[7] != "-":
        return None
    try:
        return int(t[:4]) * 10000 + int(t[5:7]) * 100 + int(t[8:10])
    except ValueError:
        return None


def _freq(days):
    if len(days) < 3:
        return "m"
    tail = days[-60:]
    gaps = sorted(R._ord(b) - R._ord(a) for a, b in zip(tail, tail[1:]))
    g = gaps[len(gaps) // 2]
    return "d" if g <= 4 else "w" if g <= 10 else "m" if g <= 45 else "q"


def weekly(points):
    """Last observation of each ISO week, oldest first. points: [(yyyymmdd, value)]."""
    out, last = [], None
    for d, v in points:
        key = (R._ord(d) + 0) // 7                  # R._ord of a Monday is a multiple of 7
        if key == last:
            out[-1] = [d, v]
        else:
            out.append([d, v])
            last = key
    return out


def shape(observations):
    """A resolver answer's observations -> {"f", "n_raw", "p"} as the record takes them."""
    pts = []
    for o in observations or []:
        d, v = (o[0], o[1]) if isinstance(o, (list, tuple)) else (o.get("date"), o.get("value"))
        day = _ymd(d)
        try:
            if day is not None:
                pts.append((day, float(v)))
        except (TypeError, ValueError):
            continue
    pts.sort()
    if len(pts) < 5:
        return None
    f = _freq([d for d, _ in pts])
    return {"f": f, "n_raw": len(pts), "p": weekly(pts) if f == "d" else [[d, v] for d, v in pts]}


def series_ids(defs):
    """Every plain warehouse series the definitions need, in a stable order."""
    ids = []
    for d in defs.get("grid") or []:
        for spec in d["sources"]:
            if spec.startswith(("feed:", "ecb:", "ecbspread:")) or "/" in spec:
                continue
            if spec.startswith(("spread:", "ratio:")):
                ids += spec.split(":", 1)[1].split(":")
            elif spec.startswith("sum:"):
                cur = ""
                for ch in spec[4:] + "+":
                    if ch in "+-":
                        if cur:
                            ids.append(cur)
                        cur = ""
                    else:
                        cur += ch
            else:
                ids.append(spec)
    ids += [p["id"] for p in defs.get("plumbing") or [] if p.get("source") == "FRED"]
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def ask_resolver(sid, timeout=45):
    req = urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "JustHodl-CanaryRecord/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8", "replace"))
    return (payload or {}).get("obs") if isinstance(payload, dict) else None


def closes_from_bars(doc):
    out = []
    for b in (doc or {}).get("bars") or []:
        try:
            day = datetime.fromtimestamp(b[0], timezone.utc)
            out.append([day.year * 10000 + day.month * 100 + day.day, float(b[4])])
        except Exception:  # noqa: BLE001
            continue
    out.sort()
    return out


def ciss_from(doc, vote_first=40):
    """The CISS engine's series as the record takes them; the first forty with a percentile are the ones that vote."""
    ser = (doc or {}).get("series") or []
    items = list(ser.values()) if isinstance(ser, dict) else ser
    out, live, voting = {}, {}, 0
    for i, s in enumerate(items):
        if not isinstance(s, dict):
            continue
        pts = []
        for row in s.get("chart_points") or []:
            try:
                day = _ymd(row[0])
                if day is not None and row[1] is not None:
                    pts.append([day, float(row[1])])
            except Exception:  # noqa: BLE001
                continue
        label = str(s.get("label") or s.get("name") or s.get("key") or i)
        st = s.get("stats") or s
        votes = False
        if st.get("pctile") is not None:
            voting += 1
            votes = voting <= vote_first
            if votes:
                live["ciss:" + label] = round(float(st["pctile"]), 1)
        out["%03d" % i] = {"label": label, "category": s.get("category"), "freq": s.get("freq"), "p": pts, "votes_live": votes}
    return out, live


def live_from(grid_doc, plumb_doc, defs):
    """Each engine's own stress today, by the record's ids."""
    live = {}
    for s in (grid_doc or {}).get("signals") or []:
        if s.get("available") and s.get("stress") is not None:
            live["macro_grid:" + str(s.get("key"))] = float(s["stress"])
    raw = (plumb_doc or {}).get("raw_indicators") or {}
    for p in defs.get("plumbing") or []:
        m = raw.get(p["id"]) if isinstance(raw, dict) else None
        if isinstance(m, dict):
            st = m.get("stress_score_0_100")
            if st is None:
                st = m.get("stress_score")
            if st is not None:
                live["plumbing:" + p["id"]] = float(st)
    return live


def gather(s3, bucket, defs, ask=None, sleep=time.sleep, log=print):
    """Everything the record reads. Returns (inputs, notes)."""
    ask = ask or ask_resolver

    def kept(key, gz=False):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(gzip.decompress(body) if gz else body)

    series, missing = {}, []
    for sid in series_ids(defs):
        try:
            got = shape(ask(sid))
            if got:
                series[sid] = got
            else:
                missing.append(sid)
        except Exception as exc:  # noqa: BLE001
            missing.append(sid)
            log("[record] warehouse did not answer for %s: %s" % (sid, type(exc).__name__))
        sleep(PAUSE)
    raw_closes = closes_from_bars(kept(SPX_KEY, gz=True))
    closes, dropped = R.clean_closes(raw_closes)
    ciss_doc = kept("data/ciss-stress.json")
    ciss, live = ciss_from(ciss_doc)
    try:
        live.update(live_from(kept("data/canary-grid.json"), kept("data/plumbing-stress.json"), defs))
    except Exception as exc:  # noqa: BLE001
        log("[record] an engine's own reading was not read: %s" % type(exc).__name__)
    notes = {"series_asked": len(series_ids(defs)), "series_kept": len(series), "series_missing": missing, "closes_raw": len(raw_closes),
             "closes_dropped": dropped, "ciss_series": len(ciss), "live_readings": len(live), "ciss_document_at": (ciss_doc or {}).get("generated_at")}
    return {"series": series, "closes": closes, "ciss": ciss, "live": live}, notes


def fit_to_stand(record):
    """Why a record must not be published, or None."""
    months = len((record.get("barometer") or {}).get("months") or [])
    if record.get("tested", 0) < MIN_TESTED:
        return "only %s canaries could be rebuilt (need %d)" % (record.get("tested"), MIN_TESTED)
    if months < MIN_BAROMETER_MONTHS:
        return "the barometer has only %d months (need %d)" % (months, MIN_BAROMETER_MONTHS)
    if (record.get("market") or {}).get("closes", 0) < MIN_CLOSES:
        return "only %s index closes (need %d)" % ((record.get("market") or {}).get("closes"), MIN_CLOSES)
    return None


def run(s3, bucket, dry_run=False, ask=None, sleep=time.sleep, log=print, now=None):
    started = time.time()
    with open(os.path.join(HERE, "canary_defs.json"), encoding="utf-8") as fh:
        defs = json.load(fh)
    inputs, notes = gather(s3, bucket, defs, ask=ask, sleep=sleep, log=log)
    record = R.build(defs, inputs["series"], inputs["closes"], ciss=inputs["ciss"], cut_day=inputs["closes"][-1][0] if inputs["closes"] else None, live=inputs["live"])
    now = now or datetime.now(timezone.utc)
    record.update({"engine": "justhodl-warroom-weights", "document": "canary-record.v1", "generated_at": now.isoformat(), "inputs": notes,
                   "duration_s": round(time.time() - started, 1),
                   "note": "What each canary's stress was followed by on the S&P 500, counted on the warehouse's own history. A count of what happened, not a forecast."})
    why = fit_to_stand(record)
    summary = {"ok": why is None, "tested": record.get("tested"), "defined": record.get("defined"), "verdicts": record.get("verdicts"),
               "months": len(record["barometer"]["months"]), "missing": notes["series_missing"], "duration_s": record["duration_s"], "refused": why, "written": False}
    if why:
        log("[record] not published: %s" % why)
    elif not dry_run:
        body = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=OUT_KEY, Body=body, ContentType="application/json; charset=utf-8", CacheControl="max-age=3600")
        summary["written"] = True
        summary["bytes"] = len(body)
    log("[record] %s" % json.dumps(summary)[:600])
    return summary, record
