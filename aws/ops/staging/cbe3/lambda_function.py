"""justhodl-credit-before-equity 2.1.0 - the bond desk reprices a company before the equity desk does.

1.0 read a structural default model for 26 hand-listed issuers - a model computed from each company's own stock - and
compared each day with the day before. 2.0 reads what the credit market itself prints, for every company it can:

  bonds     every US-dollar corporate bond the corporate bonds desk holds (data/corp-bonds.json: the daily holdings of the
            iShares investment-grade, high-yield and emerging-market corporate books), turned into a spread over the
            Treasury par curve (data/warm/treasury-par/curve.json.gz) and gathered by company
  CDS       every corporate reference entity of the CDS desk (data/cds-desk.json: DTCC's public prints)
  stock     the Polygon EOD bank's closes, split-repaired, for every company tied to a listed stock (cbe_names)
  model     the 1.0 structural model (data/cds-monitor.json), kept for its 26 names and said to be a model

A company's spread change is crossed with its stock over the same days. A signal fires only when credit moved past its
own book and the stock has not followed (cbe_core). Since 2.1 every company also has a stress rank that does not look at
the stock (how wide it is for its book, how far it widened over the year and the month), and a theme (cbe_themes: the AI
stack) groups companies the bond market's own sectors keep apart.

Each day's spreads are kept by the bond desk's own bond key under credit/cbe-spreads/, so the one-week and three-month
comparisons build from the board's own ledger; the bond desk's change since its previous holdings date and since the
year's start seed the first two reference days.

The warehouse only: no provider is asked for anything. Output data/credit-before-equity.json (contract
credit-before-equity.v2 - every 1.0 key is still there). Daily 22:35 UTC.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import gzip
import json
import time
import os

import boto3

import bank_enrich          # aws/shared: the whole-market factor book - S3 only
import cbe_core as C
import cbe_names as N
import cbe_study as S
import leaders_tape as T

VERSION = C.VERSION
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BONDS_KEY = "data/corp-bonds.json"
CDS_KEY = "data/cds-desk.json"
MODEL_KEY = "data/cds-monitor.json"
PAR_KEY = "data/warm/treasury-par/curve.json.gz"
OUT_KEY = "data/credit-before-equity.json"
BONDS_OUT_KEY = "data/credit-before-equity-bonds.json"      # every bond the board read, under its company: opened by the page when a company is opened
EVIDENCE_KEY = "data/credit-before-equity-evidence.json"    # what stocks did after each kind of reading, measured on the board's own month-ends; redone when a month finishes
HISTORY_OUT_KEY = "data/credit-before-equity-history.json"  # every company's spread at each kept month-end: the chart in a company's sheet
CALLS_ROOT = "credit/cbe-calls/"                            # one file a holdings day: the chance of a fall the board gave each stock that day, kept as it was said
TRACK_KEY = "credit/cbe-track.json"                         # those chances set beside what happened, once their time is up
STUDY_NEEDS_MS = 420000                                     # the study is started only with this much of the run's time left
MAX_SCORE_DAYS = 4                                          # call days scored in one run (the rest wait for the next)
HIST_KEY = "credit/credit-before-equity-history.json"      # the 1.0 model ledger, still kept
LEDGER_ROOT = "credit/cbe-spreads/"                         # one file a holdings day: every bond's spread
CDS_HIST_KEY = "data/cds-desk-history.json"                 # the CDS desk's daily level of every name it prints
YIELDS_ROOT = "data/warm/official-yields/"                  # the warehouse's government yields (banked by justhodl-bond-warroom and justhodl-repo)
# the government curve of each other currency the board reads: {currency: {maturity: file in the lane}}. Only series the lane
# holds with years of history, so that a spread and its reference day are measured against the same maturities.
CURVE_FILES = {"EUR": {"2Y": "de-2y-bbk", "10Y": "de-10y-bbk", "30Y": "de-30y-bbk"}, "GBP": {"5Y": "gb-5y-boe", "10Y": "gb-10y-boe", "20Y": "gb-20y-boe"},
               "CAD": {"2Y": "ca-2y-boc", "5Y": "ca-5y-boc", "10Y": "ca-10y-boc", "30Y": "ca-long-boc"}, "AUD": {"2Y": "au-2y-rba", "3Y": "au-3y-rba", "5Y": "au-5y-rba", "10Y": "au-10y-rba"}}
CURVE_FROM = "2025-01-01"                                   # days before this are not needed by any window of the daily board
HOLD_ROOT = "credit/cbe-holdings/"                          # one file a month-end: the bond desk's dated holdings (ops 6950, then this engine)
EOD_ROOT = "data/warm/polygon-full/eod/"
SPLITS_ROOT = "data/warm/polygon-bank/splits/"

BOOK_FIELDS = ("name", "sector", "industry", "mcap", "exchange", "country", "security_type", "is_common", "is_adr", "is_etf", "is_fund", "is_preferred",
               "is_warrant_unit", "is_shell", "is_debt_or_structured", "is_otc", "dollar_vol_20d")
MIN_CAP = 5e7
BARS = 330                    # sessions read back: the year's start and a quarter before it
THREADS = 32
HIST_KEEP = 180
MIN_DD_MOVE = 0.05            # the 1.0 model rule, unchanged
MIN_CDS_MOVE_BP = 5.0
BOND_DESK_LATE_DAYS = 5
CDS_DESK_LATE_DAYS = 5
MIN_BONDS, MIN_COMPANIES, MIN_LISTED = 5000, 1000, 300      # under these the bond desk is broken: nothing is published
ORDER = {"CREDIT_LEADS_DOWN": 0, "CREDIT_LEADS_UP": 1, "NONE": 2, "INSUFFICIENT_HISTORY": 3}


# ----------------------------------------------------------------------------------------------------------------- s3
def _s3():
    return boto3.client("s3", REGION)


def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ in ("NoSuchKey", "KeyError") or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def get_json(s3, key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def optional(s3, key):
    try:
        return get_json(s3, key), None
    except Exception as exc:  # noqa: BLE001
        return None, ("not there" if missing(exc) else "%s: %s" % (type(exc).__name__, str(exc)[:120]))


def put_json(s3, key, doc, gz=False):
    body = json.dumps(doc, separators=(",", ":"), default=str).encode()
    extra = {"ContentType": "application/json"}
    if gz:
        body, extra["ContentEncoding"] = gzip.compress(body, 6), "gzip"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, **extra)
    return len(body)


def bank_key(ticker):
    return EOD_ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


# -------------------------------------------------------------------------------------------------------------- readers
def fund_bars(doc):
    """{date, o, h, l, c, v} oldest first from one EOD bank document. Pure."""
    out = []
    for r in (doc or {}).get("rows") or []:
        if not isinstance(r, list) or len(r) < 6 or r[0] is None or r[4] is None:
            continue
        b = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}
        b["date"] = datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date().isoformat()
        out.append(b)
    return out


def months_between(first_iso, last_iso):
    y, m = int(first_iso[:4]), int(first_iso[5:7])
    ly, lm = int(last_iso[:4]), int(last_iso[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def recorded_splits(s3, first_iso, last_iso, want):
    """Polygon's split reference rows for the tickers in `want`: ({ticker: [{date, split_from, split_to}]}, files read)."""
    out, n = {}, 0

    def one(month):
        doc, _ = optional(s3, SPLITS_ROOT + month + ".json.gz")
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, last_iso)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t, d = str(r.get("ticker") or "").upper(), str(r.get("execution_date") or "")
                if t in want and first_iso <= d <= last_iso and r.get("split_from") and r.get("split_to"):
                    out.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
    return out, n


def repair(raw, recorded, bars_back=BARS):
    """A stock's closes as one continuous, split-adjusted history: ({day: close}, splits rebased). Pure.
    bars_back None reads the whole banked history (the study); the daily board needs only the last year and a quarter."""
    own, _ = T.own_history(raw if bars_back is None else raw[-(bars_back + 40):])
    bars, events = T.rebase(own, recorded or [], [])
    out = {}
    for b in (bars if bars_back is None else bars[-bars_back:]):
        try:
            c = float(b["c"])
        except (TypeError, ValueError, KeyError):
            continue
        if c > 0 and c == c:
            out[b["date"]] = c
    return out, events


def bank_names(ticker):
    """The price bank's spellings of a factor-book ticker: BRK-B is BRK.B there."""
    t = str(ticker).upper()
    return [t] if "-" not in t else [t, t.replace("-", ".")]


def read_closes(s3, tickers, first_iso, last_iso, bars_back=BARS):
    """({ticker: {day: close}}, tickers with no file, splits rebased) from the Polygon EOD bank, split-repaired."""
    want = {n for t in tickers for n in bank_names(t)}
    recorded, _ = recorded_splits(s3, first_iso, last_iso, want)

    def one(t):
        for name in bank_names(t):
            doc, err = optional(s3, bank_key(name))
            if doc is None:
                continue
            try:
                closes, events = repair(fund_bars(doc), recorded.get(name), bars_back)
            except Exception:  # noqa: BLE001
                return t, None, 0
            return t, closes, len(events)
        return t, None, 0
    out, none, n_events = {}, [], 0
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, closes, n in pool.map(one, sorted(tickers)):
            if closes:
                out[t] = closes
                n_events += n
            else:
                none.append(t)
    return out, none, n_events


def other_curves(s3, log=print):
    """({currency: {day: {maturity: yield}}}, what could not be read) from the warehouse's government yields. A currency is
    offered only when every one of its maturities was read; a day is kept only when it carries all of them."""
    out, missing_ = {}, []
    for ccy, files in CURVE_FILES.items():
        series = {}
        for tenor, slug in files.items():
            doc, err = optional(s3, YIELDS_ROOT + slug + ".json")
            obs = [(o.get("date"), o.get("value")) for o in ((doc or {}).get("observations") or []) if isinstance(o, dict)]
            series[tenor] = {d: float(v) for d, v in obs if isinstance(d, str) and d >= CURVE_FROM and isinstance(v, (int, float))}
            if not series[tenor]:
                missing_.append("%s %s (%s)" % (ccy, tenor, err or "no observations"))
        if all(series.values()):
            days = set.intersection(*(set(v) for v in series.values()))
            if days:
                out[ccy] = {d: {t: series[t][d] for t in files} for d in days}
    if missing_:
        log("[cbe] government curves not read: %s" % "; ".join(missing_))
    return out, missing_


def stored_days(s3, root):
    """The days a dated family of files holds, oldest first."""
    days, token = set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": root}
        if token:
            kw["ContinuationToken"] = token
        got = s3.list_objects_v2(**kw)
        for o in got.get("Contents") or []:
            name = o["Key"][len(root):]
            if len(name) >= 10 and name[4] == "-" and name[7] == "-":
                days.add(name[:10])
        token = got.get("NextContinuationToken")
        if not token:
            return sorted(days)


class MemoMatcher:
    """The name matcher with its answers kept: the same issuer is asked for at every month-end of the history."""

    def __init__(self, matcher):
        self.inner, self.rows, self.seen = matcher, matcher.rows, {}

    def match(self, key, cut=False, fund_sector=None, foreign=False):
        k = (key, cut, fund_sector, foreign)
        if k not in self.seen:
            self.seen[k] = self.inner.match(key, cut, fund_sector, foreign=foreign)
        return self.seen[k]


def build_panel(s3, now=None, log=print, first=None):
    """The board rebuilt at every kept month-end, and what each listed company's stock did afterwards. Writes nothing.

    Returns {"panel": rows (cbe_study.PANEL_COLS), "history": {dates, companies {id: [spread a date]}, names}, "meta"}.
    """
    now = now or datetime.now(timezone.utc)
    today = now.date().isoformat()
    t0 = time.time()
    days = [d for d in S.month_ends(stored_days(s3, HOLD_ROOT), today) if not first or d >= first]
    if len(days) < 6:
        raise RuntimeError("the history holds %d month-ends; the study needs at least six" % len(days))
    par_rows = get_json(s3, PAR_KEY).get("rows") or {}
    book_rows = bank_enrich.rows(fields=list(BOOK_FIELDS)) or []
    listed = listed_companies(book_rows)
    known = {r["symbol"] for r in listed}
    matcher = MemoMatcher(N.Matcher(listed + [x for x in N.EXTRA_LISTED if x["symbol"] not in known]))
    snaps, months, tickers, skipped = {}, [], set(), []
    spreads, names, books = {}, {}, {}
    for day in days:
        doc = get_json(s3, HOLD_ROOT + day + ".json.gz")
        bonds = [dict(zip(doc["cols"], r)) for r in doc.get("rows") or []]
        points, counts, par_days = C.bond_points(bonds, day, par_rows, {})
        if par_days.get("as_of") is None or counts["with a spread"] < max(MIN_BONDS // 2, 0.6 * counts["read"]):       # a month the feed served without its yields is not a month of the study
            skipped.append((day, "no Treasury curve" if par_days.get("as_of") is None else "%d of %d bonds with a spread" % (counts["with a spread"], counts["read"])))
            continue
        snaps[day] = C.snapshot_of(points, day)["bonds"]
        windows = C.pick_windows(day, [d for d in snaps if d < day])
        rows, _ = C.companies(points, matcher, snaps, windows)
        C.add_excess(rows, C.book_medians(rows, windows))
        C.add_stress(rows, windows, day)
        months.append((day, windows, rows))
        books[day] = C.book_levels(rows)
        tickers |= {r["ticker"] for r in rows if r["ticker"]}
        for r in rows:
            if r["spread_bp"] is not None:
                spreads.setdefault(r["id"], {})[day] = r["spread_bp"]
                names[r["id"]] = r["name"]
        for old in [d for d in snaps if C.days_between(d, day) > 400]:           # a snapshot is a reference for a year at most
            del snaps[old]
    log("[cbe study] %d month-ends rebuilt (%s .. %s) in %.0f s, %d skipped %s; %d listed companies ever tied" % (
        len(months), months[0][0], months[-1][0], time.time() - t0, len(skipped), skipped[:4], len(tickers)))
    closes, no_file, n_events = read_closes(s3, tickers | {"SPY"}, months[0][0], today, bars_back=None)
    if not closes.get("SPY"):
        raise RuntimeError("the price bank holds no SPY: the study has no market to measure against")
    tape = S.Tape(closes["SPY"])
    panel = []
    for day, windows, rows in months:
        panel.extend(S.panel_rows(day, windows, rows, closes, tape))
    kept = [d for d, _, _ in months]
    log("[cbe study] stocks: %d of %d read whole from the price bank (%s .. %s), %d splits rebased; panel %d rows; %.0f s in all" % (
        len(closes), len(tickers) + 1, tape.days[0], tape.days[-1], n_events, len(panel), time.time() - t0))
    return {"panel": panel, "history": {"dates": kept, "companies": {i: [v.get(d) for d in kept] for i, v in spreads.items()}, "names": names,
                                        "books": {b: [(books.get(d) or {}).get(b) for d in kept] for b in C.BOOK_WORDS}},
            "meta": {"month_ends": kept, "skipped": skipped, "listed_ever": len(tickers), "no_price_file": len(no_file), "first_session": tape.days[0], "last_session": tape.days[-1],
                     "built_at": now.isoformat(timespec="seconds")}}


def study(s3, now=None, log=print):
    """The board's own history read whole: {"evidence": the evidence document, "history": the month-by-month document}. Writes nothing."""
    now = now or datetime.now(timezone.utc)
    t0 = time.time()
    built = build_panel(s3, now, log)
    try:
        cds = build_cds_panel(s3, now, log)
    except Exception as exc:  # noqa: BLE001 - the weekly CDS record is one part of the evidence; the rest stands without it
        log("[cbe study] the traded CDS could not be studied: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        cds = None
    ev = S.evidence(built["panel"], cds["panel"] if cds else None)
    meta = built["meta"]
    ev.update({"version": VERSION, "generated_at": now.isoformat(timespec="seconds"), "skipped": [list(x) for x in meta["skipped"]], "stocks_to": meta["last_session"],
               "seconds": round(time.time() - t0, 1),
               "caveats": [
                   "A company is tied to a stock through today's listings. One that has since been delisted is not in the history at all: the bankruptcies - the worst outcomes after a bad "
                   "credit reading - and the take-overs are both missing. The averages after a wide spread are flattered by that.",
                   "The history is %d month-ends, %s to %s: one credit cycle at most, and it holds one bear market." % (ev["month_ends"], ev["first"], ev["last"]),
                   "Returns are price returns; dividends are not in them. The stock is entered at the close one session after the holdings day, because a fund publishes a day's holdings "
                   "the next morning.",
                   "A reading is a month-end reading. What a spread did between two month-ends, and what the stock did the same day, is not seen by this record."],
               "method": ("At every kept month-end the board is rebuilt exactly as the live run builds it - the same bonds, spread, company roll-up, book and rule - and each listed company's "
                          "reading is set beside what its stock did over the next %s. Against the board's average stock means: less the plain average of every stock on the board that month. "
                          "How sure is the month-by-month average over its standard error; past 2 either way is unlikely to be luck for one test, and with many tests at once the bar is 3."
                          % " and the next ".join("%d sessions" % n for _, _, n in S.HORIZONS))})
    hist = built["history"]
    history = {"version": VERSION, "generated_at": ev["generated_at"], "dates": hist["dates"], "companies": hist["companies"], "books": hist["books"],
               "note": "Every company's spread over Treasuries at each kept month-end, in basis points (null where its bonds were not in the dated holdings), and the middle company of each book."}
    ev["history"] = {"key": HISTORY_OUT_KEY, "month_ends": len(hist["dates"]), "companies": len(hist["companies"]), "first": hist["dates"][0], "last": hist["dates"][-1]}
    log("[cbe study] evidence: %d company-months over %d month-ends, %d readings, in %.0f s" % (ev["company_months"], ev["month_ends"], len(ev["readings"]), time.time() - t0))
    return {"evidence": ev, "history": history, "panel": built["panel"], "cds_panel": cds["panel"] if cds else None}


def study_due(evidence, held_days, today):
    """Why the study should be redone now, or None. It is redone when a month has finished since it was last done."""
    if not isinstance(evidence, dict) or not evidence.get("readings"):
        return "the evidence has never been written"
    if evidence.get("version") != VERSION:
        return "the evidence was written by engine %s" % evidence.get("version")
    ends = S.month_ends(held_days, today)
    if ends and ends[-1] > str(evidence.get("last") or ""):
        return "a month has finished since: %s" % ends[-1]
    if str(evidence.get("stocks_to") or "")[:7] < today[:7]:
        return "a calendar month has passed: the newest readings have more of their outcome now"
    return None


def forecast(evidence, band, third):
    """({"risk_1m", "risk_3m", "lo_3m", "hi_3m"}, what the 3-month chance rests on) for a level band and a volatility third, from the evidence's own tables."""
    if not isinstance(evidence, dict) or band is None:
        return {}, None
    out, basis = {}, None
    for key, name in (("h1", "risk_1m"), ("h3", "risk_3m")):
        table = {int(k): v for k, v in (((evidence.get("risk") or {}).get(key)) or {}).items()}
        chance, why = S.chance(table, band, third)
        out[name] = chance
        if key == "h3":
            basis = why
    level = next((x for x in evidence.get("readings") or [] if x.get("key") == "level:%d" % band), None)
    if level:
        out["lo_3m"], out["hi_3m"] = (level.get("h3") or {}).get("p10"), (level.get("h3") or {}).get("p90")
    return out, basis


def score_calls(s3, track, closes, tape, log=print):
    """Set the chances the board gave on earlier days beside what happened, for every call day whose time is now up. Returns the track.

    track: {"days": {"h1": {call day: {"n", "skipped", "buckets": [[n, said in sum, fell], ...]}}, "h3": {...}}}. A call day is
    scored once for each horizon. At most MAX_SCORE_DAYS days a run; the rest wait."""
    track = track if isinstance(track, dict) and isinstance(track.get("days"), dict) else {"days": {}}
    days = stored_days(s3, CALLS_ROOT)
    done = 0
    for key, _, n in S.HORIZONS:
        mine = track["days"].setdefault(key, {})
        for day in days:
            if day in mine or done >= MAX_SCORE_DAYS:
                continue
            entry, exit_ = tape.window(day, n)
            if entry is None:                                   # its time is not up yet, and so is every later day's
                break
            doc, err = optional(s3, CALLS_ROOT + day + ".json.gz")
            if doc is None:
                log("[cbe track] the calls of %s could not be read (%s)" % (day, err))
                continue
            cols = doc.get("cols") or []
            buckets = [[0, 0.0, 0] for _ in S.CHECK_EDGES[1:]]
            n_scored = skipped = 0
            for row in doc.get("rows") or []:
                r = dict(zip(cols, row))
                said, worst = r.get("risk_" + {"h1": "1m", "h3": "3m"}[key]), tape.worst(closes.get(r.get("t")), day, n)
                if said is None or worst is None:
                    skipped += 1
                    continue
                for i, (lo, hi) in enumerate(zip(S.CHECK_EDGES, S.CHECK_EDGES[1:])):
                    if lo <= said < hi:
                        buckets[i][0] += 1
                        buckets[i][1] += said
                        buckets[i][2] += 1 if worst <= -S.BIG else 0
                n_scored += 1
            mine[day] = {"n": n_scored, "skipped": skipped, "to": exit_, "buckets": [[b[0], round(b[1], 1), b[2]] for b in buckets]}
            done += 1
            log("[cbe track] %s scored over %s: %d stocks (%d with no price or no chance)" % (day, key, n_scored, skipped))
    return track


def track_summary(track, call_days, tape):
    """The track as the page reads it: for each horizon, what was said beside what happened, and when the next call day is due."""
    out = {"calls_kept": len(call_days), "first_call": call_days[0] if call_days else None, "last_call": call_days[-1] if call_days else None, "horizons": []}
    for key, words, n in S.HORIZONS:
        mine = ((track or {}).get("days") or {}).get(key) or {}
        buckets = []
        for i, (lo, hi) in enumerate(zip(S.CHECK_EDGES, S.CHECK_EDGES[1:])):
            cnt = sum(d["buckets"][i][0] for d in mine.values())
            if cnt:
                buckets.append({"from": lo, "to": min(hi, 100), "n": cnt, "said": round(sum(d["buckets"][i][1] for d in mine.values()) / cnt, 1),
                                "happened": round(100.0 * sum(d["buckets"][i][2] for d in mine.values()) / cnt, 1)})
        waiting = [d for d in call_days if d not in mine]
        due = None
        if waiting and tape is not None:
            i = tape.session_on_or_before(waiting[0])
            ahead = (i + S.SKIP + n) - (len(tape.days) - 1) if i is not None else None
            due = {"call_day": waiting[0], "sessions_left": max(ahead, 0) if ahead is not None else None}
        out["horizons"].append({"key": key, "words": words, "sessions": n, "days_scored": len(mine), "stocks_scored": sum(d["n"] for d in mine.values()), "buckets": buckets, "next_due": due})
    return out


def build_cds_panel(s3, now=None, log=print):
    """Every traded CDS the board ties to a listed stock, week by week, and what the stock did afterwards. Writes nothing."""
    now = now or datetime.now(timezone.utc)
    today = now.date().isoformat()
    desk = get_json(s3, CDS_KEY)
    hist = (get_json(s3, CDS_HIST_KEY).get("names")) or {}
    listed = listed_companies(bank_enrich.rows(fields=list(BOOK_FIELDS)) or [])
    known = {r["symbol"] for r in listed}
    matcher = N.Matcher(listed + [x for x in N.EXTRA_LISTED if x["symbol"] not in known])
    series, names = {}, {}
    for group, foreign in (("us_corp", False), ("global_corp", True)):
        blk = (desk.get("groups") or {}).get(group) or {}
        for kind in ("rows", "unpriced", "dormant"):
            for x in blk.get(kind) or []:
                if not isinstance(x, dict) or not x.get("name") or not x.get("key"):
                    continue
                key, _ = N.issuer_key(x["name"])
                co, _ = matcher.match(key, False, None, foreign=foreign)
                pts = (hist.get(x["key"]) or {}).get("points") or []
                if co and len(pts) > len(series.get(co["symbol"]) or []):        # two contracts on one company: the one that prints most
                    series[co["symbol"]], names[co["symbol"]] = pts, x["name"]
    first = min((p[0][0] for p in series.values() if p), default=today)
    closes, _, _ = read_closes(s3, set(series) | {"SPY"}, first, today, bars_back=None)
    if not closes.get("SPY"):
        raise RuntimeError("the price bank holds no SPY: the study has no market to measure against")
    tape = S.Tape(closes["SPY"])
    panel = S.cds_panel(series, closes, tape)
    log("[cbe study] traded CDS: %d names tied to a listed stock, %d with enough prints; %d name-weeks from %s" % (len(series), len({r["t"] for r in panel}), len(panel), min((r["day"] for r in panel), default="-")))
    return {"panel": panel, "names": names, "meta": {"tied": len(series), "studied": len({r["t"] for r in panel}), "first_print": first, "built_at": now.isoformat(timespec="seconds")}}


def listed_companies(rows):
    """The factor book's listed companies: common stock or a depositary receipt on an exchange, $50M or more."""
    out = []
    for r in rows or []:
        if not r.get("symbol") or not r.get("name") or (r.get("mcap") or 0) < MIN_CAP:
            continue
        if r.get("is_common") is False or r.get("is_otc") or r.get("is_etf") or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
            continue
        if r.get("security_type") not in (None, "CS", "ADRC"):
            continue
        out.append(r)
    return out


def ledger_days(s3):
    """The holdings days the board's own ledger holds."""
    days, token = set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": LEDGER_ROOT}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        for o in page.get("Contents") or []:
            name = o["Key"][len(LEDGER_ROOT):]
            if len(name) == 18 and name.endswith(".json.gz"):
                days.add(name[:10])
        token = page.get("NextContinuationToken")
        if not token:
            return days


# ---------------------------------------------------------------------------------------------------------- the model
def model_rows(model_doc, hist, today, last_close):
    """The 1.0 board for its own names, by its own rule (each day against the observation before it). Returns {ticker: row}."""
    snc = (model_doc or {}).get("single_name_cds") or {}
    obs = hist.setdefault("obs", {})
    out = {}
    for grp in ("corporates", "banks"):
        for r in snc.get(grp) or []:
            tk = str(r.get("ticker") or "").upper()
            if not tk:
                continue
            dd, cds, price = C.f(r.get("distance_to_default"), 6), C.f(r.get("synthetic_cds_bp"), 4), last_close.get(tk)
            rec = obs.setdefault(tk, {})
            rec[today] = {"dd": dd, "cds": cds, "px": price}
            dates = sorted(rec)
            prior_d = dates[-2] if len(dates) >= 2 else None
            prior = rec.get(prior_d) if prior_d else None
            d_dd = d_cds = d_px = None
            if prior:
                if dd is not None and C.f(prior.get("dd"), 6) is not None:
                    d_dd = round(dd - C.f(prior["dd"], 6), 3)
                if cds is not None and C.f(prior.get("cds"), 4) is not None:
                    d_cds = round(cds - C.f(prior["cds"], 4), 1)
                pp = C.f(prior.get("px"), 6)
                if price is not None and pp:
                    d_px = round(100 * (price / pp - 1), 2)
            direction = None
            if d_dd is not None and abs(d_dd) >= MIN_DD_MOVE:
                direction = "IMPROVING" if d_dd > 0 else "DETERIORATING"
            elif d_cds is not None and abs(d_cds) >= MIN_CDS_MOVE_BP:
                direction = "IMPROVING" if d_cds < 0 else "DETERIORATING"
            flat = d_px is not None and abs(d_px) <= C.FLAT_BAND_PCT
            signal = "INSUFFICIENT_HISTORY" if prior is None else "NONE"
            if direction == "IMPROVING" and d_px is not None and (flat or d_px < 0):
                signal = "CREDIT_LEADS_UP"
            elif direction == "DETERIORATING" and d_px is not None and (flat or d_px > 0):
                signal = "CREDIT_LEADS_DOWN"
            out[tk] = {"name": r.get("name"), "group": r.get("group") or grp, "distance_to_default": dd, "synthetic_cds_bp": cds, "default_prob_5y_pct": C.f(r.get("default_prob_5y_pct"), 4),
                       "regime": r.get("regime"), "peer_rank": r.get("peer_rank"), "market_cap_usd_bn": C.f(r.get("market_cap_usd_bn"), 3), "d_distance_to_default": d_dd,
                       "d_synthetic_cds_bp": d_cds, "model_d_price_pct": d_px, "model_credit_direction": direction, "model_signal": signal, "prior_obs_date": prior_d, "hist_n": len(dates)}
    for rec in obs.values():
        for old in sorted(rec)[:-HIST_KEEP]:
            rec.pop(old, None)
    return out


# ---------------------------------------------------------------------------------------------------------- the build
ISSUER_COLS = ["id", "ticker", "name", "industry", "book", "country", "n_bonds", "value", "yield_pct", "dur", "spread_bp", "d_w1", "d_m1", "x_m1", "pct_m1", "n_m1", "d_m3", "d_ytd",
               "cds_bp", "cds_d1m", "px_m1", "direction", "signal", "distressed", "tie",
               "pct_ytd", "stress", "rk_level", "rk_year", "rk_month", "theme", "layer", "cds_d3m", "cds_rank", "px_ytd", "pct_w1", "pct_m3",          # added by 2.1: always after the 2.0 columns
               "cell", "pd_1y", "pd_5y", "wam", "due2y", "coupon", "step_bp", "px", "px_min", "s_short", "s_long", "curve_bp", "n_spread", "n_issuers", "fund_sector",
               "from_m1", "bar_m1", "x_w1", "x_m3", "x_ytd", "px_w1", "px_m3", "cds_d1w", "cds_last", "vol", "band", "risk_1m", "risk_3m", "lo_3m", "hi_3m", "ccy", "n_other"]        # added by 3.0: always after the 2.1 columns
CALL_COLS = ["t", "close", "s", "band", "vt", "vol", "risk_1m", "risk_3m", "cell", "signal"]
HOLD_COLS = ["k", "issuer", "sector", "country", "ccy", "book", "coupon", "maturity", "issued", "rate", "price", "dur", "demand", "nf"]          # as ops 6950 kept the month-ends
MARKET = "SPY"
BOND_COLS = ["co", "issuer", "coupon", "maturity", "yield_pct", "price", "dur", "tenor", "spread_bp", "d_w1", "d_m1", "d_m3", "d_ytd", "value", "why", "ccy"]


def build(s3, now=None, log=print, evidence=None):
    """Everything the run will publish and keep, with nothing written: {payload, snapshots, hist, bonds, holdings, calls}. Raises when the bond desk is broken.

    evidence: the evidence document (what stocks did after each kind of reading); with it every listed company carries the chance of a fall its reading has meant."""
    now = now or datetime.now(timezone.utc)
    today = now.date().isoformat()
    degraded, gaps = [], []

    bonds_doc = get_json(s3, BONDS_KEY)
    issues = bonds_doc.get("issues") or {}
    as_of, move_as_of, ytd_as_of = issues.get("as_of"), issues.get("move_as_of"), issues.get("ytd_as_of")
    if not as_of or not issues.get("bonds"):
        raise RuntimeError("refusing to publish: the corporate bonds desk holds no bonds or no holdings date")
    par = get_json(s3, PAR_KEY)
    par_rows = par.get("rows") or {}
    refs = {k: v for k, v in (("chg", move_as_of), ("chg_y", ytd_as_of)) if v and v < as_of}
    curves, curves_missing = other_curves(s3, log)
    points, counts, par_days = C.bond_points(issues["bonds"], as_of, par_rows, refs, curves)
    for ccy in CURVE_FILES:
        if ccy not in (par_days.get("other") or {}):
            degraded.append("The %s government curve could not be read for %s: bonds in that currency are left out on this run." % (ccy, as_of))
    if par_days.get("as_of") is None:
        raise RuntimeError("refusing to publish: no Treasury par curve within %d days of %s" % (C.PAR_STALE_DAYS, as_of))
    if counts["with a spread"] < MIN_BONDS:
        raise RuntimeError("refusing to publish: only %d bonds have a spread (the bond desk usually holds %d or more)" % (counts["with a spread"], MIN_BONDS))
    late = C.days_between(as_of, today)
    if late > BOND_DESK_LATE_DAYS:
        degraded.append("The corporate bonds desk's holdings date is %s, %d days ago: the credit leg is that old." % (as_of, late))
    log("[cbe] bonds: %d read, %d with a spread, as of %s (changes since %s, year since %s)" % (counts["read"], counts["with a spread"], as_of, move_as_of, ytd_as_of))

    # the ledger: today's spreads, and the two reference days the bond desk's own changes give
    have = ledger_days(s3)
    snapshots = [C.snapshot_of(points, as_of)]
    mem = {as_of: snapshots[0]}
    for ref in sorted(set(refs.values())):
        if ref not in have and par_days.get(ref):
            snap = C.snapshot_of(points, ref, derived_from=as_of)
            if snap["n"]:
                snapshots.append(snap)
                mem[ref] = snap
    windows = C.pick_windows(as_of, sorted((have | set(mem)) - {as_of}))
    snaps, window_meta = {}, {}
    for wkey, ref in windows.items():
        words = C.window_words(wkey)
        if not ref:
            aim = {k: (a, t) for k, _, a, t in C.WINDOWS}.get(wkey)
            when = C.available_from(as_of, sorted(have | set(mem) | {as_of}), aim[0], aim[1]) if aim else None          # today's own day is stored by this run
            window_meta[wkey] = {"words": words, "ref": None, "available_from": when,
                                 "why": ("The board's ledger holds no day %d days back yet, give or take %d; one will be from about %s." % (aim[0], aim[1], when)) if when else
                                        "The board's ledger holds no day near enough yet; it adds a day each run."}
            continue
        snap = mem.get(ref)
        if snap is None:
            snap, err = optional(s3, LEDGER_ROOT + ref + ".json.gz")
            if snap is None:
                window_meta[wkey] = {"words": words, "ref": None, "why": "The ledger's day %s could not be read (%s)." % (ref, err)}
                windows[wkey] = None
                continue
        stored = dict(snap.get("bonds") or {})
        filled = 0
        if ref in refs.values() and not snap.get("derived_from"):
            # the ledger's day does not hold a bond the board reads today (a bond in another currency, read since 3.0; a bond a fund bought since):
            # where the bond desk's own yield change reaches back to that very day, the bond's spread that day is worked back from it
            for pt in points:
                was = pt["ref"].get(ref)
                if was is not None and pt["k"] not in stored:
                    stored[pt["k"]] = was
                    filled += 1
        snaps[ref] = stored
        window_meta[wkey] = {"words": words, "ref": ref, "days": C.days_between(ref, as_of), "n_bonds": len(snaps[ref]), "n_worked_back": filled,
                             "source": ("worked back from the bond desk's own yield change since that day" if snap.get("derived_from") else
                                        "the board's own ledger" + ("; %d bonds it did not hold that day are worked back from the bond desk's own yield change" % filled if filled else ""))}

    book_rows = bank_enrich.rows(fields=list(BOOK_FIELDS)) or []
    listed = listed_companies(book_rows)
    known = {r["symbol"] for r in listed}
    first_iso = (date.fromisoformat(min([r for r in windows.values() if r] + [as_of])) - timedelta(days=60)).isoformat()
    extra = [x for x in N.EXTRA_LISTED if x["symbol"] not in known]
    extra_closes, _, _ = read_closes(s3, [x["symbol"] for x in extra], first_iso, today) if extra else ({}, [], 0)
    extra = [x for x in extra if extra_closes.get(x["symbol"]) and C.days_between(max(extra_closes[x["symbol"]]), today) <= 10]
    matcher = N.Matcher(listed + extra)
    log("[cbe] factor book: %d rows, %d listed companies, %d more the book does not hold but the price bank does (%s)" % (len(book_rows), len(listed), len(extra), ", ".join(x["symbol"] for x in extra)))

    rows, ties = C.companies(points, matcher, snaps, windows)
    medians = C.book_medians(rows, windows)
    C.add_excess(rows, medians)
    long = C.add_stress(rows, windows, as_of)
    n_spread = sum(1 for r in rows if r["spread_bp"] is not None)
    n_listed = sum(1 for r in rows if r["ticker"])
    if n_spread < MIN_COMPANIES or n_listed < MIN_LISTED:
        raise RuntimeError("refusing to publish: %d companies with a spread, %d of them listed (the board usually has %d and %d or more)" % (n_spread, n_listed, MIN_COMPANIES, MIN_LISTED))

    cds_doc, cds_err = optional(s3, CDS_KEY)
    if cds_doc is None:
        degraded.append("The CDS desk's document could not be read (%s): no traded CDS on this run." % cds_err)
    cds_priced, cds_bare, cds_by_ticker, cds_by_key = C.cds_rows(cds_doc, matcher)
    cds_as_of = str((cds_doc or {}).get("as_of") or "")[:10] or None
    if cds_as_of and C.days_between(cds_as_of, today) > CDS_DESK_LATE_DAYS:
        degraded.append("The CDS desk's levels are as of %s, %d days ago." % (cds_as_of, C.days_between(cds_as_of, today)))

    model_doc, model_err = optional(s3, MODEL_KEY)
    if model_doc is None:
        degraded.append("The structural model's document could not be read (%s): the model columns are empty on this run." % model_err)
    model_tickers = {str(r.get("ticker") or "").upper() for g in ("corporates", "banks") for r in ((model_doc or {}).get("single_name_cds") or {}).get(g) or [] if r.get("ticker")}

    tickers = {r["ticker"] for r in rows if r["ticker"]} | set(cds_by_ticker) | {t for t in model_tickers if t in matcher.rows}
    closes, no_file, n_events = read_closes(s3, (tickers | {MARKET}) - set(extra_closes), first_iso, today)
    market = closes.pop(MARKET, None) if MARKET not in tickers else closes.get(MARKET)       # the market fund is read for its calendar, not as a company
    no_file = [t for t in no_file if t != MARKET or MARKET in tickers]
    closes.update({t: c for t, c in extra_closes.items() if t in tickers})
    newest = max((max(c) for c in closes.values()), default=None)
    log("[cbe] stocks: %d of %d read from the price bank, newest close %s, %d splits rebased, %d with no file" % (len(closes), len(tickers), newest, n_events, len(no_file)))
    if tickers and len(no_file) > 0.10 * len(tickers):
        degraded.append("%d of %d tied companies have no file in the price bank: their stock leg is empty." % (len(no_file), len(tickers)))

    hist, _ = optional(s3, HIST_KEY)
    hist = hist if isinstance(hist, dict) else {"obs": {}}
    model = model_rows(model_doc, hist, today, {t: c[max(c)] for t, c in closes.items()})

    # ------------------------------------------------------------------ what each listed company's reading has meant for its stock
    tape = S.Tape(market or {})
    for r in rows:
        r["vol"] = tape.volatility(closes.get(r["ticker"]), as_of) if r["ticker"] else None
        r["band"] = S.band_of(r["spread_bp"])
    vol_cuts = S.thirds([r["vol"] for r in rows if r["ticker"]])
    n_forecast = 0
    for r in rows:
        r["vt"] = S.third_of(r["vol"], vol_cuts)
        measured = r["ticker"] and closes.get(r["ticker"]) and r["book"] in C.DOLLAR_BOOKS            # the history holds dollar bonds only: a non-dollar company gets no chance
        r["fc"], r["fc_basis"] = forecast(evidence, r["band"], r["vt"]) if measured else ({}, None)
        n_forecast += 1 if r["fc"].get("risk_3m") is not None else 0

    # ------------------------------------------------------------------ the listed companies (names) and every company (issuers)
    by_ticker = {r["ticker"]: r for r in rows if r["ticker"]}
    names = []
    for tk in sorted(tickers | set(model)):
        co, r, cds, mod = matcher.rows.get(tk), by_ticker.get(tk), cds_by_ticker.get(tk), model.get(tk)
        cl = closes.get(tk)
        win = (r or {}).get("win") or {}
        px = {w: C.price_change(cl, ref)[0] for w, ref in windows.items() if ref}
        last_day = max(cl) if cl else None
        direction, bar = C.credit_direction(win.get(C.PRIMARY))
        basis = "bond spreads" if direction is not None else None
        d_px = px.get(C.PRIMARY)
        cds_dir = C.cds_direction(cds)
        if direction is None and cds_dir is not None:
            direction = {"WIDER": "DETERIORATING", "TIGHTER": "IMPROVING", "FLAT": "FLAT"}[cds_dir]
            basis = "traded CDS"
            d_px = C.price_change(cl, (date.fromisoformat(cds["last_date"]) - timedelta(days=30)).isoformat())[0] if cl and cds.get("last_date") else None
        signal, state = C.signal_of(direction, d_px)
        agrees = None
        if basis == "bond spreads" and cds_dir in ("WIDER", "TIGHTER") and direction in ("DETERIORATING", "IMPROVING"):
            agrees = "confirms" if (cds_dir == "WIDER") == (direction == "DETERIORATING") else "contradicts"
        conflict = agrees == "contradicts" and signal in C.LEADS
        if conflict:
            signal = "NONE"                 # the bonds say one thing and a liquid CDS the other: the two credit markets disagree, no lead is called
        row = {"ticker": tk, "name": (co or {}).get("name") or (mod or {}).get("name") or (r or {}).get("name"),
               "group": (mod or {}).get("group") or ("banks" if (r or {}).get("industry") == "Banking" else "corporates"),
               "industry": (r or {}).get("industry"), "book": (r or {}).get("book"),
               "spread_bp": (r or {}).get("spread_bp"), "n_bonds": (r or {}).get("n_bonds"),
               "d_spread_bp": (win.get(C.PRIMARY) or {}).get("d_bp"), "x_spread_bp": (win.get(C.PRIMARY) or {}).get("x_bp"), "from_spread_bp": (win.get(C.PRIMARY) or {}).get("from_bp"),
               "bar_bp": bar, "n_matched": (win.get(C.PRIMARY) or {}).get("n"), "d_spread_w1": (win.get("w1") or {}).get("d_bp"), "d_spread_m3": (win.get("m3") or {}).get("d_bp"),
               "d_spread_ytd": (win.get("ytd") or {}).get("d_bp"),
               "cds_bp": (cds or {}).get("spread_bp"), "cds_d1w": (cds or {}).get("d1w"), "cds_d1m": (cds or {}).get("d1m"), "cds_d3m": (cds or {}).get("d3m"),
               "cds_last": (cds or {}).get("last_date"), "cds_says": agrees,
               "price": round(cl[last_day], 4) if cl else None, "price_date": last_day, "d_price_pct": d_px, "d_price_w1": px.get("w1"), "d_price_m3": px.get("m3"), "d_price_ytd": px.get("ytd"),
               "credit_direction": direction if direction in ("DETERIORATING", "IMPROVING") else None, "credit_state": direction,
               "equity_flat": d_px is not None and abs(d_px) <= C.FLAT_BAND_PCT, "equity_state": state, "signal": signal, "signal_basis": basis,
               "distressed": True if (r or {}).get("distressed") else None, "credit_conflict": True if conflict else None,
               "stress": (r or {}).get("stress"), "theme": (r or {}).get("theme"), "layer": (r or {}).get("layer"),
               "cell": C.cell_of(direction, d_px), "pd_1y": C.implied_default((r or {}).get("spread_bp")),
               "wam": (r or {}).get("wam"), "due2y": (r or {}).get("due2y"), "step_bp": (r or {}).get("step_bp"), "bond_px": (r or {}).get("px"), "curve_bp": (r or {}).get("curve_bp"),
               "vol": (r or {}).get("vol"), "risk_1m": ((r or {}).get("fc") or {}).get("risk_1m"), "risk_3m": ((r or {}).get("fc") or {}).get("risk_3m")}
        if mod:
            row.update({k: v for k, v in mod.items() if k not in ("name", "group")})
        names.append({k: v for k, v in row.items() if v is not None or k in ("ticker", "name", "signal", "credit_direction", "d_price_pct")})
        if r is not None:
            r["_px"], r["_signal"], r["_direction"], r["_px_ytd"] = d_px if basis == "bond spreads" else px.get(C.PRIMARY), signal, direction, px.get("ytd")
            r["_cell"], r["_bar"], r["_px_w1"], r["_px_m3"] = row["cell"], bar, px.get("w1"), px.get("m3")
    names.sort(key=lambda x: (ORDER.get(x["signal"], 9), -abs(x.get("x_spread_bp") or x.get("cds_d1m") or 0), x["ticker"]))
    leads = [x for x in names if x["signal"] in C.LEADS]
    awaiting = [x for x in names if x["signal"] == "INSUFFICIENT_HISTORY"]

    issuers = []
    for r in rows:
        win = r["win"]
        cds = cds_by_ticker.get(r["ticker"]) if r["ticker"] else next((cds_by_key[k] for k in (r["id"][2:],) if k in cds_by_key), None)
        m1 = win.get(C.PRIMARY) or {}
        direction = r.get("_direction") if r["ticker"] else C.credit_direction(m1)[0]
        issuers.append([r["id"], r["ticker"], r["name"], r["industry"], r["book"], r["country"], r["n_bonds"], int(r["value"]), r["yield_pct"], r["dur"], r["spread_bp"],
                        (win.get("w1") or {}).get("d_bp"), m1.get("d_bp"), m1.get("x_bp"), m1.get("d_pct"), m1.get("n"), (win.get("m3") or {}).get("d_bp"), (win.get("ytd") or {}).get("d_bp"),
                        (cds or {}).get("spread_bp"), (cds or {}).get("d1m"), r.get("_px"), direction, r.get("_signal"), 1 if r["distressed"] else 0, r["tie"],
                        (win.get("ytd") or {}).get("d_pct"), r.get("stress"), r.get("rk_level"), r.get("rk_year"), r.get("rk_month"), r.get("theme"), r.get("layer"),
                        (cds or {}).get("d3m"), (cds or {}).get("rank_1y"), r.get("_px_ytd"), (win.get("w1") or {}).get("d_pct"), (win.get("m3") or {}).get("d_pct"),
                        r.get("_cell"), C.implied_default(r["spread_bp"]), C.implied_default(r["spread_bp"], 5.0), r.get("wam"), r.get("due2y"), r.get("coupon"), r.get("step_bp"),
                        r.get("px"), r.get("px_min"), r.get("s_short"), r.get("s_long"), r.get("curve_bp"), r.get("n_spread"), r.get("n_issuers"), r.get("fund_sector"),
                        m1.get("from_bp"), r.get("_bar") if r["ticker"] else C.credit_direction(m1)[1], (win.get("w1") or {}).get("x_bp"), (win.get("m3") or {}).get("x_bp"),
                        (win.get("ytd") or {}).get("x_bp"), r.get("_px_w1"), r.get("_px_m3"), (cds or {}).get("d1w"), (cds or {}).get("last_date"),
                        r.get("vol"), r.get("band"), r["fc"].get("risk_1m"), r["fc"].get("risk_3m"), r["fc"].get("lo_3m"), r["fc"].get("hi_3m"), r.get("ccy"), r.get("n_other")])

    # every bond the board read, under its company - the page opens this when a company is opened
    ref_snaps = {w: snaps.get(ref) for w, ref in windows.items() if ref}
    order = {r["id"]: i for i, r in enumerate(rows)}
    bond_rows = []
    for pt in sorted((x for x in points if x.get("co")), key=lambda x: (order.get(x["co"], 1e9), x.get("m") or "9999", -(x["v"] or 0))):
        ch = [C.f(pt["s"] - ref_snaps[w][pt["k"]], 1) if pt["s"] is not None and ref_snaps.get(w) and ref_snaps[w].get(pt["k"]) is not None else None for w in ("w1", "m1", "m3", "ytd")]
        bond_rows.append([pt["co"], N.clean_issuer(pt["issuer"]), pt.get("c"), pt.get("m"), pt["y"] if isinstance(pt["y"], (int, float)) else None, pt.get("px"),
                          pt["dur"] if isinstance(pt["dur"], (int, float)) else None, pt["t"], pt["s"], ch[0], ch[1], ch[2], ch[3], int(pt["v"] or 0), pt["why"], pt.get("ccy")])
    cells = {}
    for x in names:
        if x.get("cell"):
            cells[x["cell"]] = cells.get(x["cell"], 0) + 1

    for c in cds_priced:
        cl = closes.get(c["ticker"]) if c["ticker"] else None
        c["px_1m"] = C.price_change(cl, (date.fromisoformat(c["last_date"]) - timedelta(days=30)).isoformat())[0] if cl and c.get("last_date") else None
        c["direction"] = C.cds_direction(c)
        c.pop("key", None)
    for c in cds_bare:
        c.pop("key", None)

    for wkey, meta in window_meta.items():
        meta["n_companies"] = sum(1 for r in rows if wkey in r["win"])
        meta["book_median_bp"] = medians.get(wkey) or {}
    value_all = sum(r["value"] for r in rows) or 1.0
    value_tied = sum(r["value"] for r in rows if r["ticker"])
    how = {}
    for t in ties:
        how[t["how"]] = how.get(t["how"], 0) + 1
    untied = sorted((t for t in ties if t["how"] == "untied"), key=lambda t: -t["value"])
    coverage = {"bonds_read": counts["read"], "bonds_with_spread": counts["with a spread"],
                "bonds_left_out": {k: v for k, v in counts.items() if k not in ("read", "with a spread", "of them not in US dollars")},
                "bonds_not_usd": counts["of them not in US dollars"], "by_book": {b: sum(1 for r in rows if r["book"] == b) for b in C.BOOK_WORDS},
                "by_currency": dict(sorted(Counter(r.get("ccy") or "USD" for r in rows).items())),
                "companies": len(rows), "companies_with_spread": n_spread, "listed": n_listed, "unlisted": len(rows) - n_listed,
                "value_held": round(value_all, 0), "value_tied_pct": C.f(100.0 * value_tied / value_all, 1), "ties": how,
                "cds_priced": len(cds_priced), "cds_priced_tied": sum(1 for c in cds_priced if c["ticker"]), "cds_no_level": len(cds_bare),
                "stocks_read": len(closes), "stocks_no_file": sorted(no_file)[:40], "model_names": len(model),
                "listed_companies_known": len(listed), "extra_listed": [x["symbol"] for x in extra]}
    if any(w.get("ref") is None for w in window_meta.values()):
        gaps.append("Not every comparison exists yet: " + " ".join("%s - %s" % (w["words"], w["why"]) for w in window_meta.values() if w.get("ref") is None))
    gaps.append("Floating-rate notes, convertibles and bonds in a currency the warehouse has no government curve for are not read: %d bonds of the bond desk are outside the board." % (
        counts["not in US dollars"] + counts["floating-rate, convertible or another book"]))
    if counts["of them not in US dollars"]:
        gaps.append("%d bonds in euros, sterling, Canadian and Australian dollars are read against their own government's yields (%s), joined by straight lines between those "
                    "maturities and flat past the ends - a coarser curve than the Treasury's. Their companies sit in books of their own (non-dollar investment grade, non-dollar high "
                    "yield) and are never ranked against dollar spreads. What stocks did next is measured on the dollar books only." % (
                        counts["of them not in US dollars"], "; ".join("%s: %s" % (c, C.CURVE_WORDS[c]) for c in sorted(par_days.get("other") or {}))))
    gaps.append("%d companies have bonds but no listed stock the board could tie them to (private, foreign-listed or a subsidiary of a conglomerate): credit only, no signal. They hold %.1f%% of the market value read." % (
        len(rows) - n_listed, 100.0 - (coverage["value_tied_pct"] or 0)))
    gaps.append("Traded CDS has a level for %d corporate names (%d tied to a listed stock); %d more trade without a level the desk can print." % (len(cds_priced), coverage["cds_priced_tied"], len(cds_bare)))
    gaps.append("These are the bonds inside the scanned funds, not every bond outstanding; a spread is a spread to worst against the Treasury par curve, not an option-adjusted spread.")
    if awaiting:
        gaps.append("%d of %d listed names have no credit reading for the comparison (no bond seen at both dates and no liquid CDS)." % (len(awaiting), len(names)))

    payload = {
        "version": VERSION, "contract": C.CONTRACT, "generated_at": now.isoformat(),
        "as_of": as_of, "bond_desk": {"as_of": as_of, "generated_at": bonds_doc.get("generated_at"), "version": bonds_doc.get("version"), "move_as_of": move_as_of, "ytd_as_of": ytd_as_of},
        "curve": {"as_of": par_days.get("as_of"), "source": par.get("source"),
                  "other": {c: {"as_of": d, "words": C.CURVE_WORDS.get(c)} for c, d in sorted((par_days.get("other") or {}).items())}}, "stock_as_of": newest,
        "n_names": len(names), "n_leads": len(leads), "n_awaiting_history": len(awaiting),
        "names": names, "leads": leads[:20],
        "issuers": {"cols": ISSUER_COLS, "rows": issuers},
        "industries": {"all": C.industries(rows, windows), **{b: C.industries(rows, windows, b) for b in C.BOOK_WORDS}},
        "themes": C.themes(rows, windows),
        "matrix": {"cells": {k: cells.get(k, 0) for k in C.CELLS}, "words": C.CELLS, "n": sum(cells.values()), "window": C.PRIMARY, "band_pct": C.FLAT_BAND_PCT},
        "bonds": {"key": BONDS_OUT_KEY, "n": len(bond_rows), "cols": BOND_COLS},
        "evidence": evidence if isinstance(evidence, dict) and evidence.get("readings") else None,
        "history": (evidence or {}).get("history") if isinstance(evidence, dict) else None,
        "risk": {"n": n_forecast, "big": S.BIG, "vol_cuts": list(vol_cuts) if vol_cuts else None, "vol_sessions": S.VOL_SESSIONS,
                 "one_in_three": sum(1 for r in rows if (r["fc"].get("risk_3m") or 0) >= 100.0 / 3.0), "one_in_five": sum(1 for r in rows if (r["fc"].get("risk_3m") or 0) >= 20.0),
                 "words": "The chance that the stock is at some point %g%% or more under where it starts, inside one month and inside three: how often that happened, in the board's own "
                          "history, to stocks whose bonds traded as wide and whose own volatility stood in the same third." % S.BIG} if n_forecast else None,
        "stress": {"long": long, "long_ref": windows.get(long) if long else None, "stressed_at": C.STRESSED, "book_spread_bp": C.book_levels(rows),
                   "n_stressed": sum(1 for r in rows if r.get("stress") is not None and r["stress"] >= C.STRESSED),
                   "n_stressed_listed": sum(1 for r in rows if r["ticker"] and r.get("stress") is not None and r["stress"] >= C.STRESSED)},
        "cds": {"as_of": cds_as_of, "generated_at": (cds_doc or {}).get("generated_at"), "source": (cds_doc or {}).get("source"), "priced": cds_priced, "no_level": cds_bare},
        "windows": window_meta, "primary": C.PRIMARY, "books": C.BOOK_WORDS,
        "coverage": coverage,
        "match": {"by_rule": [t for t in ties if t["how"] in ("core", "truncated", "subsidiary")], "largest_untied": untied[:60]},
        "degraded": degraded, "gaps": gaps, "rules": C.RULES,
        "thresholds": {"move_floor_bp": C.MOVE_FLOOR_BP, "move_rel": C.MOVE_REL, "own_move_bp": C.OWN_MOVE_BP, "equity_flat_band_pct": C.FLAT_BAND_PCT, "cds_move_bp": C.CDS_MOVE_BP,
                       "cds_move_rel": C.CDS_MOVE_REL, "min_duration_years": C.MIN_DUR, "distressed_bp": C.DISTRESSED_BP, "min_dd_move": MIN_DD_MOVE, "min_cds_move_bp": MIN_CDS_MOVE_BP},
        "method": ("Each company's credit is the spread of its bonds over the Treasury par curve, from the corporate bonds desk's daily fund holdings, weighted by the market value "
                   "held. Its change is measured on the same bonds at both dates and compared with the change of its own book (investment grade, high yield, emerging markets). The stock is "
                   "the split-repaired close from the reference day to the newest close. A lead fires only when credit moved past its book and the stock has not followed. A traded CDS "
                   "level, where the CDS desk prints one, is shown beside it and says whether it confirms; when it says the opposite, no lead is called. Apart from the leads, every company "
                   "has a stress rank - where it stands in its own book on how wide its spread is and how far it widened over the year and the month - which does not look at the stock. "
                   "The 1.0 structural default model is kept for its own names and is a model, not a market price. Since 3.0 the board also measures itself: it is rebuilt at "
                   "every kept month-end and each reading is set beside what the stock did next. That record says what a reading has and has not meant, and gives each listed "
                   "company the chance of a fall its spread and its own volatility have meant. Bonds in euros, sterling, Canadian and Australian dollars are read against their "
                   "own governments' yields, in books of their own."),
        "attribution": "bond spreads from justhodl-corp-bonds (iShares and SPDR daily holdings) over the Treasury par curve; traded CDS from justhodl-cds-desk (DTCC public prints); "
                       "closes from the Polygon EOD bank; structural model from justhodl-cds-monitor (CreditGrades); other governments' yields from the warehouse's official-yields "
                       "lane (Bundesbank, Bank of England, Bank of Canada, Reserve Bank of Australia)",
        "estimate_type": "bond spreads over Treasuries and traded CDS where a row carries them (spread_bp, cds_bp); the fields distance_to_default and synthetic_cds_bp are a structural "
                         "default model, not a traded quote",
    }
    last_close = {t: c[max(c)] for t, c in closes.items() if c}
    calls = {"version": VERSION, "as_of": as_of, "stock_as_of": newest, "generated_at": now.isoformat(), "cols": CALL_COLS,
             "rows": [[r["ticker"], last_close.get(r["ticker"]), r["spread_bp"], r["band"], r["vt"], r["vol"], r["fc"].get("risk_1m"), r["fc"].get("risk_3m"), r.get("_cell"), r.get("_signal")]
                      for r in rows if r["ticker"] and r["fc"].get("risk_3m") is not None],
             "note": "The chance of a fall of %g%% or more the board gave each stock on this holdings day, kept as it was said so it can be set beside what happened." % S.BIG}
    holdings = {"as_of": as_of, "asked": as_of, "source": "justhodl-corp-bonds (data/corp-bonds.json), kept by justhodl-credit-before-equity", "cols": HOLD_COLS,
                "rows": [[b.get(c) for c in HOLD_COLS] for b in issues["bonds"] if isinstance(b, dict) and not b.get("left") and (
                    C.BOOKS.get(b.get("book")) if b.get("ccy") == "USD" else (b.get("ccy") in curves and C.GRADE.get(b.get("book"))))]}
    bonds_doc_out = {"version": VERSION, "generated_at": now.isoformat(), "as_of": as_of, "cols": BOND_COLS, "rows": bond_rows,
                     "note": "Every bond the board read, under the company it was rolled into (co = the company's id in the main document). Spread: the yield less the Treasury par "
                             "yield at the duration-matched maturity (tenor). d_*: the bond's own spread change over each window. value: market value held by the scanned funds. "
                             "why: the reason a bond has no spread. No ISIN or CUSIP is published."}
    return {"payload": payload, "snapshots": snapshots, "hist": hist, "bonds": bonds_doc_out, "calls": calls, "holdings": holdings, "closes": closes, "tape": tape}


def _now():
    return datetime.now(timezone.utc)


def lambda_handler(event=None, context=None):
    s3 = _s3()
    now = _now()
    today = now.date().isoformat()
    dry = isinstance(event, dict) and bool(event.get("dry_run"))
    notes = []

    # the evidence: read as it stands; redone inside this run when a month has finished since (never on a dry run, and only with the time for it)
    evidence, _ = optional(s3, EVIDENCE_KEY)
    studied = None
    try:
        why = study_due(evidence, stored_days(s3, HOLD_ROOT), today)
        left = context.get_remaining_time_in_millis() if context is not None and hasattr(context, "get_remaining_time_in_millis") else None
        forced = isinstance(event, dict) and bool(event.get("study"))
        if (why or forced) and not dry:
            if left is not None and left < STUDY_NEEDS_MS:
                notes.append("The evidence is due (%s) and was not redone on this run: %d seconds were left of the %d it needs." % (why, left // 1000, STUDY_NEEDS_MS // 1000))
            else:
                print("[cbe] the study is due: %s" % (why or "asked for"))
                studied = study(s3, now)
                evidence = studied["evidence"]
    except Exception as exc:  # noqa: BLE001 - the board is published with the evidence it had; the failure is said on the page
        notes.append("The evidence could not be redone on this run (%s: %s); the board shows the evidence it had." % (type(exc).__name__, str(exc)[:160]))
        print("[cbe] study failed: %s: %s" % (type(exc).__name__, exc))

    built = build(s3, now, evidence=evidence)
    payload = built["payload"]
    payload["degraded"].extend(notes)
    call_days = stored_days(s3, CALLS_ROOT)
    track_doc, _ = optional(s3, TRACK_KEY)
    if dry:                                                         # the whole build inside Lambda, nothing written
        cov = payload["coverage"]
        payload["track"] = track_summary(track_doc, call_days, built["tape"])
        return {"statusCode": 200, "dry_run": True, "as_of": payload["as_of"], "companies": cov["companies"], "listed": cov["listed"], "names": payload["n_names"], "leads": payload["n_leads"],
                "cds_levels": cov["cds_priced"], "windows": {k: v.get("ref") for k, v in payload["windows"].items()}, "degraded": payload["degraded"],
                "ledger_days": [s["as_of"] for s in built["snapshots"]], "bytes": len(json.dumps(payload, separators=(",", ":"), default=str)),
                "version": payload["version"], "stressed": payload["stress"]["n_stressed"], "themes": {t["key"]: t["n"] for t in payload["themes"]},
                "evidence": bool(payload["evidence"]), "study_due": study_due(evidence, stored_days(s3, HOLD_ROOT), today), "forecasts": (payload["risk"] or {}).get("n"),
                "bonds_rows": len(built["bonds"]["rows"]), "calls": len(built["calls"]["rows"])}

    for snap in built["snapshots"]:
        put_json(s3, LEDGER_ROOT + snap["as_of"] + ".json.gz", snap, gz=True)
    put_json(s3, HOLD_ROOT + payload["as_of"] + ".json.gz", built["holdings"], gz=True)
    if studied:
        put_json(s3, EVIDENCE_KEY, studied["evidence"])
        put_json(s3, HISTORY_OUT_KEY, studied["history"])
    if built["calls"]["rows"]:
        put_json(s3, CALLS_ROOT + payload["as_of"] + ".json.gz", built["calls"], gz=True)
        call_days = sorted(set(call_days) | {payload["as_of"]})
    try:
        before = json.dumps(track_doc, sort_keys=True)
        track_doc = score_calls(s3, track_doc, built["closes"], built["tape"])
        if json.dumps(track_doc, sort_keys=True) != before:
            track_doc["updated_at"] = now.isoformat()
            put_json(s3, TRACK_KEY, track_doc)
    except Exception as exc:  # noqa: BLE001 - scoring earlier calls is bookkeeping; today's board does not wait on it
        payload["degraded"].append("Earlier calls could not be scored on this run (%s: %s)." % (type(exc).__name__, str(exc)[:120]))
    payload["track"] = track_summary(track_doc, call_days, built["tape"])
    size = put_json(s3, OUT_KEY, payload)
    put_json(s3, BONDS_OUT_KEY, built["bonds"])
    built["hist"]["updated_at"] = now.isoformat()
    put_json(s3, HIST_KEY, built["hist"])
    cov = payload["coverage"]
    print("[cbe] %s: %d companies (%d listed), %d leads, %d CDS levels, %d forecasts, %d bytes; ledger days written %s; study %s" % (
        payload["as_of"], cov["companies"], cov["listed"], payload["n_leads"], cov["cds_priced"], (payload["risk"] or {}).get("n") or 0, size, [s["as_of"] for s in built["snapshots"]],
        "redone" if studied else "not due"))
    return {"statusCode": 200, "body": json.dumps({"names": payload["n_names"], "leads": payload["n_leads"], "companies": cov["companies"], "as_of": payload["as_of"], "study": bool(studied)})}
