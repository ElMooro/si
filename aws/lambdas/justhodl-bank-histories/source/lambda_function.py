"""justhodl-bank-histories v1.2.0 - the HISTORY BOOK: per-symbol statement / ratio / metric / growth histories, inverted
once a day from the FMP bank's whole-market year-files (bank fusion wave 3, 2026-10-26).

Why.  The FMP bank holds every statement as one whole-market CSV per (endpoint, fiscal year, period) - the right shape
for a whole-market join (the factor book), the wrong shape for an on-demand per-ticker desk: justhodl-equity-research
(the why.html desk) asks 20 annual + 22 quarterly years of three statements plus 15 years of ratios / key metrics for
ONE ticker, which is ~60 whole-market tables (hundreds of MB decoded) for one request.  Its 1 GB container thrashed the
table cache, the bank-first shim stepped aside, the live FMP call answered 429 (bandwidth block) and the desk returned
"No data for ticker AAPL".  This engine pays the inversion once a day and writes, per symbol and block, one small object:

    data/warm/history-book/<bulk>/<annual|quarter>/<SYMBOL>.json.gz
        {"symbol", "endpoint", "bulk", "period", "schema", "as_of", "n", "years": [..], "rows": [newest first, verbatim typed rows]}

for the nine dated bulks (income / balance / cash-flow statements, their growth files, financial-growth, ratios,
key-metrics) x annual (floor year 2000) and quarter (floor year 2016; the deepest product ask is 22 quarters).  A
consumer (aws/shared/fmp_bank.statements -> fmp_bankfirst) reads exactly one object per (symbol, statement, period),
any limit, no key, no whole-market decode.

How.  One block at a time: stream every year-file of the block (GzipFile -> csv.reader, never a whole string) into a
SQLite staging table on /tmp keyed by symbol, then emit one document per symbol (US-listed universe = the factor book's
universe when present, else every symbol in the files), write-if-changed by content hash (state hashes per block),
bounded in-flight puts.  Blocks are processed oldest-refreshed first inside the run budget; `manifest.json` says which
block was refreshed when and how many symbols it holds - a block refreshed > 2 days ago is listed under `stale_blocks`
(the reader labels `as_of` per block, never guesses).  Lease-guarded (one run at a time), idempotent, no provider call.

v1.2.0 (2026-11-21) - write a document only when its content changed.  Until now the write-if-changed hash covered the
whole body INCLUDING `as_of` (the FMP bank manifest's updated_at, a new value every time the bank runs), so no document
ever matched its stored hash: every run rewrote every document it reached - 110,000 to 400,000 identical files a day,
each write billed twice (the live bucket and its archive copy) and each leaving an old version behind.  Now:
  * the hash covers the document WITHOUT `as_of`; an unchanged document is not written.  `as_of` inside a document is
    therefore the bank vintage at which that document's content last changed; the vintage a block was last checked
    against is the block record's `as_of` in manifest.json (what the readers have always used);
  * the first run after the upgrade recognises a document written by v1.0-1.1 from its old-style hash (rebuilt with the
    vintage the block was last written at), so the upgrade itself rewrites nothing that did not change;
  * a block whose inputs are byte-identical to its last complete read (every source file's ETag and size, the universe,
    this engine's version) is confirmed without being read again - one run then covers all 25 blocks, the phase reaches
    STEADY and the conductor has nothing to kick.  Each block is still read in full once a week on its own day;
  * a document whose put failed keeps its OLD hash, so the next run writes it (before, the new hash was recorded first
    and only the daily full rewrite hid it);
  * `{"dry_run": true}` reads and compares, writes nothing (no lease, no state, no document) and returns the counts.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import csv
import gzip
import hashlib
import io
import json
import os
import sqlite3
import sys
import time

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fmp_bank  # noqa: E402  (bundled shared module: the FMP bank manifest + value typing)

VERSION = "1.2.0"
SCHEMA = "history-book/1.0"
ENGINE = "justhodl-bank-histories"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/history-book/"
FMP_ROOT = fmp_bank.ROOT                                   # data/warm/fmp-bank/
FACTOR_UNIVERSE_KEY = "data/warm/factor-book/universe.json.gz"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
BUDGET_S = float(os.environ.get("HISTORY_BOOK_BUDGET_S", "840"))
WORKERS = int(os.environ.get("HISTORY_BOOK_WORKERS", "24"))
FLOOR_ANNUAL = int(os.environ.get("HISTORY_BOOK_FLOOR_ANNUAL", "2000"))
FLOOR_QUARTER = int(os.environ.get("HISTORY_BOOK_FLOOR_QUARTER", "2016"))
DB_PATH = os.environ.get("HISTORY_BOOK_DB", "/tmp/history-book.sqlite")
UNIVERSE_MODE = os.environ.get("HISTORY_BOOK_UNIVERSE", "factor-book")      # factor-book | all
STALE_AFTER_S = 2 * 86400
HASH_REV = 2                                               # 2 = document hash without `as_of` (v1.2.0); absent/1 = whole body incl. as_of
FULL_CHECK_EVERY_S = 7 * 86400                             # every block is read in full at least this often, whatever its signature says
FRESH_WITHIN_S = int(os.environ.get("HISTORY_BOOK_FRESH_WITHIN_S", str(20 * 3600)))   # a block older than this is pending again (phase BACKFILL -> the conductor kicks)
# v1.1.0: the SNAPSHOT bulks too - one tiny document per symbol under <bulk>/latest/<SYMBOL>.json.gz, so a per-ticker desk's
# /profile, /ratios-ttm, /key-metrics-ttm, /financial-scores, /rating, /discounted-cash-flow, /price-target-summary,
# /grades-consensus, /stock-peers asks are one small read each instead of a whole-market table decode (equity-research's
# 1 GB container sat at 1,023 MB resident from table churn and died on the next allocation).
SNAPSHOT_BULKS = [("profile", "profile-bulk"), ("ratios-ttm", "ratios-ttm-bulk"), ("key-metrics-ttm", "key-metrics-ttm-bulk"), ("financial-scores", "scores-bulk"),
                  ("rating", "rating-bulk"), ("discounted-cash-flow", "dcf-bulk"), ("price-target-summary", "price-target-summary-bulk"),
                  ("grades-consensus", "upgrades-downgrades-consensus-bulk"), ("stock-peers", "peers-bulk")]
# (endpoint, bulk) in priority order: the desk's statements first, then the metric families.
BULKS = [("income-statement", "income-statement-bulk"), ("balance-sheet-statement", "balance-sheet-statement-bulk"),
         ("cash-flow-statement", "cash-flow-statement-bulk"), ("ratios", "ratios-bulk"), ("key-metrics", "key-metrics-bulk"),
         ("financial-growth", "financial-growth-bulk"), ("income-statement-growth", "income-statement-growth-bulk"),
         ("balance-sheet-statement-growth", "balance-sheet-statement-growth-bulk"), ("cash-flow-statement-growth", "cash-flow-statement-growth-bulk")]
PERIODS = {"annual": ["annual"], "quarter": ["Q1", "Q2", "Q3", "Q4"]}
_CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=5, read_timeout=120, max_pool_connections=64)
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"), config=_CFG)
_LOCAL = __import__("threading").local()


def _s3():
    c = getattr(_LOCAL, "c", None)
    if c is None:
        c = _LOCAL.c = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"), config=_CFG)
    return c


def _get_json(key, gz=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception:  # noqa: BLE001
        return None
    if gz or (gz is None and key.endswith(".gz")):
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")


def _put_json_gz(key, obj, meta=None, client=None):
    body = gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8"), compresslevel=6)
    (client or s3).put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json", ContentEncoding="gzip",
                              CacheControl="max-age=300", Metadata={k: str(v)[:250] for k, v in (meta or {}).items()})
    return len(body)


def _stream_csv(key):
    """(header, row iterator) over a banked gzip CSV without holding it in memory; (None, empty) when absent."""
    try:
        body = _s3().get_object(Bucket=BUCKET, Key=key)["Body"]
    except Exception:  # noqa: BLE001
        return None, iter(())
    gz = gzip.GzipFile(fileobj=body)
    text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
    reader = csv.reader(text)
    try:
        header = next(reader)
    except StopIteration:
        return [], iter(())
    return header, reader


def held_files(man, bulk, kind):
    """[(year, period, key)] newest first for a (bulk, annual|quarter) block from the FMP bank manifest's dated_keys."""
    floor = FLOOR_ANNUAL if kind == "annual" else FLOOR_QUARTER
    want = set(PERIODS[kind])
    out = []
    for k in man.get("dated_keys") or []:
        parts = str(k).split("/")
        if len(parts) != 3 or parts[0] != bulk or parts[2] not in want:
            continue
        try:
            y = int(parts[1])
        except ValueError:
            continue
        if y >= floor:
            out.append((y, parts[2], FMP_ROOT + "%s/%d/%s.csv.gz" % (bulk, y, parts[2])))
    out.sort(key=lambda t: (-t[0], t[1]))
    return out


def _head(key):
    """(etag, size) of an object, or None when it is not there (or could not be asked - the caller then reads the block)."""
    try:
        h = _s3().head_object(Bucket=BUCKET, Key=key)
    except Exception:  # noqa: BLE001
        return None
    return (str(h.get("ETag") or "").strip('"'), int(h.get("ContentLength") or 0))


def _exists(key):
    return _head(key) is not None


def universe_signature(universe):
    return "all" if universe is None else hashlib.sha1(",".join(sorted(universe)).encode("utf-8")).hexdigest()[:16]


def source_signature(files, extra):
    """(signature, missing files): one hash over everything a block is built from - each source file's key, ETag and
    size, plus `extra` (the universe, the document shape, this engine's version).  Equal to the signature stored at the
    block's last complete read = the block would come out byte for byte the same, so it need not be read again."""
    with ThreadPoolExecutor(max_workers=max(1, min(WORKERS, 16))) as pool:
        heads = list(pool.map(lambda f: (f[2], _head(f[2])), files))
    h = hashlib.sha1(extra.encode("utf-8"))
    for key, hd in sorted(heads, key=lambda t: t[0]):
        h.update(("|%s:%s:%s" % (key, hd[0] if hd else "missing", hd[1] if hd else "-")).encode("utf-8"))
    return h.hexdigest()[:24], sum(1 for _, hd in heads if hd is None)


def doc_hash(doc):
    """Content hash of a per-symbol document: everything but the `as_of` stamp (the stamp alone is not a change)."""
    core = {k: v for k, v in doc.items() if k != "as_of"}
    return hashlib.sha1(json.dumps(core, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()[:20]


def legacy_hash(doc, as_of):
    """The v1.0-1.1 hash: the whole body with the `as_of` it was written at.  Used once per document, on the first run
    after v1.2.0, to see that a document written by the old engine has not changed."""
    return hashlib.sha1(json.dumps(dict(doc, as_of=as_of), separators=(",", ":"), default=str).encode("utf-8")).hexdigest()[:20]


def full_check_due(bid, prev, now):
    """True when the block must be read in full although its signature matches: never read by this version, overdue, or
    today is its weekly day (the 25 blocks are spread over the week so one run always fits)."""
    last = prev.get("staged_at")
    if not last:
        return True
    try:
        age = (now - datetime.fromisoformat(last)).total_seconds()
    except (TypeError, ValueError):
        return True
    if age >= FULL_CHECK_EVERY_S + 86400:
        return True
    slot = int(hashlib.sha1(bid.encode("utf-8")).hexdigest(), 16) % 7
    return now.toordinal() % 7 == slot and age > 20 * 3600


def snapshot_files(bulk):
    """[(None, "latest", key)] for a snapshot bulk: one latest.csv.gz or its part-N files (the FMP bank's layout)."""
    whole = FMP_ROOT + bulk + "/latest.csv.gz"
    if _exists(whole):
        return [(0, "latest", whole)]
    out = []
    for part in range(0, 41):
        k = FMP_ROOT + "%s/part-%d/latest.csv.gz" % (bulk, part)
        if not _exists(k):
            break
        out.append((0, "latest", k))
    return out


def load_universe():
    """The factor book's US-listed universe (symbol set) or None (= every symbol in the files)."""
    if UNIVERSE_MODE != "factor-book":
        return None
    doc = _get_json(FACTOR_UNIVERSE_KEY)
    syms = (doc or {}).get("symbols") if isinstance(doc, dict) else None
    if not syms:
        return None
    return {str(s).upper() for s in syms}


class Staging:
    """SQLite staging on /tmp: (sym, ord, period, row_json) for one block; ord sorts newest first."""

    def __init__(self, path=DB_PATH):
        try:
            os.remove(path)
        except OSError:
            pass
        self.con = sqlite3.connect(path)
        self.con.execute("PRAGMA journal_mode=OFF")
        self.con.execute("PRAGMA synchronous=OFF")
        self.con.execute("PRAGMA temp_store=MEMORY")
        self.con.execute("CREATE TABLE rows (sym TEXT, ord TEXT, period TEXT, row TEXT)")
        self.buf = []
        self.n = 0

    def add(self, sym, ord_, period, row_json):
        self.buf.append((sym, ord_, period, row_json))
        if len(self.buf) >= 5000:
            self.flush()

    def flush(self):
        if self.buf:
            self.con.executemany("INSERT INTO rows VALUES (?,?,?,?)", self.buf)
            self.n += len(self.buf)
            self.buf = []

    def finish(self):
        self.flush()
        self.con.execute("CREATE INDEX ix ON rows(sym, ord DESC)")
        self.con.commit()

    def iter_symbols(self):
        """Yield (sym, [row dicts newest first]) grouped, in one ordered scan."""
        cur = self.con.execute("SELECT sym, ord, period, row FROM rows ORDER BY sym, ord DESC")
        cur_sym, rows = None, []
        for sym, _ord, _period, row in cur:
            if sym != cur_sym:
                if cur_sym is not None:
                    yield cur_sym, rows
                cur_sym, rows = sym, []
            rows.append(json.loads(row))
        if cur_sym is not None:
            yield cur_sym, rows

    def close(self):
        try:
            self.con.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            os.remove(DB_PATH)
        except OSError:
            pass


def stage_block(files, universe, deadline, notes):
    """Stream the block's year-files into staging (universe-filtered); returns (staging, files_done, rows) or stops at the
    deadline with files_done < len(files) (the block is then NOT emitted - a partial history is never written)."""
    st = Staging()
    done, rows_n = 0, 0
    for year, period, key in files:
        if time.time() > deadline:
            break
        header, reader = _stream_csv(key)
        if header is None:
            notes.append("missing %s" % key)
            done += 1
            continue
        lk = {h.lower().replace("_", ""): i for i, h in enumerate(header)}
        i_sym = lk.get("symbol")
        i_date = lk.get("date")
        i_fy = lk.get("fiscalyear")
        i_per = lk.get("period")
        if i_sym is None:
            notes.append("no symbol column in %s" % key)
            done += 1
            continue
        for row in reader:
            if len(row) <= i_sym:
                continue
            sym = row[i_sym].strip().upper()
            if not sym or (universe is not None and sym not in universe):
                continue
            d = (row[i_date].strip() if (i_date is not None and i_date < len(row)) else "") or ""
            fy = (row[i_fy].strip() if (i_fy is not None and i_fy < len(row)) else "") or str(year)
            per = (row[i_per].strip() if (i_per is not None and i_per < len(row)) else "") or period
            ord_ = "%s|%s|%s" % (d or ("%s-00-00" % fy), fy, per) if period != "latest" else "latest"
            typed = fmp_bank._typed(dict(zip(header, row)))
            st.add(sym, ord_, period, json.dumps(typed, separators=(",", ":"), default=str))
            rows_n += 1
        done += 1
    st.finish()
    return st, done, rows_n


def emit_block(endpoint, bulk, kind, st, as_of, hashes, deadline, stats, legacy_as_of=None, dry_run=False, on_doc=None):
    """Write one document per symbol whose content changed (the block's hash state decides); returns the new hash state.

    legacy_as_of: the vintage the block was last written at by v1.0-1.1 (their hash covered it) - lets the first v1.2.0
    run see an unchanged document as unchanged.  dry_run: compare only, put nothing.  on_doc(sym, key, doc, verdict):
    called per document with verdict same | changed | new (the dry-run proof samples through it)."""
    new_hashes = {}
    futs = {}
    inflight_max = WORKERS * 2
    counts = {"written": 0, "unchanged": 0, "failed": 0, "bytes": 0, "changed": 0, "new": 0, "migrated": 0, "symbols": 0}

    def settle(done):
        for f in as_completed(done):
            sym, h = done[f]
            try:
                counts["bytes"] += f.result()
                counts["written"] += 1
                new_hashes[sym] = h
            except Exception:  # noqa: BLE001
                counts["failed"] += 1                    # the old hash (or none) stands: the next run writes it
                if sym in hashes:
                    new_hashes[sym] = hashes[sym]

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for sym, rows in st.iter_symbols():
            years = sorted({str(r.get("fiscalYear") or str(r.get("date") or "")[:4]) for r in rows if (r.get("fiscalYear") or r.get("date"))}, reverse=True) if kind != "latest" else []
            doc = {"symbol": sym, "endpoint": endpoint, "bulk": bulk, "period": kind, "schema": SCHEMA, "as_of": as_of, "n": len(rows), "years": years, "rows": rows}
            key = ROOT + "%s/%s/%s.json.gz" % (bulk, kind, sym.replace("/", "_"))
            h = doc_hash(doc)
            old = hashes.get(sym)
            counts["symbols"] += 1
            same = old == h
            if not same and old and legacy_as_of and old == legacy_hash(doc, legacy_as_of):
                same = True
                counts["migrated"] += 1
            if same:
                new_hashes[sym] = h
                counts["unchanged"] += 1
                if on_doc:
                    on_doc(sym, key, doc, "same")
                continue
            verdict = "changed" if old else "new"
            counts[verdict] += 1
            if on_doc:
                on_doc(sym, key, doc, verdict)
            if dry_run:
                counts["written"] += 1                   # what a real run would put
                new_hashes[sym] = h
            else:
                futs[pool.submit(_put_json_gz, key, doc, {"symbol": sym, "endpoint": endpoint, "period": kind, "n": len(rows)}, _s3())] = (sym, h)
                if len(futs) >= inflight_max:
                    settle(futs)
                    futs = {}
            if time.time() > deadline:
                stats["deadline_hit"] = True
                break
        settle(futs)
    stats.update(counts)
    if stats.get("deadline_hit"):
        # a partial emit keeps the OLD hashes for the symbols not reached so they are compared (and written) next time
        for sym, h in hashes.items():
            new_hashes.setdefault(sym, h)
    return new_hashes


def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event or {}
    dry = bool(event.get("dry_run"))                    # read and compare only: no lease, no state, no manifest, no document
    budget = float(event.get("budget_s") or BUDGET_S)
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        budget = min(budget, max(60.0, context.get_remaining_time_in_millis() / 1000.0 - 45.0))
    deadline = t0 + budget
    now = datetime.now(timezone.utc)
    state = _get_json(STATE_KEY, gz=False) or {}
    try:
        lease = float(state.get("lease_until") or 0)                 # epoch seconds - the conductor's contract (scripts/fmp_backfill_conductor.py)
    except (TypeError, ValueError):
        lease = 0.0
    if not dry:
        if lease > time.time() and not event.get("force"):
            return {"ok": False, "skipped": "lease held until %s" % datetime.fromtimestamp(lease, timezone.utc).isoformat()}
        state["lease_until"] = deadline + 120
        _put_json(STATE_KEY, state)
    man_fmp = fmp_bank.manifest() or {}
    as_of = man_fmp.get("updated_at")
    if not man_fmp.get("dated_keys"):
        if not dry:
            state.pop("lease_until", None)
            _put_json(STATE_KEY, state)
        return {"ok": False, "error": "fmp-bank manifest has no dated_keys; nothing to invert"}
    universe = load_universe()
    sig_extra = "%s|v%s|h%d|u:%s" % (SCHEMA, VERSION, HASH_REV, universe_signature(universe))
    blocks = state.setdefault("blocks", {})
    only = set(event.get("blocks") or [])
    order = []
    for endpoint, bulk in BULKS:
        for kind in ("annual", "quarter"):
            bid = "%s/%s" % (bulk, kind)
            if only and bid not in only:
                continue
            files = held_files(man_fmp, bulk, kind)
            if not files:
                blocks.setdefault(bid, {})["files"] = 0
                continue
            order.append((bid, endpoint, bulk, kind, files))
    for endpoint, bulk in SNAPSHOT_BULKS:
        bid = "%s/latest" % bulk
        if only and bid not in only:
            continue
        files = snapshot_files(bulk)
        if not files:
            blocks.setdefault(bid, {})["files"] = 0
            continue
        order.append((bid, endpoint, bulk, "latest", files))
    prio = {b: i for i, (_, b) in enumerate(BULKS + SNAPSHOT_BULKS)}
    # oldest-refreshed first; never-built blocks first of all, then the priority order (statements, metrics, snapshots)
    order.sort(key=lambda t: (blocks.get(t[0], {}).get("refreshed_at") or "", prio.get(t[2], 99)))
    notes, done_blocks, confirmed = [], [], []
    reserve = min(120.0, budget * 0.15)                 # time kept for the manifest / state writes
    for bid, endpoint, bulk, kind, files in order:
        if time.time() > deadline - reserve:
            notes.append("budget: %s not reached" % bid)
            continue
        prev = blocks.get(bid) or {}
        tb = time.time()
        sig, missing = source_signature(files, sig_extra + "|" + kind)
        if missing:
            notes.append("%s: %d of %d source files missing" % (bid, missing, len(files)))
        if (prev.get("source_sig") == sig and prev.get("hash_rev") == HASH_REV and prev.get("refreshed_at") and not prev.get("partial")
                and not prev.get("failed") and not event.get("full") and not full_check_due(bid, prev, now)):
            # nothing this block is built from has changed since it was last read in full: confirmed, not read again
            blocks[bid] = dict(prev, written=0, unchanged=prev.get("symbols", 0), failed=0, bytes=0, refreshed_at=now.isoformat(), as_of=as_of,
                               checked="inputs unchanged", check_seconds=round(time.time() - tb, 2), missing_files=missing)
            done_blocks.append(bid)
            confirmed.append(bid)
            continue
        est = prev.get("seconds")
        if est and time.time() + est * 1.2 > deadline:
            notes.append("budget: %s needs ~%ds, skipped this run" % (bid, est))
            continue
        st, files_done, rows_n = stage_block(files, universe, deadline - reserve / 2, notes)
        if files_done < len(files):
            st.close()
            notes.append("%s: %d/%d files staged before the deadline - block not emitted" % (bid, files_done, len(files)))
            blocks.setdefault(bid, {}).update({"files": len(files), "last_attempt": now.isoformat(), "partial": True})
            break
        hashes_key = ROOT + "_state/hashes/%s__%s.json.gz" % (bulk, kind)
        hashes = _get_json(hashes_key) or {}
        # a block last written by v1.0-1.1 carries hashes that cover the vintage it was written at
        legacy_as_of = (prev.get("legacy_as_of") or prev.get("as_of")) if prev.get("hash_rev") != HASH_REV else None
        stats = {}
        new_hashes = emit_block(endpoint, bulk, kind, st, as_of, hashes, deadline, stats, legacy_as_of=legacy_as_of, dry_run=dry)
        st.close()
        if not dry and new_hashes != hashes:
            _put_json_gz(hashes_key, new_hashes)
        secs = round(time.time() - tb, 1)
        complete = not stats.get("deadline_hit") and not stats.get("failed")
        blocks[bid] = {"endpoint": endpoint, "bulk": bulk, "period": kind, "files": len(files), "years": ([files[-1][0], files[0][0]] if kind != "latest" else None), "rows": rows_n,
                       "symbols": stats.get("symbols", 0), "written": stats.get("written", 0), "unchanged": stats.get("unchanged", 0), "failed": stats.get("failed", 0),
                       "changed": stats.get("changed", 0), "new": stats.get("new", 0), "migrated": stats.get("migrated", 0),
                       "bytes": stats.get("bytes", 0), "seconds": secs, "refreshed_at": now.isoformat(), "as_of": as_of, "partial": bool(stats.get("deadline_hit")),
                       "checked": "read in full", "staged_at": now.isoformat(), "missing_files": missing,
                       # the signature and the hash revision stand only for a complete read: anything less is read again next run
                       "source_sig": sig if complete else None, "hash_rev": HASH_REV if complete else prev.get("hash_rev"),
                       "legacy_as_of": None if complete else legacy_as_of}
        done_blocks.append(bid)
        print("[history-book] %s: %d files, %d rows, %d symbols, written %d unchanged %d failed %d, %.0f s%s" % (bid, len(files), rows_n, stats.get("symbols", 0), stats.get("written", 0), stats.get("unchanged", 0), stats.get("failed", 0), secs, " (dry run)" if dry else ""))
        if not dry:
            _put_json(STATE_KEY, state)
    stale = [bid for bid, b in blocks.items() if b.get("refreshed_at") and (now - datetime.fromisoformat(b["refreshed_at"])).total_seconds() > STALE_AFTER_S]
    def _fresh(bid):
        ra = blocks.get(bid, {}).get("refreshed_at")
        return bool(ra) and (now - datetime.fromisoformat(ra)).total_seconds() <= FRESH_WITHIN_S and not blocks.get(bid, {}).get("partial")
    pending = [bid for bid, *_ in order if bid not in done_blocks and not _fresh(bid)]
    phase = "STEADY" if not pending and not stale else "BACKFILL"
    puts = sum(blocks[b].get("written", 0) for b in done_blocks)
    unchanged = sum(blocks[b].get("unchanged", 0) for b in done_blocks)
    last_run = {"started": now.isoformat(), "seconds": round(time.time() - t0, 1), "blocks": done_blocks, "confirmed_unread": confirmed, "notes": notes}
    if dry:
        return {"ok": True, "dry_run": True, "version": VERSION, "phase": phase, "blocks": done_blocks, "confirmed_unread": confirmed, "notes": notes,
                "seconds": round(time.time() - t0, 1), "stale_blocks": stale, "pending": pending, "would_put": puts, "unchanged": unchanged,
                "detail": {b: {k: blocks[b].get(k) for k in ("symbols", "written", "unchanged", "changed", "new", "migrated", "seconds", "checked")} for b in done_blocks}}
    manifest = {"schema": SCHEMA, "version": VERSION, "engine": ENGINE, "updated_at": now.isoformat(), "as_of": as_of, "prefix": ROOT, "phase": phase, "pending_blocks": pending,
                "layout": ROOT + "<bulk>/<annual|quarter|latest>/<SYMBOL>.json.gz", "universe": "factor-book" if universe else "all symbols in the files",
                "universe_size": len(universe) if universe else None, "floor_annual": FLOOR_ANNUAL, "floor_quarter": FLOOR_QUARTER,
                "as_of_meaning": "blocks.<id>.as_of = the bank vintage the block was last checked against; as_of inside a document = the vintage at which that document's content last changed (a document is written only when it changes)",
                "blocks": blocks, "stale_blocks": stale, "last_run": last_run,
                "read_with": "aws/shared/fmp_bank.statements (history book first) / fmp_bankfirst"}
    _put_json(MANIFEST_KEY, manifest)
    state.pop("lease_until", None)
    state["last_run"] = dict(last_run, puts=puts, unchanged=unchanged)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["phase"] = phase
    _put_json(STATE_KEY, state)
    return {"ok": True, "version": VERSION, "phase": phase, "blocks": done_blocks, "confirmed_unread": confirmed, "notes": notes, "seconds": round(time.time() - t0, 1),
            "stale_blocks": stale, "pending": pending, "puts": puts, "unchanged": unchanged}
