"""justhodl-bank-histories v1.0.0 - the HISTORY BOOK: per-symbol statement / ratio / metric / growth histories, inverted
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

VERSION = "1.0.0"
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
            ord_ = "%s|%s|%s" % (d or ("%s-00-00" % fy), fy, per)
            typed = fmp_bank._typed(dict(zip(header, row)))
            st.add(sym, ord_, period, json.dumps(typed, separators=(",", ":"), default=str))
            rows_n += 1
        done += 1
    st.finish()
    return st, done, rows_n


def emit_block(endpoint, bulk, kind, st, as_of, hashes, deadline, stats):
    """Write one document per symbol (write-if-changed via the block's hash state); returns the new hash state."""
    new_hashes = {}
    futs = []
    inflight_max = WORKERS * 2
    written = unchanged = failed = 0
    bytes_out = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for sym, rows in st.iter_symbols():
            years = sorted({str(r.get("fiscalYear") or str(r.get("date") or "")[:4]) for r in rows if (r.get("fiscalYear") or r.get("date"))}, reverse=True)
            doc = {"symbol": sym, "endpoint": endpoint, "bulk": bulk, "period": kind, "schema": SCHEMA, "as_of": as_of, "n": len(rows), "years": years, "rows": rows}
            body = json.dumps(doc, separators=(",", ":"), default=str)
            h = hashlib.sha1(body.encode("utf-8")).hexdigest()[:20]
            new_hashes[sym] = h
            if hashes.get(sym) == h:
                unchanged += 1
                continue
            key = ROOT + "%s/%s/%s.json.gz" % (bulk, kind, sym.replace("/", "_"))
            futs.append(pool.submit(_put_json_gz, key, doc, {"symbol": sym, "endpoint": endpoint, "period": kind, "n": len(rows)}, _s3()))
            if len(futs) >= inflight_max:
                for f in as_completed(futs):
                    try:
                        bytes_out += f.result()
                        written += 1
                    except Exception:  # noqa: BLE001
                        failed += 1
                futs = []
            if time.time() > deadline:
                stats["deadline_hit"] = True
                break
        for f in as_completed(futs):
            try:
                bytes_out += f.result()
                written += 1
            except Exception:  # noqa: BLE001
                failed += 1
    stats.update({"written": written, "unchanged": unchanged, "failed": failed, "bytes": bytes_out, "symbols": len(new_hashes)})
    if stats.get("deadline_hit"):
        # a partial emit keeps the OLD hashes for the symbols not reached so they are written next time
        for sym, h in hashes.items():
            new_hashes.setdefault(sym, h)
    return new_hashes


def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event or {}
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
    if lease > time.time() and not event.get("force"):
        return {"ok": False, "skipped": "lease held until %s" % datetime.fromtimestamp(lease, timezone.utc).isoformat()}
    state["lease_until"] = deadline + 120
    _put_json(STATE_KEY, state)
    man_fmp = fmp_bank.manifest() or {}
    as_of = man_fmp.get("updated_at")
    if not man_fmp.get("dated_keys"):
        state.pop("lease_until", None)
        _put_json(STATE_KEY, state)
        return {"ok": False, "error": "fmp-bank manifest has no dated_keys; nothing to invert"}
    universe = load_universe()
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
    # oldest-refreshed first; never-built blocks first of all, in BULKS priority order
    order.sort(key=lambda t: (blocks.get(t[0], {}).get("refreshed_at") or "", BULKS.index((t[1], t[2]))))
    notes, done_blocks = [], []
    reserve = min(120.0, budget * 0.15)                 # time kept for the manifest / state writes
    for bid, endpoint, bulk, kind, files in order:
        if time.time() > deadline - reserve:
            notes.append("budget: %s not reached" % bid)
            continue
        est = blocks.get(bid, {}).get("seconds")
        if est and time.time() + est * 1.2 > deadline:
            notes.append("budget: %s needs ~%ds, skipped this run" % (bid, est))
            continue
        tb = time.time()
        st, files_done, rows_n = stage_block(files, universe, deadline - reserve / 2, notes)
        if files_done < len(files):
            st.close()
            notes.append("%s: %d/%d files staged before the deadline - block not emitted" % (bid, files_done, len(files)))
            blocks.setdefault(bid, {}).update({"files": len(files), "last_attempt": now.isoformat(), "partial": True})
            break
        hashes_key = ROOT + "_state/hashes/%s__%s.json.gz" % (bulk, kind)
        hashes = _get_json(hashes_key) or {}
        stats = {}
        new_hashes = emit_block(endpoint, bulk, kind, st, as_of, hashes, deadline, stats)
        st.close()
        _put_json_gz(hashes_key, new_hashes)
        secs = round(time.time() - tb, 1)
        blocks[bid] = {"endpoint": endpoint, "bulk": bulk, "period": kind, "files": len(files), "years": [files[-1][0], files[0][0]], "rows": rows_n,
                       "symbols": stats.get("symbols", 0), "written": stats.get("written", 0), "unchanged": stats.get("unchanged", 0), "failed": stats.get("failed", 0),
                       "bytes": stats.get("bytes", 0), "seconds": secs, "refreshed_at": now.isoformat(), "as_of": as_of, "partial": bool(stats.get("deadline_hit"))}
        done_blocks.append(bid)
        print("[history-book] %s: %d files, %d rows, %d symbols, written %d unchanged %d failed %d, %.0f s" % (bid, len(files), rows_n, stats.get("symbols", 0), stats.get("written", 0), stats.get("unchanged", 0), stats.get("failed", 0), secs))
        _put_json(STATE_KEY, state)
    stale = [bid for bid, b in blocks.items() if b.get("refreshed_at") and (now - datetime.fromisoformat(b["refreshed_at"])).total_seconds() > STALE_AFTER_S]
    pending = [bid for bid, *_ in order if bid not in done_blocks and not (blocks.get(bid, {}).get("refreshed_at") and bid not in stale)]
    phase = "STEADY" if not pending and not stale else "BACKFILL"
    manifest = {"schema": SCHEMA, "version": VERSION, "engine": ENGINE, "updated_at": now.isoformat(), "as_of": as_of, "prefix": ROOT, "phase": phase, "pending_blocks": pending,
                "layout": ROOT + "<bulk>/<annual|quarter>/<SYMBOL>.json.gz", "universe": "factor-book" if universe else "all symbols in the files",
                "universe_size": len(universe) if universe else None, "floor_annual": FLOOR_ANNUAL, "floor_quarter": FLOOR_QUARTER,
                "blocks": blocks, "stale_blocks": stale, "last_run": {"started": now.isoformat(), "seconds": round(time.time() - t0, 1), "blocks": done_blocks, "notes": notes},
                "read_with": "aws/shared/fmp_bank.statements (history book first) / fmp_bankfirst"}
    _put_json(MANIFEST_KEY, manifest)
    state.pop("lease_until", None)
    state["last_run"] = dict(manifest["last_run"], puts=sum(blocks[b].get("written", 0) for b in done_blocks), unchanged=sum(blocks[b].get("unchanged", 0) for b in done_blocks))
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["phase"] = phase
    _put_json(STATE_KEY, state)
    return {"ok": True, "version": VERSION, "phase": phase, "blocks": done_blocks, "notes": notes, "seconds": round(time.time() - t0, 1),
            "stale_blocks": stale, "pending": pending}
