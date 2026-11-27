"""justhodl-sec-datasets - the SEC's bulk data sets, kept in the warehouse and read into compact tables.

Khalid 2026-11-26: "go and add as many files as my system needs especially the ones that would improve my system and
its data exponentially". 45 engines read SEC filings one company at a time; none read the SEC's own bulk data sets
except fails-to-deliver and the statement sets. ops 6986 measured the library (23 data sets) and this bank keeps seven:

    nport      Form N-PORT       every registered fund's holdings (bonds, loans, swaps, stock on loan)   quarterly
    nmfp       Form N-MFP        every money market fund's portfolio and daily flows                     monthly
    bdc        BDC data sets     business development companies' schedules of investments                monthly
    form13f    Form 13F          every institutional manager's holdings                                  quarterly
    insiders   Forms 3 / 4 / 5   every insider transaction since 2006                                    quarterly
    formd      Form D            private offerings since 2008                                            quarterly
    ncen       Form N-CEN        the annual census of registered funds                                   quarterly

One run a day, no self-chaining, no event, no invoke:
  1. read each data set's page on sec.gov and list the files it advertises;
  2. bank files the warehouse lacks, verbatim (sha256 / pulled_at / the SEC's own Last-Modified in the object's
     metadata), inside a byte budget: the newest file of every set first, then the history, newest first. A banked
     file whose size on sec.gov changed is banked again (the SEC refreshes files);
  3. read banked files into tables: every month of N-MFP (sec_derive.nmfp_month) and one quarter of N-PORT a run
     (sec_derive.nport_quarter);
  4. publish data/sec-money-funds.json, data/sec-fund-holdings.json and the bank's manifest.

Identifiers: the raw files carry CUSIP and ISIN and are kept verbatim OUTSIDE the site's /data/ tree (RAW_PREFIX).
Nothing under data/ that this engine writes carries a CUSIP or an ISIN.

Event: {} = the daily run. {"dry_run": true} = pages read, plan returned, nothing fetched or written.
{"only": ["nmfp"]} limits a run to the named sets. {"bank": false} or {"derive": false} skips a phase.
"""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import gzip
import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.request
import zipfile

import boto3
from botocore.config import Config

import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import sec_derive
import sec_sets

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "sec-datasets/raw/")
OUT_PREFIX = os.environ.get("OUT_PREFIX", "data/warm/sec-datasets/")
MANIFEST_KEY = OUT_PREFIX + "manifest.json"
MONEY_FUNDS_KEY = os.environ.get("MONEY_FUNDS_KEY", "data/sec-money-funds.json")
HOLDINGS_KEY = os.environ.get("HOLDINGS_KEY", "data/sec-fund-holdings.json")
BANK_BYTES_PER_RUN = int(os.environ.get("BANK_BYTES_PER_RUN", str(2500 * 1024 * 1024)))
NMFP_PER_RUN = int(os.environ.get("NMFP_PER_RUN", "12"))
NPORT_SECONDS = int(os.environ.get("NPORT_SECONDS", "430"))      # a quarter is only started with this much time left
RESERVE_SECONDS = int(os.environ.get("RESERVE_SECONDS", "40"))   # kept back to publish
WORK = os.environ.get("WORK_DIR", "/tmp/sec-datasets")
MIN_MONEY_FUNDS, MIN_MONEY_NET = 150, 3e12                       # a month below either is kept but not shown as the newest
MIN_FUNDS, MIN_SECURITIES = 5000, 50000                          # the same for a quarter of holdings
RECHECK_NEWEST = 2                                               # the newest files of each set are re-measured every run

_S3 = None
_UA = None


def s3():
    global _S3
    if _S3 is None:
        _S3 = boto3.client("s3", config=Config(retries={"max_attempts": 4, "mode": "standard"}, max_pool_connections=16))
    return _S3


def user_agent():
    """The house SEC contact string (aws/shared/edgar.py). Sent as a header; never logged, returned or stored."""
    global _UA
    if _UA is None:
        from edgar import USER_AGENT
        _UA = USER_AGENT
    return _UA


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sec_get(url, to_path=None, method="GET", timeout=120):
    """(status, body bytes or None, headers). A zip goes straight to disk. One request, then a short pause (the SEC asks for under ten a second)."""
    if not url.startswith("https://www.sec.gov/"):
        return 0, None, {}
    req = urllib.request.Request(url, method=method, headers={"User-Agent": user_agent(), "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            headers = {k.lower(): v for k, v in resp.headers.items()}
            if method == "HEAD":
                return resp.status, None, headers
            if to_path:
                with open(to_path, "wb") as fh:
                    shutil.copyfileobj(resp, fh, 1 << 20)
                return resp.status, None, headers
            return resp.status, resp.read(), headers
    except urllib.error.HTTPError as exc:
        return exc.code, None, {}
    except Exception:                         # noqa: BLE001 - a timeout or reset is a status, not a crash
        return 0, None, {}
    finally:
        time.sleep(0.2)


def get_json(key, default=None):
    try:
        body = s3().get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    except Exception:                         # noqa: BLE001
        return default
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body)


def put_json(key, doc, cache="max-age=300"):
    body = json.dumps(doc, separators=(",", ":"), allow_nan=False).encode("utf-8")
    extra = {}
    if key.endswith(".gz"):
        body, extra["ContentEncoding"] = gzip.compress(body, 6), "gzip"
    s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json", CacheControl=cache, **extra)
    return len(body)


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def new_manifest():
    return {"engine": "justhodl-sec-datasets", "sets": {}, "runs": []}


def read_library(manifest, only, log):
    """{set: [advertised file, ...]} from the SEC's pages; a page that does not answer keeps the set out of this run."""
    library = {}
    for name, spec in sec_sets.SETS.items():
        if only and name not in only:
            continue
        entry = manifest["sets"].setdefault(name, {"what": spec["what"], "page": spec["page"], "files": {}})
        entry["what"], entry["page"] = spec["what"], spec["page"]
        status, body, _ = sec_get(spec["page"])
        files = sec_sets.advertised(body, spec["page"], spec["match"]) if status == 200 and body else []
        entry["page_status"], entry["page_read_at"] = status, now_iso()
        if not files:
            log.append("%s: page answered %s with no files; the set is left as it is" % (name, status))
            continue
        entry["advertised"] = len(files)
        entry["newest"], entry["oldest"] = files[0]["name"], files[-1]["name"]
        library[name] = files
    return library


def modified_after(http_date, pulled_at):
    """True when the SEC's Last-Modified is later than our pull: a refresh that kept the file's size."""
    try:
        return parsedate_to_datetime(http_date) > datetime.fromisoformat(pulled_at)
    except Exception:                         # noqa: BLE001 - no header, or one that does not parse, is not a change
        return False


def remeasure(manifest, library, log):
    """The newest banked files of each set are measured again: one whose size changed on sec.gov, or that was modified after we pulled it, is banked again."""
    for name, files in library.items():
        have = manifest["sets"][name]["files"]
        for f in files[:RECHECK_NEWEST]:
            rec = have.get(f["name"])
            if not rec or not rec.get("bytes"):
                continue
            status, _, headers = sec_get(f["url"], method="HEAD")
            size = int(headers.get("content-length") or 0)
            newer = modified_after(headers.get("last-modified"), rec.get("pulled_at"))
            if status == 200 and ((size and size != rec["bytes"]) or newer):
                log.append("%s %s changed on sec.gov (%d -> %d bytes%s): banked again" % (name, f["name"], rec["bytes"], size, ", modified since we pulled it" if newer else ""))
                rec["superseded"] = dict(bytes=rec["bytes"], sha256=rec.get("sha256"), pulled_at=rec.get("pulled_at"))
                have[f["name"] + "#stale"] = rec
                del have[f["name"]]


def bank_file(set_name, f, log):
    """One advertised file to local disk and to the warehouse. (record, local path) or (None, None)."""
    os.makedirs(WORK, exist_ok=True)
    path = os.path.join(WORK, f["name"])
    status, _, headers = sec_get(f["url"], to_path=path, timeout=600)
    if status != 200 or not os.path.exists(path):
        log.append("%s %s: download answered %s" % (set_name, f["name"], status))
        return None, None
    size = os.path.getsize(path)
    declared = int(headers.get("content-length") or 0)
    try:
        with zipfile.ZipFile(path) as zf:
            members = len(zf.infolist())
    except zipfile.BadZipFile:
        members = 0
    if (declared and declared != size) or not members:
        log.append("%s %s: incomplete (%d of %d bytes, %d members); not banked" % (set_name, f["name"], size, declared, members))
        os.remove(path)
        return None, None
    digest = sha256_of(path)
    key = "%s%s/%s" % (RAW_PREFIX, set_name, f["name"])
    s3().upload_file(path, S3_BUCKET, key, ExtraArgs={"ContentType": "application/zip", "Metadata": {
        "sha256": digest, "pulled_at": now_iso(), "source": "sec.gov", "sec_last_modified": headers.get("last-modified", "")[:40]}})
    return {"key": key, "bytes": size, "sha256": digest, "pulled_at": now_iso(), "sec_last_modified": headers.get("last-modified"),
            "period": sec_sets.iso(f["period"]), "members": members}, path


def local_copy(rec, name, kept):
    """A banked file on local disk: the copy this run downloaded, or one fetched from the warehouse."""
    if name in kept and os.path.exists(kept[name]):
        return kept[name]
    os.makedirs(WORK, exist_ok=True)
    path = os.path.join(WORK, name)
    s3().download_file(S3_BUCKET, rec["key"], path)
    kept[name] = path
    return path


def drop_local(name, kept):
    path = kept.pop(name, None)
    if path and os.path.exists(path):
        os.remove(path)


def pending(files):
    """Banked files not read into tables yet, newest first."""
    out = [(n, r) for n, r in files.items() if "#" not in n and r.get("key") and "derived" not in r]
    return sorted(out, key=lambda nr: (nr[1].get("period") or "", nr[0]), reverse=True)


def derive_nmfp(manifest, kept, left, log):
    done = 0
    files = manifest["sets"].get("nmfp", {}).get("files", {})
    for name, rec in pending(files):
        if done >= NMFP_PER_RUN or left() < 90:
            break
        path = local_copy(rec, name, kept)
        try:
            with zipfile.ZipFile(path) as zf:
                doc = sec_derive.nmfp_month(zf)
        except sec_derive.NotHandled as exc:
            rec["derived"] = {"not_handled": str(exc)[:200], "at": now_iso()}
            log.append("nmfp %s: not read (%s)" % (name, str(exc)[:120]))
            drop_local(name, kept)
            continue
        doc.update(engine="justhodl-sec-datasets", version=VERSION, source_file=name, generated_at=now_iso())
        key = "%sderived/nmfp/%s.json.gz" % (OUT_PREFIX, doc["month"])
        put_json(key, doc)
        rec["derived"] = {"key": key, "month": doc["month"], "at": now_iso(), "summary": sec_derive.nmfp_summary(doc)}
        drop_local(name, kept)
        done += 1
    return done


class OutOfTime(Exception):
    """Raised between stages of a long read when the run's time is nearly spent."""


def derive_nport(manifest, kept, left, log):
    files = manifest["sets"].get("nport", {}).get("files", {})
    todo = pending(files)
    if not todo:
        return 0
    if left() < NPORT_SECONDS:
        log.append("nport: %d quarter(s) wait for a run with %d s to spare" % (len(todo), NPORT_SECONDS))
        return 0
    name, rec = todo[0]
    path = local_copy(rec, name, kept)
    notes = []

    def progress(msg):
        if msg:
            notes.append("%s (%d s left)" % (msg, left()))
        if left() < 20:
            raise OutOfTime()
    try:
        docs = sec_derive.nport_quarter(path, WORK, progress=progress)
    except sec_derive.NotHandled as exc:
        rec["derived"] = {"not_handled": str(exc)[:200], "at": now_iso()}
        log.append("nport %s: not read (%s)" % (name, str(exc)[:120]))
        drop_local(name, kept)
        return 0
    except OutOfTime:
        rec["read_attempts"] = int(rec.get("read_attempts") or 0) + 1
        log.append("nport %s: out of time on attempt %d (%s)" % (name, rec["read_attempts"], "; ".join(notes[-2:])))
        if rec["read_attempts"] >= 3:
            rec["derived"] = {"not_handled": "three runs ran out of time reading it", "at": now_iso()}
        drop_local(name, kept)
        return 0
    stem = name.rsplit(".", 1)[0]
    keys = {}
    for part, suffix in (("summary", "summary.json"), ("bonds", "bonds.json.gz"), ("cds", "cds.json.gz"), ("stock_loan", "stock-loan.json.gz")):
        docs[part].update(engine="justhodl-sec-datasets", version=VERSION, source_file=name, generated_at=now_iso())
        keys[part] = "%sderived/nport/%s/%s" % (OUT_PREFIX, stem, suffix)
        put_json(keys[part], docs[part])
    rec["derived"] = {"keys": keys, "at": now_iso(), "summary": sec_derive.nport_summary(docs), "notes": notes[-6:]}
    drop_local(name, kept)
    return 1


def publish_money_funds(manifest):
    """data/sec-money-funds.json: the newest month whole, and every month's headline figures."""
    months = sorted((r["derived"] for n, r in manifest["sets"].get("nmfp", {}).get("files", {}).items() if "#" not in n and (r.get("derived") or {}).get("summary")),
                    key=lambda d: d["month"])
    by_month = {d["month"]: d for d in months}                   # a later file for the same month replaces the earlier
    months = [by_month[m] for m in sorted(by_month)]
    good = [d for d in months if d["summary"]["funds"] >= MIN_MONEY_FUNDS and d["summary"]["net_assets"] >= MIN_MONEY_NET]
    if not good:
        return None
    newest = get_json(good[-1]["key"])
    if not newest:
        return None
    doc = {"engine": "justhodl-sec-datasets", "version": VERSION, "generated_at": now_iso(), "as_of": newest["month"], "source": {
        "name": "SEC Form N-MFP data sets", "url": sec_sets.SETS["nmfp"]["page"], "file": newest.get("source_file"),
        "note": "Every money market fund reports its whole portfolio for month-end; the SEC posts the month about five weeks later."},
        "month": newest, "history": [d["summary"] for d in months],
        "history_note": "One row per month read so far. Older months arrive as the bank fills, newest first.",
        "set_aside": [d["month"] for d in months if d not in good]}
    put_json(MONEY_FUNDS_KEY, doc)
    return doc["as_of"]


def publish_holdings(manifest):
    """data/sec-fund-holdings.json: the newest quarter's summary, its credit swaps and stock on loan, and where the tables are."""
    quarters = sorted(((n, r["derived"]) for n, r in manifest["sets"].get("nport", {}).get("files", {}).items() if "#" not in n and (r.get("derived") or {}).get("summary")),
                      key=lambda nd: nd[1]["summary"]["as_of"])
    good = [(n, d) for n, d in quarters if d["summary"]["funds"] >= MIN_FUNDS and d["summary"]["securities"] >= MIN_SECURITIES]
    if not good:
        return None
    name, newest = good[-1]
    summary, cds, loan = get_json(newest["keys"]["summary"]), get_json(newest["keys"]["cds"]), get_json(newest["keys"]["stock_loan"])
    if not (summary and cds and loan):
        return None
    most_lent = sorted((r for r in loan["rows"] if r[3] >= 5e8 and r[6] is not None), key=lambda r: -r[6])[:150]
    doc = {"engine": "justhodl-sec-datasets", "version": VERSION, "generated_at": now_iso(), "as_of": summary["as_of"], "source": {
        "name": "SEC Form N-PORT data sets", "url": sec_sets.SETS["nport"]["page"], "file": name,
        "note": "Funds report holdings as of their own quarter-end month; the SEC posts a quarter of filings at the end of the next quarter, so "
                "holdings are two to five months old. Marks are the funds' own valuations."},
        "summary": summary, "tables": newest["keys"],
        "cds": {"totals": cds["totals"], "cols": cds["cols"], "names": cds["names"][:300], "indices": cds["indices"][:40],
                "counterparties": cds["counterparties"][:25], "reading": cds["reading"]},
        "stock_loan": {"totals": loan["totals"], "cols": loan["cols"], "most_lent": most_lent, "largest": loan["rows"][:150], "reading": loan["reading"],
                       "most_lent_rule": "stocks funds hold $500m or more of, by the share of that out on loan"},
        "history": [d["summary"] for _, d in quarters], "set_aside": [n for n, d in quarters if (n, d) not in good]}
    put_json(HOLDINGS_KEY, doc)
    return doc["as_of"]


def totals(manifest):
    out = {}
    for name, entry in manifest["sets"].items():
        files = {n: r for n, r in entry.get("files", {}).items() if "#" not in n and r.get("key")}
        out[name] = {"advertised": entry.get("advertised"), "banked": len(files), "bytes": sum(r.get("bytes") or 0 for r in files.values()),
                     "newest_banked": max(files, key=lambda n: (files[n].get("period") or "", n)) if files else None,
                     "read_into_tables": sum(1 for r in files.values() if (r.get("derived") or {}).get("summary")),
                     "waiting_to_be_read": len(pending(entry.get("files", {}))) if sec_sets.SETS.get(name, {}).get("derive") else None}
    return out


def run(event, context):
    t0 = time.time()
    budget = (context.get_remaining_time_in_millis() / 1000.0) if context is not None and hasattr(context, "get_remaining_time_in_millis") else 840.0
    left = lambda: budget - (time.time() - t0) - RESERVE_SECONDS       # noqa: E731
    event = event if isinstance(event, dict) else {}
    dry = bool(event.get("dry_run"))
    only = [s for s in (event.get("only") or []) if s in sec_sets.SETS] or None
    log = []
    manifest = get_json(MANIFEST_KEY) or new_manifest()
    manifest.setdefault("sets", {})
    library = read_library(manifest, only, log)
    if not library:
        raise RuntimeError("no SEC data-set page answered with files: %s" % "; ".join(log)[:400])
    if not dry:
        remeasure(manifest, library, log)
    banked = {name: {n: r for n, r in manifest["sets"][name]["files"].items() if "#" not in n and r.get("key")} for name in library}
    plan = sec_sets.plan(library, banked, BANK_BYTES_PER_RUN) if event.get("bank", True) else []
    if dry:
        return {"dry_run": True, "version": VERSION, "seconds": round(time.time() - t0, 1), "notes": log,
                "advertised": {n: len(f) for n, f in library.items()}, "newest": {n: f[0]["name"] for n, f in library.items()},
                "already_banked": {n: len(b) for n, b in banked.items()},
                "would_bank": [[s, f["name"], f.get("size_shown"), "newest" if tier == 0 else "history"] for s, f, tier in plan],
                "would_bank_bytes_shown": sum(f.get("size_shown") or 0 for _, f, _ in plan),
                "waiting_to_be_read": {n: len(pending(manifest["sets"][n]["files"])) for n in library if sec_sets.SETS[n]["derive"]}}

    kept, counts = {}, {"banked": 0, "bytes": 0}

    def bank(tier, floor):
        """Bank the plan's files of one tier while more than `floor` seconds are left."""
        for set_name, f, t in plan:
            if t != tier:
                continue
            if left() < floor:
                log.append("banking stopped with %d s left; the rest waits for the next run" % left())
                return
            rec, path = bank_file(set_name, f, log)
            if not rec:
                continue
            manifest["sets"][set_name]["files"][f["name"]] = rec
            counts["banked"] += 1
            counts["bytes"] += rec["bytes"]
            if sec_sets.SETS[set_name]["derive"] and tier == 0:
                kept[f["name"]] = path
            else:
                os.remove(path)

    def checkpoint():
        manifest.update(engine="justhodl-sec-datasets", version=VERSION, generated_at=now_iso(), raw_prefix=RAW_PREFIX, totals=totals(manifest))
        put_json(MANIFEST_KEY, manifest, cache="no-cache")

    # the newest file of every set first, then what they hold is read, then history with the time that is left
    bank(0, 150)
    if counts["banked"]:
        checkpoint()                          # what was banked is on record before the long read begins
    n_months = n_quarters = 0
    if event.get("derive", True):
        n_months = derive_nmfp(manifest, kept, left, log)
        n_quarters = derive_nport(manifest, kept, left, log)
        if n_months or n_quarters:
            checkpoint()
    before = counts["banked"]
    bank(1, 150)
    if event.get("derive", True) and counts["banked"] > before:
        n_months += derive_nmfp(manifest, kept, left, log)        # months of history banked just now (a quarter of holdings waits for its own run)
    n_banked, bytes_banked = counts["banked"], counts["bytes"]
    for name in list(kept):
        drop_local(name, kept)
    published = {"money_funds": publish_money_funds(manifest), "fund_holdings": publish_holdings(manifest)}
    stats = {"at": now_iso(), "seconds": round(time.time() - t0, 1), "banked": n_banked, "bytes_banked": bytes_banked, "months_read": n_months,
             "quarters_read": n_quarters, "published": published, "notes": log[-20:], "trigger": event.get("trigger")}
    manifest.update(engine="justhodl-sec-datasets", version=VERSION, generated_at=now_iso(), raw_prefix=RAW_PREFIX, totals=totals(manifest),
                    documents={"money_funds": MONEY_FUNDS_KEY, "fund_holdings": HOLDINGS_KEY},
                    note="Raw files are kept verbatim under raw_prefix, outside the site's /data/ tree, because they carry security identifiers. "
                         "Everything under data/ is identifier-free.")
    manifest["runs"] = (manifest.get("runs") or [])[-29:] + [stats]
    put_json(MANIFEST_KEY, manifest, cache="no-cache")
    return {"ok": True, "version": VERSION, **{k: stats[k] for k in ("seconds", "banked", "bytes_banked", "months_read", "quarters_read", "published")},
            "totals": manifest["totals"], "notes": log[-8:]}


def lambda_handler(event, context):
    chain_guard.begin(event, context)   # trigger cap: a root run (the schedule) or whatever chain started it
    try:
        return run(event, context)
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
