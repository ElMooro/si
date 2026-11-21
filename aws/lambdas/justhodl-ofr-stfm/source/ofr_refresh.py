"""Daily refresh of the OFR bank (2026-11-21).

The bank fetched every mnemonic's full history once and never asked again: a mnemonic in state.done was skipped for
good.  ops 6893 measured the result - all 442 series files last written 2026-08-06, 312 of them behind the source,
the daily reference rates and the tri-party / GCF / DVP venues by 74 observations - and with it the repo board's
venue data, which reads this bank and nothing else.

One dataset call returns every series of a dataset with its whole history (fnyr 1.7 MB, repo 9.6 MB, nypd 3.7 MB,
mmf 0.2 MB, tyld 2.2 MB: about 13 seconds together), so five calls a day keep the whole bank current.  A series file
is written only when its observations changed; a series that comes back much shorter than the banked one is refused
and the banked file kept (a truncated answer must not replace a full history).
"""
import gzip
import hashlib
import json

DATASETS = ("fnyr", "repo", "nypd", "mmf", "tyld")
HF_DATASETS = ("ficc",)
SHRINK_FLOOR = 0.9          # a series may lose a few revised points, never a tenth of its history


def unzip(raw):
    """The dataset calls answer gzip whatever the request asked for."""
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def series_of(doc):
    """{mnemonic: {timeseries, metadata}} out of a dataset answer ({short_name, long_name, timeseries: {...}})."""
    if not isinstance(doc, dict):
        return {}
    inner = doc.get("timeseries") if isinstance(doc.get("timeseries"), dict) else doc
    return {str(m): body for m, body in inner.items() if isinstance(body, dict) and "timeseries" in body}


def observations(body):
    """Every [date, value] row of a series body, across its aggregations, in the order the source gives them."""
    ts = body.get("timeseries") if isinstance(body, dict) else None
    rows = []
    for part in (ts.values() if isinstance(ts, dict) else [ts] if isinstance(ts, list) else []):
        if isinstance(part, list):
            rows += [r for r in part if isinstance(r, (list, tuple)) and len(r) >= 2]
    return rows


def signature(body):
    """What makes a series file worth writing again: its observations.  Metadata alone does not."""
    ts = body.get("timeseries") if isinstance(body, dict) else None
    return hashlib.sha1(json.dumps(ts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def newest(body):
    """The newest date that carries a value, and how many rows do."""
    rows = [r for r in observations(body) if r[1] is not None]
    return (str(max(r[0] for r in rows))[:10] if rows else None), len(rows)


def series_doc(mnemonic, body, url, now):
    """The bank's layout for a short-term-funding series - the layout /series/full was stored in."""
    return {"mnemonic": mnemonic, "source_url": url, "raw_snapshot_key": None, "as_of": now, "refreshed_by": "dataset",
            "payload": {mnemonic: body}}


def refresh(fetch, put, known, base, datasets, now, prefix, layout="stfm", dry_run=False, log=print):
    """Bring every series of `datasets` up to the source.

    fetch(url) -> bytes; put(key, bytes) writes one series file; known = {mnemonic: [signature, rows, newest date]}
    as of the last write, updated in place (never in a dry run).  -> the run record.
    """
    out = {"at": now, "dry_run": bool(dry_run), "datasets": {}, "written": 0, "unchanged": 0, "shrank": [], "failed": [],
           "written_ids": [], "newest": {}}
    for ds in datasets:
        url = "%s/series/dataset?dataset=%s" % (base, ds)
        row = {"series": 0, "written": 0, "unchanged": 0}
        out["datasets"][ds] = row
        try:
            source = series_of(json.loads(unzip(fetch(url))))
        except Exception as e:  # noqa: BLE001 - one dataset failing must not stop the others
            row["error"] = "%s: %s" % (type(e).__name__, str(e)[:80])
            log("[ofr] dataset %s failed: %s" % (ds, row["error"]))
            continue
        if not source:
            row["error"] = "no series in the answer"
            continue
        row["series"] = len(source)
        for m in sorted(source):
            body = source[m]
            sig, (last, n) = signature(body), newest(body)
            if last:
                out["newest"][last] = out["newest"].get(last, 0) + 1
            had = known.get(m) or [None, 0, None]
            if had[0] == sig:
                row["unchanged"] += 1
                out["unchanged"] += 1
                continue
            if had[1] and n < had[1] * SHRINK_FLOOR:
                out["shrank"].append({"mnemonic": m, "banked_rows": had[1], "source_rows": n})
                continue
            if not dry_run:
                try:
                    if layout == "stfm":
                        payload = gzip.compress(json.dumps(series_doc(m, body, url, now)).encode())
                    else:                      # the hedge-fund catalog keeps the bare /series/full answer
                        payload = gzip.compress(json.dumps({m: body}).encode())
                    put("%s%s.json.gz" % (prefix, m), payload)
                except Exception as e:  # noqa: BLE001 - the old file and the old signature stay; the next run tries again
                    out["failed"].append({"mnemonic": m, "error": "%s: %s" % (type(e).__name__, str(e)[:60])})
                    continue
                known[m] = [sig, n, last]
            row["written"] += 1
            out["written"] += 1
            out["written_ids"].append(m)
    out["newest"] = dict(sorted(out["newest"].items(), reverse=True)[:6])
    return out


def record(run):
    """The part of a run record worth keeping in the state file."""
    keep = {k: run[k] for k in ("at", "dry_run", "datasets", "written", "unchanged", "newest")}
    keep["shrank"] = run["shrank"][:20]
    keep["failed"] = run["failed"][:20]
    return keep
