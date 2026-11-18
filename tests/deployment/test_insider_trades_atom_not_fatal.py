"""justhodl-insider-trades survives a failed EDGAR atom feed (daily health run 2026-11-18, ops 6837).

EDGAR's `browse-edgar?action=getcurrent` atom endpoint answered 404 on Nov 13-14 and 400 on Nov 16-17; the handler's step 1
treated that as FATAL and returned 502 before the daily-index backfill, the merge and the write - so data/insider-trades.json
aged 62 h (Nov 15 21:10 -> Nov 18 11:19) while EDGAR's daily form.idx (a different, archive endpoint) still listed every
Form 4. Now the atom phase is skipped (0 filings), the error is kept in `stats.diagnostics.atom_feed_error`, and the
daily-index backfill alone feeds the rolling window and the write.

Pinned here, offline (no network, no AWS): with `fetch_recent_form4_filings` raising, lambda_handler runs the backfill,
writes the three outputs and returns 200 with the error in the diagnostics; with the feed healthy nothing changes.
"""
import json
import sys
import time
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from test_insider_trades_deadline import _Ctx, _S3, _fake_index, _load  # noqa: E402


class _NoSuchKey(Exception):
    pass


class _S3Empty(_S3):
    """A bucket with nothing in it: every read is NoSuchKey, every write is recorded by key."""
    exceptions = types.SimpleNamespace(NoSuchKey=_NoSuchKey, ClientError=Exception)

    def __init__(self):
        super().__init__(cursor=None)
        self.puts = {}

    def get_object(self, Bucket, Key):
        raise _NoSuchKey(Key)

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body

    def head_object(self, **kw):
        raise _NoSuchKey("head")

    def list_objects_v2(self, **kw):
        return {}


def _run(mod, feed):
    mod.fetch_recent_form4_filings = feed
    s3 = _S3Empty()
    mod.boto3 = types.SimpleNamespace(client=lambda *a, **k: s3)
    mod.time = types.SimpleNamespace(time=time.time, sleep=lambda s: None)
    out = mod.lambda_handler({}, _Ctx(299_000))
    doc = json.loads(s3.puts[mod.S3_KEY])
    return out, s3, doc


def test_failed_atom_feed_still_backfills_and_writes():
    mod = _load()
    calls = _fake_index(mod, 3)

    def feed():
        raise Exception("HTTP Error 400: Bad Request")
    out, s3, doc = _run(mod, feed)
    assert out.get("statusCode") == 200, out
    assert mod.S3_KEY in s3.puts and mod.CURSOR_KEY in s3.puts, list(s3.puts)
    assert calls["idx"] > 0, "the daily-index backfill did not run"
    assert doc["stats"]["atom_feed_count"] == 0
    assert doc["stats"]["diagnostics"]["atom_feed_error"] == "Exception: HTTP Error 400: Bad Request"
    assert doc["coverage"]["filings_scanned_this_run"] == calls["sub"]


def test_healthy_atom_feed_unchanged():
    mod = _load()
    _fake_index(mod, 2)
    out, s3, doc = _run(mod, lambda: [])
    assert out.get("statusCode") == 200
    assert doc["stats"]["atom_feed_count"] == 0
    assert "atom_feed_error" not in doc["stats"]["diagnostics"]


if __name__ == "__main__":
    test_failed_atom_feed_still_backfills_and_writes()
    test_healthy_atom_feed_unchanged()
    print("ok")
