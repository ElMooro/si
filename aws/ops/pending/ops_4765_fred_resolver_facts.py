"""ops/4765 -- the two facts that decide the dollar fix (read-only):
(1) does data/warm/fred-scoped/ hold per-series files (targeted
DTWEXBGS listing + first samples)? (2) what does fred-scoped's
manifest.json actually map (page ranges? series index?) -- print its
top-level structure and one page file's first record shape."""
import gzip
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def ls(prefix, n=6):
    r = s3.list_objects_v2(Bucket=B, Prefix=prefix, MaxKeys=n)
    return [o["Key"] for o in r.get("Contents") or []]


def get(key, nbytes=None):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw if nbytes is None else raw[:nbytes]


def main():
    with report("4765_fred_resolver_facts") as rep:
        rep.heading("ops 4765 -- fred resolver facts")
        rep.section("warm/fred-scoped shape")
        for k in ls("data/warm/fred-scoped/", 6):
            rep.log("  " + k)
        for k in ls("data/warm/fred-scoped/DTWEXBGS", 3):
            rep.ok("  WARM PER-SERIES HIT: " + k)
        rep.section("manifest structure")
        m = json.loads(get("data/providers/fred-scoped/manifest.json"))
        if isinstance(m, dict):
            rep.log("manifest keys: " + ", ".join(list(m.keys())[:12]))
            for k, v in list(m.items())[:3]:
                rep.log(f"  {k}: {json.dumps(v)[:220]}")
            pages = m.get("pages") or m.get("files") or []
            if isinstance(pages, list) and pages:
                rep.log("first page entry: " + json.dumps(pages[0])[:260])
                rep.log("last page entry: " + json.dumps(pages[-1])[:260])
            idx = m.get("index") or m.get("series_index")
            rep.kv(has_series_index=bool(idx))
        rep.section("one page file: first record shape")
        raw = get("data/providers/fred-scoped/series/page-0000.json", None)
        d = json.loads(raw)
        rep.kv(page0_type=type(d).__name__,
               page0_len=len(d) if hasattr(d, "__len__") else None)
        first = (d[0] if isinstance(d, list) and d else
                  list(d.items())[0] if isinstance(d, dict) and d else None)
        rep.log("first record: " + json.dumps(first, default=str)[:400])


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
