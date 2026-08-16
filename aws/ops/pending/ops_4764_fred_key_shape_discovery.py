"""ops/4764 -- discover the REAL fred-scoped per-series key shape.
justhodl-repo's three candidates all missed (dollar rows = 0). Instead
of guessing a fourth: list the actual layout -- root sample, then
targeted prefixes for DTWEXBGS across plausible roots -- and print the
exact keys. Read-only; the engine patch follows from the evidence."""
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


def main():
    with report("4764_fred_key_shape_discovery") as rep:
        rep.heading("ops 4764 -- fred-scoped key shape (read-only)")
        rep.section("root sample")
        for k in ls("data/providers/fred-scoped/", 8):
            rep.log("  " + k)
        rep.section("targeted DTWEXBGS across plausible roots")
        for p in ("data/providers/fred-scoped/series/DTWEXBGS",
                   "data/providers/fred-scoped/DTWEXBGS",
                   "data/providers/fred-scoped/D/DTWEXBGS",
                   "data/providers/fred-scoped/DT/DTWEXBGS",
                   "data/warm/fred/DTWEXBGS",
                   "data/fred/DTWEXBGS",
                   "data/providers/fred/DTWEXBGS"):
            hits = ls(p, 3)
            rep.kv(prefix=p, hits=len(hits))
            for k in hits:
                rep.ok("  FOUND " + k)
        rep.section("if nothing hit: where do 279k fred keys live at all?")
        for root in ("data/providers/", "data/warm/"):
            r = s3.list_objects_v2(Bucket=B, Prefix=root, Delimiter="/",
                                     MaxKeys=40)
            subs = [c["Prefix"] for c in r.get("CommonPrefixes") or []]
            fredish = [x for x in subs if "fred" in x.lower()]
            rep.log(f"{root} fred-ish subdirs: {fredish}")
            for f in fredish[:2]:
                for k in ls(f, 5):
                    rep.log("   sample " + k)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
