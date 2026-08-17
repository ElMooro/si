"""ops/4852 -- Fusion 3 schema probe (report-only, ZERO writes).
The catalyst chain will fuse catalyst x readthrough x backlog-miner
x estimate-revisions into second-order propagation.  Before any
design: dump the REAL schemas -- top keys, per-ticker container
shape, one sample row verbatim, counts, freshness -- for each
feed.  Hard-fail only if catalyst.json (the spine) is unreadable.
"""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

CANDIDATES = {
    "catalyst": ["data/catalyst.json"],
    "readthrough": ["data/readthrough.json"],
    "backlog": ["data/backlog-miner.json", "data/backlog.json",
                "data/backlog-mined.json"],
    "est_revisions": ["data/estimate-revisions.json",
                      "data/est-revisions.json"],
}


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def clip(o, n=340):
    s = json.dumps(o, ensure_ascii=False, default=str)
    return s[:n] + ("..." if len(s) > n else "")


def dump(rep, name, doc):
    rep.ok("%s: top keys %s" % (name, sorted(doc)[:14]))
    rep.log("  generated_at=%s v=%s status=%s"
            % (doc.get("generated_at", "")[:19], doc.get("v"),
               doc.get("status")))
    for k, v in doc.items():
        if isinstance(v, dict) and v and len(v) > 5:
            fk = next(iter(v))
            rep.log("  dict '%s' n=%d sample key=%s"
                    % (k, len(v), fk))
            rep.log("   %s" % clip(v[fk]))
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            rep.log("  list '%s' n=%d row0:" % (k, len(v)))
            rep.log("   %s" % clip(v[0]))


def main():
    with report("ops 4852 -- fusion3 schema probe") as rep:
        for name, keys in CANDIDATES.items():
            rep.heading(name)
            found = False
            for key in keys:
                try:
                    doc = sread(key)
                except ClientError:
                    rep.log("  %s absent" % key)
                    continue
                rep.log("  KEY=%s" % key)
                dump(rep, name, doc)
                found = True
                break
            if not found:
                if name == "catalyst":
                    rep.fail("catalyst spine unreadable -- "
                             "cannot design the chain")
                    sys.exit(1)
                rep.warn("  %s: no candidate key found -- list "
                         "sweep next" % name)
        rep.heading("verdict")
        rep.ok("schemas dumped -- Fusion 3 design binds ONLY "
               "what is printed above")


if __name__ == "__main__":
    main()
