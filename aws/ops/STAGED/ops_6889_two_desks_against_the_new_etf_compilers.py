"""ops 6889 - READ-ONLY dry run: would the bond desk's cohort flows and the sector fusion come back if their pins named
the ETF compilers that are actually deployed?

Found 2026-11-21 while preparing the history-book reader lane: justhodl-bond-desk's own suite is red, and so is
production.  Both justhodl-bond-desk (bond_flow_qualification.UPSTREAM) and the sector fusion (aws/shared/
sector_fusion_pins.ETF_COMPILERS, used by justhodl-sector-flow-state and justhodl-sector-capital-fusion) accept the
ETF research publication only when it was compiled by four files with pinned sha256.  justhodl-etf-true-flows was
changed on 2026-11-11 / 11-12 (3.0.1, 3.1.0: two of the four files), the pins were not, and since then the bond desk
publishes with flow_status "withheld_ValueError" and the sector fusion refuses the issuer packet.

This runs, on the runner and against the live bucket, exactly the consumers' own code - once with the pins as they
are (the refusal must reproduce) and once with the pins set to the digests of the files in this checkout (which must
equal the digests the live publication names) - and reports what each would publish: the bond desk's cohort arithmetic
with its independent proof, the sector fusion's restored issuer packet.  Nothing is written, nothing is invoked: the
functions used (compile_raw, restore) only read.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys
import traceback

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
# 3.1.0 added a fifth compiler to the publication's inventory (etf_issuers); the first run of this script (19:27 UTC) showed it: "Complete reviewed upstream compiler required"
FILES = {"etf_issuers": "aws/lambdas/justhodl-etf-true-flows/source/etf_issuers.py", "etf_native": "aws/lambdas/justhodl-etf-true-flows/source/etf_native.py",
         "etf_research": "aws/lambdas/justhodl-etf-true-flows/source/etf_research.py", "etf_universe": "aws/lambdas/justhodl-etf-true-flows/source/etf_universe.py",
         "evidence_store": "aws/shared/evidence_store.py"}


def repo_digests(root=ROOT):
    return {name: hashlib.sha256((root / path).read_bytes()).hexdigest() for name, path in FILES.items()}


def compare(pinned, live, repo):
    """-> rows (name, pinned==live, repo==live) and whether re-pinning to the repo digests would match the live publication."""
    rows = [(n, pinned.get(n) == live.get(n), repo.get(n) == live.get(n)) for n in sorted(set(pinned) | set(live) | set(repo))]
    return rows, set(live) == set(repo) and all(r for _n, _p, r in rows)


def selftest():
    rows, ok = compare({"a": "1", "b": "2"}, {"a": "1", "b": "3"}, {"a": "1", "b": "3"})
    assert rows == [("a", True, True), ("b", False, True)] and ok is True
    assert compare({"a": "1"}, {"a": "2"}, {"a": "1"})[1] is False and compare({"a": "1"}, {"a": "1", "c": "9"}, {"a": "1"})[1] is False
    assert set(repo_digests()) == set(FILES) and all(len(v) == 64 for v in repo_digests().values())


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120))
    at = datetime.now(timezone.utc).isoformat()
    repo = repo_digests()
    with report("ops_6889_two_desks_against_the_new_etf_compilers") as r:
        r.heading("ops 6889 - bond desk and sector fusion against the deployed ETF compilers (read-only dry run), %s" % at[:16])

        r.section("1. What is published today")
        for key, pick in (("data/bond-desk.json", lambda d: {"generated_at": d.get("generated_at"), "version": d.get("version"), "flow_status": (((d.get("regions") or {}).get("us") or {}).get("flows") or {}).get("flow_status")}),
                          ("data/sector-flow-state.json", lambda d: {k: d.get(k) for k in ("generated_at", "contract", "status", "version") if k in d}),
                          ("data/sector-capital-fusion.json", lambda d: {k: d.get(k) for k in ("generated_at", "contract", "status", "version") if k in d}),
                          ("data/etf-true-flows.json", lambda d: {"generated_at": d.get("generated_at"), "contract": d.get("contract"), "version": d.get("version"), "run": ((d.get("replay") or {}).get("manifest_key") or "")[-24:]})):
            try:
                o = s3.get_object(Bucket=LIVE, Key=key)
                r.log("%-34s written %s | %s" % (key, o["LastModified"].strftime("%Y-%m-%d %H:%M"), json.dumps(pick(json.loads(o["Body"].read())))[:200]))
            except Exception as e:  # noqa: BLE001
                r.log("%-34s %s" % (key, type(e).__name__))

        r.section("2. Bond desk: the cohort compiler, pins as they are and as they would be")
        bond_ok = False
        try:
            sys.path[:0] = [str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-bond-desk" / "source")]
            import bond_flow_store as store
            read = store.reader(s3, LIVE)
            raw = read(store.SOURCE)
            packet = store.strict(raw)
            manifest = store.strict(read(packet["replay"]["manifest_key"]))
            live = {n: c.get("sha256") for n, c in (manifest.get("compilers") or {}).items()}
            rows, would_match = compare(dict(store.UPSTREAM), live, repo)
            for n, p, q in rows:
                r.log("  %-15s pinned %s | live publication %s | this checkout %s | pin matches live: %s | checkout matches live: %s" % (
                    n, str(store.UPSTREAM.get(n))[:12], str(live.get(n))[:12], str(repo.get(n))[:12], p, q))
            try:
                store.compile_raw(raw, at, read)
                r.log("with the pins as they are: accepted (nothing to fix)")
            except Exception as e:  # noqa: BLE001
                r.log("with the pins as they are: refused - %s: %s" % (type(e).__name__, e))
            if not would_match:
                r.warn("the live publication was not compiled by the files in this checkout; re-pinning to them would not help")
            else:
                store.UPSTREAM = dict(repo)
                output, proof = store.compile_raw(raw, at, read)
                compat = store.compatibility(output)
                r.log("with the pins set to this checkout's digests: accepted | binding %s" % json.dumps({k: output["upstream_binding"][k] for k in ("whole_packet_bound", "issuer_originals_replayed_here")}))
                r.log("independent proof: %s" % json.dumps(proof)[:400])
                for name, row in compat.items():
                    r.log("  cohort %-22s 5-observation flow %s USD | funds %s of %s | %s" % (name, row.get("flow_5d_usd"), row.get("n"), row.get("configured_n"), row.get("status")))
                bond_ok = True
                r.ok("the bond desk's cohort flows compile and prove against the live publication once the pins name the deployed compilers")
        except Exception as e:  # noqa: BLE001
            r.warn("bond desk dry run failed: %s: %s" % (type(e).__name__, str(e)[:200]))
            r.log(traceback.format_exc()[-700:])

        r.section("3. Sector fusion: the issuer restore, pins as they are and as they would be")
        sector_ok = False
        try:
            import sector_fusion_issuer as issuer
            import sector_fusion_store as fstore
            fread = fstore.reader(s3, LIVE)
            fpacket = json.loads(fread("data/etf-true-flows.json"))
            try:
                issuer.restore(fpacket, fread)
                r.log("with the pins as they are: accepted (nothing to fix)")
            except Exception as e:  # noqa: BLE001
                r.log("with the pins as they are: refused - %s: %s" % (type(e).__name__, e))
            issuer.ETF_COMPILERS = dict(repo)
            issuance = issuer.restore(fpacket, fread)
            shape = {k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in issuance.items()} if isinstance(issuance, dict) else type(issuance).__name__
            r.log("with the pins set to this checkout's digests: restored | %s" % json.dumps(shape, default=str)[:500])
            sector_ok = True
            r.ok("the sector fusion restores the issuer packet from the retained originals once the pins name the deployed compilers")
        except Exception as e:  # noqa: BLE001
            r.warn("sector fusion dry run: %s: %s" % (type(e).__name__, str(e)[:240]))
            r.log(traceback.format_exc()[-900:])
        r.kv(bond_desk_would_publish_flows=bond_ok, sector_fusion_would_restore=sector_ok, repo_digests={k: v[:16] for k, v in repo.items()})
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
