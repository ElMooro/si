"""ops 6902 - DRY RUN from the checkout: the dealer engine's whole run, with every write kept in memory.

ops 6899 / 6900: justhodl-nyfed-pd has published nothing since 2026-11-06 because its one request for all 70 series
answers 422 (35 names in a request still answer, 40 do not).  The checkout asks in five parts of 14.

This runs dealer_research_store.run() exactly as the Lambda does - real sources, the real previous output and
manifest read from the bucket - on a storage object that reads the bucket and keeps every write in memory.  Nothing
reaches S3.  It then reads what would have been published and compares it with what is published now.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path[:0] = [str(ROOT / "aws" / "lambdas" / "justhodl-nyfed-pd" / "source"), str(ROOT / "aws" / "shared")]
LIVE = "justhodl-dashboard-live"


class Refused(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class Overlay:
    """Reads fall through to the bucket; writes stay here.  Conditional writes behave as S3's do."""

    def __init__(self, real):
        self.real, self.mem, self.meta, self.writes, self.real_reads = real, {}, {}, [], 0

    def _load(self, key):
        if key not in self.mem:
            obj = self.real.get_object(Bucket=LIVE, Key=key)           # a missing key raises the bucket's own error
            self.real_reads += 1
            self.mem[key], self.meta[key] = obj["Body"].read(), obj.get("Metadata", {})
        return self.mem[key]

    def _exists(self, key):
        if key in self.mem:
            return True
        try:
            self.real.head_object(Bucket=LIVE, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    def get_object(self, Bucket, Key, **kw):
        raw = self._load(Key)
        return {"Body": io.BytesIO(raw), "ETag": hashlib.sha256(raw).hexdigest(), "Metadata": self.meta.get(Key, {})}

    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get("IfNoneMatch") == "*" and self._exists(Key):
            raise Refused("PreconditionFailed")
        if kw.get("IfMatch") and (not self._exists(Key) or kw["IfMatch"] != hashlib.sha256(self._load(Key)).hexdigest()):
            raise Refused("PreconditionFailed")
        self.mem[Key], self.meta[Key] = Body, kw.get("Metadata", {})
        self.writes.append(Key)


def selftest():
    class Real:
        def __init__(self):
            self.puts = 0

        def get_object(self, Bucket, Key):
            if Key != "a":
                raise Refused("NoSuchKey")
            return {"Body": io.BytesIO(b"one"), "Metadata": {"m": "1"}}

        def head_object(self, Bucket, Key):
            if Key != "a":
                raise Refused("404")

        def put_object(self, **kw):
            self.puts += 1

    real = Real()
    o = Overlay(real)
    assert o.get_object(Bucket=LIVE, Key="a")["Body"].read() == b"one" and o.get_object(Bucket=LIVE, Key="a")["Metadata"] == {"m": "1"}
    for kw in ({"IfNoneMatch": "*"}, {"IfMatch": "wrong"}):
        try:
            o.put_object(Bucket=LIVE, Key="a", Body=b"two", **kw)
            raise AssertionError("conditional write accepted")
        except Refused:
            pass
    o.put_object(Bucket=LIVE, Key="a", Body=b"two", IfMatch=hashlib.sha256(b"one").hexdigest())
    o.put_object(Bucket=LIVE, Key="b", Body=b"new", IfNoneMatch="*")
    assert o.get_object(Bucket=LIVE, Key="a")["Body"].read() == b"two" and o.writes == ["a", "b"] and real.puts == 0
    try:
        o.get_object(Bucket=LIVE, Key="zz")
        raise AssertionError("a missing key was read")
    except Refused as e:
        assert e.response["Error"]["Code"] == "NoSuchKey"


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import dealer_original as native
    import dealer_research as model
    import dealer_research_store as store
    real = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    with report("ops_6902_dealer_engine_dry_run") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6902 - the dealer engine's run from the checkout, writes kept in memory, %s" % now.isoformat(timespec="minutes"))
        before = json.loads(real.get_object(Bucket=LIVE, Key=model.CURRENT)["Body"].read())
        bt = (before.get("financing") or {}).get("treasury") or {}
        r.log("published now: version %s | generated_at %s | as_of %s | quality %s | Treasury financing %s: repo out %s, reverse repo in %s (%s)" % (
            before.get("version"), before.get("generated_at"), before.get("as_of"), json.dumps((before.get("quality") or {}).get("counts")), bt.get("as_of"),
            bt.get("repo_out_b"), bt.get("reverse_repo_in_b"), (bt.get("quality") or {}).get("status")))
        r.log("request parts: %s | longest URL %d characters" % ({k: len(v) for k, v in native.OBSERVATION_KEYS.items()}, max(len(u) for u in native.OBSERVATION_URLS.values())))
        overlay = Overlay(real)
        try:
            result = store.run(overlay, LIVE)
        except Exception as e:  # noqa: BLE001
            import traceback
            r.log("run() raised %s: %s" % (type(e).__name__, str(e)[:300]))
            r.log(traceback.format_exc()[-1500:])
            r.fail("the checkout's run does not complete")
            sys.exit(1)
        r.log("run(): %s" % json.dumps(result)[:700])
        r.log("writes kept in memory: %d (%s) | objects read from the bucket: %d" % (len(overlay.writes), dict(Counter(k.split("/")[1] + "/" + (k.split("/")[2] if k.count("/") > 2 else "") for k in overlay.writes)), overlay.real_reads))
        after = json.loads(overlay.mem[model.CURRENT])
        at = (after.get("financing") or {}).get("treasury") or {}
        r.log("would publish: version %s | generated_at %s | source_generated_at %s | as_of %s | quality %s" % (
            after.get("version"), after.get("generated_at"), after.get("source_generated_at"), after.get("as_of"), json.dumps(after.get("quality"))[:260]))
        r.log("  Treasury financing %s: repo out %s, reverse repo in %s, two-sided %s (%s; missing %s)" % (
            at.get("as_of"), at.get("repo_out_b"), at.get("reverse_repo_in_b"), at.get("gross_two_sided_b"), (at.get("quality") or {}).get("status"), (at.get("quality") or {}).get("missing")))
        fin = after.get("financing") or {}
        r.log("  all-collateral financing: repo out %s, reverse repo in %s (%s; missing %s)" % (fin.get("repo_out_b"), fin.get("reverse_repo_in_b"), (fin.get("quality") or {}).get("status"), (fin.get("quality") or {}).get("missing")))
        corp = after.get("corporate") or {}
        r.log("  corporate bonds %s: net %s (%s) | specific-issue net settled %s" % (corp.get("as_of"), corp.get("net_bonds_b"), (corp.get("quality") or {}).get("status"), after.get("specific_issue_net_settled_b")))
        ser = after.get("native_series") or {}
        newest = Counter(v.get("as_of") for v in ser.values())
        r.log("  %d native series | newest report dates %s | observations %d (now published: %d) | sources %s" % (
            len(ser), dict(newest.most_common(3)), sum(len(v["history"]) for v in ser.values()), sum(len(v["history"]) for v in (before.get("native_series") or {}).values()), sorted(after.get("sources") or {})))
        r.log("  original responses named by the series: %s" % dict(Counter(v.get("original_response") for v in ser.values())))
        rev = after.get("revisions") or {}
        r.log("  against the previous output: %s changed values | first: %s" % (rev.get("known_changed_values"), json.dumps((rev.get("changes") or [])[:4])))
        weeks = sorted({row[0] for row in ser["PDSORA-UTSETTOT"]["history"]})[-4:]
        for key in ("PDSORA-UTSETTOT", "PDSORA-UTSTTOT", "PDSIRRA-UTSETTOT", "PDSIRRA-UTSTTOT"):
            by = {row[0]: row[1] for row in ser[key]["history"]}
            r.log("    %-18s %s" % (key, " | ".join("%s %s" % (w, by.get(w)) for w in weeks)))
        ok = (result.get("published") is True and (after.get("as_of") or "") > (before.get("as_of") or "") and (at.get("quality") or {}).get("status") == "fresh"
              and at.get("repo_out_b") is not None and len(ser) == 70)
        r.kv(published=result.get("published"), as_of_now=before.get("as_of"), as_of_after=after.get("as_of"), treasury_repo_out_b=at.get("repo_out_b"),
             treasury_status=(at.get("quality") or {}).get("status"), writes_to_s3=0, changed_values=rev.get("known_changed_values"))
        if ok:
            r.ok("dry run; nothing written - the checkout's engine completes and would publish the week of %s" % after.get("as_of"))
        else:
            r.fail("the dry run does not produce a current Treasury financing figure")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
