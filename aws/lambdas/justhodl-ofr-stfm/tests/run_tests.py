"""justhodl-ofr-stfm offline tests: the daily refresh of the OFR bank.  No network, no AWS.

The bank had no refresh: a banked mnemonic was never fetched again and every series file stayed as written on
2026-08-06 (ops 6893).  These hold the refresh to its contract - every series of the five datasets reaches the bank,
a file is written only when its observations changed, a truncated answer never replaces a full history, one failing
dataset does not stop the others, and a dry run writes nothing.

Run: python3 aws/lambdas/justhodl-ofr-stfm/tests/run_tests.py
"""
import copy
import gzip
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "source"), os.path.join(HERE, "..", "..", "..", "shared")]
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

import ofr_refresh as R  # noqa: E402

NOW = "2026-11-21T02:29:00+00:00"
BASE = "https://example.invalid/v1"


def body(rows, name="A series"):
    return {"timeseries": {"aggregation": [list(r) for r in rows]}, "metadata": {"mnemonic": name, "description": {"name": name}}}


def daily(n, last_value=3.91):
    rows = [["2026-%02d-%02d" % (1 + i // 28, 1 + i % 28), 3.0 + i / 1000.0] for i in range(n)]
    rows[-1][1] = last_value
    return rows


class Source:
    """A stand-in for the OFR API: one answer per dataset, gzip like the real one."""

    def __init__(self):
        self.data = {"fnyr": {"FNYR-SOFR-A": body(daily(40)), "FNYR-EFFR-A": body(daily(40, 3.88))},
                     "repo": {"REPO-TRI_AR_OO-P": body(daily(30)), "REPO-GCF_AR_OO-P": body(daily(30, 3.95))},
                     "nypd": {"NYPD-PD_AFtD_T-A": body([["2026-11-04", 9.0e10], ["2026-11-11", 1.0e11]])},
                     "mmf": {"MMF-MMF_RP_TOT-M": body([["2026-09-30", 2.9e12], ["2026-10-31", 3.0e12]])},
                     "tyld": {"TYLD-TCMR-10Yr-A": body(daily(20, 4.1))},
                     "ficc": {"FICC-SPONSORED_REPO_VOL": body(daily(25, 1.08e12))}}
        self.calls, self.broken = [], set()

    def fetch(self, url):
        self.calls.append(url)
        ds = url.rsplit("=", 1)[-1]
        if ds in self.broken:
            raise OSError("connection reset")
        return gzip.compress(json.dumps({"short_name": ds, "long_name": ds.upper(), "timeseries": self.data[ds]}).encode())


class Bank(dict):
    def put(self, key, payload):
        self[key] = payload
        self.puts = getattr(self, "puts", 0) + 1

    def doc(self, key):
        return json.loads(gzip.decompress(self[key]))


def run(src, bank, known, **kw):
    return R.refresh(src.fetch, bank.put, known, BASE, kw.pop("datasets", R.DATASETS), NOW, kw.pop("prefix", "data/warm/ofr/series/"),
                     log=lambda *_: None, **kw)


def test_first_refresh_banks_every_series_in_the_layout_the_board_reads():
    src, bank, known = Source(), Bank(), {}
    out = run(src, bank, known)
    assert out["written"] == 7 and out["unchanged"] == 0 and not out["failed"] and not out["shrank"], out
    assert [u.rsplit("=", 1)[-1] for u in src.calls] == list(R.DATASETS)
    doc = bank.doc("data/warm/ofr/series/FNYR-SOFR-A.json.gz")
    assert doc["mnemonic"] == "FNYR-SOFR-A" and doc["as_of"] == NOW and doc["refreshed_by"] == "dataset"
    assert doc["source_url"] == BASE + "/series/dataset?dataset=fnyr"
    assert doc["payload"]["FNYR-SOFR-A"]["timeseries"]["aggregation"][-1][1] == 3.91      # the /series/full shape, one level in
    assert known["FNYR-SOFR-A"][1:] == [40, "2026-02-12"] and len(known) == 7
    assert out["datasets"]["repo"] == {"series": 2, "written": 2, "unchanged": 0}


def test_a_second_run_on_an_unchanged_source_writes_nothing():
    src, bank, known = Source(), Bank(), {}
    run(src, bank, known)
    before = bank.puts
    out = run(src, bank, known)
    assert out["written"] == 0 and out["unchanged"] == 7 and bank.puts == before, out


def test_only_the_series_that_gained_an_observation_is_written():
    src, bank, known = Source(), Bank(), {}
    run(src, bank, known)
    src.data["fnyr"]["FNYR-SOFR-A"]["timeseries"]["aggregation"].append(["2026-11-20", 3.92])
    out = run(src, bank, known)
    assert out["written_ids"] == ["FNYR-SOFR-A"] and out["unchanged"] == 6, out
    assert bank.doc("data/warm/ofr/series/FNYR-SOFR-A.json.gz")["payload"]["FNYR-SOFR-A"]["timeseries"]["aggregation"][-1] == ["2026-11-20", 3.92]
    assert known["FNYR-SOFR-A"][1:] == [41, "2026-11-20"]


def test_a_revised_value_is_a_change_and_metadata_alone_is_not():
    src, bank, known = Source(), Bank(), {}
    run(src, bank, known)
    src.data["repo"]["REPO-TRI_AR_OO-P"]["metadata"]["description"]["name"] = "renamed"
    assert run(src, bank, known)["written"] == 0
    src.data["repo"]["REPO-TRI_AR_OO-P"]["timeseries"]["aggregation"][5][1] = 9.99
    assert run(src, bank, known)["written_ids"] == ["REPO-TRI_AR_OO-P"]


def test_a_truncated_answer_never_replaces_a_full_history():
    src, bank, known = Source(), Bank(), {}
    run(src, bank, known)
    full = bank["data/warm/ofr/series/FNYR-SOFR-A.json.gz"]
    src.data["fnyr"]["FNYR-SOFR-A"] = body(daily(10))
    out = run(src, bank, known)
    assert out["shrank"] == [{"mnemonic": "FNYR-SOFR-A", "banked_rows": 40, "source_rows": 10}] and out["written"] == 0, out
    assert bank["data/warm/ofr/series/FNYR-SOFR-A.json.gz"] == full and known["FNYR-SOFR-A"][1] == 40
    src.data["fnyr"]["FNYR-SOFR-A"] = body(daily(38))          # a few revised points fewer is still the series
    assert run(src, bank, known)["written_ids"] == ["FNYR-SOFR-A"]


def test_one_failing_dataset_does_not_stop_the_others_and_is_retried():
    src, bank, known = Source(), Bank(), {}
    src.broken = {"repo"}
    out = run(src, bank, known)
    assert out["written"] == 5 and "OSError" in out["datasets"]["repo"]["error"] and out["datasets"]["nypd"]["written"] == 1, out
    assert "REPO-TRI_AR_OO-P" not in known
    src.broken = set()
    assert sorted(run(src, bank, known)["written_ids"]) == ["REPO-GCF_AR_OO-P", "REPO-TRI_AR_OO-P"]


def test_a_failed_put_keeps_the_old_signature_so_the_next_run_writes_it():
    src, bank, known = Source(), Bank(), {}
    run(src, bank, known)
    src.data["tyld"]["TYLD-TCMR-10Yr-A"]["timeseries"]["aggregation"].append(["2026-11-20", 4.2])
    old = list(known["TYLD-TCMR-10Yr-A"])

    def failing(key, payload):
        raise OSError("slow down")

    out = R.refresh(src.fetch, failing, known, BASE, R.DATASETS, NOW, "data/warm/ofr/series/", log=lambda *_: None)
    assert out["written"] == 0 and out["failed"][0]["mnemonic"] == "TYLD-TCMR-10Yr-A" and known["TYLD-TCMR-10Yr-A"] == old, out
    assert run(src, bank, known)["written_ids"] == ["TYLD-TCMR-10Yr-A"]


def test_a_dry_run_reports_and_writes_nothing():
    src, bank, known = Source(), Bank(), {}
    out = run(src, bank, known, dry_run=True)
    assert out["dry_run"] and out["written"] == 7 and len(bank) == 0 and known == {}, out
    assert out["newest"] and max(out["newest"]) == "2026-11-11"


def test_the_hedge_fund_pair_keeps_its_bare_layout():
    src, bank, known = Source(), Bank(), {}
    out = run(src, bank, known, datasets=R.HF_DATASETS, prefix="data/warm/ofr-hfm/series/", layout="raw")
    assert out["written_ids"] == ["FICC-SPONSORED_REPO_VOL"]
    doc = bank.doc("data/warm/ofr-hfm/series/FICC-SPONSORED_REPO_VOL.json.gz")
    assert list(doc) == ["FICC-SPONSORED_REPO_VOL"] and "timeseries" in doc["FICC-SPONSORED_REPO_VOL"]


def test_plain_and_gzip_answers_and_odd_envelopes():
    plain = json.dumps({"timeseries": {"A": body([["2026-01-02", 1.0]])}}).encode()
    assert R.unzip(plain) == plain and R.unzip(gzip.compress(plain)) == plain
    assert set(R.series_of(json.loads(plain))) == {"A"} and R.series_of([]) == {} and R.series_of({"timeseries": {"A": 5}}) == {}
    assert R.newest(body([["2026-01-02", 1.0], ["2026-01-05", None]])) == ("2026-01-02", 1)
    assert R.newest({"timeseries": {}}) == (None, 0)
    assert R.signature(body([["2026-01-02", 1.0]], "x")) == R.signature(body([["2026-01-02", 1.0]], "y"))
    assert R.signature(body([["2026-01-02", 1.0]])) != R.signature(body([["2026-01-02", 1.1]]))


class FakeS3:
    def __init__(self, objects=None):
        self.objects, self.puts = dict(objects or {}), []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)

        class Body:
            def __init__(self, raw):
                self.raw = raw

            def read(self):
                return self.raw
        return {"Body": Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body
        self.puts.append((Key, kw))


def handler_with(src, s3):
    import lambda_function as lf
    lf.s3 = s3
    lf.snapshot = None
    lf.time.sleep = lambda *_: None
    catalog = sorted(m for ds in R.DATASETS for m in src.data[ds])

    def fetch(url):
        if url.endswith("/metadata/mnemonics") and "/hf/" not in url:
            return json.dumps(catalog + ["X%03d" % i for i in range(60)]).encode()     # discovery wants more than 50 names
        if "/hf/" in url and url.endswith("/metadata/mnemonics"):
            return json.dumps(sorted(src.data["ficc"])).encode()
        if "/series/full?mnemonic=" in url:
            m = url.rsplit("=", 1)[-1]
            for ds in src.data.values():
                if m in ds:
                    return json.dumps({m: ds[m]}).encode()
            raise OSError("404")
        raise AssertionError(url)

    lf._fetch = fetch
    lf._fetch_dataset = src.fetch
    return lf


def test_the_engine_refreshes_a_bank_that_was_complete_and_frozen():
    src = Source()
    frozen = {m: body(daily(5)) for ds in R.DATASETS for m in src.data[ds]}
    state = {"done": sorted(frozen) + ["X%03d" % i for i in range(60)], "catalog": sorted(frozen) + ["X%03d" % i for i in range(60)], "status": "COMPLETE-maintaining"}
    s3 = FakeS3({"data/warm/ofr/state.json": json.dumps(state).encode(),
                 "data/warm/ofr-hfm/state.json": json.dumps({"done": ["FICC-SPONSORED_REPO_VOL"], "catalog": ["FICC-SPONSORED_REPO_VOL"], "refresh_cursor": 0}).encode()})
    lf = handler_with(src, s3)
    res = json.loads(lf.lambda_handler({}, None)["body"])
    assert res["refresh"]["written"] == 7 and res["refresh_problems"] == {"shrank": 0, "failed": 0, "datasets": {}}, res
    assert res["hf_refresh"]["written"] == 1
    sofr = json.loads(gzip.decompress(s3.objects["data/warm/ofr/series/FNYR-SOFR-A.json.gz"]))
    assert len(sofr["payload"]["FNYR-SOFR-A"]["timeseries"]["aggregation"]) == 40
    saved = json.loads(s3.objects["data/warm/ofr/state.json"])
    assert saved["refresh"]["written"] == 7 and saved["series"]["FNYR-SOFR-A"][2] == "2026-02-12" and saved["status"] == "COMPLETE-maintaining"
    hf_put = [kw for key, kw in s3.puts if key.endswith("FICC-SPONSORED_REPO_VOL.json.gz")][-1]
    assert hf_put == {"ContentType": "application/json", "ContentEncoding": "gzip"}, hf_put
    res2 = json.loads(lf.lambda_handler({}, None)["body"])
    assert res2["refresh"]["written"] == 0 and res2["refresh"]["unchanged"] == 7 and res2["hf_refresh"]["written"] == 0
    assert len([1 for key, _ in s3.puts if "/ofr/series/" in key]) == 7          # the second run wrote no funding series again


def test_the_engine_dry_run_writes_nothing_at_all():
    src = Source()
    s3 = FakeS3({"data/warm/ofr/state.json": json.dumps({"done": [], "catalog": []}).encode()})
    lf = handler_with(src, s3)
    res = json.loads(lf.lambda_handler({"dry_run": True}, None)["body"])
    assert res["dry_run"] and len(res["would_write"]) == 7 and res["hf"]["would_write"] == ["FICC-SPONSORED_REPO_VOL"], res
    assert s3.puts == []


def main():
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for name, fn in tests:
        fn()
    print("justhodl-ofr-stfm: %d tests passed" % len(tests))


if __name__ == "__main__":
    main()
