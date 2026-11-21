"""The backfill conductor's pacing rule (2026-11-21): the two FMP engines share one bandwidth quota (150 GB per rolling
30 days) with every engine that calls FMP live, so the conductor gives them no extra kicks unless told to; every other
engine keeps the general cap.  Fake S3 and Lambda; no network."""
import importlib.util
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _conductor():
    spec = importlib.util.spec_from_file_location("fmp_backfill_conductor_under_test", ROOT / "scripts" / "fmp_backfill_conductor.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _S3:
    def __init__(self, docs):
        self.docs = docs

    def get_object(self, Bucket, Key):
        if Key not in self.docs:
            raise KeyError(Key)
        return {"Body": io.BytesIO(json.dumps(self.docs[Key]).encode())}


class _Lambda:
    """Each invoke is one engine run: a new state write; the engine turns STEADY after `steady_after` runs."""

    def __init__(self, s3, engines, steady_after):
        self.s3, self.engines, self.steady_after, self.calls = s3, engines, steady_after, []

    def invoke(self, FunctionName, InvocationType, Payload):
        self.calls.append(FunctionName)
        e = self.engines[FunctionName]
        n = self.calls.count(FunctionName)
        phase = "STEADY" if n >= self.steady_after.get(FunctionName, 99) else "BACKFILL"
        self.s3.docs[e["state"]] = {"phase": phase, "updated_at": "run-%d" % n, "lease_until": 0, "last_run": {"puts": 1}}
        self.s3.docs[e["manifest"]] = {"phase": phase, "objects": n}
        return {"StatusCode": 202}


def _run(mod, phases, argv, steady_after=None):
    docs = {}
    for n, e in mod.ENGINES.items():
        docs[e["state"]] = {"phase": phases.get(n, "STEADY"), "updated_at": "start", "lease_until": 0}
        docs[e["manifest"]] = {"phase": phases.get(n, "STEADY")}
    s3 = _S3(docs)
    lam = _Lambda(s3, mod.ENGINES, steady_after or {})
    out = io.StringIO()
    real_sleep = mod.time.sleep                      # the conductor waits 20 s between state reads; the whole gate shares one `time`
    mod.time.sleep = lambda _s: None
    try:
        with redirect_stdout(out):
            rc = mod.main(argv, s3=s3, lam=lam)
    finally:
        mod.time.sleep = real_sleep
    return rc, lam.calls, out.getvalue()


def test_kick_cap_is_the_general_cap_except_for_the_provider_paced_engines():
    mod = _conductor()
    assert mod.KICK_CAPS == {"justhodl-fmp-bank": 0, "justhodl-fmp-walker": 0}
    assert mod.kick_cap("justhodl-fmp-bank", 12) == 0 and mod.kick_cap("justhodl-fmp-walker", 12) == 0
    assert mod.kick_cap("justhodl-polygon-bank", 12) == 12 and mod.kick_cap("justhodl-bank-histories", 3) == 3
    assert mod.kick_cap("justhodl-fmp-bank", 12, fmp_kicks=2) == 2 and mod.kick_cap("justhodl-fmp-bank", 1, fmp_kicks=5) == 1
    assert mod.kick_cap("justhodl-fmp-walker", 12, fmp_kicks=-3) == 0 and mod.kick_cap("justhodl-eia-bank", 12, fmp_kicks=0) == 12


def test_the_fmp_engines_are_not_kicked_and_the_backfill_is_not_called_complete():
    mod = _conductor()
    rc, calls, out = _run(mod, {"justhodl-fmp-bank": "BACKFILL", "justhodl-fmp-walker": "BACKFILL", "justhodl-polygon-bank": "BACKFILL"},
                          ["--max-minutes", "5", "--max-kicks", "3"], steady_after={"justhodl-polygon-bank": 2})
    assert rc == 0 and calls == ["justhodl-polygon-bank", "justhodl-polygon-bank"], calls              # the others keep the general cap
    assert "justhodl-fmp-bank PACED: phase BACKFILL" in out and "justhodl-fmp-walker PACED: phase BACKFILL" in out, out
    lines = out.splitlines()
    assert "PACED justhodl-fmp-bank justhodl-fmp-walker" in lines and "ALL_STEADY" not in lines, out   # the workflow must not switch itself off
    assert "justhodl-fmp-bank final: phase BACKFILL kicks 0 (paced: cap 0)" in out


def test_a_steady_fmp_engine_counts_as_steady_and_an_override_kicks_it():
    mod = _conductor()
    rc, calls, out = _run(mod, {}, ["--max-minutes", "5"])
    assert rc == 0 and calls == [] and "ALL_STEADY" in out.splitlines() and "PACED" not in out, out
    rc, calls, out = _run(mod, {"justhodl-fmp-bank": "BACKFILL"}, ["--max-minutes", "5", "--fmp-kicks", "2", "--engines", "justhodl-fmp-bank"], steady_after={"justhodl-fmp-bank": 9})
    assert calls == ["justhodl-fmp-bank", "justhodl-fmp-bank"] and "PACED justhodl-fmp-bank" in out.splitlines() and "ALL_STEADY" not in out.splitlines(), (calls, out)
    rc, calls, out = _run(mod, {"justhodl-fmp-bank": "BACKFILL"}, ["--max-minutes", "5", "--fmp-kicks", "2", "--engines", "justhodl-fmp-bank"], steady_after={"justhodl-fmp-bank": 1})
    assert calls == ["justhodl-fmp-bank"] and "ALL_STEADY" in out.splitlines(), (calls, out)


def test_the_workflow_passes_the_cap_and_defaults_it_to_zero():
    wf = (ROOT / ".github" / "workflows" / "fmp-backfill.yml").read_text(encoding="utf-8")
    assert "fmp_kicks:" in wf and "FMP_KICKS: ${{ inputs.fmp_kicks || '0' }}" in wf and '--fmp-kicks "${FMP_KICKS}"' in wf


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok " + name)
