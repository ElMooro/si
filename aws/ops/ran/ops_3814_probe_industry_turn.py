#!/usr/bin/env python3
"""ops 3814 — probe: can we detect industries TURNING (not just already booming)?

Khalid liked the VALUE_TRAP book (industry decaying => stay away) and wants the
mirror: industries BOOMING or TURNING to boom.

But the mirror is NOT symmetric, and that distinction decides the whole design:
  - "industry decaying" is a WARNING. It is useful precisely because it is
    already visible and still ignored by a cheapness screen.
  - "industry booming" is mostly ALREADY PRICED. Telling someone to buy into a
    visibly booming industry is a momentum call the market has already made.
    The alpha is in the TURN — an industry whose boom score is rising fast from
    a low base, before the league table shows it at the top.

So this probe answers whether industry-boom carries the data to detect a TURN:
  1. Does league carry score_delta_20d (momentum of the boom score)?
  2. What is the distribution — can we separate BOOMING (high level) from
     TURNING (low/mid level, strongly rising) from DECAYING?
  3. How many ledger names sit in each bucket?
  4. Is there history to confirm a turn is real rather than one noisy print?

Writes no engine code.
"""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


def main():
    with report("3814_probe_industry_turn") as rep:
        rep.heading("ops 3814 — can we detect industries TURNING, not just booming?")

        ib = json.loads(s3.get_object(Bucket=B, Key="data/industry-boom.json")["Body"].read())
        league = ib.get("league") or []
        rep.kv(n_industries=len(league), generated=ib.get("generated_at"))

        rep.section("1. Fields available per industry")
        if league:
            rep.log("  keys: %s" % sorted(league[0].keys()))
            rep.log("  sample: %s" % json.dumps(league[0])[:300])

        rep.section("2. Distribution of boom_score and its 20d delta")
        bs = [x.get("boom_score") for x in league if isinstance(x.get("boom_score"), (int, float))]
        dl = [x.get("score_delta_20d") for x in league
              if isinstance(x.get("score_delta_20d"), (int, float))]
        gate(rep, "PROBE.boom_score", len(bs) > 20, "%d industries carry boom_score" % len(bs))
        gate(rep, "PROBE.delta", len(dl) > 20, "%d carry score_delta_20d" % len(dl))
        if bs:
            b = sorted(bs)
            rep.kv(score_min=round(b[0], 1), score_p25=round(b[len(b)//4], 1),
                   score_med=round(b[len(b)//2], 1), score_p75=round(b[3*len(b)//4], 1),
                   score_max=round(b[-1], 1))
        if dl:
            v = sorted(dl)
            rep.kv(delta_min=round(v[0], 1), delta_p25=round(v[len(v)//4], 1),
                   delta_med=round(v[len(v)//2], 1), delta_p75=round(v[3*len(v)//4], 1),
                   delta_max=round(v[-1], 1))

        rep.section("3. Can we separate TURNING from already-BOOMING?")
        if bs and dl:
            hi = sorted(bs)[int(len(bs)*0.75)]
            lo = sorted(bs)[int(len(bs)*0.25)]
            dhi = sorted(dl)[int(len(dl)*0.75)]
            booming, turning, decaying, fading = [], [], [], []
            for x in league:
                s_, d_ = x.get("boom_score"), x.get("score_delta_20d")
                if not isinstance(s_, (int, float)):
                    continue
                if s_ >= hi:
                    (booming if not isinstance(d_, (int, float)) or d_ >= 0 else fading).append(x)
                elif isinstance(d_, (int, float)) and d_ >= dhi and s_ < hi:
                    turning.append(x)
                elif s_ <= lo:
                    decaying.append(x)
            rep.kv(thresholds="hi=%.1f lo=%.1f delta_p75=%.1f" % (hi, lo, dhi),
                   booming=len(booming), turning=len(turning),
                   decaying=len(decaying), fading_from_high=len(fading))
            gate(rep, "PROBE.turn_detectable", len(turning) > 0,
                 "%d industries are rising fast from a non-top base" % len(turning))

            rep.section("TURNING — the interesting bucket (rising, not yet at the top)")
            for x in sorted(turning, key=lambda z: -(z.get("score_delta_20d") or 0))[:12]:
                rep.log("  %-34s score=%5.1f  delta20d=%+6.1f  n=%-4s mcap=%sB" % (
                    (x.get("industry") or "")[:34], x.get("boom_score") or 0,
                    x.get("score_delta_20d") or 0, x.get("n"), x.get("mcap_b")))

            rep.section("BOOMING — already visible, mostly priced")
            for x in sorted(booming, key=lambda z: -(z.get("boom_score") or 0))[:8]:
                rep.log("  %-34s score=%5.1f  delta20d=%+6.1f" % (
                    (x.get("industry") or "")[:34], x.get("boom_score") or 0,
                    x.get("score_delta_20d") or 0))

            rep.section("FADING — high score but rolling over (the trap nobody sees)")
            for x in sorted(fading, key=lambda z: (z.get("score_delta_20d") or 0))[:8]:
                rep.log("  %-34s score=%5.1f  delta20d=%+6.1f" % (
                    (x.get("industry") or "")[:34], x.get("boom_score") or 0,
                    x.get("score_delta_20d") or 0))

        rep.section("4. Ledger exposure to each bucket")
        ck = json.loads(s3.get_object(Bucket=B, Key="data/chokepoint.json")["Body"].read())
        rows = (ck.get("capture_gap") or {}).get("all_rows") or []
        byind = {}
        for r in rows:
            byind[r.get("industry")] = byind.get(r.get("industry"), 0) + 1
        if bs and dl:
            for label, group in (("BOOMING", booming), ("TURNING", turning),
                                 ("DECAYING", decaying), ("FADING", fading)):
                n = sum(byind.get(x.get("industry"), 0) for x in group)
                rep.log("  %-9s %3d industries -> %4d scored names" % (label, len(group), n))

        rep.section("VERDICT")
        rep.log("A booming-industry book is only useful if it separates ALREADY PRICED")
        rep.log("from NOT YET PRICED. The design that earns its place:")
        rep.log("  TURNING  = boom score rising hard from a mid/low base  -> the alpha")
        rep.log("  BOOMING  = high and still rising -> confirmation, mostly priced")
        rep.log("  FADING   = high but rolling over -> a trap NO cheapness screen catches")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — probe complete")


if __name__ == "__main__":
    main()
