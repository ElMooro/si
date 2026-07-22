"""
ops 3723 — LEDGER PROBE: does obs[0] hold real EPS numbers?

THE QUESTION THIS SETTLES
═════════════════════════
ops 3722's in-Lambda probe overturned the premise of ops 3716-3721. Benzinga is
NOT dead: from inside the Lambda, Massive returns 200 OK and
fetch_calendar(min_importance=2) yields 735 rows, 687 carrying estimated_eps.
So the engine has plenty of input and still emits upward=0 / downward=0.

Tracing the real path shows no obvious bug:

    eps_rev_pct = (cur_eps - b_eps) / |b_eps| * 100      # b_eps = obs[0][1]
    signals.append(row)  IFF  |eps_rev_pct| >= REV_THRESHOLD_PCT (= 1.0)
    up/down are filtered from `signals`

eps_rev_pct is a REVISION metric: it needs a stored baseline for the same
ticker|fiscal_period|fiscal_year key. Two very different explanations produce
identical empty arrays:

  (A) HEALTHY — the ledger holds real baseline EPS numbers, and consensus
      simply has not moved >=1% on any tracked name. Empty is the correct
      reading of a quiet tape. Nothing to fix.

  (B) POISONED — obs entries were written while cur_eps was None (which is what
      the whole 3716-3721 arc assumed), so obs[0][1] is null. Then
      `isinstance(b_eps, (int,float))` is False on every row, eps_rev_pct stays
      None forever, and the engine can NEVER produce a direction no matter how
      much the tape moves. Self-inflicted permanent silence.

The difference is one field, and only the ledger can answer it. No fix is
written until it does — this engine has already absorbed two wrong diagnoses
from me (3717 patched a loop that never ran; 3720 shimmed a source that was
never down).

WHAT IS MEASURED (read-only, S3 GET on estimate-revisions/state.json)
    n_keys, and per key: len(obs), obs[0] eps null-vs-number, obs[-1] likewise
    -> pct of baselines that are USABLE (non-null, non-zero)
    -> distribution of |implied eps_rev_pct| for keys that have >=2 obs, so we
       can see whether real moves exist but sit under the 1.0% threshold
    -> obs date span, to confirm the ledger is actually accreting day over day

EXITS 0 when it returns readings (diagnosis is not breakage); exits 1 only if
the ledger cannot be read at all.
"""
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

BUCKET = "justhodl-dashboard-live"
STATE_KEY = "estimate-revisions/state.json"
ART_KEY = "data/estimate-revisions.json"
REV_THRESHOLD_PCT = 1.0

S3C = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3723_ledger_probe") as rep:
        rep.heading("ops 3723 — estimate-revisions ledger probe (obs[0] baselines)")
        out = {}

        def note(n, d):
            rep.log(f"{n}: {d}")
            print(f"  {n}: {d}")
            out[n] = d

        try:
            st = json.loads(S3C.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
        except Exception as e:  # noqa: BLE001
            note("ledger_read_error", str(e)[:200])
            rep.log("VERDICT: FAIL — ledger unreadable")
            print("\nVERDICT: FAIL — ledger unreadable")
            sys.exit(1)

        keys = st.get("keys") or {}
        note("ledger_updated", st.get("updated"))
        note("n_keys", len(keys))

        n_obs_hist = Counter()
        base_usable = base_null = base_zero = 0
        last_usable = last_null = 0
        movers, all_dates = [], []
        multi = 0

        for k, v in keys.items():
            obs = v.get("obs") or []
            n_obs_hist[len(obs)] += 1
            for o in obs:
                if isinstance(o, (list, tuple)) and o:
                    all_dates.append(o[0])
            if not obs:
                continue

            b = obs[0]
            b_eps = b[1] if len(b) > 1 else None
            if b_eps is None:
                base_null += 1
            elif isinstance(b_eps, (int, float)) and b_eps == 0:
                base_zero += 1
            elif isinstance(b_eps, (int, float)):
                base_usable += 1

            l = obs[-1]
            l_eps = l[1] if len(l) > 1 else None
            if l_eps is None:
                last_null += 1
            elif isinstance(l_eps, (int, float)):
                last_usable += 1

            if len(obs) >= 2 and isinstance(b_eps, (int, float)) and b_eps \
                    and isinstance(l_eps, (int, float)):
                multi += 1
                movers.append((abs((l_eps - b_eps) / abs(b_eps) * 100.0),
                               v.get("t"), b_eps, l_eps, len(obs)))

        note("obs_length_distribution", dict(sorted(n_obs_hist.items())[:12]))
        note("baseline_eps",
             f"usable={base_usable} null={base_null} zero={base_zero} "
             f"(usable_pct={round(100.0*base_usable/max(1,len(keys)),1)}%)")
        note("latest_eps", f"usable={last_usable} null={last_null}")
        note("keys_with_2plus_obs_and_usable_eps", multi)

        if all_dates:
            note("obs_date_span", f"{min(all_dates)} .. {max(all_dates)} "
                                  f"({len(set(all_dates))} distinct days)")

        # do real moves exist, and where do they sit vs the 1.0% gate?
        movers.sort(reverse=True)
        band = Counter()
        for m, *_ in movers:
            band["ZERO" if m == 0 else
                 "<0.5%" if m < 0.5 else
                 "0.5-1%" if m < 1.0 else
                 "1-3%" if m < 3.0 else
                 "3-10%" if m < 10.0 else ">=10%"] += 1
        note("move_distribution_vs_threshold", dict(band))
        note("would_pass_threshold",
             f"{sum(v for k, v in band.items() if k in ('1-3%','3-10%','>=10%'))} "
             f"of {len(movers)} (REV_THRESHOLD_PCT={REV_THRESHOLD_PCT})")

        if movers:
            print("\n  top implied moves in the ledger:")
            for m, t, b_eps, l_eps, n in movers[:12]:
                print(f"    {str(t):6} |{round(m,2):8}% |  base={b_eps} -> last={l_eps} "
                      f"(obs={n})")

        # cross-read the artifact for context
        try:
            art = json.loads(S3C.get_object(Bucket=BUCKET, Key=ART_KEY)["Body"].read())
            note("artifact",
                 f"version={art.get('version')} n_tracked={art.get('n_tracked')} "
                 f"n_with_history={art.get('n_with_history')} "
                 f"n_state_keys={art.get('n_state_keys')} "
                 f"up={len(art.get('upward_revisions') or [])} "
                 f"down={len(art.get('downward_revisions') or [])} "
                 f"strength_leaders={len(art.get('estimate_strength_leaders') or [])}")
        except Exception as e:  # noqa: BLE001
            note("artifact_read_error", str(e)[:140])

        # ── the verdict this probe exists to deliver ────────────────────────
        usable_pct = 100.0 * base_usable / max(1, len(keys))
        if usable_pct < 25:
            diag = ("POISONED — most baselines are null, so eps_rev_pct can NEVER "
                    "compute. Ledger needs a re-seed with non-null EPS.")
        elif multi == 0:
            diag = ("NOT YET ACCRUED — baselines are fine but almost nothing has a "
                    "SECOND observation for the same fiscal key, so no diff exists "
                    "yet. Expect directions after the next scheduled runs.")
        elif sum(v for k, v in band.items() if k in ("1-3%", "3-10%", ">=10%")) == 0:
            diag = ("HEALTHY BUT QUIET — real baselines and real diffs exist, but no "
                    f"move clears REV_THRESHOLD_PCT={REV_THRESHOLD_PCT}. Empty arrays "
                    "are the CORRECT reading. Consider lowering the threshold or "
                    "surfacing sub-threshold drift as a separate 'estimate drift' row.")
        else:
            diag = ("CONTRADICTION — moves above threshold exist in the ledger yet "
                    "the artifact publishes none. Bug is in signal assembly, not data.")
        note("DIAGNOSIS", diag)

        for _k, _v in out.items():
            rep.kv(probe=_k, value=str(_v)[:300])
        print("\nVERDICT: DIAGNOSIS COMPLETE —", diag[:80])
        rep.log("VERDICT: DIAGNOSIS COMPLETE")

        if not keys:
            sys.exit(1)


if __name__ == "__main__":
    main()
