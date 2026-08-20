# BIS / SDMX aggregate-ambiguity trap — ops 4934–4935 (2026-08-20)

Applies to **every SDMX source**, not just BIS.

## What happened

`justhodl-usd-funding` v1.0.1 queried `WS_LBS_D_PUB` with
`Q.S.{pos}.A.USD.......` — pinning 5 of 12 dimensions and wildcarding
`L_CURR_TYPE`, `L_PARENT_CTY`, `L_REP_BANK_TYPE`, `L_POS_TYPE`.

**40 distinct series survived on the claims side, 30 on liabilities.**
The parse did `best[period] = val` — last row wins, arbitrarily.

Claims landed on `L_CURR_TYPE=D` (domestic-currency USD, $3,934bn);
liabilities landed on something narrower still. The board published
**USD claims $3,934bn against liabilities $49bn — an 80:1 ratio**, which
is impossible for a two-sided bank book. Khalid caught it on the
rendered page.

The guard against *summing* overlapping rows held the whole time. There
was no guard against **choosing** between them — the quieter version of
the same sin.

## Doctrine

1. A wildcarded SDMX dimension is not "unspecified", it is **"give me
   every slice"**. Pin every dimension explicitly.
2. **Assert the surviving series count == 1.** If it isn't, publish
   `ambiguous: true` plus a note. Never pick.
3. Add a **domain plausibility gate** beside the structural ones. Every
   shape/structure gate passed this payload. A ratio test would have
   caught it on day one. v1.1.0 requires claims/liabilities ∈ [0.2, 5.0]
   and claims > $5,000bn.

## Canonical LBS key

Dimension order (proven ops 4928 from the 25-column CSV header):

    FREQ.L_MEASURE.L_POSITION.L_INSTR.L_DENOM.L_CURR_TYPE.L_PARENT_CTY.
    L_REP_BANK_TYPE.L_REP_CTY.L_CP_SECTOR.L_CP_COUNTRY.L_POS_TYPE

Canonical USD cross-border aggregate:

    Q.S.{pos}.A.USD.A.5J.A.5A.A.5J.N

Live 2026-Q1: claims **$21,796.9bn**, liabilities **$19,518.5bn**,
net **+$2,278.4bn**, ratio 1.12.

Codes: `L_POS_TYPE` N=cross-border, R=related offices/intragroup,
U=unallocated. `L_DENOM` ∈ {CHF, EUR, GBP, JPY, TO1, TO3, UN9, USD}.
`L_POSITION` ∈ {C, L}. `L_MEASURE` ∈ {B, F, G, S}.

## Two further BIS traps

- **`UNIT_MEASURE=USD` is the unit every BIS figure is reported in, not
  the denomination.** Matching `,USD,` naively hits the unit column and
  returns ~100% of rows. The currency dimension is `L_DENOM`.
- The bulk SDMX walker's banked copy at
  `data/warm/bis/data/WS_LBS_D_PUB.dat.gz` carries **`truncated: True`**
  — 648,423 rows but only 2,517 USD-denominated. Never build an
  aggregate off it; query BIS directly with a pinned key.

## Page location

The offshore-USD funding board lives on **eurodollar.html** (marker
`usd-funding-panel-v2`, above the `jh-biscb` card, native
`--c1/--bd/--cyan/--amber` tokens). Removed cleanly from data.html.

## Git trap

A `git reset --soft origin/main` mid-sequence left a **detached HEAD**,
so `git push origin main` silently pushed a stale local `main` ref and
GitHub answered *"behind its remote counterpart"* for what looked like a
clean fast-forward. Five retries wrongly blamed the ops runner's
concurrent auto-commits. **Check `git branch --show-current` before
diagnosing a push rejection as a race.**
