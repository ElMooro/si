"""What the repo board gained on 2026-11-21 (Khalid: an icon on the chart for the repo engine that covers "all the
different aspect of the repo market and repo rate including their spikes in crisis").

ops 6892 / 6893 measured what the board did not carry:
  - 125 series the OFR bank holds and the 300-series inventory never listed: money-market-fund repo by collateral and
    term with the median yield funds earn on it, and primary dealers' securities borrowed and lent;
  - nothing older than August 2014 about funding rates, so no crisis before 2018 could be read off the board: the fed
    funds rate against its target, the TED spread, reserves, the discount window and the swap lines are on FRED;
  - the DTCC fails history the board banked itself, which vanished from the page the day DTCC moved its file;
  - spreads that need two banked series: SOFR against fed funds, the SOFR tail, SOFR against the rate the Fed pays on
    reserves across the 2021 change of name (IOER, then IORB), fed funds against target, the Fed's repo operations summed.

It also asked FRED about every one of ~1,000 series every day - annual flow-of-funds lines included - while a daily
series such as SOFR was asked only once it was more than three days old.  refresh_due() asks by the series' own rhythm.

Pure functions; the engine wires them in.  No network, no S3.
"""
from datetime import date
import zlib

FUNDING_CORE = ("DFF", "EFFR", "OBFR", "DFEDTAR", "DFEDTARU", "DFEDTARL", "IOER", "TEDRATE",
                "WRESBAL", "WTREGEN", "WLCFLPCL", "SWPT", "WALCL")
FUNDING_BUCKET = "Policy rates, reserves and Fed lending"
OFR_SERIES_KEY = "data/warm/ofr/series/%s.json.gz"
OFR_API = "https://data.financialresearch.gov/v1/series/full?mnemonic=%s"
DTCC_BANKED = (("DTCC-TREASURY-FAILS", "data/warm/dtcc-fails/treasury.json",
                "FICC daily GROSS Treasury settlement fails (USD; FtD+FtR aggregated, can double-count -- NY Fed caveat)", 1),
               ("DTCC-AGENCY-FAILS", "data/warm/dtcc-fails/agency.json", "FICC daily GROSS Agency settlement fails (USD)", 2))


def ofr_extras(names, have_ids, catalog=()):
    """Scope entries for banked OFR series the inventory does not list: money-market funds and dealer securities lending.

    names = {mnemonic: series name} from the OFR metadata call; catalog = the bank's own list, used when that call failed.
    Treasury yields (TYLD) are not repo and stay out; repo and reference-rate mnemonics are all in the inventory already.
    """
    pool = dict.fromkeys(catalog or (), "")
    pool.update(names or {})
    out = []
    for m in sorted(pool):
        if m in have_ids:
            continue
        if m.startswith("MMF-"):
            family = "MMFRP"
        elif m.startswith("NYPD-"):
            family = "PDSEC" if ("_SB_" in m or "_SL_" in m) else "PDFIN"
        else:
            continue
        out.append({"mnemonic": m, "name": pool[m] or "", "family": family, "s3_key": OFR_SERIES_KEY % m, "tier": 2,
                    "api_url": OFR_API % m})
    return out


def dtcc_banked(have_ids):
    """The two banked DTCC fails documents, for the days the live file cannot be fetched (it has answered 404 since autumn 2026)."""
    return [{"mnemonic": m, "name": title + " [banked history; DTCC's file is not reachable]", "family": "DTCCFAILS", "s3_key": key,
             "api_url": "https://www.dtcc.com/charts/daily-total-us-treasury-trade-fails", "tier": tier}
            for m, key, title, tier in DTCC_BANKED if m not in have_ids]


def _day(s):
    return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))


def spacing_days(pairs):
    """The series' own rhythm: the median gap, in days, between its last observations."""
    tail = [p[0] for p in pairs[-7:]]
    gaps = sorted((_day(b) - _day(a)).days for a, b in zip(tail, tail[1:]))
    return gaps[len(gaps) // 2] if gaps else None


def refresh_due(series_id, pairs, today, asked=None):
    """Whether FRED should be asked for this series today.  `asked` = the day it was last asked (the banked document's
    own stamp), None when unknown.

    daily    as soon as yesterday's print can be out (the old rule waited until the bank was more than 3 days old)
    weekly   once the last observation is a week old
    slower   only once the next observation can exist (a month / quarter / year plus the usual publication delay, with
             a few days of spread so lines of one release are not all asked on one day), then every fourth day
    silent   a series far behind its own rhythm has either ended or sits in a bank that stopped asking: it is asked
             at once if it was never asked, or not for 30 days, and then once every 30 days
    """
    if not pairs:
        return True
    gap = spacing_days(pairs)
    if gap is None:                       # one observation: its rhythm is unknown, ask
        return True
    age = (today - _day(pairs[-1][0])).days
    since = (today - asked).days if asked else None

    def every(n):
        return since is None or since >= n

    if gap <= 4:                          # daily
        return age > 1 if age <= 45 else every(30)
    if gap <= 10:                         # weekly
        return age > 7 if age <= 60 else every(30)
    wait = gap + (10 if gap <= 40 else 45 if gap <= 100 else 60) + zlib.crc32(str(series_id).encode()) % 5
    if age <= wait:
        return False
    if age > 3 * gap + 90:
        return every(30)
    return every(4)


def stamp_day(text):
    """The day in a banked document's own time stamp ("2026-11-21T06:10:17+00:00"), or None."""
    try:
        return _day(str(text)[:10])
    except Exception:  # noqa: BLE001 - an unreadable stamp means 'never asked'
        return None


def _first(pairs, *ids):
    for i in ids:
        p = pairs(i)
        if p:
            return p
    return {}


def _stepwise(points, dates):
    """A rate that changes by announcement (IOER, the target), carried forward onto `dates`."""
    out, i, last = {}, 0, None
    for d in sorted(dates):
        while i < len(points) and points[i][0] <= d:
            last = points[i][1]
            i += 1
        if last is not None:
            out[d] = last
    return out


def derived_series(pairs):
    """[(id, label, {date: value}, tier)] - spreads that need two banked series.  pairs(id) -> {date: value}.

    Each is skipped by the engine when it comes out shorter than 30 observations (a component not banked yet).
    """
    out = []
    sofr = _first(pairs, "SOFR", "FNYR-SOFR-A")
    effr = dict(_first(pairs, "DFF"))
    effr.update(_first(pairs, "EFFR"))
    effr.update(_first(pairs, "FNYR-EFFR-A"))
    out.append(("D_SOFR_EFFR", "SOFR − fed funds effective (bps): secured over unsecured; a repo squeeze prints here first",
                {d: (sofr[d] - effr[d]) * 100.0 for d in sofr if d in effr}, 1))
    p99 = _first(pairs, "SOFR_P99", "FNYR-SOFR_99Pctl-A")
    out.append(("D_SOFR_TAIL", "SOFR 99th percentile − SOFR (bps): what the marginal borrower pays over the median",
                {d: (p99[d] - sofr[d]) * 100.0 for d in p99 if d in sofr}, 1))
    iorb, ioer = _first(pairs, "IORB"), _first(pairs, "IOER")
    admin_points = sorted({**ioer, **iorb}.items())
    admin = _stepwise(admin_points, sofr) if admin_points else {}
    out.append(("D_SOFR_ADMIN", "SOFR − interest on reserves (bps; IOER until July 2021, IORB since): repo against the rate banks earn at the Fed",
                {d: (sofr[d] - admin[d]) * 100.0 for d in sofr if d in admin}, 1))
    dff = _first(pairs, "DFF")
    target_points = sorted({**_first(pairs, "DFEDTAR"), **_first(pairs, "DFEDTARU")}.items())
    target = _stepwise(target_points, dff) if target_points else {}
    out.append(("D_EFFR_TARGET", "Fed funds effective − target (bps; the single target until December 2008, the top of the range since)",
                {d: (dff[d] - target[d]) * 100.0 for d in dff if d in target}, 1))
    legs = [_first(pairs, i) for i in ("RPONTSYD", "RPONAGYD", "RPONMBSD")]
    days = set().union(*[set(x) for x in legs]) if any(legs) else set()
    out.append(("D_FED_REPO", "Fed overnight repo operations, all collateral ($ billions a day): Treasury + agency + MBS",
                {d: sum(x.get(d, 0.0) for x in legs) for d in days}, 1))
    return out
