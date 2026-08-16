"""
ops/4771 -- PD depth splice: push the 106 NYPD board rows toward the
1998 FR2004 inception, with every join VERIFIED before it ships.

Stages (each reported, each skippable without poisoning the rest):
  A. Read the banked break table (pd/_meta/seriesbreaks.csv.gz) and
     the current timeseries list (pd/_meta/timeseries.csv.gz):
     exact break ids + date ranges, keyid universe, description text.
  B. Candidate mapping: for each of the 106 NYPD mnemonics on the
     board, score every NY Fed keyid by token rules over descriptions
     (measure FtD/FtR/repo/rev, asset class, settlement bucket,
     tenor); keep the top candidate.
  C. VERIFY by value overlap: compare the candidate keyid's banked
     values (current break) against the board row's OFR values on
     their 24 most recent common dates -- accept only if >=90% agree
     within 0.5% (or exactly, for integer millions). Unverified rows
     stay OFR-only and are listed honestly.
  D. Cross-break probe: for one verified fails keyid and one financing
     keyid, GET /api/pd/get/{break}/timeseries/{keyid}.json for each
     OLDER break -- do keyids persist? What date ranges come back?
  E. For verified pairs, fetch every older break's history, merge
     (older dates strictly before the newer break's first date -- the
     survey redefinitions make overlapping values non-comparable),
     and bank permanently: data/warm/nyfed-markets/pd-spliced/
     {keyid}.json.gz  + the map the engine will consume:
     data/warm/nyfed-markets/pd-splice-map.json
     {mnemonic: {keyid, breaks_used, first, last, n}}.
Nothing overwrites the OFR mirror; the splice is an additive layer.
"""
import csv
import gzip
import io
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org/api"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")
T0 = time.time()


def sread(key, as_json=True):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def fetch(url, timeout=40):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def pairs_of(doc):
    out = {}
    def walk(o, depth=0):
        if depth > 7:
            return
        if isinstance(o, list):
            for row in o:
                if isinstance(row, dict):
                    d = row.get("asofdate") or row.get("asOfDate") or \
                        row.get("date")
                    v = row.get("value")
                    if isinstance(d, str) and len(d) >= 10:
                        try:
                            out[d[:10]] = float(str(v).replace(",", ""))
                        except Exception:
                            pass
                walk(row, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)
    walk(doc)
    return dict(sorted(out.items()))


TOK_ASSET = {"T": ["u.s. treasury", "treasury (ex", "treasury secur"],
              "UST": ["u.s. treasury"],
              "AG": ["agency and gse", "federal agency and gse securities",
                      "agency debt"],
              "FGM": ["mbs", "mortgage-backed"],
              "FGEM": ["mbs", "mortgage-backed"],
              "CS": ["corporate"], "CORD": ["corporate"],
              "OS": ["other"], "O": ["other"], "TOT": ["total", "all "]}


def norm(s):
    return re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower())


def main():
    with report("4771_pd_1998_splice") as rep:
        rep.heading("ops 4771 -- PD splice toward 1998 (verified joins only)")

        rep.section("A. break table + keyid universe")
        raw = sread("data/warm/nyfed-markets/pd/_meta/seriesbreaks.csv.gz",
                     as_json=False).decode("utf-8", "replace")
        rep.log("seriesbreaks.csv RAW:\n" + raw[:600])
        rdr = list(csv.DictReader(io.StringIO(raw)))
        breaks = []
        for r0 in rdr:
            kid = (r0.get("keyid") or r0.get("KEYID") or
                    r0.get("seriesbreak") or list(r0.values())[0] or "").strip()
            if kid:
                breaks.append({k.lower(): (v or "").strip()
                                for k, v in r0.items()})
        rep.kv(check="breaks_rows", value=len(breaks))
        tsraw = sread("data/warm/nyfed-markets/pd/_meta/timeseries.csv.gz",
                       as_json=False).decode("utf-8", "replace")
        ts = list(csv.DictReader(io.StringIO(tsraw)))
        cols = list(ts[0].keys()) if ts else []
        rep.kv(check="timeseries_rows", value=len(ts))
        rep.log("timeseries columns: " + ", ".join(cols))
        def kid_of(row):
            return (row.get("keyid") or row.get("KEYID") or
                     row.get("Key Id") or "").strip()
        def desc_of(row):
            for c in cols:
                if "desc" in c.lower():
                    return row.get(c) or ""
            return " ".join(str(v) for v in row.values())
        _fails = [f"{kid_of(r0)}={desc_of(r0)[:60]}" for r0 in ts
                   if kid_of(r0).startswith("PDFTD")][:3]
        rep.log("sample fails rows: " + "; ".join(_fails))

        rep.section("B+C. map candidates and VERIFY by value overlap")
        board = sread("data/repo.json")
        nypd_rows = [s0 for g in board["groups"] for s0 in g["series"]
                      if s0["id"].startswith("NYPD-")]
        rep.kv(check="board_nypd_rows", value=len(nypd_rows))
        verified = {}
        unverified = []
        for r0 in nypd_rows:
            if time.time() - T0 > 60 * 40:
                unverified.append((r0["id"], "time_cap"))
                continue
            m = r0["id"]
            mn = norm(m)
            is_ftd = "AFtD" in m
            is_ftr = "AFtR" in m
            cands = []
            for row in ts:
                kid = kid_of(row)
                dsc = norm(desc_of(row))
                sc = 0
                if is_ftd and kid.startswith("PDFTD-"):
                    sc += 4
                elif is_ftr and kid.startswith("PDFTR-"):
                    sc += 4
                elif not (is_ftd or is_ftr) and kid.startswith(
                        ("PDSIRRA", "PDSORA", "PDSIOSB", "PDSOOS",
                          "PDSI", "PDSO")):
                    sc += 2
                else:
                    continue
                if "cumulative" in dsc:
                    sc -= 3
                leg = m.split("_")[-1].split("-")[0]
                for t in TOK_ASSET.get(leg, [norm(leg)]):
                    if t in dsc:
                        sc += 3
                        break
                for w in ("repo", "reverse", "overnight", "term",
                           "30", "settle"):
                    if w in mn and w in dsc:
                        sc += 1
                cands.append((sc, kid))
            cands.sort(reverse=True)
            hit = None
            for sc, kid in cands[:3]:
                if sc < 4:
                    break
                try:
                    bank = pairs_of(sread(
                        f"data/warm/nyfed-markets/pd/{kid}.json.gz"))
                except Exception:
                    continue
                hist = sread(f"data/repo-history/{r0['sid']}.json")
                ofr = dict(zip(hist["dates"], hist["values"]))
                common = sorted(set(bank) & set(ofr))[-24:]
                if len(common) < 10:
                    continue
                ok = sum(1 for d0 in common
                          if abs(bank[d0] - ofr[d0]) <=
                          max(0.005 * abs(ofr[d0]), 0.51))
                if ok / len(common) >= 0.9:
                    hit = (kid, len(common), ok)
                    break
            if hit:
                verified[m] = {"keyid": hit[0], "sid": r0["sid"],
                                "overlap": hit[1], "agree": hit[2]}
            else:
                unverified.append((m, "no value-verified candidate"))
        rep.kv(check="verified_mappings", value=len(verified))
        rep.kv(check="unverified", value=len(unverified))
        for m, why in unverified[:12]:
            rep.log(f"  unverified: {m} -- {why}")
        for m, v in list(verified.items())[:8]:
            rep.log(f"  ✓ {m} <-> {v['keyid']} "
                    f"({v['agree']}/{v['overlap']} values agree)")

        rep.section("D. cross-break keyid probe")
        bids = []
        for br in breaks:
            for c in br.values():
                if re.match(r"^SB[A-Z]*\d{4}", str(c)):
                    bids.append(str(c))
        bids = list(dict.fromkeys(bids))
        rep.kv(check="break_ids", value=",".join(bids))
        probe_kids = [v["keyid"] for v in list(verified.values())[:2]]
        persists = {}
        for kid in probe_kids:
            for bid in bids:
                try:
                    doc = json.loads(fetch(
                        f"{NYF}/pd/get/{bid}/timeseries/{kid}.json"))
                    pp = pairs_of(doc)
                    persists[(kid, bid)] = (len(pp),
                                              min(pp) if pp else None,
                                              max(pp) if pp else None)
                except Exception as e:
                    persists[(kid, bid)] = (f"ERR {type(e).__name__}",
                                              None, None)
        for (kid, bid), (n, a, bz) in persists.items():
            rep.log(f"  {kid} @ {bid}: n={n} span={a}->{bz}")

        rep.section("E. fetch older breaks, merge, bank")
        cur_first = {}
        spliced = 0
        new_floors = []
        map_out = {}
        for m, v in verified.items():
            if time.time() - T0 > 60 * 55:
                rep.warn("splice time cap -- remaining rows next run")
                break
            kid = v["keyid"]
            try:
                cur = pairs_of(sread(
                    f"data/warm/nyfed-markets/pd/{kid}.json.gz"))
            except Exception:
                continue
            if not cur:
                continue
            cur_first[kid] = min(cur)
            merged = dict(cur)
            used = ["current"]
            for bid in bids:
                try:
                    doc = json.loads(fetch(
                        f"{NYF}/pd/get/{bid}/timeseries/{kid}.json"))
                    older = pairs_of(doc)
                except Exception:
                    continue
                add = {d0: x for d0, x in older.items()
                        if d0 < min(merged)}
                if add:
                    merged.update(add)
                    used.append(bid)
                time.sleep(0.12)
            merged = dict(sorted(merged.items()))
            if min(merged) < cur_first[kid]:
                spliced += 1
                new_floors.append((m, cur_first[kid], min(merged)))
            s3.put_object(
                Bucket=B,
                Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"keyid": kid, "mnemonic": m, "breaks_used": used,
                      "built_at": datetime.now(timezone.utc).isoformat(),
                      "note": ("older-break dates only where strictly "
                               "before the newer break's first date; "
                               "survey redefinitions make overlaps "
                               "non-comparable"),
                      "dates": list(merged.keys()),
                      "values": list(merged.values())},
                     separators=(",", ":")).encode()),
                ContentType="application/json", ContentEncoding="gzip")
            map_out[m] = {"keyid": kid, "breaks_used": used,
                           "first": min(merged), "last": max(merged),
                           "n": len(merged)}
        s3.put_object(
            Bucket=B, Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": map_out},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        rep.kv(check="spliced_docs_banked", value=len(map_out))
        rep.kv(check="rows_with_deeper_floor", value=spliced)
        for m, old, new in sorted(new_floors, key=lambda x: x[2])[:15]:
            rep.ok(f"  {m}: floor {old} -> {new}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
