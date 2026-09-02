"""ops_5101 -- probe the live lanes that fill v3's gaps (trade, curve, CCI, REER).

ops 5100 (first real run): feature store 34/34 in 12s, GBC v3.0.0 live with
32/34 multi-pillar countries. Gaps: trade pillar 0 (the walked KEI file carries
only PRVM/TOVM/PP/H_EARN/BCICP), curve 0, CCI 0, REER 8 (BIS WS_EER walked
file truncated). The OECD STES family is 429'd in bulk but a filtered CLI pull
works; BIS's API is generous. This op finds the exact URLs/dimension keys
that work from the runner for our 34 countries:

  P1  OECD DSD_STES@DF_FINMARK  (long/short rates, share prices)
  P2  OECD DSD_STES@DF_BTS      (business tendency: BCI, order books, prod. expectations)
  P3  OECD DSD_STES@DF_CS       (consumer surveys: CCI)
  P4  OECD DSD_STES@DF_TRADE / DF_TRADE_*  (exports/imports value)
  P5  OECD DSD_STES@DF_INDSERV  (industrial production incl. countries KEI misses)
  P6  OECD DSD_KEI@DF_KEI live  (does the live flow carry more measures than the walked file?)
  P7  BIS WS_EER monthly real broad  (https://stats.bis.org/api/v2 ...)
  P8  the walked KEI file: full measure x country inventory (why only 5 measures?)
Each probe: HTTP status, bytes, rows, dims, our-country coverage, latest
periods; results to data/audit/cycle-lanes-probe-5101.json. Never RED
(sys.exit(1) only if boto3/S3 unusable).
"""
import csv
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
ISO3 = ["USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA", "KOR", "AUS", "ESP", "MEX", "IDN", "NLD",
        "TUR", "CHE", "POL", "BEL", "SWE", "IRL", "AUT", "NOR", "ZAF", "DNK", "FIN", "CZE", "HUN", "CHL", "PRT", "GRC",
        "NZL", "ISR"]
ISO2 = ["US", "CN", "JP", "DE", "IN", "GB", "FR", "IT", "CA", "BR", "KR", "AU", "ES", "MX", "ID", "NL", "TR", "CH", "PL",
        "BE", "SE", "IE", "AT", "NO", "ZA", "DK", "FI", "CZ", "HU", "CL", "PT", "GR", "NZ", "IL"]
AREAS = "+".join(ISO3)
UA = {"User-Agent": "JustHodl.AI ops5101 (+https://justhodl.ai)", "Accept": "text/csv, application/vnd.sdmx.data+csv, */*"}
OUT = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ops": 5101, "probes": {}}


def fetch(url, timeout=120):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), round(time.time() - t0, 1), None
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:400], round(time.time() - t0, 1), f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return None, b"", round(time.time() - t0, 1), str(e)[:120]


def profile(body, area_cols=("REF_AREA", "LOCATION"), max_rows=2_000_000):
    text = body.decode("utf-8", "replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    rd = csv.reader(io.StringIO(text))
    try:
        hdr = [h.strip() for h in next(rd)]
    except StopIteration:
        return {"rows": 0}
    col = {h: i for i, h in enumerate(hdr)}
    tp, ov = col.get("TIME_PERIOD"), col.get("OBS_VALUE")
    ac = next((col[c] for c in area_cols if c in col), None)
    mc = next((col[c] for c in ("MEASURE", "SUBJECT", "INDICATOR") if c in col), None)
    fc = col.get("FREQ", col.get("FREQUENCY"))
    dims = [h for h in hdr if h not in ("STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS",
                                        "UNIT_MULT", "DECIMALS", "OBS_CONF", "TIME_FORMAT", "TITLE_TS", "COLLECTION") and not h.startswith("Observation")]
    distinct = {h: Counter() for h in dims[:14]}
    latest = {}
    n = 0
    for row in rd:
        n += 1
        if n > max_rows or len(row) < len(hdr) - 2:
            continue
        a = row[ac] if ac is not None else "_"
        m = row[mc] if mc is not None else "_"
        f = row[fc] if fc is not None else "_"
        for h in distinct:
            if len(distinct[h]) < 50:
                distinct[h][row[col[h]]] += 1
        if tp is not None:
            t = row[tp]
            k = (a, f"{m}/{f}")
            if t > latest.get(k, ""):
                latest[k] = t
    per = defaultdict(dict)
    for (a, m), t in latest.items():
        per[a][m] = t
    ours = {a: v for a, v in per.items() if a in ISO3 or a in ISO2}
    return {"rows": n, "header": hdr[:30], "dims": {h: [k for k, _ in c.most_common(30)] for h, c in distinct.items()},
            "n_areas": len(per), "ours": len(ours), "ours_latest": {a: v for a, v in list(ours.items())[:40]}}


def main():
    with report("5101-cycle-lanes-probe") as r:
        r.heading("ops 5101 -- probe live OECD STES / BIS lanes for the v3 trade, curve, CCI, REER gaps")
        probes = [
            ("oecd_finmark_all", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_FINMARK,/all?startPeriod=2000-01&format=csvfile"),
            ("oecd_finmark_key", f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_FINMARK,/{AREAS}.M.IRLT+IR3TIB+IRSTCI+SHARE......?startPeriod=2000-01&format=csvfile"),
            ("oecd_bts_all", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_BTS,/all?startPeriod=2000-01&format=csvfile"),
            ("oecd_cs_all", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CS,/all?startPeriod=2000-01&format=csvfile"),
            ("oecd_trade_all", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_TRADE,/all?startPeriod=2000-01&format=csvfile"),
            ("oecd_indserv_key", f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_INDSERV,/{AREAS}.M.........?startPeriod=2000-01&format=csvfile"),
            ("oecd_kei_live", f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,/{AREAS}.M........?startPeriod=2000-01&format=csvfile"),
            ("oecd_kei_live_all", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,/all?startPeriod=2000-01&format=csvfile"),
            ("bis_eer_monthly", f"https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/M.R.B.{'+'.join(ISO2)}?startPeriod=2000-01&format=csv"),
            ("bis_eer_monthly_all", "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/M.R.B.?startPeriod=2000-01&format=csv"),
            ("bis_cbpol_monthly", f"https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M.{'+'.join(ISO2)}?startPeriod=2000-01&format=csv"),
        ]
        for name, url in probes:
            r.section(name)
            st, body, dt, err = fetch(url)
            row = {"url": url, "status": st, "bytes": len(body), "seconds": dt, "error": err}
            r.log(f"HTTP {st} {len(body)} B in {dt}s {err or ''}")
            if st == 200 and len(body) > 200:
                try:
                    p = profile(body)
                    row.update(p)
                    r.log(f"  rows={p.get('rows')} areas={p.get('n_areas')} ours={p.get('ours')}/34 header={p.get('header')[:14]}")
                    r.log(f"  dims={json.dumps(p.get('dims'))[:1400]}")
                    r.log(f"  ours latest sample: {json.dumps(dict(list((p.get('ours_latest') or {}).items())[:6]))[:900]}")
                except Exception as e:  # noqa: BLE001
                    r.warn(f"  profile failed: {str(e)[:120]}; head={body[:300]!r}")
            else:
                r.log(f"  body head: {body[:300]!r}")
            OUT["probes"][name] = row
            r.kv(probe=name, status=st, bytes=len(body), rows=row.get("rows"), ours=row.get("ours"), seconds=dt)
            time.sleep(4)   # pace OECD

        r.section("P8 walked KEI inventory")
        try:
            o = s3.get_object(Bucket=B, Key="data/warm/oecd/data/DSD_KEI@DF_KEI.dat.gz")
            body = gzip.decompress(o["Body"].read())
            text = body.decode("utf-8", "replace")
            rd = csv.DictReader(io.StringIO(text))
            inv = defaultdict(lambda: defaultdict(set))
            n = 0
            for row in rd:
                n += 1
                if row.get("FREQ") != "M":
                    continue
                inv[row.get("MEASURE")][row.get("REF_AREA")].add(row.get("TRANSFORMATION"))
            r.log(f"walked KEI: {n} rows; monthly measures: " + "; ".join(f"{m}: {len(a)} areas, ours {sum(1 for x in a if x in ISO3)}, transforms {sorted({t for s in a.values() for t in s})}" for m, a in inv.items()))
            last = text.rstrip().rsplit("\n", 1)[-1]
            r.log(f"  last line (truncation check): {last[:200]!r}")
            OUT["kei_walked"] = {m: {"areas": len(a), "ours": sum(1 for x in a if x in ISO3)} for m, a in inv.items()}
        except Exception as e:  # noqa: BLE001
            r.warn(f"KEI inventory failed: {str(e)[:120]}")

        s3.put_object(Bucket=B, Key="data/audit/cycle-lanes-probe-5101.json", Body=json.dumps(OUT, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.section("verdict")
        ok = [k for k, v in OUT["probes"].items() if v.get("status") == 200 and (v.get("ours") or 0) >= 10]
        r.ok(f"lanes with >= 10 of our countries: {ok}")


if __name__ == "__main__":
    main()
