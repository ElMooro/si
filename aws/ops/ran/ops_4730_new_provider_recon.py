"""
ops/4730 -- new-provider reconnaissance (read-only).

Khalid asked to: finish out OECD (leaving FRED + StatCan as acceptable
as-is), pull Trading Economics with full history for series not already
in FRED, add Bank of Japan + Japan MOF, Chile copper exports, Finland
exports/IP/manufacturing, and Korea + Taiwan manufacturing/IP.

Before writing a single new pull lambda: probe every candidate source
live. None of this is answerable from repo code alone -- OECD needs a
live cache check against the rev-C triplet fix, and the rest need live
reachability + auth checks against government endpoints this sandbox
has no egress to. This op is read-only: S3 reads + HEAD/GET probes
against public statistical-agency endpoints. No lambda deploys, no
writes to any production data key.
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-recon/1.0)"}

s3 = boto3.client("s3", region_name=REGION)


def get_json_s3(key, gz=False):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if gz or key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def probe(url, timeout=15, headers=None):
    h = dict(UA)
    if headers:
        h.update(headers)
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"ok": True, "status": r.status, "bytes": len(body),
                     "elapsed_ms": round((time.time() - t0) * 1000),
                     "sample": body[:200].decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        try:
            sample = e.read()[:200].decode("utf-8", "replace")
        except Exception:
            sample = ""
        return {"ok": False, "status": e.code,
                 "elapsed_ms": round((time.time() - t0) * 1000), "sample": sample}
    except Exception as e:
        return {"ok": False, "status": None,
                 "error": f"{type(e).__name__}: {str(e)[:120]}",
                 "elapsed_ms": round((time.time() - t0) * 1000)}


def main():
    with report("4730_new_provider_recon") as rep:
        rep.heading("ops 4730 -- new-provider reconnaissance (read-only)")
        rep.log("Scope: OECD finish-out, BOJ/MOF expansion, Chile copper, "
                "Finland exports/IP/mfg, Korea mfg/IP, Taiwan mfg/IP expansion, "
                "TE full-history beyond the FRED-mirror. FRED + StatCan left "
                "as-is per Khalid -- not touched by this op.")

        rep.section("1. OECD triplet-fix (rev-C) -- live cache state")
        try:
            trip = get_json_s3("data/warm/oecd/flow-triplets.json.gz", gz=True)
            tmap = (trip or {}).get("map") or {}
            rep.kv(check="oecd_triplets_cached", value=len(tmap))
            rep.ok(f"triplet cache has {len(tmap)} resolved flow IDs")
        except Exception as e:
            rep.warn(f"no triplet cache object yet ({type(e).__name__}: {str(e)[:80]}) "
                     "-- rev-C fix is written in source but hasn't resolved/cached "
                     "anything yet")
        try:
            oecd_state = get_json_s3("data/_state/sdmx-walk-oecd.json")
            fails = oecd_state.get("failures") or {}
            rep.kv(check="oecd_live_failures", value=len(fails))
            rep.kv(check="oecd_walk_status", value=oecd_state.get("status"))
            if fails:
                sk = next(iter(fails))
                rep.log(f"sample failure [{sk}]: {str(fails[sk])[:160]}")
        except Exception as e:
            rep.warn(f"couldn't read oecd walk-state: {type(e).__name__}: {str(e)[:100]}")

        rep.section("2. Taiwan MOEA -- open-data catalog, looking for more mfg/IP files")
        r = probe("https://service.moea.gov.tw/opendata/api/", timeout=20)
        rep.kv(check="taiwan_moea_catalog_reachable", value=r["ok"])
        rep.log(f"catalog root: {json.dumps(r)[:280]}")

        rep.section("3. Korea -- Bank of Korea ECOS + KOSIS reachability")
        r_ecos = probe("https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/5/200Y001/M",
                       timeout=15)
        rep.kv(check="korea_ecos_reachable", value=r_ecos["ok"])
        rep.log(f"ECOS sample-key probe: {json.dumps(r_ecos)[:280]}")
        r_kosis = probe("https://kosis.kr/openapi/", timeout=15)
        rep.kv(check="korea_kosis_reachable", value=r_kosis["ok"])
        rep.log(f"KOSIS root: {json.dumps(r_kosis)[:260]}")

        rep.section("4. Chile -- Cochilco (no-auth alt) + BCCh reachability")
        r_cochilco = probe("https://www.cochilco.cl/Paginas/Estadisticas.aspx", timeout=20)
        rep.kv(check="chile_cochilco_reachable", value=r_cochilco["ok"])
        rep.log(f"Cochilco: {json.dumps(r_cochilco)[:260]}")
        r_bcch = probe("https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?"
                       "user=x&pass=x&function=GetSeries&timeseries=F019.COT.PRE.LME.CN.D",
                       timeout=15)
        rep.kv(check="chile_bcch_domain_reachable", value=r_bcch["ok"] or r_bcch.get("status") is not None)
        rep.log(f"BCCh (dummy creds -- just checking the domain answers, per gov-sources "
                f"note this needs Khalid to register a real account): {json.dumps(r_bcch)[:260]}")

        rep.section("5. Finland -- Statistics Finland (StatFin PxWeb) reachability")
        r_fi = probe("https://statfin.stat.fi/PxWeb/api/v1/en/StatFin/", timeout=20)
        rep.kv(check="finland_statfin_reachable", value=r_fi["ok"])
        rep.log(f"StatFin PxWeb root: {json.dumps(r_fi)[:300]}")

        rep.section("6. BOJ -- sanity ping (headroom is 792 MD11 series vs 24 keys banked)")
        r_boj = probe("https://www.stat-search.boj.or.jp/ssi/mtshtml/fm01_m_1.html", timeout=15)
        rep.kv(check="boj_domain_reachable", value=r_boj["ok"])

        rep.section("7. Are peru-copper / taiwan-moea actually fresh right now?")
        for key, label in [("data/peru-copper.json", "peru_copper"),
                            ("data/taiwan-moea.json", "taiwan_moea")]:
            try:
                obj = s3.head_object(Bucket=BUCKET, Key=key)
                age_h = round((time.time() - obj["LastModified"].timestamp()) / 3600, 1)
                rep.kv(check=f"{label}_age_hours", value=age_h)
                rep.ok(f"{label}: last written {age_h}h ago")
            except Exception as e:
                rep.warn(f"{label}: {type(e).__name__} -- {str(e)[:100]}")

        rep.section("Summary")
        rep.log("Read-only recon only -- nothing deployed or written to production "
                 "in this op. Next ops act on whichever sources above came back "
                 "reachable, in priority order, and will call out anything that "
                 "genuinely needs Khalid to register an account (BCCh already "
                 "looks like one, per gov-sources' own note).")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("RECON ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
