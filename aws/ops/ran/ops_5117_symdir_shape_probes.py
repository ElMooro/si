"""ops_5117 -- symdir shape probes (read-only) for the 5116 residuals.

  * statcan: what is inside data/warm/statcan/data/10100001.dat.gz (browse said
    "no VECTOR column") -- magic bytes, first 600 chars decompressed
  * boj: exact shape of RESULTSET[i]["VALUES"] in an api part that HAS data
  * eurostat: does the English TOC (catalogue/toc/txt?lang=en) resolve the
    French/German dataflow names the walker catalog carries
  * fred: how many banked ids have no title in series-meta (the CPILFESL case)
    and what the banked file's meta carries for them
  * treasury: the warm doc keys (observations shape)
Never RED.
"""
import gzip
import io
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def get(key, rng=None):
    kw = {"Bucket": B, "Key": key}
    if rng:
        kw["Range"] = rng
    return s3.get_object(**kw)["Body"].read()


def main():
    with report("5117-symdir-shape-probes") as r:
        r.heading("ops 5117 -- shape probes for the symbol directory residuals")
        r.section("statcan cube bytes")
        for pid in ("10100001", "10100002", "14100287"):
            try:
                raw = get("data/warm/statcan/data/%s.dat.gz" % pid)
            except Exception as e:  # noqa: BLE001
                r.log(f"  {pid}: missing ({str(e)[:80]})")
                continue
            magic = raw[:4]
            r.log(f"  {pid}: {len(raw)} bytes magic={magic!r}")
            try:
                body = gzip.decompress(raw)
                r.log(f"    gunzipped {len(body)} bytes, inner magic={body[:4]!r}")
                if body[:2] == b"PK":
                    z = zipfile.ZipFile(io.BytesIO(body))
                    names = z.namelist()
                    r.log(f"    ZIP members: {names[:6]}")
                    with z.open(names[0]) as f:
                        head = f.read(700).decode("utf-8", "replace")
                    r.log("    member head: " + head.replace("\n", "⏎")[:600])
                else:
                    r.log("    head: " + body[:700].decode("utf-8", "replace").replace("\n", "⏎"))
            except Exception as e:  # noqa: BLE001
                r.log(f"    not gzip? {str(e)[:80]}; raw head: {raw[:300]!r}")
        r.section("boj VALUES shape")
        found = 0
        tok = None
        while found < 2:
            kw = {"Bucket": B, "Prefix": "data/warm/boj-full/api/BP01/", "MaxKeys": 200}
            if tok:
                kw["ContinuationToken"] = tok
            d = s3.list_objects_v2(**kw)
            for o in d.get("Contents") or []:
                if not o["Key"].endswith(".json.gz") or o["Size"] < 3000:
                    continue
                j = json.loads(gzip.decompress(get(o["Key"])))
                rs = j.get("RESULTSET") or []
                for rec in rs[:1]:
                    v = rec.get("VALUES")
                    r.log(f"  {o['Key']} size={o['Size']} resultset={len(rs)} msg={j.get('MESSAGE')!r}")
                    r.log(f"    record keys: {list(rec.keys())}")
                    r.log(f"    VALUES type={type(v).__name__} " + (f"keys={list(v.keys())} sample={ {k: (str(x)[:120]) for k, x in v.items()} }" if isinstance(v, dict) else f"sample={str(v)[:300]}"))
                    found += 1
                if found >= 2:
                    break
            tok = d.get("NextContinuationToken")
            if not tok:
                break
        r.section("eurostat english TOC")
        try:
            req = urllib.request.Request("https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en", headers={"User-Agent": "justhodl-ops"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                txt = resp.read().decode("utf-8", "replace")
            lines = txt.split("\n")
            r.log(f"  TOC lines={len(lines)} header={lines[0][:200]!r}")
            hits = [l for l in lines if "\tNAMA_10_GDP\t" in l or "\tnama_10_gdp\t" in l or "\tUNE_RT_M\t" in l or "\tune_rt_m\t" in l]
            for h in hits[:4]:
                r.log("  " + h[:220])
            ds = [l for l in lines if "\tdataset\t" in l]
            r.log(f"  dataset rows={len(ds)} sample={ds[100][:200]!r}")
        except Exception as e:  # noqa: BLE001
            r.log(f"  TOC failed: {str(e)[:120]}")
        r.section("fred untitled banked ids")
        titled = set()
        tok = None
        n_pages = 0
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-catalog/series-meta/", "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            d = s3.list_objects_v2(**kw)
            for o in d.get("Contents") or []:
                n_pages += 1
            tok = d.get("NextContinuationToken")
            if not tok:
                break
        # sample: is CPILFESL / PAYEMS / CPIAUCSL in any meta page? read pages in parallel
        from concurrent.futures import ThreadPoolExecutor
        want = {"CPILFESL", "CPIAUCSL", "PAYEMS", "M2SL", "SP500", "DEXUSEU", "VIXCLS", "T10Y2Y", "DFF", "WALCL"}
        keys = ["data/warm/fred-catalog/series-meta/page-%04d.json" % i for i in range(n_pages)]

        def rd(k):
            try:
                return {row["id"] for row in json.loads(get(k)).get("rows") or [] if row.get("id")}
            except Exception:  # noqa: BLE001
                return set()
        with ThreadPoolExecutor(32) as ex:
            for ids in ex.map(rd, keys):
                titled |= ids
        r.log(f"  meta pages={n_pages} titled ids={len(titled)} famous present={sorted(want & titled)} famous MISSING={sorted(want - titled)}")
        roots = [c["Prefix"] for c in s3.list_objects_v2(Bucket=B, Prefix="data/warm/fred-scoped/", Delimiter="/").get("CommonPrefixes", [])]
        banked = {}
        for pre in roots:
            tok = None
            while True:
                kw = {"Bucket": B, "Prefix": pre, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                d = s3.list_objects_v2(**kw)
                for o in d.get("Contents") or []:
                    base = o["Key"].rsplit("/", 1)[-1]
                    if base.endswith(".json") and not base.startswith("_"):
                        banked[base[:-5]] = o["Key"]
                tok = d.get("NextContinuationToken")
                if not tok:
                    break
        untitled = [k for k in banked if k not in titled]
        r.log(f"  banked={len(banked)} untitled banked={len(untitled)} sample={untitled[:12]}")
        for sid in untitled[:2] + [x for x in ("CPILFESL", "PAYEMS") if x in banked][:2]:
            j = json.loads(get(banked[sid]))
            r.log(f"  banked meta {sid}: {json.dumps(j.get('meta'))[:300]} n_obs={len(j.get('observations') or [])}")
        r.section("treasury warm doc keys")
        for k in ("data/warm/treasury/debt_to_penny.json.gz", "data/warm/treasury/avg_interest_rates.json.gz"):
            j = json.loads(gzip.decompress(get(k)))
            r.log(f"  {k}: keys={list(j.keys())} obs_sample={str((j.get('observations') or j.get('payload') or [])[:2])[:300]}")
        r.ok("probes done")


if __name__ == "__main__":
    main()
