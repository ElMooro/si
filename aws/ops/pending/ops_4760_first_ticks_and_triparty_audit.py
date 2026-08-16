"""
ops/4760 -- first-tick verification + triparty completeness + data.html
visibility. Read-only: every claim below is read from the live
artifacts the scheduled engines write, not inferred.

  A. soma-cusip: hot status + state -- did the 3h ticks fire, which
     endpoint validated, convergence %, and open one banked date file
     to count real per-CUSIP rows.
  B. repo-deep v2: has the 05:22 self-extend run? Read the summary's
     fxs/ambs/tsy/seclending/soma_summary blocks and the fxs doc's
     refreshed_at.
  C. HFM extension: ofr-hfm/state.json -- catalog/done/progress/status
     plus S3 object count under the series prefix.
  D. Triparty, ALL sources: OFR TRI* mnemonic census + depth proof
     (rate + volume legs), GCF census, TGCR/BGCR + underlying-volume
     mnemonics present, and both haircut workbooks HEAD-verified with
     manifest office_verified flags.
  E. data.html: read the generated data/provider-catalog.json and
     report key-counts for ofr, ofr-hfm, ofr-bsrm, ofr-fsi, ofr-site,
     nyfed-research, nyfed -- what the page actually shows right now.
"""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)


def sread(key, as_json=True):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def count_prefix(prefix):
    n = 0
    token = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        cont = resp.get("Contents") or []
        n += len(cont)
        token = resp.get("NextContinuationToken")
        if not token:
            return n, cont


def deep_dates(obj, out=None, depth=0):
    if out is None:
        out = []
    if depth > 7:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:40000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def main():
    with report("4760_first_ticks_and_triparty_audit") as rep:
        rep.heading("ops 4760 -- first ticks + triparty completeness + data.html")

        rep.section("A. soma-cusip engine")
        try:
            hot = sread("data/soma-cusip-status.json")
            rep.kv(check="soma_fired", value=True)
            for k in ("as_of", "banked_this_run", "errors_this_run",
                       "done_pairs", "total_pairs", "progress_pct",
                       "pending_before"):
                rep.kv(**{f"soma_{k}": hot.get(k)})
            rep.log(f"soma endpoints validated: {json.dumps(hot.get('endpoint'))}")
            st = sread("data/warm/nyfed-markets/soma-cusip/_state.json")
            for typ in ("tsy", "agency"):
                done = sorted(st.get("done", {}).get(typ) or [])
                rep.kv(**{f"soma_{typ}_dates_done": len(done)})
                if done:
                    doc = sread(f"data/warm/nyfed-markets/soma-cusip/{typ}/"
                                 f"{done[-1]}.json.gz")
                    rows = None
                    if isinstance(doc, dict):
                        for v in doc.values():
                            if isinstance(v, list) and v:
                                rows = len(v)
                                break
                            if isinstance(v, dict):
                                for v2 in v.values():
                                    if isinstance(v2, list) and v2:
                                        rows = len(v2)
                                        break
                    rep.ok(f"{typ} sample {done[-1]}: {rows} per-CUSIP rows")
        except Exception as e:
            rep.kv(check="soma_fired", value=False)
            rep.warn(f"soma status: {type(e).__name__}: {str(e)[:110]}")

        rep.section("B. repo-deep v2 first self-extend")
        try:
            summ = sread("data/warm/nyfed-markets/repo-deep-summary.json")
            fams = [k for k in ("fxs", "ambs", "tsy", "seclending",
                                  "soma_summary") if k in summ]
            rep.kv(check="v2_ran", value=bool(fams))
            rep.kv(check="v2_summary_as_of", value=summ.get("as_of"))
            for k in fams:
                rep.log(f"  v2 {k}: {json.dumps(summ[k])[:150]}")
            if fams:
                fx = sread("data/warm/nyfed-markets/fxs-history.json.gz")
                rep.kv(check="fxs_refreshed_at", value=fx.get("refreshed_at"))
                rep.kv(check="fxs_latest_opdate", value=fx.get("latest"))
                rep.kv(check="fxs_n_rows", value=fx.get("n_rows"))
        except Exception as e:
            rep.warn(f"repo-deep summary: {type(e).__name__}: {str(e)[:110]}")

        rep.section("C. HFM /hf/v1 extension")
        try:
            hf = sread("data/warm/ofr-hfm/state.json")
            for k in ("as_of", "status", "progress_pct"):
                rep.kv(**{f"hfm_{k}": hf.get(k)})
            rep.kv(hfm_catalog=len(hf.get("catalog") or []),
                   hfm_done=len(hf.get("done") or []))
        except Exception as e:
            rep.kv(check="hfm_state", value="absent")
            rep.log(f"  ofr-hfm/state.json not written yet "
                    f"({type(e).__name__}) -- stfm tick pending")
        n_hfm, _ = count_prefix("data/warm/ofr-hfm/series/")
        rep.kv(check="hfm_series_objects", value=n_hfm)

        rep.section("D. Triparty -- every source")
        st = sread("data/warm/ofr/state.json")
        cat = sorted(map(str, st.get("catalog") or []))
        tri = [m for m in cat if "TRI" in m.upper()]
        gcf = [m for m in cat if "GCF" in m.upper()]
        rep.kv(check="ofr_TRI_mnemonics", value=len(tri))
        rep.kv(check="ofr_GCF_mnemonics", value=len(gcf))
        rep.log("TRI sample: " + ", ".join(tri[:12]))
        for probe in ("REPO-TRIV1_AR_TOT-P", "REPO-TRIV1_TV_TOT-P"):
            if probe in cat:
                try:
                    doc = sread(f"data/warm/ofr/series/{probe}.json.gz")
                    dd = sorted(set(deep_dates(doc)))
                    rep.ok(f"{probe}: {len(dd)} obs, "
                            f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
                except Exception as e:
                    rep.warn(f"{probe}: {type(e).__name__}")
            else:
                near = [m for m in tri if "AR_TOT" in m or "TV_TOT" in m][:4]
                rep.log(f"{probe} not in catalog; totals present: {near}")
        for r_m in ("FNYR-TGCR-A", "FNYR-BGCR-A", "FNYR-TGCR_UV-A",
                     "FNYR-BGCR_UV-A", "FNYR-SOFR_UV-A"):
            rep.kv(**{f"held_{r_m.replace('-', '_')}": r_m in cat})
        man = sread("data/warm/nyfed-research/_manifest.json")
        for hk in ("haircuts/tri-party-repo_data_current.xlsx",
                    "haircuts/tri-party-repo_preNov25_history.xlsx"):
            ent = man.get("files", {}).get(hk)
            try:
                h = s3.head_object(Bucket=BUCKET,
                                    Key=f"data/warm/nyfed-research/{hk}")
                rep.ok(f"haircut file on S3: {hk} "
                        f"({h['ContentLength']} bytes, "
                        f"office_verified={ent.get('office_verified') if ent else '?'})")
            except Exception as e:
                rep.warn(f"haircut file MISSING: {hk} ({type(e).__name__})")

        rep.section("E. data.html -- what the page shows right now")
        try:
            pc = sread("data/provider-catalog.json")
            provs = pc.get("providers") or pc.get("rows") or pc
            idx = {}
            if isinstance(provs, list):
                for p in provs:
                    if isinstance(p, dict):
                        idx[p.get("slug") or p.get("id") or
                            p.get("name", "")[:20]] = p
            elif isinstance(provs, dict):
                idx = provs
            rep.kv(check="catalog_generated_at",
                   value=(pc.get("generated_at") or pc.get("as_of")))
            for slug in ("ofr", "ofr-hfm", "ofr-bsrm", "ofr-fsi", "ofr-site",
                          "nyfed", "nyfed-research"):
                p = idx.get(slug)
                if isinstance(p, dict):
                    rep.kv(**{f"page_{slug}": f"keys={p.get('keys') or p.get('n_keys')}"
                              f" mb={p.get('mb') or p.get('size_mb')}"})
                else:
                    hit = None
                    if isinstance(provs, list):
                        for q in provs:
                            if isinstance(q, dict) and slug in \
                                    json.dumps(q)[:300]:
                                hit = q
                                break
                    rep.kv(**{f"page_{slug}": (f"keys={hit.get('keys')}"
                              if hit else "NOT ON PAGE YET")})
        except Exception as e:
            rep.warn(f"provider-catalog.json: {type(e).__name__}: {str(e)[:110]}")
            rep.log("  if absent/stale, the catalog engine's next scheduled "
                    "tick regenerates it with the new REG entries")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
