"""ops/4774 -- why won't fails verify? Print raw values side by side.
Four hypotheses tested on the 6 most recent common dates each:
  H1 NYPD-PD_AFtD_TIPS-A  == PDFTD-UST      (label said TIPS)
  H2 NYPD-PD_AFtD_T_eTIPS == PDFTD-USTET    (ex-TIPS)
  H3 NYPD-PD_AFtD_AG_eMBS == PDFTD-FGEM     (agency ex-MBS)
  H4 NYPD-PD_AFtD_TOT-A   == sum(all PDFTD current kids)
Also: list the PDFTD/PDFTR kids actually banked + each one's last
date, and print one bank doc's first raw row (field names + formats).
Read-only; the verdict dictates the real map."""
import gzip
import json
import re
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def sread(key, as_json=True):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def iso(d):
    d = (d or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", d):
        return d[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", d)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def pairs_of(doc):
    out = {}
    def walk(o, depth=0):
        if depth > 7:
            return
        if isinstance(o, list):
            for row in o:
                if isinstance(row, dict):
                    d0 = iso(str(row.get("asofdate") or row.get("asOfDate")
                                  or row.get("date") or ""))
                    try:
                        out[d0] = float(str(row.get("value")
                                             ).replace(",", ""))
                    except Exception:
                        pass
                walk(row, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)
    walk(doc)
    out.pop(None, None)
    return dict(sorted(out.items()))


def main():
    with report("4774_pd_join_diagnostic") as rep:
        rep.heading("ops 4774 -- fails join diagnostic (raw values)")
        tsraw = sread("data/warm/nyfed-markets/pd/_meta/timeseries.csv.gz",
                       as_json=False).decode("utf-8", "replace")
        kids = sorted({m.group(0) for m in
                        re.finditer(r"PDFT[DR]-[A-Z0-9_]+", tsraw)})
        rep.kv(check="pdft_kids_in_meta", value=len(kids))
        rep.log("kids: " + ", ".join(kids))

        rep.section("one raw bank row (field truth)")
        doc = sread(f"data/warm/nyfed-markets/pd/{kids[0]}.json.gz")
        blob = json.dumps(doc)[:500]
        rep.log(f"{kids[0]} doc[:500]: {blob}")

        bank = {k: pairs_of(sread(f"data/warm/nyfed-markets/pd/{k}.json.gz"))
                 for k in kids}
        for k in kids:
            pp = bank[k]
            rep.log(f"  {k}: n={len(pp)} last={max(pp) if pp else '-'}")

        d = sread("data/repo.json")
        hist = {}
        for g in d["groups"]:
            for s0 in g["series"]:
                if s0["id"].startswith("NYPD-PD_AFt"):
                    h = sread(f"data/repo-history/{s0['sid']}.json")
                    hist[s0["id"]] = dict(zip(h["dates"], h["values"]))
        rep.kv(check="ofr_fails_rows", value=len(hist))
        rep.log("ofr fails ids: " + ", ".join(sorted(hist)))

        rep.section("hypotheses, raw side-by-side")
        cases = [("NYPD-PD_AFtD_TIPS-A", "PDFTD-UST"),
                  ("NYPD-PD_AFtD_T_eTIPS-A", "PDFTD-USTET"),
                  ("NYPD-PD_AFtD_AG_eMBS-A", "PDFTD-FGEM")]
        for m, k in cases:
            o = hist.get(m, {})
            bk = bank.get(k, {})
            common = sorted(set(o) & set(bk))[-6:]
            rep.log(f"{m} vs {k}: common_dates={len(set(o) & set(bk))}")
            for d0 in common:
                rep.log(f"    {d0}: ofr={o[d0]}  bank={bk[d0]}")
        tot = hist.get("NYPD-PD_AFtD_TOT-A", {})
        ftd_kids = [k for k in kids if k.startswith("PDFTD-")
                     and "TOT" not in k]
        common = sorted(set(tot) & set.intersection(
            *[set(bank[k]) for k in ftd_kids])) if ftd_kids else []
        rep.log(f"TOT vs sum({len(ftd_kids)} kids): common={len(common)}")
        for d0 in common[-6:]:
            s = sum(bank[k][d0] for k in ftd_kids)
            rep.log(f"    {d0}: ofr_TOT={tot[d0]}  sum={round(s,1)}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
