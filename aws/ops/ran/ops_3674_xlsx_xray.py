"""ops 3674 — XLSX X-RAY: per-sheet cell diagnostics + raw markup around the
'Unloaded' sharedString index. Ends the guessing on CAD cell format."""
import io, json, re, sys, urllib.parse, urllib.request, zipfile
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

XLSX = "https://www.cad.gov.hk/english/./pdf/Stat Webpage.xlsx"
UA = {"User-Agent": "Mozilla/5.0 Chrome/126.0"}

with report("3674_xlsx_xray") as rep:
    rep.heading("ops 3674 — CAD xlsx x-ray")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3674.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        rb = urllib.request.urlopen(urllib.request.Request(
            urllib.parse.quote(XLSX, safe=":/"), headers=UA), timeout=40).read()
        zf = zipfile.ZipFile(io.BytesIO(rb))
        out["names"] = zf.namelist()[:20]
        sh = zf.read("xl/sharedStrings.xml").decode("utf-8", "replace")
        strings = ["".join(re.findall(r"<t[^>]*>([^<]*)</t>", si))
                   for si in re.split(r"<si>", sh)[1:]]
        idxU = next((i for i, s2 in enumerate(strings)
                     if "unload" in s2.lower()), None)
        out["strings_all"] = [s2[:34] for s2 in strings]
        out["idx_unloaded"] = idxU
        diags = []
        for nm2 in sorted(zf.namelist()):
            if not nm2.startswith("xl/worksheets/sheet"):
                continue
            xml = zf.read(nm2).decode("utf-8", "replace")
            d2 = {"sheet": nm2, "len": len(xml),
                  "rows": len(re.findall(r"<row", xml)),
                  "c_cells": len(re.findall(r"<c[ >]", xml)),
                  "v_cells": len(re.findall(r"<v>", xml)),
                  "t_s": len(re.findall(r't="s"', xml)),
                  "inline": len(re.findall(r"<is>", xml))}
            if idxU is not None:
                hit = re.search(r".{{0,140}}>{}<.{{0,140}}".format(idxU), xml, re.S)
                if hit:
                    d2["around_unloaded"] = hit.group(0)[:300]
            d2["head"] = xml[:220]
            diags.append(d2)
        out["sheets"] = diags
        out["gates"]["G1_xray"] = {"ok": True, "detail": json.dumps(diags)[:900]}
        print("PASS  G1_xray —", json.dumps(diags)[:860])
        out["verdict"] = "PASS_ALL"
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3674.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
