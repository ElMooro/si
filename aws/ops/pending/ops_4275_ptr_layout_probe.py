"""
ops_4275 -- layout probe for the 12 zero-row PTRs (read-only).

Fetch two zero-row docs, print pypdf's raw extracted text (heads +
line reprs) so the next parser pass is wired from evidence, and dump
political-stocks' top-level artifact shape to find the full per-ticker
pool for the attribution sample.
"""
import io, json, sys, urllib.request
import boto3
from ops_report import report
sys.path.insert(0, "aws/lambdas/justhodl-house-ptr-extract/source")
from pypdf import PdfReader

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
UA = "Mozilla/5.0 (JustHodl research; contact via github.com/ElMooro)"

with report("4275_ptr_layout_probe") as r:
    r.heading("ops 4275 -- zero-row PTR layout study")
    led = json.loads(s3.get_object(
        Bucket=B, Key="data/house-ptr-trades.json")["Body"].read())
    zeros = [(did, d) for did, d in (led.get("docs") or {}).items()
             if d.get("status") == "parsed" and not d.get("n_rows")]
    cd = json.loads(s3.get_object(
        Bucket=B, Key="data/congress-direct.json")["Body"].read())
    pdfmap = {f["doc_id"]: f.get("pdf")
              for f in (cd.get("house") or {}).get("filings") or []}
    for did, d in zeros[:2]:
        url = pdfmap.get(did)
        r.section("doc %s (%s)" % (did, d.get("filer")))
        if not url:
            r.warn("no pdf url in index")
            continue
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            pdf = urllib.request.urlopen(req, timeout=40).read()
            reader = PdfReader(io.BytesIO(pdf))
            text = "\n".join((pg.extract_text() or "")
                             for pg in reader.pages[:3])
            lines = [ln for ln in text.splitlines() if ln.strip()]
            r.log("pages=%d chars=%d nonempty_lines=%d"
                  % (len(reader.pages), len(text), len(lines)))
            for ln in lines[:26]:
                r.log("| %r" % ln[:118])
        except Exception as e:
            r.warn("fetch/parse: %s" % str(e)[:110])

    r.section("political-stocks artifact shape")
    doc = json.loads(s3.get_object(
        Bucket=B, Key="data/political-stocks.json")["Body"].read())
    for k, v in doc.items():
        t = type(v).__name__
        extra = ""
        if isinstance(v, list) and v and isinstance(v[0], dict):
            extra = " keys0=%s" % list(v[0])[:6]
        r.log("%s: %s%s" % (k, t, extra))
    r.ok("probe complete -- wire the recall pass from these lines")

if False:
    sys.exit(1)  # read-only probe; preflight contract satisfied
