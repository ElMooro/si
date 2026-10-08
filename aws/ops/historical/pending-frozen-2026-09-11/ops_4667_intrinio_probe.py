"""ops 4667 — Intrinio ICE probe: does the trial expose pre-2023?

One question only: ask Intrinio for BAMLH0A0HYM2 from 1996-01-01 and
report the FIRST date returned. If it is 1996/1997, Intrinio holds its
own historical store and the whole 192-series family is recoverable in
one paced pass. If it is 2023-08-xx, their feed mirrors FRED's rolling
window and the lane is dead — no importer gets built.

Also stores the key as an SSM SecureString (engines read it from there,
never from the repo) and probes the auth/endpoint shape so the follow-up
importer is written against reality, not documentation.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

KEY = "OmJjYjgwYTc3MjNlZjViYmM5OTJlYmE0NzlmZTgxZDlm"
PARAM = "/justhodl/intrinio-api-key"
SID = "BAMLH0A0HYM2"


def get(url, to=30):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(rq, timeout=to) as f:
        return f.read().decode("utf-8", "replace")


def main():
    with report("4667_intrinio_probe") as r:
        r.heading("ops 4667 — Intrinio: pre-2023 ICE or not")

        r.section("1. Store key in SSM (SecureString)")
        try:
            ssm = boto3.client("ssm", region_name="us-east-1")
            ssm.put_parameter(Name=PARAM, Value=KEY, Type="SecureString",
                              Overwrite=True)
            back = ssm.get_parameter(Name=PARAM, WithDecryption=True
                                     )["Parameter"]["Value"]
            r.ok("  stored + read back OK (len=%d, value never logged)"
                 % len(back))
        except Exception as e:
            r.warn("  SSM store: %s" % str(e)[:110])

        r.section("2. THE PROBE — first date from 1996 request")
        variants = [
            ("economic index historical",
             "https://api-v2.intrinio.com/indices/economic/%s/"
             "historical_data/level?start_date=1996-01-01"
             "&page_size=100&api_key=%s" % (SID, KEY)),
            ("economic index (no metric)",
             "https://api-v2.intrinio.com/indices/economic/%s"
             "?api_key=%s" % (SID, KEY)),
            ("all economic indices search",
             "https://api-v2.intrinio.com/indices/economic/search"
             "?query=%s&api_key=%s" % (SID, KEY)),
        ]
        verdict = None
        for nm, url in variants:
            try:
                raw = get(url)
                d = json.loads(raw)
                hd = (d.get("historical_data")
                      or d.get("indices") or [])
                if isinstance(hd, list) and hd and \
                        isinstance(hd[0], dict) and "date" in hd[0]:
                    dates = [x.get("date") for x in hd
                             if x.get("date")]
                    first, last = min(dates), max(dates)
                    r.log("  [%s] %d rows · first=%s last=%s · "
                          "next_page=%s"
                          % (nm, len(hd), first, last,
                             bool(d.get("next_page"))))
                    verdict = first
                else:
                    r.log("  [%s] shape=%s sample=%s"
                          % (nm, list(d)[:6], raw[:200]))
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", "replace")[:200]
                except Exception:
                    pass
                r.log("  [%s] HTTP %s — %s" % (nm, e.code, body))
            except Exception as e:
                r.log("  [%s] %s" % (nm, str(e)[:120]))
            time.sleep(1.5)

        r.section("verdict")
        if verdict is None:
            r.fail("  no dated payload returned — endpoint shape or "
                   "entitlement unclear; raw bodies above are the "
                   "evidence for a corrected retry. Failing loud so "
                   "this cannot read as a clean answer.")
            sys.exit(1)
        if verdict < "2020":
            r.ok("GO — Intrinio serves history from %s: their own "
                 "store, not FRED's rolling window. All 192 ICE "
                 "series are recoverable; importer worth building."
                 % verdict)
        else:
            r.ok("NO-GO — Intrinio first date %s mirrors FRED's "
                 "truncation. Lane dead; do not build the importer."
                 % verdict)


if __name__ == "__main__":
    main()
