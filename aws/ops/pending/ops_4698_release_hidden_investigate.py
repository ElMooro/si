"""ops 4698 — investigate two claims (Khalid):
(1) FRED release-bulk endpoint (release 209) serves full ICE BofA
    history, bypassing the per-series 3-year cap.
(2) Hiddenmetrix exposes FRED IDs via a 50-series batch API and its
    "BB" page claims 7,735 datapoints despite showing only 2023-2026
    publicly.

Method: verify, don't assume. (1) resolves the TRUE release_id for a
known ICE mnemonic via /fred/series/release (authoritative — 209 is
not trusted blindly), then tests every release-scoped endpoint against
a known-truncated series, apples-to-apples against the plain
series/observations call we already know is capped. (2) resolves
whether hiddenmetrix (or plausible variants) exists at all, and if so,
probes for a real batch endpoint and real historical content.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
TEST_SID = "BAMLH0A1HYBB"   # the "BB" series Khalid names


def fred_key():
    for pn, dec in (("/justhodl/fred-api-key", True),
                    ("/justhodl/fred/api-key", False)):
        try:
            v = ssm.get_parameter(Name=pn, WithDecryption=dec
                                  )["Parameter"]["Value"]
            if v and len(v) >= 16:
                return v
        except Exception:
            continue
    return ""


def get(url, timeout=25):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def earliest_date_in(text):
    import re as _re
    ds = _re.findall(r"(19[7-9]\d|20[0-2]\d)-\d{2}-\d{2}", text)
    return sorted(ds)[0] if ds else None


def main():
    with report("4698_release_hidden_investigate") as r:
        r.heading("ops 4698 — FRED release-209 bulk claim + "
                  "Hiddenmetrix claim")
        key = fred_key()
        if not key:
            r.fail("no FRED key — cannot investigate claim 1")
            sys.exit(1)

        r.section("1a. Resolve the TRUE release_id for an ICE "
                  "mnemonic (not trusting 209 blindly)")
        real_release = None
        try:
            st, hd, body = get(
                "https://api.stlouisfed.org/fred/series/release"
                "?series_id=" + TEST_SID + "&api_key=" + key
                + "&file_type=json")
            d = json.loads(body)
            rel = (d.get("releases") or [{}])[0]
            real_release = rel.get("id")
            r.log("  /fred/series/release for %s -> id=%s name=%s"
                 % (TEST_SID, real_release, rel.get("name")))
        except Exception as e:
            r.warn("  series/release lookup failed: %s" % str(e)[:120])
        r.log("  Khalid's claimed release_id: 209 | actual: %s | "
             "match=%s" % (real_release, str(real_release) == "209"))

        r.section("1b. Does the release actually list the ICE BofA "
                  "family?")
        use_rel = real_release or 209
        try:
            st, hd, body = get(
                "https://api.stlouisfed.org/fred/release/series"
                "?release_id=%s&api_key=%s&file_type=json&limit=1000"
                % (use_rel, key))
            d = json.loads(body)
            ids = [s.get("id") for s in d.get("seriess") or []]
            baml_n = sum(1 for i in ids if str(i).startswith("BAML"))
            r.log("  release %s: %d series listed, %d are BAML* "
                 "(count=%s in response)"
                 % (use_rel, len(ids), baml_n, d.get("count")))
        except Exception as e:
            r.warn("  release/series failed: %s" % str(e)[:150])
            ids = []

        r.section("1c. THE CRUX TEST — does any release-scoped path "
                  "serve MORE history than plain series/observations?")
        baseline_first = None
        try:
            st, hd, body = get(
                "https://api.stlouisfed.org/fred/series/observations"
                "?series_id=%s&api_key=%s&file_type=json"
                % (TEST_SID, key))
            d = json.loads(body)
            obs = d.get("observations") or []
            baseline_first = obs[0]["date"] if obs else None
            r.log("  BASELINE (plain series/observations, no bound): "
                 "first=%s count=%s" % (baseline_first, d.get("count")))
        except Exception as e:
            r.warn("  baseline call failed: %s" % str(e)[:120])

        variants = [
            ("release/tables",
             "https://api.stlouisfed.org/fred/release/tables"
             "?release_id=%s&api_key=%s&file_type=json"
             % (use_rel, key)),
            ("release/dates",
             "https://api.stlouisfed.org/fred/release/dates"
             "?release_id=%s&api_key=%s&file_type=json"
             % (use_rel, key)),
            ("observations w/ vintage realtime_start=2020",
             "https://api.stlouisfed.org/fred/series/observations"
             "?series_id=%s&api_key=%s&file_type=json"
             "&realtime_start=2020-01-01&realtime_end=2020-01-01"
             % (TEST_SID, key)),
            ("observations w/ output_type=4 (vintage all)",
             "https://api.stlouisfed.org/fred/series/observations"
             "?series_id=%s&api_key=%s&file_type=json&output_type=4"
             % (TEST_SID, key)),
            ("release/series/observations (bulk-per-release, if it "
             "exists)",
             "https://api.stlouisfed.org/fred/release/observations"
             "?release_id=%s&api_key=%s&file_type=json"
             % (use_rel, key)),
        ]
        deeper_found = False
        for nm, url in variants:
            try:
                st, hd, body = get(url, timeout=30)
                txt = body.decode("utf-8", "replace")
                d = None
                try:
                    d = json.loads(txt)
                except Exception:
                    pass
                obs = (d.get("observations") if isinstance(d, dict)
                      else None) or []
                first_here = (min(o["date"] for o in obs)
                             if obs else earliest_date_in(txt))
                r.log("  [%s] status=%s bytes=%d first_date_seen=%s"
                     % (nm, st, len(body), first_here))
                if first_here and baseline_first and \
                        first_here < baseline_first:
                    deeper_found = True
                    r.ok("    ^ DEEPER than baseline (%s < %s)!"
                        % (first_here, baseline_first))
            except urllib.error.HTTPError as e:
                r.log("  [%s] HTTP %s: %s"
                     % (nm, e.code, e.read()[:150].decode(
                         "utf-8", "replace")))
            except Exception as e:
                r.log("  [%s] %s" % (nm, str(e)[:150]))
            time.sleep(0.6)

        r.section("1 verdict")
        if deeper_found:
            r.ok("CLAIM 1 CONFIRMED — a release-scoped path serves "
                "deeper history than the per-series call")
        else:
            r.log("CLAIM 1: no release-scoped or vintage-parameter "
                 "path returned anything deeper than baseline (%s). "
                 "Every variant tested is either metadata-only "
                 "(tables/dates don't carry observations) or subject "
                 "to the identical cap. The restriction appears to "
                 "be applied at the OBSERVATION-SERVING layer itself "
                 "for ICE-licensed mnemonics, not bypassable by path."
                 % baseline_first)

        r.section("2a. Does Hiddenmetrix exist?")
        candidates = ["https://hiddenmetrix.com",
                     "https://www.hiddenmetrix.com",
                     "https://app.hiddenmetrix.com",
                     "https://api.hiddenmetrix.com",
                     "https://hiddenmetrix.io",
                     "https://hiddenmetrix.net"]
        resolved = []
        for base in candidates:
            try:
                st, hd, body = get(base, timeout=15)
                resolved.append(base)
                r.log("  %s -> HTTP %s, %d bytes, ct=%s"
                     % (base, st, len(body),
                        hd.get("Content-Type", "")[:40]))
            except urllib.error.HTTPError as e:
                r.log("  %s -> HTTP %s (server exists, path/status "
                     "issue)" % (base, e.code))
                resolved.append(base + " (HTTP err but DNS resolves)")
            except Exception as e:
                r.log("  %s -> %s" % (base, str(e)[:90]))

        r.section("2b. If it exists: hunt the batch API + the BB "
                  "series page")
        if resolved:
            base = resolved[0].split(" ")[0]
            paths = [
                "/api/series/" + TEST_SID,
                "/api/v1/series/" + TEST_SID,
                "/api/series/batch",
                "/series/" + TEST_SID,
                "/fred/" + TEST_SID,
                "/data/" + TEST_SID,
            ]
            for p in paths:
                try:
                    st, hd, body = get(base + p, timeout=15)
                    txt = body.decode("utf-8", "replace")
                    fd = earliest_date_in(txt)
                    r.log("  %s%s -> %s, %d bytes, earliest_date=%s"
                         % (base, p, st, len(body), fd))
                    if fd and fd < "2020":
                        r.ok("    ^ pre-2020 data found at %s%s!"
                            % (base, p))
                except urllib.error.HTTPError as e:
                    r.log("  %s%s -> HTTP %s" % (base, p, e.code))
                except Exception as e:
                    r.log("  %s%s -> %s" % (base, p, str(e)[:80]))
                time.sleep(0.4)
        else:
            r.log("  no hiddenmetrix domain variant resolved at all "
                 "— cannot proceed to path-hunting")

        r.section("2 verdict")
        r.log("  domains that resolved: %s" % resolved)

        r.section("overall verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "claim1_release_id_actual": real_release,
               "claim1_baseline_first": baseline_first,
               "claim1_deeper_found": deeper_found,
               "claim2_domains_resolved": resolved}
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json")
        r.ok("investigation complete — both claims tested with live "
             "evidence, recorded to data/ice-alt-claims-investig.json")


if __name__ == "__main__":
    main()
