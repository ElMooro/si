"""ops 4612 — port-cargo root-cause repair: self-healing layer resolve.

Khalid: fix the FAILED PortWatch fetch and check for bugs. Root cause
class: one hardcoded ArcGIS service name + a hardcoded date field in
five call sites — a single upstream rename kills every probe. v1.2.0
resolves the layer at runtime (cached choice -> candidate names ->
live org directory scan), detects the date field from the schema, and
publishes the resolver path in the payload.

This op probes the org RAW from the runner first (ground truth on what
IMF actually serves now), then settles v1.2.0, invokes, contracts a
healed fetch (fetch_status OK, fresh data date, real port count), and
confirms the physical-economy signal picks up its fifth leg.
"""
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-port-cargo"
PFN = "justhodl-physical-econ"
B = "justhodl-dashboard-live"
BASE = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/"
        "rest/services")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4612"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def probe(r, tag, url):
    try:
        b = http_get(url, 40)
        r.log("%s → 200, %d bytes, head: %s"
              % (tag, len(b), b[:170].decode("utf-8", "replace")
                 .replace("\n", " ")))
        return b
    except Exception as e:
        r.log("%s → %s" % (tag, str(e)[:120]))
        return None


def main():
    misses = 0
    with report("4612_portcargo_heal") as r:
        r.heading("ops 4612 — port-cargo self-healing layer resolve")

        r.section("raw upstream probes (ground truth)")
        probe(r, "old layer",
              BASE + "/Daily_Ports_Data/FeatureServer/0/query?"
              + urllib.parse.urlencode(
                  {"where": "1=1", "resultRecordCount": 1,
                   "outFields": "*", "f": "pjson"}))
        b = probe(r, "org directory", BASE + "?f=pjson")
        if b:
            try:
                names = [(x.get("name") or "").split("/")[-1]
                         for x in json.loads(b).get("services", [])]
                ports = [n for n in names
                         if "port" in n.lower()][:12]
                r.log("services containing 'port': %s"
                      % json.dumps(ports))
            except Exception as e:
                r.log("directory parse: %s" % str(e)[:80])

        r.section("deploy-settle v1.2.0")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "resolve_layer" in src and '"1.2.0"' in src:
                    settled = True
                    r.log("v1.2.0 live (attempt %d)" % (att + 1))
                    break
            except Exception as e:
                r.log("attempt %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "port-cargo v1.2.0")
        if not settled:
            sys.exit(1)
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg["Timeout"] < 600 or cfg["MemorySize"] < 1024:
            lam.update_function_configuration(
                FunctionName=FN, Timeout=max(cfg["Timeout"], 600),
                MemorySize=max(cfg["MemorySize"], 1024))
            for _ in range(20):
                stc = lam.get_function_configuration(FunctionName=FN)
                if stc.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(5)
            r.log("config floored to >=600s/1024MB for the "
                  "full-window pull")

        r.section("invoke + healed-fetch contracts")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.log("invoke status=%s" % inv.get("StatusCode"))
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/port-cargo.json")["Body"].read())
        lay = pl.get("layer") or {}
        r.kv(fetch_status=pl.get("fetch_status"),
             resolver=lay.get("service_path"),
             datefield=lay.get("datefield"),
             latest_data_date=pl.get("latest_data_date"),
             data_age_days=pl.get("data_age_days"),
             n_ports=pl.get("n_ports_with_data"),
             n_rows=pl.get("n_rows_window"),
             gaps=json.dumps(pl.get("gaps")
                             or pl.get("gap_notes") or [])[:400])
        misses += contract(r, "fetch", pl.get("fetch_status") == "OK",
                           "fetch_status OK (resolver=%s)"
                           % lay.get("service_path"))
        misses += contract(r, "fresh",
                           pl.get("latest_data_date") is not None
                           and isinstance(pl.get("data_age_days"),
                                          (int, float))
                           and pl.get("data_age_days") <= 10,
                           "data date %s, age %s d (PortWatch lag "
                           "~4-6d)" % (pl.get("latest_data_date"),
                                       pl.get("data_age_days")))
        misses += contract(r, "breadth",
                           (pl.get("n_ports_with_data") or 0) >= 300,
                           "%s ports carrying data"
                           % pl.get("n_ports_with_data"))

        r.section("fifth leg: physical-economy join")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        names = [x.get("name", "")
                 for x in pe.get("components") or []]
        has_port = any("Port" in n for n in names)
        misses += contract(r, "fifth-leg",
                           has_port and len(names) >= 5,
                           "physical signal now %d legs: %s"
                           % (len(names), json.dumps(names)[:240]))
        sig = pe.get("trade_signal") or {}
        r.kv(physical=json.dumps(
            {"composite": pe.get("composite_score"),
             "signal": sig.get("signal"),
             "confidence": sig.get("confidence")}))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/port-cargo.json?cb=%d"
                    % time.time()))
                if jd.get("fetch_status") == "OK":
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge port-cargo.json shows OK")

        r.section("verdict")
        if misses:
            r.fail("port-cargo heal: %d red (raw probes above hold "
                   "the ground truth for the next patch)" % misses)
            sys.exit(1)
        r.ok("PORT-CARGO HEALED — resolver %s, data %s (age %sd), "
             "%s ports; physical signal at %d legs (%s, %s)"
             % (lay.get("service_path"), pl.get("latest_data_date"),
                pl.get("data_age_days"), pl.get("n_ports_with_data"),
                len(names), sig.get("signal"), sig.get("confidence")))


if __name__ == "__main__":
    main()
