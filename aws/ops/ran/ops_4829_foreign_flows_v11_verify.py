"""ops/4829 -- foreign-flows v1.1.0 verify: official/private splits
+ country decomposition (legend proven by ops 4827-4828).
 G0  existence probe of the five 99991 (private) ids on FRED --
     FORLTTOTALNET99991 hard (core signal), the rest warn-only (the
     engine excludes unproven families honestly).
 (1) marker settle 'foreign-flows v1.1.0'; Event-invoke; poll <=5m.
 (2) truths: LIVE v1.1.0; LIVE IDENTITY -- op refetches the lt_total
     all/official/private trio, asserts |all-(off+prv)| <= 0.2B AND
     signal.latest == round(prv-off,1) AND matches the doc's own
     recon_gap; lt_total/lt_treas/st_treas families OK (hard),
     others named-warn; st_treas official latest == refetch (the
     -61B May T-bill dump); five countries OK with china holdings ==
     independent refetch and identity_gap == d12-tx-val; new banks
     exist deep; core six untouched.
 (3) readout: official-vs-private per family + country table + the
     divergence headline.
"""
import gzip
import io
import json
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-foreign-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/foreign-flows.json"
MARKER = "foreign-flows v1.1.0"
PRIVATE_IDS = ("FORLTTOTALNET99991", "FORLTTREASNET99991",
               "FORLTEQTYNET99991", "FORLTCORPNET99991",
               "FORLTAGCYNET99991", "FORSTTREASNET99991")
HARD_FAMS = ("lt_total", "lt_treas", "st_treas")
BANK_SPOT = ("FORLTTOTALNET99990", "FORLTTOTALNET99991",
             "FORLTTREASPOS41408", "FORLTTREASVALCHG41408")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def donor_key():
    for d in ("dollar-strength-agent", "justhodl-risk-gate"):
        try:
            env = (lam.get_function_configuration(FunctionName=d)
                   .get("Environment") or {}).get("Variables", {})
            if env.get("FRED_KEY"):
                return env["FRED_KEY"]
        except ClientError:
            continue
    return None


def fred(path, key, **params):
    params.update({"api_key": key, "file_type": "json"})
    url = ("https://api.stlouisfed.org/fred/%s?%s"
           % (path, urllib.parse.urlencode(params)))
    with urllib.request.urlopen(
            urllib.request.Request(url), timeout=60) as r:
        return json.loads(r.read())


def latest_bn(key, sid):
    o = fred("series/observations", key, series_id=sid,
             sort_order="desc", limit=3)
    for row in o.get("observations") or []:
        try:
            return row["date"], float(row["value"]) / 1000.0
        except (KeyError, ValueError):
            continue
    return None, None


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("marker settled (attempt %d)" % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def main():
    with report("ops 4829 -- foreign-flows v1.1.0 splits "
                "verify") as rep:
        rep.heading("G0. private (99991) id existence")
        key = donor_key()
        if not key:
            rep.fail("no donor FRED_KEY")
            sys.exit(1)
        for sid in PRIVATE_IDS:
            try:
                ti = (fred("series", key, series_id=sid)
                      .get("seriess") or [{}])[0].get("title") or ""
                rep.ok("  %-22s EXISTS '%s'" % (sid, ti[:56]))
            except Exception:  # noqa: BLE001
                if sid == "FORLTTOTALNET99991":
                    rep.fail("  %-22s ABSENT (core signal)" % sid)
                    FAILED.append("g0_" + sid)
                else:
                    rep.warn("  %-22s absent -- family will be "
                             "honestly excluded" % sid)
        if FAILED:
            rep.fail("G0 broken")
            sys.exit(1)

        rep.heading("1. settle + invoke + poll")
        if not settle(rep):
            sys.exit(1)
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 300:
            time.sleep(12)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc within 5 min")
            sys.exit(1)
        rep.ok("fresh doc in %ds runtime_ms=%s"
               % (int(time.time() - t0),
                  (doc.get("diag") or {}).get("runtime_ms")))

        rep.heading("2. truths")
        if doc.get("v") == "1.1.0" and doc.get("status") == "LIVE":
            rep.ok("  LIVE v1.1.0")
        else:
            rep.fail("  v=%s status=%s" % (doc.get("v"),
                                           doc.get("status")))
            FAILED.append("live")
        hs = doc.get("holder_splits") or {}
        sig = (doc.get("signals") or {}).get("official_private") \
            or {}
        da, va = latest_bn(key, "FORLTTOTALNET99996")
        do_, vo = latest_bn(key, "FORLTTOTALNET99990")
        dp, vp = latest_bn(key, "FORLTTOTALNET99991")
        if None not in (va, vo, vp) and da == do_ == dp:
            gap = abs(va - (vo + vp))
            exp = round(vp - vo, 1)
            if gap <= 0.2 and sig.get("latest_bn") == exp:
                rep.ok("  LIVE IDENTITY: all=off+prv (gap %.3fB); "
                       "signal %+0.1fB == prv-off @ %s"
                       % (gap, exp, da))
            else:
                rep.fail("  identity broken: gap=%.3f sig=%s "
                         "exp=%s" % (gap, sig.get("latest_bn"),
                                     exp))
                FAILED.append("identity")
            dg = ((hs.get("lt_total") or {})
                  .get("recon_gap_bn"))
            if dg is not None and abs(dg - gap) <= 0.05:
                rep.ok("  doc recon_gap consistent (%.3f)" % dg)
            else:
                rep.warn("  doc recon_gap %s vs live %.3f" % (dg,
                                                              gap))
        else:
            rep.fail("  refetch trio incomplete/misaligned")
            FAILED.append("refetch")
        for fam in ("lt_total", "lt_treas", "lt_equity", "lt_corp",
                    "lt_agency", "st_treas"):
            st = (hs.get(fam) or {}).get("status")
            if st == "OK":
                f = hs[fam]
                rep.ok("  %-9s OK  off %+8.1f (12m %+9.1f)  "
                       "prv %+8.1f (12m %+9.1f)"
                       % (fam, f["official"]["latest"],
                          f["official"]["sum_12m"],
                          f["private"]["latest"],
                          f["private"]["sum_12m"]))
            elif fam in HARD_FAMS:
                rep.fail("  %-9s %s: %s" % (fam, st,
                                            (hs.get(fam)
                                             or {}).get("why")))
                FAILED.append("fam_" + fam)
            else:
                rep.warn("  %-9s %s: %s" % (fam, st,
                                            (hs.get(fam)
                                             or {}).get("why")))
        _, v_off_st = latest_bn(key, "FORSTTREASNET99990")
        got = ((hs.get("st_treas") or {}).get("official")
               or {}).get("latest")
        if v_off_st is not None and got == round(v_off_st, 1):
            rep.ok("  st_treas official == refetch (%+.1fB -- the "
                   "official T-bill flow line)" % got)
        else:
            rep.fail("  st_treas official diverges: %s vs %s"
                     % (got, v_off_st))
            FAILED.append("st_off")
        cc = doc.get("country_lt_treasury") or {}
        bad = [c for c in ("china", "japan", "united_kingdom",
                           "belgium", "cayman")
               if (cc.get(c) or {}).get("status") != "OK"]
        if not bad:
            rep.ok("  five countries OK")
        else:
            rep.fail("  countries broken: %s" % bad)
            FAILED.append("countries")
        _, v_cn = latest_bn(key, "FORLTTREASPOS41408")
        cn = cc.get("china") or {}
        if v_cn is not None and cn.get("holdings_bn") \
                == round(v_cn, 1):
            rep.ok("  china holdings == refetch (%.1fB)" % v_cn)
        else:
            rep.fail("  china holdings diverge: %s vs %s"
                     % (cn.get("holdings_bn"), v_cn))
            FAILED.append("cn_hold")
        if cn.get("identity_gap_bn") is not None and abs(
                cn["identity_gap_bn"]
                - round(cn["d12m_holdings_bn"] - cn["tx_12m_bn"]
                        - cn["valchg_12m_bn"], 1)) < 0.05:
            rep.ok("  china decomposition identity (gap %+0.1fB = "
                   "'other adjustments')" % cn["identity_gap_bn"])
        else:
            rep.fail("  china decomposition inconsistent: %s"
                     % json.dumps(cn)[:120])
            FAILED.append("cn_ident")
        for sid in BANK_SPOT:
            try:
                bank = sread("data/providers/tic-cslt/%s.json"
                             % sid)
                n = len(bank.get("rows") or {})
                if n >= 200:
                    rep.ok("  bank %-24s n=%d" % (sid, n))
                else:
                    rep.fail("  bank %s thin n=%d" % (sid, n))
                    FAILED.append("bank_" + sid)
            except ClientError:
                rep.fail("  bank %s MISSING" % sid)
                FAILED.append("bank_" + sid)
        if len(doc.get("flows_bn") or {}) == 6:
            rep.ok("  core six untouched")
        else:
            rep.fail("  core series count changed")
            FAILED.append("core")

        rep.heading("3. country readout")
        for c in ("china", "japan", "united_kingdom", "belgium",
                  "cayman"):
            r = cc.get(c) or {}
            rep.log("  %-15s hold %8.1fB  d12 %+7.1f  tx12 %+7.1f"
                    "  val12 %+7.1f  other %+6.1f"
                    % (c, r.get("holdings_bn", 0),
                       r.get("d12m_holdings_bn", 0),
                       r.get("tx_12m_bn", 0),
                       r.get("valchg_12m_bn", 0),
                       r.get("identity_gap_bn", 0)))
        rep.log("  SIGNAL official_private %+0.1fB (12m %+0.1fB, "
                "z=%s)" % (sig.get("latest_bn", 0),
                           sig.get("sum_12m_bn", 0),
                           sig.get("z_10y")))

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("foreign-flows v1.1.0 LIVE -- the official/private "
               "divergence and five-country decomposition are now "
               "fleet facts, identity-proven end to end")


if __name__ == "__main__":
    main()
