"""justhodl-air-cargo v1.0 — high-value air-freight canary (HKIA).

Khalid's value-vs-volume insight: Korea ports flat while chip exports +52%
— high-value goods FLY. HKIA (Hong Kong Intl) is the world's #1 cargo
airport and publishes free monthly traffic figures. Probe-first design
(proven on PBoC): multiple candidate sources, tolerant parses, body_probe
forensics on miss so the next cycle lands with certainty. Self-building
levels ledger air/hkia-cargo-levels.json -> yoy once 13 months. Feeds
data/air-cargo.json. stdlib-only; never fabricates.
"""
import json
import re
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/air-cargo.json"
LEVELS_KEY = "air/hkia-cargo-levels.json"
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/126.0 Safari/537.36"),
      "Accept-Language": "en"}
S3 = boto3.client("s3", region_name="us-east-1")

CANDIDATES = [
    ("fact_figures", "https://www.hongkongairport.com/en/the-airport/"
     "hkia-at-a-glance/fact-figures.page"),
    ("press_list", "https://www.hongkongairport.com/en/media-centre/"
     "press-release.page"),
]
MONTHS = ("january february march april may june july august september "
          "october november december").split()


def _get(url, timeout=25):
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=timeout)
        return r.read(900_000).decode("utf-8", "replace"), None
    except Exception as e:
        return "", str(e)[:110]


def _parse_cargo(text):
    """Find monthly cargo tonnage + yoy near 'cargo' mentions."""
    t = re.sub(r"\s+", " ", text)
    out = {}
    # tonnage: e.g. 'cargo throughput of 410,000 tonnes' / '0.41 million tonnes'
    for m in re.finditer(r"[Cc]argo[^.]{0,160}?([\d][\d,\.]*)\s*"
                         r"(million\s+)?tonnes", t):
        v = float(m.group(1).replace(",", ""))
        if m.group(2):
            v *= 1_000_000
        if 50_000 <= v <= 900_000:  # monthly HKIA range sanity
            out["tonnes"] = v
            seg = t[max(0, m.start() - 200):m.end() + 200]
            ym = re.search(r"(" + "|".join(mo.capitalize()
                                           for mo in MONTHS) + r")\s+(20\d\d)",
                           seg)
            if ym:
                out["month"] = f"{ym.group(2)}-{MONTHS.index(ym.group(1).lower()) + 1:02d}"
            yy = re.search(r"(increase|decrease|up|down|rose|fell|grew|"
                           r"dropped)[^%]{0,60}?([\d.]+)\s*%", seg, re.I)
            if yy:
                sign = -1 if yy.group(1).lower() in (
                    "decrease", "down", "fell", "dropped") else 1
                out["yoy_pct"] = round(sign * float(yy.group(2)), 1)
            else:
                yp = re.search(r"\(\s*([+\-])\s*([\d.]+)\s*%\s*\)", seg)
                if yp:
                    out["yoy_pct"] = round(
                        float(yp.group(2)) * (-1 if yp.group(1) == "-" else 1), 1)
            break
    return out


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "airport": "HKIA (Hong Kong Intl) — world #1 cargo airport",
           "errors": [], "attribution": "Airport Authority Hong Kong "
           "published monthly traffic figures (free public stats)"}

    html, err = _get(CANDIDATES[0][1])
    via = CANDIDATES[0][0]
    parsed = _parse_cargo(html) if html else {}
    if err:
        out["errors"].append(f"{via}: {err}")

    if not parsed.get("tonnes"):
        listing, err2 = _get(CANDIDATES[1][1])
        if err2:
            out["errors"].append("press_list: " + err2)
        link = None
        if listing:
            lm = re.search(r'href="([^"]+)"[^>]*>[^<]*'
                           r'(?:Air\s*)?Traffic\s*Figures', listing, re.I)
            if lm:
                link = lm.group(1)
                if link.startswith("/"):
                    link = "https://www.hongkongairport.com" + link
        out["press_link"] = (link or "")[:140]
        if link:
            page, err3 = _get(link)
            if err3:
                out["errors"].append("press_page: " + err3)
            parsed = _parse_cargo(page) if page else {}
            via = "press_release"
            if not parsed.get("tonnes") and page:
                out["body_probe"] = re.sub(r"\s+", " ", re.sub(
                    r"<[^>]+>", " ", page))[:800]
        elif listing:
            out["list_probe"] = re.sub(r"\s+", " ", re.sub(
                r"<[^>]+>", " ", listing))[:600]
    elif html and not parsed.get("yoy_pct"):
        out["body_probe"] = re.sub(r"\s+", " ", re.sub(
            r"<[^>]+>", " ", html))[:800]

    if parsed.get("tonnes"):
        out.update(parsed)
        out["via"] = via
        out["tonnes_k"] = round(parsed["tonnes"] / 1000, 1)
        # levels ledger -> self yoy backup
        try:
            lv = json.loads(S3.get_object(
                Bucket=BUCKET, Key=LEVELS_KEY)["Body"].read())
        except Exception:
            lv = {"levels": {}}
        if out.get("month"):
            lv["levels"][out["month"]] = out["tonnes_k"]
            lv["levels"] = dict(sorted(lv["levels"].items())[-26:])
            S3.put_object(Bucket=BUCKET, Key=LEVELS_KEY,
                          Body=json.dumps(lv).encode(),
                          ContentType="application/json")
            out["levels_cached"] = len(lv["levels"])
            if out.get("yoy_pct") is None:
                y, mo = out["month"].split("-")
                prior = lv["levels"].get(f"{int(y) - 1}-{mo}")
                if prior:
                    out["yoy_pct"] = round(
                        100 * (out["tonnes_k"] / prior - 1), 1)
                    out["yoy_src"] = "ledger"
        out["read"] = ("HIGH-VALUE FLOW " +
                       ("ACCELERATING" if (out.get("yoy_pct") or 0) >= 5 else
                        "CONTRACTING" if (out.get("yoy_pct") or 0) <= -5 else
                        "STEADY"))
        out["ok"] = True

    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[air] ok={out['ok']} via={out.get('via')} "
          f"tonnes_k={out.get('tonnes_k')} month={out.get('month')} "
          f"yoy={out.get('yoy_pct')} errs={out['errors']}")
    return {"ok": out["ok"], "tonnes_k": out.get("tonnes_k"),
            "yoy_pct": out.get("yoy_pct"), "month": out.get("month")}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1200])
