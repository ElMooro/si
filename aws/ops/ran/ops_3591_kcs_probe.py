"""ops 3591 — KCS Korea 20-day flash source probe (data.go.kr key path dead:
US phone rejected → go straight to the PUBLIC release). Targets: tradedata.go.kr
portal + www.customs.go.kr press boards (KR + EN). Harvest links mentioning the
10-day provisional release; record body heads. If all KR hosts block the Azure
runner → the CF-worker edge-fetch route is the build path. Findings-only."""
import json, re, ssl, sys, time, urllib.parse, urllib.request
from pathlib import Path
from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
      "Accept-Language": "ko,en;q=0.8"}
CTX = ssl.create_default_context(); CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE

def fetch(url, timeout=14, limit=400_000):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout, context=CTX)
        return r.status, r.read(limit)
    except Exception as e:
        return None, str(e)[:140].encode()

def links(html, base):
    out = []
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.I | re.S):
        t = re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()
        out.append((urllib.parse.urljoin(base, m.group(1)), t[:80]))
    return out

with report("3591_kcs_probe") as rep:
    rep.heading("ops 3591 — KCS 20-day flash probe (public release path)")
    out = {"probes": {}}

    def note(name, payload):
        out["probes"][name] = payload
        line = f"{name}: {json.dumps(payload, ensure_ascii=False, default=str)[:440]}"
        print(line); rep.log(line)

    targets = [
        ("P1_tradedata_root", "https://tradedata.go.kr/"),
        ("P2_customs_root", "https://www.customs.go.kr/"),
        ("P3_customs_en", "https://www.customs.go.kr/english/main.do"),
        ("P4_kcs_press_kr", "https://www.customs.go.kr/kcs/na/ntt/selectNttList.do?mi=2889&bbsId=1362"),
        ("P5_kcs_press_en", "https://www.customs.go.kr/english/na/ntt/selectNttList.do?mi=8709&bbsId=1471"),
    ]
    for tag, url in targets:
        st, raw = fetch(url)
        body = raw.decode("utf-8", "replace") if st == 200 else ""
        rel = [(u, t) for u, t in links(body, url)
               if any(k in t for k in ("수출입 현황", "수출입현황", "10일", "잠정", "Export", "Trade Statistics"))][:8] if body else []
        note(tag, {"status": st, "len": (len(raw) if st == 200 else None),
                   "flash_str": any(k in body for k in ("10일", "잠정치", "1~20", "provisional")),
                   "links": rel[:6],
                   "err": (None if st == 200 else raw.decode()[:120])})

    # if the KR press board is reachable, drill the newest export item
    board = out["probes"].get("P4_kcs_press_kr") or {}
    if board.get("status") == 200 and board.get("links"):
        item = next(((u, t) for u, t in board["links"] if "수출입" in t or "10일" in t), board["links"][0])
        time.sleep(0.4)
        st, raw = fetch(item[0])
        body = raw.decode("utf-8", "replace") if st == 200 else ""
        nums = re.findall(r"([0-9][\d,\.]*\s*억\s*달러|[+-]?\d+\.\d+\s*%|\$\s?[\d,\.]+\s*billion)", body)[:10]
        atts = [(u, t) for u, t in links(body, item[0]) if re.search(r"\.(hwpx?|pdf|xlsx?)", u, re.I)][:4]
        note("P6_item_drill", {"item": item[1], "status": st, "numbers_found": nums,
                              "attachments": atts})
    else:
        note("P6_item_drill", {"skipped": "press board not reachable/linked"})

    out["verdict"] = "PROBE_COMPLETE"
    print("\nVERDICT: PROBE_COMPLETE"); rep.log("VERDICT: PROBE_COMPLETE")
    Path("aws/ops/reports/3591.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    sys.exit(0)
