# Renderer-only baseline from 2360dc62e8ff983e4ed42795bb3fe94a0ba22200; no client imports or recipient data.
def render_email_html(out):
    rows = ""
    for h in out["hot_stocks"]:
        b = h.get("analyst") or {}
        net = (b.get("net") or "").upper()
        col = "#2e7d32" if "early" in net.lower() or "long" in net.lower() else "#b71c1c" if "fade" in net.lower() or "short" in net.lower() else "#555"
        rows += ("<tr><td style='padding:8px 6px;border-bottom:1px solid #eee'><b>%s</b><br>"
                 "<span style='color:#888;font-size:11px'>heat %.0f · %d venues · bull %s%%</span></td>"
                 "<td style='padding:8px 6px;border-bottom:1px solid #eee;font-size:12px'>%s"
                 "<br><span style='color:#2e7d32'>+ %s</span>"
                 "<br><span style='color:#b71c1c'>− %s</span>"
                 "<br><b style='color:%s'>NET: %s</b></td></tr>") % (
            h["ticker"], h["score"], h.get("venue_count", 0), h.get("bull_pct"),
            b.get("why_hot", h.get("why", [""])[0] if h.get("why") else ""), b.get("bull", "—"),
            b.get("bear", "—"), col, b.get("net", "—"))
    warn = ", ".join(w["ticker"] for w in out.get("warnings", [])[:8]) or "none"
    return ("""<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;margin:auto">
      <h2 style="margin:0">JustHodl · Retail Flow &amp; Sentiment Brief</h2>
      <div style="color:#888;font-size:12px">%s</div>
      <p style="font-size:13px;line-height:1.5"><b>Market read:</b> %s</p>
      <table style="width:100%%;border-collapse:collapse;font-size:13px">%s</table>
      <p style="font-size:12px;color:#b71c1c"><b>Crowded / fading (handle with care):</b> %s</p>
      <p style="font-size:11px;color:#aaa">Sources: Reddit/ApeWisdom + StockTwits + buzz + news-wire/GDELT. No X/Twitter (paid feed). Research, not investment advice.</p>
    </div>""") % (out["generated_at"][:16].replace("T", " ") + "Z", out.get("market_read", ""), rows, warn)
