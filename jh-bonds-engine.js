/* jh-bonds-engine.js — global FI board: yields + total return D/W/M/3M. ICE BofA via FRED. */
(function (w) {
  "use strict";
  if (w.JHBonds) return;

  var CATS = [
    { id: "ust", title: "US Treasuries", kind: "yield",
      rows: [
        { id: "DGS3MO", label: "US 3M", etf: "BIL" },
        { id: "DGS2", label: "US 2Y", etf: "SHY" },
        { id: "DGS5", label: "US 5Y", etf: "IEI" },
        { id: "DGS10", label: "US 10Y", etf: "IEF" },
        { id: "DGS30", label: "US 30Y", etf: "TLT" },
        { id: "DFII10", label: "US 10Y real", etf: "TIP" }
      ] },
    { id: "jgb", title: "JGBs", kind: "yield",
      rows: [
        { id: "IRLTLT01JPM156N", label: "Japan 10Y (FRED, monthly)", etf: null },
        { wr: "japan", label: "JGB complex (war-room)" }
      ] },
    { id: "eu", title: "European government", kind: "yield",
      rows: [
        { id: "IRLTLT01DEM156N", label: "Germany 10Y" },
        { id: "IRLTLT01FRM156N", label: "France 10Y" },
        { id: "IRLTLT01ITM156N", label: "Italy 10Y" },
        { id: "IRLTLT01GBM156N", label: "UK 10Y" },
        { id: "IRLTLT01ESM156N", label: "Spain 10Y" }
      ] },
    { id: "ig", title: "US corporates (IG)", kind: "oas",
      rows: [
        { id: "BAMLC0A0CM", label: "ICE BofA US Corp OAS", tr: "BAMLCC0A0CMTRIV", etf: "LQD" },
        { id: "BAMLC0A2CAA", label: "ICE BofA AA OAS", tr: "BAMLCC0A2AATRIV" }
      ] },
    { id: "aaa", title: "AAA", kind: "oas",
      rows: [{ id: "BAMLC0A1CAAA", label: "ICE BofA AAA OAS", tr: "BAMLCC0A1AAATRIV" }] },
    { id: "bbb", title: "BBB", kind: "oas",
      rows: [{ id: "BAMLC0A4CBBB", label: "ICE BofA BBB OAS", tr: "BAMLCC0A4BBBTRIV", etf: "LQD" }] },
    { id: "semi", title: "Semi-worst · fallen angels / BB", kind: "oas",
      rows: [
        { id: "BAMLH0A1HYBB", label: "ICE BofA BB HY OAS", tr: "BAMLHYH0A1HYBBTRIV", etf: "FALN" },
        { etf: "ANGL", label: "Fallen Angels ETF" },
        { etf: "FALN", label: "US Fallen Angels ETF" }
      ] },
    { id: "hy", title: "High yield", kind: "oas",
      rows: [
        { id: "BAMLH0A0HYM2", label: "ICE BofA HY OAS", tr: "BAMLHYH0A0HYM2TRIV", etf: "HYG" },
        { id: "BAMLH0A2HYB", label: "ICE BofA B HY OAS", tr: "BAMLHYH0A2HYBTRIV", etf: "JNK" },
        { etf: "USHY", label: "Broad HY ETF" }
      ] },
    { id: "ccc", title: "CCC & lower", kind: "oas",
      rows: [{ id: "BAMLH0A3HYC", label: "ICE BofA CCC & lower OAS", tr: "BAMLHYH0A3HYCTRIV" }] },
    { id: "em", title: "Emerging market sovereign", kind: "oas",
      rows: [
        { id: "BAMLEMPBPUBSICRPIOAS", label: "ICE BofA EM Sovereign OAS", etf: "EMB" },
        { etf: "VWOB", label: "EM Govt Bond ETF" }
      ] },
    { id: "emc", title: "Emerging corporates", kind: "oas",
      rows: [
        { id: "BAMLEMCBPIOAS", label: "ICE BofA EM Corp OAS", etf: "CEMB" },
        { id: "BAMLEMIBHGCRPIOAS", label: "ICE BofA EM IG OAS" },
        { id: "BAMLEMHBHYCRPIOAS", label: "ICE BofA EM HY OAS", etf: "EMHY" }
      ] },
    { id: "front", title: "Frontier market", kind: "proxy",
      rows: [
        { etf: "FM", label: "MSCI Frontier 100 (equity proxy)" },
        { etf: "FEMS", label: "Frontier Small-Cap (equity proxy)" }
      ] }
  ];

  function seriesChg(bars, n) {
    if (!bars || bars.length <= n) return null;
    var a = bars[bars.length - 1 - n], b = bars[bars.length - 1];
    if (!a || !b || a.value == null || b.value == null) return null;
    return b.value - a.value;
  }
  function seriesRet(bars, n) {
    if (!bars || bars.length <= n) return null;
    var a = bars[bars.length - 1 - n], b = bars[bars.length - 1];
    if (!a || !b || !a.value) return null;
    return ((b.value - a.value) / a.value) * 100;
  }
  function last(bars) {
    if (!bars || !bars.length) return null;
    var b = bars[bars.length - 1];
    return b && b.value != null ? b.value : null;
  }
  function asof(bars) {
    if (!bars || !bars.length) return "";
    return bars[bars.length - 1].date || "";
  }

  async function run() {
    var D = w.JHDesk;
    var series = {};
    var etfs = {};
    var needFred = [];
    var needEtf = [];
    CATS.forEach(function (c) {
      c.rows.forEach(function (r) {
        if (r.id) needFred.push(r.id);
        if (r.tr) needFred.push(r.tr);
        if (r.etf) needEtf.push(r.etf);
      });
    });
    needEtf.push("TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB");

    var wr = await D.feed("data/bond-warroom.json");
    var qx = await D.quotes(needEtf);
    await D.pool(needFred.filter(function (s, i, a) { return a.indexOf(s) === i; }), 6, async function (sid) {
      try { series[sid] = await D.fred(sid, 280); } catch (e) { series[sid] = []; }
    });
    await D.pool(needEtf.filter(function (s, i, a) { return a.indexOf(s) === i; }), 6, async function (t) {
      try { etfs[t] = await D.ohlc(t, "6mo"); } catch (e) { etfs[t] = []; }
    });

    function wrRow(panel, hint) {
      var rows = (wr && wr.panels && wr.panels[panel]) || [];
      if (!hint) return rows[0] || null;
      hint = hint.toLowerCase();
      return rows.filter(function (r) { return String(r.label || "").toLowerCase().indexOf(hint) >= 0; })[0] || rows[0] || null;
    }

    var cats = CATS.map(function (c) {
      var rows = c.rows.map(function (r) {
        var yBars = r.id ? series[r.id] : [];
        var trBars = r.tr ? series[r.tr] : [];
        var eBars = r.etf ? etfs[r.etf] : [];
        var q = r.etf ? (qx[r.etf] || {}) : {};
        var level = last(yBars);
        var unit = c.kind === "oas" ? "bp" : c.kind === "yield" ? "%" : "px";
        var yD = seriesChg(yBars, 1), yW = seriesChg(yBars, 5), yM = seriesChg(yBars, 21), yQ = seriesChg(yBars, 63);
        if (c.kind === "oas") { yD = yD == null ? null : yD * 100; yW = yW == null ? null : yW * 100; yM = yM == null ? null : yM * 100; yQ = yQ == null ? null : yQ * 100; }
        else if (c.kind === "yield") { yD = yD == null ? null : yD * 100; yW = yW == null ? null : yW * 100; yM = yM == null ? null : yM * 100; yQ = yQ == null ? null : yQ * 100; }
        var retH = D.horizons(D.closesOf(eBars));
        if (q.changePct != null) retH.d = D.round(q.changePct, 2);
        if ((!eBars || eBars.length < 10) && trBars && trBars.length) {
          retH = { d: D.round(seriesRet(trBars, 1), 2), w: D.round(seriesRet(trBars, 5), 2), m: D.round(seriesRet(trBars, 21), 2), q: D.round(seriesRet(trBars, 63), 2) };
        }
        if (r.wr) {
          var wrow = wrRow(r.wr);
          if (wrow) {
            level = wrow.last;
            yD = wrow.dod; yW = wrow.d5; yM = wrow.d20;
            unit = wrow.unit || unit;
          }
        }
        var sparkSrc = (yBars && yBars.length ? yBars : eBars) || [];
        return {
          label: r.label + (r.etf ? " · " + r.etf : ""),
          sid: r.id || r.etf || r.wr,
          level: level, unit: unit,
          y: { d: yD, w: yW, m: yM, q: yQ },
          r: retH,
          asof: asof(yBars) || asof(trBars),
          spark: sparkSrc.slice(-40).map(function (b) { return b.value != null ? b.value : (b.close != null ? b.close : null); }),
          proxy: c.kind === "proxy"
        };
      });
      return { id: c.id, title: c.title, kind: c.kind, rows: rows };
    });

    return {
      generated_at: new Date().toISOString(),
      cats: cats,
      warroom: wr ? { regime: (wr.heartbeat || {}).regime, score: (wr.heartbeat || {}).score, headline: (wr.heartbeat || {}).headline } : null,
      sources: {
        yields: "FRED ICE BofA OAS / Treasury constant-maturity / OECD long rates",
        returns: "Bond ETF total-return (Polygon/Yahoo) and ICE BofA TR indices on FRED",
        warroom: "justhodl-bond-warroom · data/bond-warroom.json"
      }
    };
  }

  w.JHBonds = { CATS: CATS, run: run };
})(window);
