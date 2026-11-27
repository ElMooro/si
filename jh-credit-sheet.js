/* jh-credit-sheet.js - what engine 3.0 adds to the Credit Before Equity board (jh-credit-board.js draws the board; this
   draws what opens from it). jh-reskin-skip: the colours that measure are the board's own two palettes.

     the distressed switch     one click: only the distressed companies, widest spread first; a second click puts it back
     every published column    the company table can show every column the engine publishes, and hand it over as a file
     the company sheet         opens from any company name: every figure the engine holds for it, each one explained, every
                               bond it was read from, its spread month by month, its CDS, and what the board measured for
                               stocks that read the way it reads now
     what the stock did next   the board's own history, month-end by month-end: the reading, then the stock
     the whole document        every key the engine published, browsable, with the files themselves

   Khalid 2026-11-25: "you should add an icon that would sort distrissed. also you should increase the coverage to include a
   lot more companies and think of different ways and addition to improve this engine so that it can predict what would
   happen to the stock of these companies. also make sure all the data from the engines is viewable in the iux".

   It measures nothing. A figure the engine did not publish is shown as not published, never as zero. */
(function (root) {
  "use strict";
  var B = root.JHCreditBoard;
  if (!B) return;
  var esc = B.esc, bp = B.bp, pct = B.pct, money = B.money, day = B.day, fixed = B.fixed, sortRows = B.sortRows, textColour = B.textColour, table = B.table;
  var BASE = "https://justhodl.ai/";
  var BOOK_WORDS = { ig: "Investment grade", hy: "High yield", em: "Emerging markets", xig: "Non-dollar investment grade", xhy: "Non-dollar high yield" };
  var SIGNAL_WORDS = { CREDIT_LEADS_DOWN: "Credit leads down", CREDIT_LEADS_UP: "Credit leads up", NONE: "No lead", INSUFFICIENT_HISTORY: "No reading" };
  var DIR_WORDS = { DETERIORATING: "Worse", IMPROVING: "Better", FLAT: "Flat" };
  var TIE_WORDS = { exact: "The issuer's name is the listed company's", alias: "A reviewed alias of the listed company", subsidiary: "A named subsidiary of the listed company",
                    core: "The same name without its legal tail", truncated: "A name the fund file cut short", untied: "No listed stock tied" };
  var CELL_WORDS = { "worse/down": "Credit worse, stock already fell", "worse/flat": "Credit worse, stock flat", "worse/up": "Credit worse, stock rose",
                     "flat/down": "Credit unmoved, stock fell", "flat/flat": "Both quiet", "flat/up": "Credit unmoved, stock rose",
                     "better/down": "Credit better, stock fell", "better/flat": "Credit better, stock flat", "better/up": "Credit better, stock already rose" };
  /* The two lines of a company's history. Validated as a pair on the page's dark panel (lightness band, chroma, colour-blind and
     normal-vision separation, contrast): the company in blue, its book in amber and dashed. Kept here, where the site build
     does not re-skin them. */
  var SERIES = { co: "#4C8DF6", book: "#B8801A" };
  var ICON_DISTRESS = '<svg class="cb-ico" viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false"><path d="M8 1.6 15 14H1z" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/>' +
    '<path d="M8 6.2v3.9" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="8" cy="12" r=".95" fill="currentColor"/></svg>';

  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function isObj(v) { return !!v && typeof v === "object" && !Array.isArray(v); }
  function n1(v, d) { return isNum(v) ? fixed(v, d == null ? 1 : d).replace("-", "−") : "–"; }
  /* Dollars that may be nothing or below zero (what funds hold, protection sold less bought): $2.50T, $3.7B, $552M, $0, −$36M. */
  function usd(v, signed) {
    if (!isNum(v)) return "–";
    var a = Math.abs(v), s = a >= 1e12 ? fixed(a / 1e12, 2) + "T" : a >= 1e9 ? fixed(a / 1e9, 1) + "B" : a >= 1e6 ? fixed(a / 1e6, 0) + "M" : a >= 1e3 ? fixed(a / 1e3, 0) + "K" : fixed(a, 0);
    return (v < 0 ? "−" : signed && v > 0 ? "+" : "") + "$" + s;
  }
  function funds() { return root.JHCreditFunds || null; }          // jh-credit-funds.js: what registered funds hold; loaded after this file

  // ------------------------------------------------------------------------------------------------ every column
  /* Every column the engine publishes for a company: [key, label, kind, group, what it is]. A column the engine adds later
     that is not listed here is still shown, under its own key - nothing published is hidden for want of a label. */
  var GROUPS = ["Who it is", "Its bonds", "Spread", "Change in the spread", "Against its book", "Stress", "What the price implies", "Traded CDS", "The stock", "The reading", "Held by funds"];
  var FIELDS = [
    ["id", "Board id", "text", 0, "T: a listed company, by its ticker. I: an issuer with no listed stock tied, by its name."],
    ["ticker", "Ticker", "text", 0, "The listed stock the bonds were tied to. Empty when none was."],
    ["name", "Company", "text", 0, "The listed company's name, or the issuer's as the fund file prints it."],
    ["industry", "Industry", "text", 0, "The fund file's sector; the listed company's own where the file only says industrial, utility or financial."],
    ["fund_sector", "Fund sector", "text", 0, "The sector exactly as the fund file prints it."],
    ["book", "Book", "book", 0, "The book most of its bonds sit in. A company is compared with its own book."],
    ["country", "Country", "text", 0, "The country the fund file gives."],
    ["tie", "How it was tied", "tie", 0, "How the issuer names on the bonds were tied to the listed company."],
    ["n_issuers", "Issuer names", "int", 0, "How many legal issuer names were rolled into this company."],
    ["theme", "Theme", "theme", 0, "A theme the company belongs to, when it belongs to one."],
    ["layer", "Layer", "layer", 0, "Its layer inside the theme."],
    ["n_bonds", "Bonds", "int", 1, "Bonds of this company in the scanned funds, in the currency it is read in."],
    ["n_spread", "With a spread", "int", 1, "Of those, the bonds a spread could be read from."],
    ["value", "Held", "money", 1, "Market value of its bonds held by the scanned funds. It is the weight of every average on this row."],
    ["yield_pct", "Yield", "pc2", 1, "Average yield to worst of its bonds, weighted by value held."],
    ["dur", "Duration", "years", 1, "Average modified duration, in years."],
    ["coupon", "Coupon", "pc2", 1, "Average coupon of its bonds."],
    ["px", "Bond price", "price", 1, "Average price of its bonds, per 100 of face."],
    ["px_min", "Lowest price", "price", 1, "Its cheapest bond, per 100 of face. Far under 100 is the market doubting it is paid in full."],
    ["wam", "Average maturity", "years", 1, "Average years until its bonds come due."],
    ["due2y", "Due inside 2 years", "share", 1, "Share of its bonds, by value held, that come due inside two years: what it must repay or refinance soon."],
    ["spread_bp", "Spread", "bp", 2, "Yield of its bonds less the Treasury yield at the matching maturity, in basis points."],
    ["from_m1", "Spread a month ago", "bp", 2, "The same bonds at the month's reference day."],
    ["s_short", "Spread, near bonds", "bp", 2, "Its bonds with three years or less to run."],
    ["s_long", "Spread, far bonds", "bp", 2, "Its bonds with seven years or more to run."],
    ["curve_bp", "Far less near", "dbpflip", 2, "Far bonds' spread less near bonds'. Below zero the market charges more for the near term than the far: it is worried about the next few years."],
    ["d_w1", "1W Δ", "dbp", 3, "Change in the spread over the week, same bonds at both dates, basis points."],
    ["d_m1", "1M Δ", "dbp", 3, "Change over the month."],
    ["d_m3", "3M Δ", "dbp", 3, "Change over three months."],
    ["d_ytd", "YTD Δ", "dbp", 3, "Change since the last day of last year."],
    ["pct_w1", "1W Δ %", "dpct", 3, "The week's change as a share of where the spread started."],
    ["pct_m1", "1M Δ %", "dpct", 3, "The month's change as a share of where it started."],
    ["pct_m3", "3M Δ %", "dpct", 3, "Three months' change as a share of where it started."],
    ["pct_ytd", "YTD Δ %", "dpct", 3, "The year's change as a share of where it started."],
    ["n_m1", "Bonds matched, 1M", "int", 3, "Bonds seen at both dates of the month's comparison."],
    ["x_w1", "Past book, 1W", "dbp", 4, "Its change less the change of the middle company of its book, over the week."],
    ["x_m1", "Past book, 1M", "dbp", 4, "Its change less its book's, over the month. This is what the rule reads."],
    ["x_m3", "Past book, 3M", "dbp", 4, "Its change less its book's, over three months."],
    ["x_ytd", "Past book, YTD", "dbp", 4, "Its change less its book's, since the last day of last year."],
    ["bar_m1", "Bar to clear", "bp", 4, "How far past its book the month's change had to be to count as credit moving."],
    ["stress", "Stress rank", "rank", 5, "Where it stands in its own book, 0 to 100: the average of the three ranks beside it. It does not look at the stock."],
    ["rk_level", "Rank, level", "rank", 5, "Where its spread stands among the spreads of its book."],
    ["rk_year", "Rank, year", "rank", 5, "Where its widening over the long window stands, as a share of where it started."],
    ["rk_month", "Rank, month", "rank", 5, "Where its widening over the month stands."],
    ["distressed", "Distressed", "flag", 5, "Spread of 1,000 basis points or more, or yields past what the fund file prints."],
    ["pd_1y", "Implied default, 1Y", "share", 6, "The chance of default inside a year that the spread prices if a default pays 40 cents: a ceiling, since a spread also pays for illiquidity. Not a forecast."],
    ["pd_5y", "Implied default, 5Y", "share", 6, "The same reading over five years."],
    ["step_bp", "Refinancing step", "dbp", 6, "Its yield today less its average coupon: what replacing its debt at today's yield would add to the rate it pays."],
    ["cds_bp", "CDS", "bp", 7, "Traded five-year CDS level, where the CDS desk prints one."],
    ["cds_d1w", "CDS 1W", "dbp", 7, "Change in the CDS level over a week."],
    ["cds_d1m", "CDS 1M", "dbp", 7, "Change over a month."],
    ["cds_d3m", "CDS 3M", "dbp", 7, "Change over three months."],
    ["cds_rank", "CDS 1Y rank", "range", 7, "Where today's CDS level sits in its own range of the past year."],
    ["cds_last", "CDS last print", "date", 7, "The day of its last CDS print."],
    ["px_w1", "Stock 1W", "px", 8, "The stock's change over the week's days."],
    ["px_m1", "Stock 1M", "px", 8, "The stock's change over the month's days. This is what the rule reads."],
    ["px_m3", "Stock 3M", "px", 8, "The stock's change over three months."],
    ["px_ytd", "Stock YTD", "px", 8, "The stock's change since the last day of last year."],
    ["direction", "Credit", "dir", 9, "What credit did over the month against its book: worse, better or flat."],
    ["cell", "Credit / stock", "cell", 9, "Where it stands on both questions at once: what credit did past its book, and what the stock did over the same days."],
    ["signal", "Signal", "signal", 9, "A lead: credit moved past its book and the stock has not followed."],
    ["vol", "Stock's own volatility", "share", 8, "How much the stock itself has been moving: the yearly rate of its last 63 daily changes."],
    ["risk_3m", "Chance of a 20% fall, 3 months", "share", 9, "How often, in the board's own history, a stock whose bonds traded this wide and whose own volatility stood in the same third was at some point 20% or more under where it started, inside three months. A record, not a promise."],
    ["risk_1m", "Chance of a 20% fall, 1 month", "share", 9, "The same inside one month."],
    ["lo_3m", "Three months: one in ten did worse than", "dret", 9, "Against the board's average stock, over three months, for stocks whose bonds traded this wide: one in ten did worse than this."],
    ["hi_3m", "Three months: one in ten did better than", "dret", 9, "The other end of the same range: one in ten did better than this."],
    ["band", "Spread band", "band", 9, "Which band of spread the chance and the range above are read from."],
    ["ccy", "Read in", "text", 1, "The currency of the bonds its spread is read from. A dollar spread is over Treasuries; another currency's is over its own government."],
    ["n_other", "Bonds in another currency", "int", 1, "Its bonds in a currency other than the one it is read in: listed under it, and in none of its figures. A company with a dollar bond the board can read is always read in dollars."],
    ["f_n", "Fund-held bonds and loans", "int", 10, "How many of its bonds and loans registered funds reported holding, in the SEC's Form N-PORT file. As of the funds' own report dates, months before the board's day."],
    ["f_funds", "Funds in the most widely held", "int", 10, "The number of funds holding the one security of its that most funds hold."],
    ["f_face", "Fund-held face", "usd", 10, "The face amount funds hold of its bonds and loans, in dollars."],
    ["f_value", "Fund-held value", "usd", 10, "The same holdings at the funds' own marks."],
    ["f_px", "Funds' middle mark", "mark", 10, "The middle mark funds put on its securities, per 100 of face, weighted by value. A mark is a fund's own valuation, not a trade."],
    ["f_gap", "Widest gap between marks", "mark", 10, "For the security funds disagree about most: the mark one fund in ten is at or over, less the mark one in ten is at or under. Read only where five funds or more mark it."],
    ["f_def", "Securities flagged in default", "int", 10, "Its securities that at least half of the positions funds report flag as in default."],
    ["f_pik", "Securities paid in kind", "int", 10, "Its securities that at least half of the positions funds report flag as paying interest in kind."],
    ["f_loans", "Loans among them", "int", 10, "How many of the fund-held securities are loans rather than bonds."],
    ["fc_sold", "Fund protection sold", "usd", 10, "Credit default swap protection funds have sold on its name, in dollars: the funds carry its credit risk."],
    ["fc_bought", "Fund protection bought", "usd", 10, "Protection funds have bought on its name: they pay to be protected against it."],
    ["fc_net", "Fund protection, net sold", "susd", 10, "Sold less bought. Above zero, funds on balance carry its credit risk; below, they are on balance protected."],
    ["fc_funds", "Funds with a swap on it", "int", 10, "The most funds holding a credit default swap on any one of the names it is written under."]
  ];
  var FIELD = {};
  FIELDS.forEach(function (f) { FIELD[f[0]] = f; });

  /* The published columns in a reading order: the dictionary's order first, then anything the engine added that it does not know. */
  function allColumns(doc) {
    var cols = B.isV2(doc) ? (doc.issuers.cols || []) : [], out = [];
    FIELDS.forEach(function (f) { if (cols.indexOf(f[0]) >= 0) out.push(f); });
    cols.forEach(function (k) { if (!FIELD[k]) out.push([k, k, "raw", GROUPS.length, "Published by the engine; this page has no description for it yet."]); });
    return out;
  }
  function align(kind) { return ["text", "book", "tie", "theme", "layer", "dir", "cell", "signal", "date", "raw", "flag"].indexOf(kind) >= 0 ? "l" : "r"; }

  /* One value, written for reading. ctx: {palette, doc}. Returns {text, colour}. */
  function shown(field, v, ctx) {
    var kind = field[2], pal = (ctx || {}).palette, doc = (ctx || {}).doc;
    if (v == null || v === "") return { text: "–", colour: "" };
    switch (kind) {
      case "int": return { text: isNum(v) ? v.toLocaleString("en-US") : String(v), colour: "" };
      case "money": return { text: money(v), colour: "" };
      case "usd": return { text: usd(v), colour: "" };
      case "susd": return { text: usd(v, true), colour: "" };
      case "mark": return { text: n1(v, 2), colour: "" };
      case "bp": return { text: bp(v), colour: "" };
      case "dbp": return { text: bp(v, true), colour: textColour(v, pal) };
      case "dbpflip": return { text: bp(v, true), colour: textColour(isNum(v) ? -v : null, pal) };
      case "dpct": return { text: pct(v, true), colour: textColour(v, pal) };
      case "px": return { text: pct(v, true), colour: textColour(isNum(v) ? -v : null, pal) };
      case "dret": return { text: pct(v, true), colour: textColour(isNum(v) ? -v : null, pal) };
      case "share": return { text: pct(v), colour: "" };
      case "pc2": return { text: isNum(v) ? fixed(v, 2) + "%" : "–", colour: "" };
      case "rank": return { text: n1(v, 1), colour: "" };
      case "range": return { text: isNum(v) ? fixed(v, 0) + "%" : "–", colour: "" };
      case "years": return { text: n1(v, 1), colour: "" };
      case "price": return { text: n1(v, 1), colour: "" };
      case "num1": return { text: n1(v, 1), colour: "" };
      case "band": return { text: bandWords(doc, v), colour: "" };
      case "flag": return { text: v ? "Yes" : "–", colour: "" };
      case "date": return { text: day(String(v)), colour: "" };
      case "book": return { text: BOOK_WORDS[v] || String(v), colour: "" };
      case "tie": return { text: TIE_WORDS[v] || String(v), colour: "" };
      case "dir": return { text: DIR_WORDS[v] || "–", colour: textColour(v === "DETERIORATING" ? 1 : v === "IMPROVING" ? -1 : null, pal) };
      case "signal": return { text: SIGNAL_WORDS[v] || String(v), colour: textColour(v === "CREDIT_LEADS_DOWN" ? 1 : v === "CREDIT_LEADS_UP" ? -1 : null, pal) };
      case "cell": return { text: CELL_WORDS[v] || String(v), colour: "" };
      case "theme": return { text: ((doc && B.themeOf(doc, v)) || {}).name || String(v), colour: "" };
      case "layer": return { text: (doc && B.layerWords(doc)[v]) || String(v), colour: "" };
      default: return { text: typeof v === "object" ? JSON.stringify(v) : String(v), colour: "" };
    }
  }
  /* A spread band in words, from the evidence the engine published. */
  function bandWords(doc, i) {
    var bands = isObj(doc) && isObj(doc.evidence) && isObj(doc.evidence.risk) && Array.isArray(doc.evidence.risk.bands) ? doc.evidence.risk.bands : [];
    var b = bands.filter(function (x) { return isObj(x) && x.i === i; })[0];
    return b && typeof b.words === "string" ? b.words : isNum(i) ? "band " + i : "–";
  }
  function fieldCell(field, r, ctx) {
    var s = shown(field, r[field[0]], ctx);
    return '<td class="' + align(field[2]) + '"' + (s.colour ? ' style="color:' + s.colour + '"' : "") + ">" + esc(s.text) + "</td>";
  }

  /* A table as a file: the published keys as the header, the published values untouched. */
  function csvText(keys, rows) {
    function q(v) {
      if (v == null) return "";
      var s = typeof v === "object" ? JSON.stringify(v) : String(v);
      return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    }
    return keys.map(q).join(",") + "\r\n" + rows.map(function (r) { return keys.map(function (k) { return q(Array.isArray(r) ? r[keys.indexOf(k)] : r[k]); }).join(","); }).join("\r\n") + "\r\n";
  }

  // ------------------------------------------------------------------------------------------------ the distressed switch
  function distressed(doc) { return table(doc).filter(function (r) { return !!r.distressed; }); }
  /* The switch itself. where: "table" or "stress". */
  function distressButton(doc, on, where) {
    var n = distressed(doc).length;
    return '<button type="button" class="cb-distress" data-distress="' + esc(where) + '" aria-pressed="' + (on ? "true" : "false") + '" title="' +
      esc(on ? "Showing only the distressed companies, widest spread first. Click to show every company again." : "Show only the distressed companies, widest spread first") + '">' + ICON_DISTRESS +
      "<span>Distressed</span><b>" + n.toLocaleString("en-US") + "</b></button>";
  }
  /* The tile for the top of the page. */
  function distressTile(doc) {
    var rows = distressed(doc), listed = rows.filter(function (r) { return r.ticker; });
    var top = sortRows(listed.filter(function (r) { return isNum(r.spread_bp); }), "spread_bp", "desc").slice(0, 3);
    return '<div class="cb-tile-h"><div class="cb-lbl">' + ICON_DISTRESS + ' Distressed</div><div class="cb-big">' + esc(rows.length.toLocaleString("en-US")) + '</div><div class="cb-sub">companies at 1,000 bp or more · ' +
      esc(String(listed.length)) + " with a listed stock" + (top.length ? " · widest: " + top.map(function (r) { return "<b>" + esc(r.ticker) + "</b> " + esc(bp(r.spread_bp)); }).join(", ") : "") +
      ' · <button type="button" class="cb-link" data-distress="table" data-goto="s-companies">sort them out</button></div></div>';
  }

  /* The tile for the fall risk: how many listed companies have a one-in-three chance or more. */
  function riskTile(doc) {
    var rk = isObj(doc.risk) ? doc.risk : null;
    if (!rk || !isNum(rk.n) || !rk.n) return "";
    var top = sortRows(table(doc).filter(function (r) { return r.ticker && isNum(r.risk_3m) && r.value >= B.HELD_FIRST; }), "risk_3m", "desc").slice(0, 3), big = isNum(rk.big) ? rk.big : 20;
    return '<div class="cb-tile-h"><div class="cb-lbl">Fall risk, 3 months</div><div class="cb-big">' + esc(isNum(rk.one_in_three) ? rk.one_in_three.toLocaleString("en-US") : "–") + '</div><div class="cb-sub">listed companies whose reading has meant a 1-in-3 chance or more of a ' +
      big + "% fall · " + esc(String(rk.n)) + " have a chance" + (top.length ? " · highest: " + top.map(function (r) { return "<b>" + esc(r.ticker) + "</b> " + esc(pct(r.risk_3m)); }).join(", ") : "") +
      ' · <button type="button" class="cb-link" data-risk="1" data-goto="s-companies">rank them</button> · <button type="button" class="cb-link" data-goto="s-next">the record</button></div></div>';
  }

  // ------------------------------------------------------------------------------------------------ the company sheet
  function rowOf(doc, id) { return table(doc).filter(function (r) { return r.id === id; })[0] || null; }
  function nameRow(doc, r) { return r && r.ticker ? ((doc.names || []).filter(function (x) { return isObj(x) && x.ticker === r.ticker; })[0] || null) : null; }
  function cdsRow(doc, r) {
    var priced = isObj(doc.cds) && Array.isArray(doc.cds.priced) ? doc.cds.priced.filter(isObj) : [];
    return r && r.ticker ? (priced.filter(function (c) { return c.ticker === r.ticker; })[0] || null) : null;
  }
  /* The id a company name opens: a row of the board by ticker, else by name. */
  function idOf(doc, ticker, name) {
    var t = table(doc), hit = ticker ? t.filter(function (r) { return r.ticker === ticker; })[0] : null;
    if (!hit && name) hit = t.filter(function (r) { return r.name === name; })[0];
    return hit ? hit.id : "";
  }
  /* A company's name as the control that opens its sheet. */
  function nameButton(r, inner) {
    return '<button type="button" class="cb-name" data-sheet="' + esc(r.id || "") + '" title="Open everything the engine holds for it">' + inner + "</button>";
  }

  function kpi(label, big, sub) { return '<div class="cb-kpi"><div class="cb-lbl">' + esc(label) + '</div><div class="cb-big">' + big + '</div><div class="cb-sub">' + sub + "</div></div>"; }

  /* Every figure of the row, group by group, each with what it is. */
  function figuresHtml(doc, r, ctx) {
    var cols = allColumns(doc), h = "";
    GROUPS.concat(["Published without a description"]).forEach(function (g, gi) {
      var mine = cols.filter(function (f) { return f[3] === gi; });
      if (!mine.length) return;
      h += '<tr class="cb-grp"><td colspan="3">' + esc(g) + "</td></tr>" + mine.map(function (f) {
        var s = shown(f, r[f[0]], ctx);
        return '<tr><td class="l">' + esc(f[1]) + ' <span class="cb-key">' + esc(f[0]) + '</span></td><td class="r"' + (s.colour ? ' style="color:' + s.colour + '"' : "") + ">" + esc(s.text) +
          '</td><td class="l cb-what">' + esc(f[4]) + "</td></tr>";
      }).join("");
    });
    return '<div class="cb-scroll" tabindex="0" role="region" aria-label="Every figure for this company"><table class="cb-table cb-figs" data-jh-nosort><thead><tr><th class="l">Figure</th><th class="r">Value</th><th class="l">What it is</th></tr></thead><tbody>' +
      h + "</tbody></table></div>";
  }

  /* Any object the engine published, key by key: the listed-stock row, the CDS row. Keys already in the table above are skipped. */
  function pairsHtml(obj, skip, labels) {
    var keys = Object.keys(obj).filter(function (k) { return !skip || skip.indexOf(k) < 0; });
    if (!keys.length) return "";
    return '<dl class="cb-pairs">' + keys.map(function (k) {
      var v = obj[k];
      return "<dt>" + esc((labels && labels[k]) || k) + (labels && labels[k] ? ' <span class="cb-key">' + esc(k) + "</span>" : "") + "</dt><dd>" + esc(v == null ? "–" : typeof v === "object" ? JSON.stringify(v) : String(v)) + "</dd>";
    }).join("") + "</dl>";
  }
  var NAME_LABELS = { price: "Last close", price_date: "Close of", d_price_pct: "Stock over the rule's days", equity_state: "The stock, against credit", equity_flat: "Stock inside the flat band",
    signal_basis: "What the reading rests on", cds_says: "The traded CDS", credit_conflict: "Bonds and CDS disagree", credit_state: "Credit state", bar_bp: "Bar to clear", n_matched: "Bonds matched",
    distance_to_default: "Model: distance to default", d_distance_to_default: "Model: change in it", synthetic_cds_bp: "Model: synthetic CDS", d_synthetic_cds_bp: "Model: change in it",
    model_signal: "Model: signal", model_credit_direction: "Model: credit direction", model_d_price_pct: "Model: stock change", group: "Group" };
  var CDS_LABELS = { name: "Reference entity", spread_bp: "Level, bp", d1d: "1 day", d1w: "1 week", d1m: "1 month", d3m: "3 months", rank_1y: "Place in its year's range, %", last_date: "Last print",
    trades_30d: "Trades in 30 days", liquid: "Liquid", sector: "Sector", region: "Region", px_1m: "Stock over the month, %", direction: "Direction", tenor: "Tenor", status: "Status" };

  // the bonds
  var BOND_FIELDS = [
    ["issuer", "Issuer", "text"], ["coupon", "Coupon", "pc2"], ["maturity", "Matures", "date"], ["price", "Price", "price"], ["yield_pct", "Yield", "pc2"], ["dur", "Duration", "years"],
    ["tenor", "Read at", "years"], ["spread_bp", "Spread", "bp"], ["d_w1", "1W Δ", "dbp"], ["d_m1", "1M Δ", "dbp"], ["d_m3", "3M Δ", "dbp"], ["d_ytd", "YTD Δ", "dbp"], ["value", "Held", "money"], ["why", "No spread because", "text"]
  ];
  function bondsOf(bondsDoc, id) {
    if (!isObj(bondsDoc) || !Array.isArray(bondsDoc.cols) || !Array.isArray(bondsDoc.rows)) return null;
    var cols = bondsDoc.cols, at = cols.indexOf("co");
    if (at < 0) return null;
    return bondsDoc.rows.filter(function (r) { return Array.isArray(r) && r[at] === id; }).map(function (r) { var o = {}; cols.forEach(function (c, i) { o[c] = r[i]; }); return o; });
  }
  function bondsHtml(bonds, sort, ctx) {
    if (bonds === "loading") return '<p class="cb-empty">Loading its bonds.</p>';
    if (bonds === "unpublished") return '<p class="cb-empty">The bond-by-bond document is not published yet; it arrives with engine 3.0.</p>';
    if (bonds === "error" || bonds == null) return '<p class="cb-empty">Its bonds could not be loaded. Nothing is shown rather than something stale.</p>';
    if (!bonds.length) return '<p class="cb-empty">The bond document lists no bond under this company.</p>';
    sort = sort || { key: "maturity", dir: "asc" };
    var rows = sortRows(bonds, sort.key, sort.dir);
    var head = "<thead><tr>" + BOND_FIELDS.map(function (f) {
      var on = sort.key === f[0];
      return '<th class="' + align(f[2]) + '" aria-sort="' + (on ? (sort.dir === "asc" ? "ascending" : "descending") : "none") + '"><button type="button" data-bond-sort="' + esc(f[0]) + '">' + esc(f[1]) + (on ? (sort.dir === "asc" ? " ▲" : " ▼") : "") + "</button></th>";
    }).join("") + "</tr></thead>";
    var held = rows.reduce(function (s, b) { return s + (isNum(b.value) ? b.value : 0); }, 0), read = rows.filter(function (b) { return isNum(b.spread_bp); }).length;
    return '<p class="cb-note">' + rows.length + (rows.length === 1 ? " bond" : " bonds") + ", " + read + " with a spread, " + esc(money(held)) + " held by the scanned funds. Read at: the Treasury maturity the bond was measured against, in years. " +
      'The identifiers of the bonds are not republished. <button type="button" class="cb-link" data-csv="bonds">Download these as a file</button></p>' +
      '<div class="cb-scroll" tabindex="0" role="region" aria-label="Its bonds; scroll sideways for all columns"><table class="cb-table cb-bonds" data-jh-nosort>' + head + "<tbody>" + rows.map(function (b) {
        return "<tr>" + BOND_FIELDS.map(function (f) { return fieldCell(f, b, ctx); }).join("") + "</tr>";
      }).join("") + "</tbody></table></div>";
  }

  /* Its spread month by month, beside the middle company of its book. history: the engine's history document. */
  function historyOf(history, r, doc) {
    if (!isObj(history) || !Array.isArray(history.dates) || !isObj(history.companies)) return null;
    var mine = history.companies[r.id];
    if (!Array.isArray(mine)) return null;
    var book = isObj(history.books) && Array.isArray(history.books[r.book]) ? history.books[r.book] : [];
    var pts = history.dates.map(function (d, i) { return { d: d, v: isNum(mine[i]) ? mine[i] : null, b: isNum(book[i]) ? book[i] : null }; });
    // today's reading closes the line: the history ends at the last finished month
    var last = history.dates[history.dates.length - 1], mid = isObj(doc) && isObj(doc.stress) && isObj(doc.stress.book_spread_bp) ? doc.stress.book_spread_bp[r.book] : null;
    if (isObj(doc) && typeof doc.as_of === "string" && doc.as_of > String(last || "") && isNum(r.spread_bp)) pts.push({ d: doc.as_of, v: r.spread_bp, b: isNum(mid) ? mid : null, now: true });
    return pts.some(function (p) { return p.v !== null; }) ? pts : null;
  }
  /* Round steps for an axis: about four of them between lo and hi. */
  function niceTicks(lo, hi) {
    var raw = (hi - lo) / 4 || 1, mag = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10)), f = raw / mag, step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * mag;
    var a = Math.floor(lo / step) * step, b = Math.ceil(hi / step) * step, out = [];
    for (var t = a; t <= b + step / 2; t += step) out.push(Math.round(t * 100) / 100);
    return out;
  }
  function chartHtml(pts, r, ctx) {
    var have = pts.filter(function (p) { return p.v !== null; });
    if (have.length < 2) return '<p class="cb-empty">The history holds fewer than two month-ends for it.</p>';
    var W = 720, H = 230, L = 46, R = 96, T = 12, Bm = 26, n = pts.length;
    var all = [];
    pts.forEach(function (p) { if (p.v !== null) all.push(p.v); if (p.b !== null) all.push(p.b); });
    var ticks = niceTicks(Math.max(0, Math.min.apply(null, all)), Math.max.apply(null, all)), lo = ticks[0], hi = ticks[ticks.length - 1];
    if (hi <= lo) { hi = lo + 10; ticks = [lo, hi]; }
    function x(i) { return L + (W - L - R) * (n > 1 ? i / (n - 1) : 0.5); }
    function y(v) { return T + (H - T - Bm) * (1 - (v - lo) / (hi - lo)); }
    function path(key) {
      var d = "", pen = false;
      pts.forEach(function (p, i) { if (p[key] === null) { pen = false; return; } d += (pen ? "L" : "M") + x(i).toFixed(1) + " " + y(p[key]).toFixed(1); pen = true; });
      return d;
    }
    var years = [];
    pts.forEach(function (p, i) { if (i === 0 || p.d.slice(0, 4) !== pts[i - 1].d.slice(0, 4)) years.push([i, p.d.slice(0, 4)]); });
    var last = have[have.length - 1], lastI = pts.indexOf(last), lastB = null;
    pts.forEach(function (p) { if (p.b !== null) lastB = p; });
    var bookWord = BOOK_WORDS[r.book] || "its book", ly = y(last.v), by = lastB ? y(lastB.b) : null;
    if (by !== null && Math.abs(by - ly) < 14) by = ly + (by >= ly ? 14 : -14);
    var svg = '<svg class="cb-chart" viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="' + esc((r.ticker || r.name) + ": spread at each month-end, beside the middle company of its book") + '">' +
      ticks.map(function (t) { return '<line x1="' + L + '" x2="' + (W - R) + '" y1="' + y(t).toFixed(1) + '" y2="' + y(t).toFixed(1) + '" class="cb-grid"/><text x="' + (L - 6) + '" y="' + (y(t) + 3.5).toFixed(1) + '" text-anchor="end" class="cb-axis">' + esc(t === Math.round(t) ? Math.round(t).toLocaleString("en-US") : bp(t)) + "</text>"; }).join("") +
      years.map(function (yy) { return '<text x="' + x(yy[0]).toFixed(1) + '" y="' + (H - 8) + '" text-anchor="' + (yy[0] === 0 ? "start" : "middle") + '" class="cb-axis">' + esc(yy[1]) + "</text>"; }).join("") +
      (lastB ? '<path d="' + path("b") + '" class="cb-line-book" fill="none" stroke="' + SERIES.book + '"/>' : "") + '<path d="' + path("v") + '" class="cb-line-co" fill="none" stroke="' + SERIES.co + '"/>' +
      '<circle cx="' + x(lastI).toFixed(1) + '" cy="' + y(last.v).toFixed(1) + '" r="4" class="cb-dot-co" fill="' + SERIES.co + '"/>' +
      '<text x="' + (W - R + 8) + '" y="' + (ly + 4).toFixed(1) + '" class="cb-lab-co">' + esc(bp(last.v)) + " bp</text>" +
      (lastB ? '<text x="' + (W - R + 8) + '" y="' + (by + 4).toFixed(1) + '" class="cb-lab-book">' + esc(bp(lastB.b)) + " book</text>" : "") +
      '<g class="cb-hover" hidden><line y1="' + T + '" y2="' + (H - Bm) + '" class="cb-cross"/><circle r="4" class="cb-dot-co" fill="' + SERIES.co + '"/></g>' +
      '<rect x="' + L + '" y="' + T + '" width="' + (W - L - R) + '" height="' + (H - T - Bm) + '" fill="transparent" class="cb-hit" data-chart="' + esc(JSON.stringify({ L: L, R: R, W: W, T: T, H: H, B: Bm, lo: lo, hi: hi })) + '"/></svg>';
    var legend = '<div class="cb-leg"><span><i class="cb-sw" style="border-top:2px solid ' + SERIES.co + '"></i>' + esc(r.ticker || B.shortName(r.name)) + ', spread in basis points</span>' +
      (lastB ? '<span><i class="cb-sw" style="border-top:2px dashed ' + SERIES.book + '"></i>Middle company of ' + esc(bookWord.toLowerCase()) + "</span>" : "") +
      '<span class="cb-tip" aria-live="polite"></span></div>';
    var rows = pts.filter(function (p) { return p.v !== null || p.b !== null; }).slice().reverse();
    var tableHtml = '<details class="cb-figures"><summary data-jh-title="The month-ends as figures">The ' + rows.length + ' month-ends as figures</summary><div class="cb-scroll" tabindex="0" role="region" aria-label="Spread at each month-end"><table class="cb-table" data-jh-nosort><thead><tr>' +
      '<th class="l">Month-end</th><th class="r">Spread, bp</th><th class="r">Middle of its book, bp</th></tr></thead><tbody>' + rows.map(function (p) {
        return '<tr><td class="l">' + esc(day(p.d)) + (p.now ? ' <span class="cb-dim">today\'s holdings</span>' : "") + '</td><td class="r">' + esc(bp(p.v)) + '</td><td class="r">' + esc(bp(p.b)) + "</td></tr>"; }).join("") + "</tbody></table></div></details>";
    return '<div class="cb-chartbox" data-series="' + esc(JSON.stringify(pts.map(function (p) { return [p.d, p.v, p.b]; }))) + '">' + legend + svg + "</div>" + tableHtml;
  }

  // ------------------------------------------------------------------------------------------------ what the stock did next
  function evidenceOf(doc) { return isObj(doc) && isObj(doc.evidence) && Array.isArray(doc.evidence.readings) ? doc.evidence : null; }
  function horizonsOf(ev) { return (Array.isArray(ev.horizons) ? ev.horizons : []).filter(isObj); }
  function readingOf(ev, key) { return ev.readings.filter(function (x) { return isObj(x) && x.key === key; })[0] || null; }
  /* The keys of the measured readings a company falls under today. */
  function readingsOf(doc, r) {
    var out = [], ev = evidenceOf(doc), bands = ev && Array.isArray(ev.stress_bands) ? ev.stress_bands : [];
    if (isNum(r.band)) out.push("level:" + r.band);
    if (r.cell) out.push("cell:" + r.cell);
    if (r.signal === "CREDIT_LEADS_DOWN" || r.signal === "CREDIT_LEADS_UP") out.push("signal:" + r.signal);
    bands.forEach(function (b, i) { if (Array.isArray(b) && isNum(r.stress) && r.stress >= b[0] && (r.stress < b[1] || (i === bands.length - 1 && r.stress <= b[1]))) out.push("stress:" + b[0] + "-" + b[1]); });
    if (r.book) out.push("book:" + r.book);
    return out;
  }
  function sure(t) { return isNum(t) ? n1(t, 1) : "–"; }
  /* "1 month" read after "the next": "month"; "3 months" stays. */
  function span(words) { return String(words || "").replace(/^1 /, ""); }
  /* A set of measured readings as a table, over one horizon. */
  function recordTable(rows, ev, hkey, ctx, firstHead) {
    var pal = (ctx || {}).palette, h = horizonsOf(ev).filter(function (x) { return x.key === hkey; })[0] || horizonsOf(ev)[0] || { key: "h1", words: "1 month" }, big = isNum(ev.big) ? ev.big : 20;
    function ret(v) { return '<td class="r" style="color:' + textColour(isNum(v) ? -v : null, pal) + '">' + esc(pct(v, true)) + "</td>"; }
    function share(v) { return '<td class="r">' + esc(isNum(v) ? pct(v) : "–") + "</td>"; }
    function bold(t, text) { return '<td class="r"' + (isNum(t) && Math.abs(t) >= 3 ? ' style="font-weight:700;color:var(--text)"' : "") + ">" + esc(text) + "</td>"; }
    var head = '<th class="l">' + esc(firstHead || "Reading") + '</th><th class="r">Company-months</th><th class="r">Average</th><th class="r">Middle stock</th><th class="r">1 in 10 worse than</th><th class="r">1 in 10 better than</th>' +
      '<th class="r">Lagged by ' + big + '%+</th><th class="r">Beat by ' + big + '%+</th><th class="r">Fell ' + big + '%+</th><th class="r">How sure, average</th><th class="r">How sure, fall</th>';
    return '<div class="cb-scroll" tabindex="0" role="region" aria-label="What the stocks did over the next ' + esc(span(h.words)) + '; scroll sideways for all columns"><table class="cb-table cb-record" data-jh-nosort><thead><tr>' + head + "</tr></thead><tbody>" +
      rows.map(function (x) {
        var v = isObj(x[h.key]) ? x[h.key] : {};
        return '<tr><td class="l">' + esc(x.words || x.key) + '</td><td class="r">' + esc(isNum(v.n) ? v.n.toLocaleString("en-US") : "–") + "</td>" + ret(v.mean) + ret(v.median) + ret(v.p10) + ret(v.p90) +
          share(v.lag_big) + share(v.beat_big) + share(v.fell_big) + bold(v.t, sure(v.t)) + bold(v.t_fell, sure(v.t_fell)) + "</tr>";
      }).join("") + "</tbody></table></div>";
  }
  var RECORD_NOTE = "Average, middle stock and the two ends of the range are the stock's return less the board's average stock over the same days. Fell: the stock itself was at some point that far under where it started. " +
    "How sure: the month-by-month figure over its standard error; past 2 is unlikely to be luck for one test, and with this many tests at once the bar is 3 (shown bold).";

  /* For one company: what its reading has meant. */
  function outlookHtml(doc, r, ctx) {
    var ev = evidenceOf(doc);
    if (!r.ticker) return '<p class="cb-note">No listed stock is tied to it, so the board has no stock to measure.</p>';
    if (!ev) return '<p class="cb-note">What stocks did after each kind of reading is measured by engine 3.0; this document does not carry it yet.</p>';
    var big = isNum(ev.big) ? ev.big : 20, board = isObj(ev.board) && isObj(ev.board.h3) ? ev.board.h3 : {}, risk = isObj(doc.risk) ? doc.risk : {}, thirds = isObj(ev.risk) && Array.isArray(ev.risk.thirds) ? ev.risk.thirds : [];
    var cuts = Array.isArray(risk.vol_cuts) ? risk.vol_cuts : null, third = cuts && isNum(r.vol) ? (r.vol < cuts[0] ? 0 : r.vol < cuts[1] ? 1 : 2) : null;
    var h = "";
    if (isNum(r.risk_3m)) {
      h += '<p class="cb-verdict">Stocks whose bonds traded this wide (' + esc(bandWords(doc, r.band)) + ")" + (third !== null && thirds[third] ? " and whose own volatility stood in the " + esc(thirds[third]) : "") +
        ": <b>" + esc(pct(r.risk_3m)) + "</b> were at some point " + big + "% or more under where they started inside three months" + (isNum(board.fell_big) ? " (the whole board: " + esc(pct(board.fell_big)) + ")" : "") +
        (isNum(r.risk_1m) ? "; inside one month, " + esc(pct(r.risk_1m)) : "") + "." +
        (isNum(r.lo_3m) && isNum(r.hi_3m) ? " Against the board's average stock over three months, one in ten did worse than " + esc(pct(r.lo_3m, true)) + " and one in ten better than " + esc(pct(r.hi_3m, true)) + "." : "") + "</p>";
    } else {
      h += '<p class="cb-note">' + (r.book && ["ig", "hy", "em"].indexOf(r.book) < 0 ? "Its bonds are not in dollars, and the history the chances come from holds dollar bonds only: no chance is given." :
        "No chance of a fall is given: its stock has too little price history, or its spread could not be read.") + "</p>";
    }
    var cell = r.cell ? readingOf(ev, "cell:" + r.cell) : null, c1 = cell && isObj(cell.h1) ? cell.h1 : null;
    if (c1 && isNum(c1.mean)) {
      h += '<p class="cb-note">Which way: its reading today, ' + esc((cell.words || "").toLowerCase()) + ", has " + (isNum(c1.t) && Math.abs(c1.t) >= 3 ? "told" : "not told") + " the direction: " + esc(pct(c1.mean, true)) +
        " over the next month against the board's average stock in " + esc(isNum(c1.n) ? c1.n.toLocaleString("en-US") : "–") + " company-months (how sure: " + esc(sure(c1.t)) + ").</p>";
    }
    var rows = readingsOf(doc, r).map(function (k) { return readingOf(ev, k); }).filter(Boolean);
    if (rows.length) h += recordTable(rows, ev, "h3", ctx, "How it reads today · next 3 months") + recordTable(rows, ev, "h1", ctx, "How it reads today · next month") + '<p class="cb-note">' + esc(RECORD_NOTE) + "</p>";
    return h;
  }

  /* The chance of a fall, band by band: bars, with the whole board's rate marked. */
  function riskBars(ev, hkey, ctx) {
    var bands = isObj(ev.risk) && Array.isArray(ev.risk.bands) ? ev.risk.bands.filter(isObj) : [], tbl = isObj(ev.risk) && isObj(ev.risk[hkey]) ? ev.risk[hkey] : {}, pal = (ctx || {}).palette;
    var board = isObj(ev.board) && isObj(ev.board[hkey]) ? ev.board[hkey].fell_big : null, rows = bands.filter(function (b) { return isObj(tbl[String(b.i)]) && isNum(tbl[String(b.i)].fell); });
    if (!rows.length) return "";
    var top = Math.max.apply(null, rows.map(function (b) { return tbl[String(b.i)].fell; }).concat(isNum(board) ? [board] : [])), hi = Math.max(10, Math.ceil(top / 10) * 10);
    return '<div class="cb-bars" role="img" aria-label="The share of stocks that fell, by how wide their bonds traded">' + rows.map(function (b) {
      var c = tbl[String(b.i)];
      return '<span>' + esc(b.words) + '</span><span class="cb-bar-t" title="' + esc(b.words + ": " + pct(c.fell) + " of " + c.n.toLocaleString("en-US") + " company-months") + '"><i style="left:0;width:' + (c.fell / hi * 100).toFixed(1) + "%;background:" +
        B.colour(c.fell, { lo: 0, hi: hi }, "level", pal) + '"></i>' + (isNum(board) ? '<u style="left:' + (board / hi * 100).toFixed(1) + '%"></u>' : "") + "</span><span><b>" + esc(pct(c.fell)) + '</b> <span class="cb-dim">of ' +
        esc(c.n.toLocaleString("en-US")) + "</span></span>";
    }).join("") + "</div>" + (isNum(board) ? '<p class="cb-note">The thin line is the whole board: ' + esc(pct(board)) + ".</p>" : "");
  }
  /* The same chance, split by the third the stock's own volatility stood in: does the bond market say more than the stock already does? */
  function riskGrid(ev, hkey, ctx) {
    var bands = isObj(ev.risk) && Array.isArray(ev.risk.bands) ? ev.risk.bands.filter(isObj) : [], tbl = isObj(ev.risk) && isObj(ev.risk[hkey]) ? ev.risk[hkey] : {}, thirds = Array.isArray(ev.risk.thirds) ? ev.risk.thirds : [], pal = (ctx || {}).palette;
    function td(c) {
      if (!isObj(c) || !isNum(c.fell)) return '<td class="r cb-dim" title="Too few company-months to say">–</td>';
      return '<td class="r" style="background:' + B.colour(c.fell, { lo: 0, hi: 60 }, "level", pal) + ';color:#fff" title="' + esc(c.n.toLocaleString("en-US") + " company-months") + '">' + esc(pct(c.fell)) + "</td>";
    }
    return '<div class="cb-scroll" tabindex="0" role="region" aria-label="The chance of a fall by spread and by the stock\'s own volatility"><table class="cb-table cb-riskgrid" data-jh-nosort><thead><tr><th class="l">Spread over Treasuries</th>' +
      thirds.map(function (t) { return '<th class="r">Stock in its ' + esc(t) + "</th>"; }).join("") + '<th class="r">All of them</th><th class="r">Company-months</th></tr></thead><tbody>' + bands.filter(function (b) { return isObj(tbl[String(b.i)]); }).map(function (b) {
        var c = tbl[String(b.i)], th = Array.isArray(c.thirds) ? c.thirds : [];
        return '<tr><td class="l">' + esc(b.words) + "</td>" + thirds.map(function (_, i) { return td(th[i]); }).join("") + td(c) + '<td class="r">' + esc(c.n.toLocaleString("en-US")) + "</td></tr>";
      }).join("") + "</tbody></table></div>";
  }
  /* What was said beside what happened. check: the walk-forward check of the history; track: the board's own kept calls. */
  function saidTable(buckets, label) {
    if (!Array.isArray(buckets) || !buckets.length) return "";
    return '<div class="cb-scroll" tabindex="0" role="region" aria-label="' + esc(label) + '"><table class="cb-table" data-jh-nosort><thead><tr><th class="l">The chance the board gave</th><th class="r">Stocks</th><th class="r">Said, on average</th><th class="r">Happened</th></tr></thead><tbody>' +
      buckets.filter(isObj).map(function (b) {
        return '<tr><td class="l">' + esc(b.from + "% to " + (b.to >= 100 ? "100" : b.to) + "%") + '</td><td class="r">' + esc(isNum(b.n) ? b.n.toLocaleString("en-US") : "–") + '</td><td class="r">' + esc(pct(b.said)) + '</td><td class="r"><b>' + esc(pct(b.happened)) + "</b></td></tr>";
      }).join("") + "</tbody></table></div>";
  }
  /* The nine cells: how many listed companies stand in each today, and what each has meant. */
  function matrixHtml(doc, state, ev) {
    var mx = isObj(doc.matrix) && isObj(doc.matrix.cells) ? doc.matrix : null, pal = state.palette, hk = state.evHorizon === "h3" ? "h3" : "h1";
    if (!mx) return "";
    var credit = [["worse", "Credit worse"], ["flat", "Credit unmoved"], ["better", "Credit better"]], stock = [["down", "Stock fell"], ["flat", "Stock flat"], ["up", "Stock rose"]];
    var h = '<div class="cb-matrix"><span></span>' + stock.map(function (c) { return '<span class="cb-mx-h">' + esc(c[1]) + "</span>"; }).join("");
    credit.forEach(function (cr) {
      h += '<span class="cb-mx-h">' + esc(cr[1]) + "</span>" + stock.map(function (stc) {
        var key = cr[0] + "/" + stc[0], n = mx.cells[key], rec = ev ? readingOf(ev, "cell:" + key) : null, v = rec && isObj(rec[hk]) ? rec[hk] : {};
        var lead = key === "worse/flat" || key === "worse/up" ? "lead down" : key === "better/flat" || key === "better/down" ? "lead up" : "";
        return '<button type="button" class="cb-mx" data-cell="' + esc(key) + '" data-goto="s-companies" aria-pressed="' + (state.cell === key ? "true" : "false") + '" style="background:' + B.colour(isNum(v.mean) && isNum(v.t) && Math.abs(v.t) >= 2 ? -v.mean : 0, { hi: 2 }, "bp", pal) +
          '" title="Show these companies in the table. A cell is coloured only when its record is past 2 standard errors."><span class="cb-mx-v">' + esc(isNum(n) ? n.toLocaleString("en-US") : "–") + '</span><span class="cb-mx-s">' + (lead ? esc(lead) + " · " : "") + "today</span>" +
          (isNum(v.mean) ? '<span class="cb-mx-s">then ' + esc(pct(v.mean, true)) + " · how sure " + esc(sure(v.t)) + "</span>" : "") + "</button>";
      }).join("");
    });
    return h + "</div>";
  }

  function nextHtml(doc, state) {
    var ev = evidenceOf(doc), ctx = { palette: state.palette, doc: doc };
    if (!ev) return '<p class="cb-empty">What stocks did after each kind of reading is measured by engine 3.0 on the board\'s own month-ends. This document (version ' + esc(typeof doc.version === "string" ? doc.version : "?") + ") does not carry it yet.</p>" +
      (isObj(doc.matrix) ? matrixHtml(doc, state, null) : "");
    var hz = horizonsOf(ev), hk = state.evHorizon === "h3" ? "h3" : "h1", big = isNum(ev.big) ? ev.big : 20, dirn = isObj(ev.direction) ? ev.direction : {}, chk = isObj(ev.check) && isObj(ev.check.h3) ? ev.check.h3 : null;
    var ld = isObj(dirn.lead_down) ? dirn.lead_down : {}, lu = isObj(dirn.lead_up) ? dirn.lead_up : {}, lv = readingOf(ev, "level:0"), top = ev.readings.filter(function (x) { return x.group === "level" && x.key.indexOf("level:") === 0; }).slice(-1)[0];
    var groups = (Array.isArray(ev.groups) ? ev.groups : []).filter(isObj), g = groups.filter(function (x) { return x.key === state.evGroup; })[0] || groups[0] || { key: "level" };
    var h = '<p class="cb-cap">The board measured against its own history. At each of ' + esc(String(ev.month_ends)) + " month-ends from " + esc(day(ev.first)) + " to " + esc(day(ev.last)) + " the board was rebuilt exactly as today's is built, and each listed company's reading was set beside what its stock did next: " +
      esc(isNum(ev.company_months) ? ev.company_months.toLocaleString("en-US") : "–") + " company-months of " + esc(isNum(ev.companies) ? ev.companies.toLocaleString("en-US") : "–") + " companies. The stock is entered at " + esc(typeof ev.entry === "string" ? ev.entry : "the next close") + ".</p>";
    h += '<div class="cb-detail-cols cb-two"><div class="cb-theme" id="next-direction"><h3>Which way the stock goes next: the readings have not told</h3><p class="cb-theme-l">' + esc(typeof dirn.words === "string" ? dirn.words : "") + "</p>" +
      '<ul class="cb-facts"><li>A lead down (credit worse, the stock has not fallen): over the next month its stocks did <b>' + esc(pct(ld.mean, true)) + "</b> against the board's average stock, in " + esc(isNum(ld.n) ? ld.n.toLocaleString("en-US") : "–") +
      " company-months. How sure: " + esc(sure(ld.t)) + ".</li><li>A lead up (credit better, the stock has not risen): <b>" + esc(pct(lu.mean, true)) + "</b>, in " + esc(isNum(lu.n) ? lu.n.toLocaleString("en-US") : "–") + ". How sure: " + esc(sure(lu.t)) +
      ".</li></ul><p class=\"cb-note\">The leads are kept as they were: they say where the two markets disagree today. This is their record.</p></div>" +
      '<div class="cb-theme" id="next-size"><h3>How big its next move is: the spread tells, more than the stock\'s own volatility does</h3><p class="cb-theme-l">The share of stocks that were at some point ' + big +
      "% or more under where they started, inside three months, by how wide their bonds traded" + (lv && top && isObj(lv.h3) && isObj(top.h3) ? ": from <b>" + esc(pct(lv.h3.fell_big)) + "</b> to <b>" + esc(pct(top.h3.fell_big)) + "</b>" : "") + ".</p>" +
      riskBars(ev, "h3", ctx) + "</div></div>";
    h += '<div class="cb-ranked" id="next-risk"><h3>The chance of a ' + big + "% fall inside three months, by spread and by the stock's own volatility</h3>" + riskGrid(ev, "h3", ctx) +
      '<p class="cb-note">Read across a row: the stock\'s own volatility matters. Read down a column: among stocks that have been moving as much as each other, the ones whose bonds trade wider fell more often. ' +
      "This table is where every listed company's chance comes from (the Fall risk column). A cell with fewer than " + esc(String(isObj(ev.risk) && isNum(ev.risk.min_cell) ? ev.risk.min_cell : 150)) + " company-months is left empty and its row's figure is used.</p></div>";
    if (chk) {
      h += '<div class="cb-ranked" id="next-check"><h3>The check: what the table said beside what happened</h3><p class="cb-cap">Each month from ' + esc(day(chk.first)) + " to " + esc(day(chk.last)) +
        " was given its chances from a table built only on earlier months whose outcome was already known that day. " + esc(isNum(chk.n) ? chk.n.toLocaleString("en-US") : "–") + " company-months over " + esc(String(chk.months)) + " months; " + esc(pct(chk.fell)) +
        " of them fell " + big + "% or more.</p>" + saidTable(chk.buckets, "What was said beside what happened") + '<p class="cb-note">' + (isNum(chk.auc) ? "Of two stocks, one that went on to fall and one that did not, the table had given the faller the higher chance " +
        esc(pct(chk.auc)) + " of the time (a coin: 50%). " : "") + "This is a check on history. The board's own calls, kept each day as they are said, are scored below as their time comes up.</p>" + trackHtml(doc) + "</div>";
    }
    h += '<div class="cb-ranked" id="next-matrix"><h3>The two questions today: what credit did past its book, and what the stock did</h3><p class="cb-cap">How many listed companies stand in each cell today (click one to see them), and what that cell has meant for the stock over the next ' +
      esc(span((hz.filter(function (x) { return x.key === hk; })[0] || {}).words || "month")) + ", against the board's average stock. A cell is coloured only when its record is past 2 standard errors.</p>" + matrixHtml(doc, state, ev) + "</div>";
    h += '<div class="cb-ranked" id="next-record"><h3>The record, reading by reading</h3><div class="cb-tctl">' + B.seg("ev-group", groups.map(function (x) { return [x.key, x.words]; }), g.key, "Readings") +
      B.seg("ev-horizon", hz.map(function (x) { return [x.key, "Next " + span(x.words)]; }), hk, "Over") + "</div>" + recordTable(ev.readings.filter(function (x) { return x.group === g.key; }), ev, hk, ctx) + '<p class="cb-note">' + esc(RECORD_NOTE) + "</p></div>";
    var cds = isObj(ev.cds) && Array.isArray(ev.cds.readings) ? ev.cds : null;
    if (cds) {
      var ch = (Array.isArray(cds.horizons) ? cds.horizons : []).filter(isObj);
      h += '<div class="cb-ranked" id="next-cds"><h3>The traded CDS, week by week</h3><p class="cb-cap">' + esc(isNum(cds.names) ? String(cds.names) : "–") + " names whose CDS prints and that are tied to a listed stock, over " + esc(String(cds.weeks)) + " weeks from " + esc(day(cds.first)) +
        " (" + esc(isNum(cds.name_weeks) ? cds.name_weeks.toLocaleString("en-US") : "–") + " name-weeks). A CDS moved when it was " + esc(typeof cds.rule === "string" ? cds.rule : "") + '.</p><div class="cb-scroll" tabindex="0" role="region" aria-label="What the stock did after its CDS moved"><table class="cb-table" data-jh-nosort><thead><tr><th class="l">The week</th><th class="r">Name-weeks</th>' +
        ch.map(function (x) { return '<th class="r">Next ' + esc(span(x.words)) + '</th><th class="r">How sure</th>'; }).join("") + "</tr></thead><tbody>" + cds.readings.filter(isObj).map(function (x) {
          return '<tr><td class="l">' + esc(x.words) + '</td><td class="r">' + esc(isObj(x[ch[0].key]) && isNum(x[ch[0].key].n) ? x[ch[0].key].n.toLocaleString("en-US") : "–") + "</td>" + ch.map(function (c) {
            var v = isObj(x[c.key]) ? x[c.key] : {};
            return '<td class="r" style="color:' + textColour(isNum(v.mean) ? -v.mean : null, state.palette) + '">' + esc(pct(v.mean, true)) + '</td><td class="r"' + (isNum(v.t) && Math.abs(v.t) >= 3 ? ' style="font-weight:700"' : "") + ">" + esc(sure(v.t)) + "</td>"; }).join("") + "</tr>";
        }).join("") + '</tbody></table></div><p class="cb-note">The stock\'s return less the average stock of the names studied that week.</p></div>';
    }
    h += '<details><summary data-jh-title="What this record cannot see">What this record cannot see, and how it was measured</summary>' + (Array.isArray(ev.caveats) ? ev.caveats : []).filter(function (c) { return typeof c === "string"; }).map(function (c) { return '<p class="cb-note">' + esc(c) + "</p>"; }).join("") +
      '<p class="cb-note">' + esc(typeof ev.method === "string" ? ev.method : "") + "</p>" + (Array.isArray(ev.skipped) && ev.skipped.length ? '<p class="cb-note">Month-ends left out: ' + ev.skipped.map(function (x) { return esc(Array.isArray(x) ? day(x[0]) + " (" + x[1] + ")" : String(x)); }).join("; ") + ".</p>" : "") +
      '<p class="cb-note">Measured ' + esc(B.stamp(ev.generated_at)) + " with stocks to " + esc(day(ev.stocks_to)) + "; it is measured again when a month finishes.</p></details>";
    return h;
  }
  /* The board's own calls: kept each day as they were said, scored when their time is up. */
  function trackHtml(doc) {
    var tr = isObj(doc.track) ? doc.track : null;
    if (!tr) return "";
    var hs = (Array.isArray(tr.horizons) ? tr.horizons : []).filter(isObj);
    return '<p class="cb-note"><b>The board\'s own calls.</b> ' + (isNum(tr.calls_kept) && tr.calls_kept ? esc(String(tr.calls_kept)) + (tr.calls_kept === 1 ? " day" : " days") + " of calls kept, from " + esc(day(tr.first_call)) + ". " : "No day of calls is kept yet; the first is kept the next time the engine publishes. ") +
      hs.map(function (x) {
        var due = isObj(x.next_due) ? x.next_due : null;
        return "Over " + esc(x.words) + ": " + (x.days_scored ? esc(String(x.days_scored)) + " days scored, " + esc(isNum(x.stocks_scored) ? x.stocks_scored.toLocaleString("en-US") : "–") + " stocks" :
          "none scored yet" + (due && isNum(due.sessions_left) ? "; the first (" + esc(day(due.call_day)) + ") is due in " + esc(String(due.sessions_left)) + " sessions" : "")) + "."; }).join(" ") + "</p>" +
      hs.filter(function (x) { return Array.isArray(x.buckets) && x.buckets.length; }).map(function (x) { return "<h4>The board's own calls over " + esc(x.words) + "</h4>" + saidTable(x.buckets, "The board's own calls over " + x.words); }).join("");
  }

  /* The sheet. ctx: {palette, bonds: rows | "loading" | "error" | "unpublished", bondSort, history: doc | "loading" | "error" | null}. */
  function sheetHtml(doc, id, ctx) {
    ctx = ctx || {};
    ctx.doc = doc;
    var r = rowOf(doc, id);
    if (!r) return '<div class="cb-sheet-in"><div class="cb-sheet-h"><h3 id="sheet-title">Not on the board</h3><button type="button" data-sheet-close>Close</button></div><p class="cb-empty">The document holds no company under ' + esc(id) + ".</p></div>";
    var st = isObj(doc.stress) ? doc.stress : {}, mid = isObj(st.book_spread_bp) ? st.book_spread_bp[r.book] : null, layers = B.hasStress(doc) ? B.layerWords(doc) : {};
    var nm = nameRow(doc, r), cds = cdsRow(doc, r), theme = r.theme && B.hasStress(doc) ? B.themeOf(doc, r.theme) : null;
    var tags = (r.distressed ? '<span class="cb-tag">distressed</span> ' : "") + (theme ? '<span class="cb-tag">' + esc(theme.name) + (layers[r.layer] ? ": " + esc(layers[r.layer]) : "") + "</span> " : "") +
      (r.signal === "CREDIT_LEADS_DOWN" || r.signal === "CREDIT_LEADS_UP" ? '<span class="cb-tag">' + esc(SIGNAL_WORDS[r.signal]) + "</span> " : "") +
      (isNum(r.stress) && r.stress >= B.stressedAt(doc) ? '<span class="cb-tag">under stress</span>' : "");
    var kpis = kpi("Spread", esc(bp(r.spread_bp)) + " bp", (isNum(mid) && mid > 0 && isNum(r.spread_bp) ? esc(fixed(r.spread_bp / mid, 1)) + " times the middle of its book (" + esc(bp(mid)) + " bp)" : "") +
        (r.ccy && r.ccy !== "USD" ? " · over " + esc(r.ccy) + " government yields" : isNum(mid) ? "" : "over Treasuries")) +
      kpi("Stress rank", esc(n1(r.stress, 1)), "of 100 in its own book") +
      (cols(doc, "pd_1y") ? kpi("Implied default, 1 year", esc(isNum(r.pd_1y) ? pct(r.pd_1y) : "–"), "what the spread prices, at 40 cents recovered; " + esc(isNum(r.pd_5y) ? pct(r.pd_5y) : "–") + " over five") : "") +
      (cols(doc, "px") ? kpi("Bond price", esc(n1(r.px, 1)), "average, per 100 of face; lowest " + esc(n1(r.px_min, 1))) : "") +
      (cols(doc, "due2y") ? kpi("Due inside 2 years", esc(isNum(r.due2y) ? pct(r.due2y) : "–"), "of its bonds held; average maturity " + esc(n1(r.wam, 1)) + " years") : "") +
      (isNum(r.risk_3m) ? kpi("Chance of a 20% fall, 3 months", esc(pct(r.risk_3m)), "what its reading has meant; " + esc(isNum(r.risk_1m) ? pct(r.risk_1m) : "–") + " inside one month") : "") +
      kpi("Traded CDS", isNum(r.cds_bp) ? esc(bp(r.cds_bp)) + " bp" : "–", isNum(r.cds_bp) ? esc(bp(r.cds_d1m, true)) + " bp in a month" : "no level printed") +
      (r.ticker ? kpi("Stock, 1 month", '<span style="color:' + textColour(isNum(r.px_m1) ? -r.px_m1 : null, ctx.palette) + '">' + esc(pct(r.px_m1, true)) + "</span>", isNum(r.px_ytd) ? esc(pct(r.px_ytd, true)) + " this year" : "") : "");
    var hist = ctx.history === "loading" ? '<p class="cb-empty">Loading its history.</p>' : ctx.history === "error" ? '<p class="cb-empty">Its history could not be loaded.</p>' :
      !ctx.history ? '<p class="cb-empty">The month-by-month history is not published yet; it arrives with engine 3.0.</p>' :
      (function () { var pts = historyOf(ctx.history, r, doc); return pts ? chartHtml(pts, r, ctx) : '<p class="cb-empty">The history holds no month-end for it: its bonds were not in the dated holdings.</p>'; })();
    var skip = (doc.issuers.cols || []).concat(["d_spread_bp", "x_spread_bp", "from_spread_bp", "d_spread_w1", "d_spread_m3", "d_spread_ytd", "cds_d1w", "cds_d1m", "cds_d3m", "cds_bp", "cds_last",
      "d_price_w1", "d_price_m3", "d_price_ytd", "credit_direction", "bond_px", "n_bonds", "readings"]);
    return '<div class="cb-sheet-in"><div class="cb-sheet-h"><h3 id="sheet-title">' + (r.ticker ? "<b>" + esc(r.ticker) + "</b> " : "") + esc(r.name) + '</h3>' +
      (r.ticker ? '<a class="cb-sheet-a" data-t="' + esc(r.ticker) + '" href="/chart.html?symbol=' + encodeURIComponent(r.ticker) + '">Chart</a>' : "") + '<button type="button" data-show-co="' + esc(r.ticker || r.name) + '" data-sheet-close>Show it in the table</button>' +
      '<button type="button" data-sheet-close>Close</button></div>' +
      '<p class="cb-note">' + esc(r.industry || "") + " · " + esc((BOOK_WORDS[r.book] || r.book || "").toLowerCase()) + (r.country ? " · " + esc(r.country) : "") + " · " + esc(String(r.n_bonds)) + (r.n_bonds === 1 ? " bond" : " bonds") + " · " + esc(money(r.value)) +
      " held" + (r.ticker ? "" : " · no listed stock tied") + "</p>" + (tags ? "<p>" + tags + "</p>" : "") + '<div class="cb-kpis">' + kpis + "</div>" +
      '<p class="cb-verdict">' + esc(B.verdict(r)) + "</p>" +
      '<h4 id="sheet-next">What stocks that read this way did next</h4>' + outlookHtml(doc, r, ctx) +
      '<h4 id="sheet-history">Its spread at each month-end</h4>' + hist +
      '<h4 id="sheet-bonds">Its bonds</h4>' + bondsHtml(ctx.bonds, ctx.bondSort, ctx) +
      (funds() && (cols(doc, "f_n") || isObj(doc.funds)) ? '<h4 id="sheet-funds">Held by registered funds</h4>' + funds().sheetHtml(doc, r, ctx) : "") +
      '<h4 id="sheet-figures">Every figure the engine publishes for it</h4>' + figuresHtml(doc, r, ctx) +
      (nm ? '<h4 id="sheet-stock">Its listed-stock row</h4>' + pairsHtml(nm, skip, NAME_LABELS) : "") +
      (cds ? '<h4 id="sheet-cds">Its traded CDS row</h4>' + pairsHtml(cds, ["ticker"], CDS_LABELS) : "") + "</div>";
  }
  function cols(doc, key) { return B.isV2(doc) && (doc.issuers.cols || []).indexOf(key) >= 0; }

  // ------------------------------------------------------------------------------------------------ the whole document
  /* Every key the engine publishes at the top of its document: what it is, and where on the page it is drawn. A key the
     engine adds later that is not listed here is still shown below, under its own name. */
  var DOC_KEYS = {
    version: ["The engine's version.", "the foot of the page"], contract: ["The shape of the document.", "here"], generated_at: ["When the engine published.", "At a glance"],
    as_of: ["The holdings day the bonds are read on.", "At a glance"], bond_desk: ["The corporate bonds desk's own dates.", "here"], curve: ["The Treasury curve's day and source.", "At a glance"],
    stock_as_of: ["The newest close read.", "At a glance"], n_names: ["Listed names counted.", "Evidence status"], n_leads: ["Leads counted.", "Evidence status"],
    n_awaiting_history: ["Listed names with no reading for the comparison.", "Evidence status"], names: ["One row a listed name: the reading, the stock, the model's figures.", "Leads; a company's sheet"],
    leads: ["The first twenty lead rows.", "Leads"], issuers: ["One row a company: every column.", "Every company (choose Every column)"], industries: ["One row an industry, for every book.", "Industries"],
    themes: ["The AI stack, by layer.", "Credit stress"], matrix: ["How many listed companies stand in each of the nine credit / stock cells today.", "What the stock did next"],
    bonds: ["Where the bond-by-bond document is, and its columns.", "a company's sheet"], history: ["Where the month-by-month document is.", "a company's sheet"],
    evidence: ["What stocks did after each kind of reading, measured on the board's own history.", "What the stock did next"], track: ["The board's own calls, scored once their time is up.", "What the stock did next"],
    stress: ["The stress view's settings and counts.", "Credit stress"], cds: ["Every reference entity the CDS desk prints.", "Company CDS"], windows: ["The reference day of each comparison.", "Method"],
    primary: ["The window the rule reads.", "Method"], books: ["The books' names.", "here"], coverage: ["What was read, tied and left out.", "Coverage"],
    match: ["Ties made by a naming rule, and the largest issuers left untied.", "Coverage"], degraded: ["What the engine said was wrong with this run.", "Evidence status"], gaps: ["What the board does not cover.", "Declared gaps"],
    rules: ["The rules, in words.", "Method"], thresholds: ["The numbers the rules use.", "here"], method: ["The method, in a paragraph.", "Method"], attribution: ["Where each leg comes from.", "the foot of the page"],
    estimate_type: ["What is a market price and what is a model.", "here"], risk: ["How the chance of a fall is read, and how many stocks have one.", "What the stock did next"],
    funds: ["What registered funds hold of the board's companies (SEC Form N-PORT): what was tied and how, the widest gaps between marks, what is flagged, and the credit default swaps funds hold.", "Held by funds; a company's sheet"]
  };
  function sizeOf(v) {
    if (Array.isArray(v)) return v.length.toLocaleString("en-US") + (v.length === 1 ? " row" : " rows");
    if (isObj(v)) return Object.keys(v).length + (Object.keys(v).length === 1 ? " key" : " keys");
    return typeof v;
  }
  /* Any published value, drawn so that all of it can be read: a scalar as itself, a list of rows as a table, an object key by key. */
  function nodeHtml(v, path, state, depth) {
    var open = (state && state.dataOpen) || {};
    if (v == null) return '<span class="cb-dim">not published</span>';
    if (typeof v !== "object") return '<span class="cb-json">' + esc(String(v)) + "</span>";
    if (Array.isArray(v)) {
      if (!v.length) return '<span class="cb-dim">empty</span>';
      var rowsLike = v.every(function (x) { return isObj(x); }), cap = open[path] ? v.length : 25;
      if (rowsLike) {
        var keys = [];
        v.forEach(function (x) { Object.keys(x).forEach(function (k) { if (keys.indexOf(k) < 0) keys.push(k); }); });
        return '<div class="cb-scroll" tabindex="0" role="region" aria-label="' + esc(path) + '"><table class="cb-table" data-jh-nosort><thead><tr>' + keys.map(function (k) { return '<th class="l">' + esc(k) + "</th>"; }).join("") +
          "</tr></thead><tbody>" + v.slice(0, cap).map(function (x) {
            return "<tr>" + keys.map(function (k) { var c = x[k]; return '<td class="l">' + esc(c == null ? "–" : typeof c === "object" ? JSON.stringify(c) : String(c)) + "</td>"; }).join("") + "</tr>"; }).join("") +
          "</tbody></table></div>" + (v.length > cap ? '<div class="cb-pager"><button type="button" data-data-open="' + esc(path) + '">Show all ' + v.length.toLocaleString("en-US") + "</button></div>" : "");
      }
      return '<div class="cb-json">' + esc(v.slice(0, cap).map(function (x) { return typeof x === "object" ? JSON.stringify(x) : String(x); }).join(open[path] ? "\n" : " · ")) + "</div>" +
        (v.length > cap ? '<div class="cb-pager"><button type="button" data-data-open="' + esc(path) + '">Show all ' + v.length.toLocaleString("en-US") + "</button></div>" : "");
    }
    if (depth >= 3) return '<div class="cb-json">' + esc(JSON.stringify(v)) + "</div>";
    return '<dl class="cb-pairs">' + Object.keys(v).map(function (k) {
      return "<dt>" + esc(k) + ' <span class="cb-key">' + esc(sizeOf(v[k])) + "</span></dt><dd>" + nodeHtml(v[k], path + "." + k, state, depth + 1) + "</dd>"; }).join("") + "</dl>";
  }
  function dataHtml(doc, state) {
    if (!isObj(doc)) return "<p>The document could not be read.</p>";
    var files = [["data/credit-before-equity.json", "The board's document: everything on this page"]];
    ["bonds", "history", "funds"].forEach(function (k) { var m = doc[k]; if (isObj(m) && typeof m.key === "string") files.push([m.key, typeof m.words === "string" ? m.words : DOC_KEYS[k] ? DOC_KEYS[k][0] : k]); });
    var cols = B.isV2(doc) ? allColumns(doc) : [], t = B.isV2(doc) ? table(doc) : [];
    var h = '<p>Nothing the engine publishes is kept off this page. The files it writes: ' + files.map(function (f) {
      return '<a href="' + esc(BASE + f[0]) + '" rel="noopener">' + esc(f[0].replace("data/", "")) + "</a> (" + esc(f[1].replace(/\.$/, "").toLowerCase()) + ")"; }).join(" · ") + ".</p>";
    if (cols.length) {
      h += '<details><summary data-jh-title="Every column"><b>issuers.cols</b> — the ' + cols.length + " columns of a company row: what each is, and how many of the " + t.length.toLocaleString("en-US") + ' companies have it</summary><div class="cb-scroll" tabindex="0" role="region" aria-label="Every column">' +
        '<table class="cb-table cb-figs" data-jh-nosort><thead><tr><th class="l">Column</th><th class="l">Group</th><th class="r">Companies with it</th><th class="l">What it is</th></tr></thead><tbody>' + cols.map(function (f) {
          var n = t.filter(function (r) { return r[f[0]] != null && r[f[0]] !== "" && r[f[0]] !== 0 || r[f[0]] === 0 && f[2] !== "flag"; }).length;
          return '<tr><td class="l">' + esc(f[1]) + ' <span class="cb-key">' + esc(f[0]) + '</span></td><td class="l">' + esc(GROUPS[f[3]] || "Published without a description") + '</td><td class="r">' + n.toLocaleString("en-US") +
            '</td><td class="l cb-what">' + esc(f[4]) + "</td></tr>"; }).join("") + "</tbody></table></div></details>";
    }
    Object.keys(doc).forEach(function (k) {
      var v = doc[k], info = DOC_KEYS[k] || ["Published by the engine; this page has no description for it yet.", "here"];
      var body = k === "issuers" ? '<p class="cb-note">The company table above. Choose "Every column" there to see all of it, or download it as a file.</p>' : nodeHtml(v, k, state, 0);
      h += "<details" + ((state && state.dataOpen && state.dataOpen["key:" + k]) ? " open" : "") + '><summary data-jh-title="' + esc(k) + '"><b>' + esc(k) + "</b> — " + esc(info[0]) + ' <span class="cb-dim">' + esc(sizeOf(v)) + " · drawn in: " + esc(info[1]) + "</span></summary>" + body + "</details>";
    });
    return h;
  }
  /* Clicks that belong to what this file draws. Returns true when it changed the state. */
  function click(d, state) {
    if (d.dataOpen) { state.dataOpen[d.dataOpen] = true; state.dataOpen["key:" + d.dataOpen.split(".")[0]] = true; return true; }
    if (d.evHorizon) { state.evHorizon = d.evHorizon; return true; }
    if (d.evGroup) { state.evGroup = d.evGroup; return true; }
    return false;
  }

  root.JHCreditSheet = { DOC_KEYS: DOC_KEYS, nodeHtml: nodeHtml, dataHtml: dataHtml, click: click, nextHtml: nextHtml, matrixHtml: matrixHtml, riskBars: riskBars, riskGrid: riskGrid, saidTable: saidTable, trackHtml: trackHtml,
    evidenceOf: evidenceOf, readingOf: readingOf, bandWords: bandWords, RECORD_NOTE: RECORD_NOTE, FIELDS: FIELDS, FIELD: FIELD, GROUPS: GROUPS, BOND_FIELDS: BOND_FIELDS, BOOK_WORDS: BOOK_WORDS, CELL_WORDS: CELL_WORDS, ICON_DISTRESS: ICON_DISTRESS, BASE: BASE,
    usd: usd, SERIES: SERIES, niceTicks: niceTicks, allColumns: allColumns, align: align, shown: shown, fieldCell: fieldCell, csvText: csvText, distressed: distressed, distressButton: distressButton, distressTile: distressTile, riskTile: riskTile,
    rowOf: rowOf, idOf: idOf, nameButton: nameButton, nameRow: nameRow, cdsRow: cdsRow, figuresHtml: figuresHtml, pairsHtml: pairsHtml, bondsOf: bondsOf, bondsHtml: bondsHtml,
    historyOf: historyOf, chartHtml: chartHtml, outlookHtml: outlookHtml, readingsOf: readingsOf, recordTable: recordTable, sheetHtml: sheetHtml };
})(typeof window !== "undefined" ? window : globalThis);
