/* jh-inventory-v2.js
   Client-side upgrades for inventory-drawdown.
   Uses only fields present on inventory-drawdown + backlog + estimate-revisions + earnings-quality.
   RM/WIP/FG stay null unless those tags are on the row. Never invent composition. */
(function (root) {
  if (root.__jhInvV2) return;
  root.__jhInvV2 = true;

  function up(t) { return String(t == null ? "" : t).toUpperCase().trim(); }
  function num(v) {
    if (v == null || v === "") return null;
    var n = Number(v);
    return isFinite(n) ? n : null;
  }
  function median(arr) {
    var a = arr.filter(function (x) { return x != null && isFinite(x); }).sort(function (x, y) { return x - y; });
    if (!a.length) return null;
    var m = Math.floor(a.length / 2);
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }
  function stdev(arr) {
    var a = arr.filter(function (x) { return x != null && isFinite(x); });
    if (a.length < 2) return null;
    var m = a.reduce(function (s, x) { return s + x; }, 0) / a.length;
    var v = a.reduce(function (s, x) { return s + (x - m) * (x - m); }, 0) / (a.length - 1);
    return Math.sqrt(v);
  }

  function compositionFromRow(r) {
    return {
      raw: num(r.inventory_raw != null ? r.inventory_raw : r.InventoryRawMaterials),
      wip: num(r.inventory_wip != null ? r.inventory_wip : r.InventoryWorkInProcess),
      fg: num(r.inventory_fg != null ? r.inventory_fg : r.InventoryFinishedGoods)
    };
  }

  function isCommodityInventory(r) {
    var s = String(r.industry || "") + " " + String(r.sector || "");
    return /oil|gas refin|crude|petroleum|refining/i.test(s);
  }

  function peerAbnormal(rows) {
    var by = {};
    rows.forEach(function (r) {
      var g = r.industry || r.sector || "_";
      if (!by[g]) by[g] = [];
      by[g].push(num(r.dio_chg_pct));
    });
    var med = {}, zden = {};
    Object.keys(by).forEach(function (g) {
      med[g] = median(by[g]);
      zden[g] = stdev(by[g]);
    });
    return rows.map(function (r) {
      var g = r.industry || r.sector || "_";
      var chg = num(r.dio_chg_pct);
      var m = med[g];
      var sd = zden[g];
      var n = (by[g] || []).filter(function (x) { return x != null; }).length;
      var abn = (chg != null && m != null) ? chg - m : null;
      var z = (abn != null && sd && sd > 0 && n >= 3) ? abn / sd : null;
      return Object.assign({}, r, {
        peer_group: g,
        peer_n: n,
        peer_median_dio_chg: m,
        dio_abnormal: abn,
        dio_z: z
      });
    });
  }

  function bookState(backlogRow) {
    if (!backlogRow) return { book: null, rpo_yoy: null, accelerating: false };
    var y = num(backlogRow.rpo_yoy);
    return {
      book: y == null ? null : (y > 0 ? "UP" : (y < 0 ? "DOWN" : "FLAT")),
      rpo_yoy: y,
      accelerating: !!(backlogRow.demand_accelerating || backlogRow.deferred_accelerating)
    };
  }

  function streetState(revRow) {
    if (!revRow) return { direction: null, eps_rev_pct: null };
    return {
      direction: revRow.direction || null,
      eps_rev_pct: num(revRow.eps_rev_pct),
      rev_rev_pct: num(revRow.rev_rev_pct)
    };
  }

  function fourState(r, book, street) {
    var dio = num(r.dio_chg_pct);
    var rev = num(r.rev_growth_yoy);
    var bookDir = book && book.book;
    var bookUp = bookDir === "UP" || (book && book.accelerating);
    var bookDown = bookDir === "DOWN";
    var salesUp = rev != null && rev > 0;
    var salesDown = rev != null && rev <= 0;
    if (dio == null) return { state: null, why: "no DIO change" };
    if (isCommodityInventory(r)) {
      return { state: "COMMODITY", why: "refiner/crude inventory is a price book, not a shelf" };
    }
    if (dio < 0 && (bookUp || (bookDir == null && salesUp))) {
      if (bookUp && salesUp) return { state: "SHORTAGE", why: "DIO down + book up + sales up" };
      if (bookUp) return { state: "SHORTAGE", why: "DIO down + booked demand up" };
      return { state: "SHORTAGE", why: "DIO down + sales up (book not tagged)" };
    }
    if (dio < 0 && (bookDown || salesDown)) {
      return { state: "DESTOCK", why: "DIO down with demand not rising" };
    }
    if (dio > 0 && (bookUp || (bookDir == null && salesUp))) {
      return { state: "RESTOCK", why: "DIO up into rising demand" };
    }
    if (dio > 0) {
      return { state: "STUFFED", why: "DIO up with demand flat/down" };
    }
    return { state: "NEUTRAL", why: "DIO unchanged" };
  }

  function chainTape(sectors) {
    function pick(name) {
      return (sectors || []).find(function (s) {
        return String(s.sector || "").toLowerCase() === name.toLowerCase() || s.series === name;
      }) || null;
    }
    var factory = pick("Manufacturing") || pick("MNFCTRIRSA");
    var wholesale = pick("Wholesale") || pick("WHLSLRIRSA");
    var store = pick("Retail") || pick("RETAILIRSA");
    var auto = pick("Autos") || pick("AISRSA");
    function dir(s) {
      if (!s) return null;
      var chg = num(s.chg_6m != null ? s.chg_6m : s.chg_3m);
      if (chg == null) return null;
      return chg < 0 ? "LEAN" : "FAT";
    }
    var f = dir(factory), w = dir(wholesale), r = dir(store), a = dir(auto);
    var read = "";
    if (f === "LEAN" && r === "LEAN") read = "Factory and store both lean — real tightness if books are up.";
    else if (f === "LEAN" && r === "FAT") read = "Factory lean, store fat — bullwhip. Wholesale/manufacturer squeeze, not a store shortage.";
    else if (f === "FAT" && r === "LEAN") read = "Factory fat, store lean — destock starting at the shelf.";
    else if (f === "FAT" && r === "FAT") read = "Factory and store both building — not a shortage tape.";
    if (a === "FAT" && r === "LEAN") read += " Autos are building while broad retail draws — do not let vehicles hide the rest of retail.";
    return {
      factory: factory, wholesale: wholesale, store: store, auto: auto,
      factory_dir: f, wholesale_dir: w, store_dir: r, auto_dir: a,
      read: read
    };
  }

  function joinRows(board, backlog, revisions, quality) {
    var radar = (backlog && backlog.by_ticker) || {};
    var street = {};
    var src = (revisions && revisions.by_ticker) || revisions || {};
    Object.keys(src).forEach(function (k) {
      var row = src[k];
      if (!row || typeof row !== "object") return;
      if (row.engine || row.n_tracked) return;
      var t = up(row.ticker || k);
      if (!t || t.length > 8) return;
      if (row.current_eps_est == null && row.eps_rev_pct == null && row.direction == null) return;
      street[t] = row;
    });
    var qmap = {};
    ((quality && quality.all_ranked) || (quality && quality.top_20_high_quality) || []).forEach(function (q) {
      var t = up(q.ticker);
      if (t) qmap[t] = q;
    });
    var scored = peerAbnormal(board || []);
    return scored.map(function (r) {
      var t = up(r.ticker);
      var b = bookState(radar[t]);
      var s = streetState(street[t]);
      var q = qmap[t] || null;
      var fs = fourState(r, b, s);
      var comp = compositionFromRow(r);
      return Object.assign({}, r, {
        rpo_yoy: b.rpo_yoy,
        book: b.book,
        book_accel: b.accelerating,
        street_dir: s.direction,
        eps_rev_pct: s.eps_rev_pct,
        sloan: q ? num(q.sloan_accruals_pct_assets) : null,
        dsri: q ? num(q.dsri_beneish) : null,
        cash_conv: q ? num(q.cash_conversion_ratio) : null,
        four_state: fs.state,
        four_why: fs.why,
        raw: comp.raw, wip: comp.wip, fg: comp.fg,
        commodity: isCommodityInventory(r)
      });
    });
  }

  var api = {
    num: num, median: median, peerAbnormal: peerAbnormal, fourState: fourState,
    chainTape: chainTape, joinRows: joinRows, compositionFromRow: compositionFromRow,
    isCommodityInventory: isCommodityInventory
  };
  root.jhInventoryV2 = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
