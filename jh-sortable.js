/* jh-sortable.js — every table on every page sorts by any column, ascending or descending (Khalid 2026-11-04).
 *
 * Injected site-wide by scripts/reskin_site.py (like jh-footer.js / jh-nav-drawer.js). Works on tables that exist at
 * load and on tables that page scripts render later (MutationObserver). Click a header to sort; click again to flip;
 * a third click restores the original row order. Keyboard: Tab to a header, Enter or Space.
 *
 * Values are read from the cell text as the page shows them: money ($1.49B, −$2K, +$13.34B), percentages (+5.2%),
 * counts with separators (1,322), ratios (0.46×), dates (2026-06-30, 2026-11-04 17:12 UTC), and plain text; a dash,
 * "—" or an empty cell sorts last in both directions. A table opts out with data-jh-nosort; a header that already has
 * its own sorting (aria-sort, data-sort, onclick, a .sort class) is left alone. Pure client behaviour: nothing is
 * fetched, nothing is written, no row content changes. A table without a <thead> sorts when its first row is made of
 * <th> cells; header cells that span columns are left alone. */
(function () {
  'use strict';
  if (window.__jhSortable) return;
  window.__jhSortable = true;
  var SEEN = new WeakSet();
  var MULT = { K: 1e3, M: 1e6, B: 1e9, T: 1e12 };
  var MINUS = /[−–—]/g;

  function parseValue(text) {
    var t = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
    if (!t || t === '—' || t === '-' || t === '–' || /^n\/?a$/i.test(t) || /^none$/i.test(t)) return { k: 2, v: 0, s: '' };
    var d = t.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?/);
    if (d) {
      var ts = Date.UTC(+d[1], +d[2] - 1, +d[3], +(d[4] || 0), +(d[5] || 0), +(d[6] || 0));
      if (isFinite(ts)) return { k: 0, v: ts, s: t.toLowerCase() };
    }
    var m = t.replace(MINUS, '-').match(/^([+-]?)\s*\$?\s*([0-9][0-9,]*\.?[0-9]*|\.[0-9]+)\s*(K|M|B|T)?\s*(%|×|x|pp|d|h|s|B)?\b/i);
    if (m && m[2]) {
      var n = parseFloat(m[2].replace(/,/g, ''));
      if (isFinite(n)) {
        if (m[3]) n *= MULT[m[3].toUpperCase()] || 1;
        if (m[1] === '-') n = -n;
        return { k: 0, v: n, s: t.toLowerCase() };
      }
    }
    return { k: 1, v: 0, s: t.toLowerCase() };
  }

  function cellText(tr, i) {
    var cells = tr.cells;
    if (!cells || i >= cells.length) return '';
    var c = cells[i];
    var b = c.querySelector('b,.tk,.v');
    return (b && b.textContent && b.textContent.trim().length ? b.textContent + ' ' : '') + c.textContent;
  }

  function sortTable(table, idx, dir) {
    var rows = bodyRows(table, table.__jhHead);
    if (!rows.length) return;
    var body = rows[0].parentNode;
    if (!body) return;
    rows.forEach(function (r, i) { if (!r.hasAttribute('data-jh-i')) r.setAttribute('data-jh-i', String(i)); });   // original order, stamped once per row
    if (dir === 0) {
      rows.slice().sort(function (a, b) { return (+a.getAttribute('data-jh-i')) - (+b.getAttribute('data-jh-i')); }).forEach(function (r) { body.appendChild(r); });
      return;
    }
    var keyed = rows.map(function (r, i) { return { r: r, i: i, p: parseValue(cellText(r, idx)) }; });
    keyed.sort(function (a, b) {
      if (a.p.k !== b.p.k) return a.p.k - b.p.k;                 // numbers/dates, then text, then blanks (both directions)
      var c = a.p.k === 0 ? a.p.v - b.p.v : (a.p.s < b.p.s ? -1 : a.p.s > b.p.s ? 1 : 0);
      if (c === 0) return a.i - b.i;
      return a.p.k === 2 ? 0 : dir * c;
    });
    keyed.forEach(function (k) { body.appendChild(k.r); });
  }

  function mark(table, th, dir) {
    var ths = (table.__jhHead || table.tHead || table).querySelectorAll('th');
    Array.prototype.forEach.call(ths, function (h) {
      h.removeAttribute('aria-sort');
      var a = h.querySelector('.jh-sort-arrow');
      if (a) a.textContent = '';
    });
    if (dir) {
      th.setAttribute('aria-sort', dir > 0 ? 'ascending' : 'descending');
      var arrow = th.querySelector('.jh-sort-arrow');
      if (arrow) arrow.textContent = dir > 0 ? ' ▲' : ' ▼';
    }
  }

  function headerHasOwnSorting(th) {
    return th.hasAttribute('onclick') || th.hasAttribute('data-sort') || th.hasAttribute('data-key') || th.hasAttribute('aria-sort') || /\bsort/i.test(th.className || '') || th.querySelector('button,a,input,select');
  }

  function headerRow(table) {
    if (table.tHead && table.tHead.rows.length) return table.tHead.rows[table.tHead.rows.length - 1];
    var first = table.rows[0];                                              // no <thead>: a first row made of <th> cells is the header
    if (first && first.cells.length && Array.prototype.every.call(first.cells, function (c) { return c.tagName === 'TH'; })) return first;
    return null;
  }

  function bodyRows(table, head) {
    var rows = [];
    if (table.tBodies.length) {
      for (var i = 0; i < table.tBodies.length; i++) rows = rows.concat(Array.prototype.slice.call(table.tBodies[i].rows));
    } else {
      rows = Array.prototype.slice.call(table.rows);                       // rows appended straight to the <table> (script-built widgets)
    }
    return rows.filter(function (r) { return r !== head; });
  }

  function attach(table) {
    if (SEEN.has(table)) return;
    if (table.hasAttribute('data-jh-nosort') || table.closest('[data-jh-nosort]')) return;
    var head = headerRow(table);
    if (!head || bodyRows(table, head).length < 1) return;
    var ths = Array.prototype.slice.call(head.cells);
    if (!ths.length || ths.some(function (th) { return th.colSpan > 1; })) return;
    if (ths.some(headerHasOwnSorting)) return;
    SEEN.add(table);
    table.__jhHead = head;
    table.setAttribute('data-jh-sortable', '1');
    ths.forEach(function (th, idx) {
      th.style.cursor = 'pointer';
      th.setAttribute('tabindex', '0');
      th.setAttribute('role', 'button');
      th.setAttribute('title', 'Sort by ' + (th.textContent || '').trim() + ' (click again to flip, a third time to restore)');
      var arrow = document.createElement('span');
      arrow.className = 'jh-sort-arrow';
      arrow.setAttribute('aria-hidden', 'true');
      th.appendChild(arrow);
      var go = function (ev) {
        if (ev && ev.type === 'keydown' && ev.key !== 'Enter' && ev.key !== ' ') return;
        if (ev) ev.preventDefault();
        var probe = bodyRows(table, table.__jhHead)[0] || { cells: [] };
        var first = parseValue(cellText(probe, idx)).k === 0 ? -1 : 1;   // numbers: largest first; text: A→Z
        var cur = table.__jhSortIdx === idx ? table.__jhSortDir : 0;
        var next = cur === 0 ? first : cur === first ? -first : 0;
        table.__jhSortIdx = idx;
        table.__jhSortDir = next;
        sortTable(table, idx, next);
        mark(table, th, next);
      };
      th.addEventListener('click', go);
      th.addEventListener('keydown', go);
    });
  }

  function scan(root) {
    var tables = (root && root.querySelectorAll ? root.querySelectorAll('table') : []);
    Array.prototype.forEach.call(tables, attach);
    if (root && root.tagName === 'TABLE') attach(root);
  }

  var pending = null;
  function schedule() {
    if (pending) return;
    pending = setTimeout(function () { pending = null; scan(document.body); }, 120);
  }

  function start() {
    scan(document.body);
    try {
      new MutationObserver(function (muts) {
        for (var i = 0; i < muts.length; i++) {
          if (muts[i].addedNodes && muts[i].addedNodes.length) { schedule(); return; }
        }
      }).observe(document.body, { childList: true, subtree: true });
    } catch (e) { /* no observer: tables present at load still sort */ }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
