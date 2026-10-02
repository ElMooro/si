/* Research calculation availability. No fetch, inference, or trade permission. */
(function (root) {
  'use strict';
  function summary(packet) {
    const q = packet && packet.numeric_quality;
    const count = v => Number.isSafeInteger(v) && v >= 0;
    if (!q || q.contract !== 'ranker-numeric.v1') return 'Numeric validation is not reported by this packet.';
    if (!['usable', 'partial', 'unavailable'].includes(q.status) ||
        ![q.input_tickers, q.rankable_tickers, q.unranked_tickers].every(count) ||
        q.rankable_tickers + q.unranked_tickers !== q.input_tickers ||
        (q.status === 'unavailable') !== (q.rankable_tickers === 0) ||
        (q.status === 'partial') !== (q.rankable_tickers > 0 && q.unranked_tickers > 0))
      return 'Reported numeric validation is inconsistent; inspect the calculation record.';
    return `${q.rankable_tickers} of ${q.input_tickers} tickers have a usable calculation; ${q.unranked_tickers} withheld for unavailable inputs. Zero is a measured score, not a missing value.`;
  }
  function render(packet, id) {
    const host = root.document && root.document.getElementById(id);
    if (!host) return;
    host.replaceChildren();
    host.style.cssText = 'min-width:0;max-width:100%;margin:12px 0;padding:12px;border:1px solid var(--line,#384555);border-radius:6px;overflow-wrap:anywhere;font-size:12px;line-height:1.5';
    const p = root.document.createElement('p'); p.textContent = summary(packet); host.appendChild(p);
    const note = root.document.createElement('p');
    note.textContent = 'Calculation checks do not establish independent evidence, forecast skill, or portfolio suitability.'; host.appendChild(note);
    const details = root.document.createElement('details'), label = root.document.createElement('summary'), pre = root.document.createElement('pre');
    label.textContent = 'Inspect calculation and withholding records'; details.appendChild(label);
    pre.tabIndex = 0; pre.setAttribute('aria-label', 'Complete reported ranker calculations');
    pre.style.cssText = 'max-height:320px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;max-width:100%;font-size:11px';
    pre.textContent = JSON.stringify({numeric_quality: packet?.numeric_quality ?? null,
      calibration_quality: packet?.calibration_quality ?? null,
      ranked_calculations: Array.isArray(packet?.top_tickers) ? packet.top_tickers.map(row => ({ticker: row?.ticker ?? null, score: row?.score ?? null, calculation: row?.score_calculation ?? null})) : null,
      unranked_tickers: packet?.unranked_tickers ?? null}, null, 2);
    details.appendChild(pre); host.appendChild(details);
  }
  root.JHRankerNumeric = {summary, render};
  if (typeof module !== 'undefined' && module.exports) module.exports = {summary, render};
})(typeof window === 'undefined' ? globalThis : window);
