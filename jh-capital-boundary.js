(function (root) {
  'use strict';
  const BASIS = 'capital-flow-stock-score-retired.v1';
  function describe(packet) {
    const revised = packet?.capital_flow_exclusion?.basis === BASIS;
    return {
      revised,
      title: revised ? 'CapitalFlow stock-flow inference excluded' : 'CapitalFlow calculation revision unconfirmed',
      text: revised
        ? 'Reported holdings-value changes contribute no direct CapitalFlow stock score, distress flag or trade narrative here. This excludes one input; it does not establish independent evidence, predictive returns or position sizes for the other inputs.'
        : 'This stored output may include the retired CapitalFlow inference. Treat its scores and trade narratives as unqualified historical calculations until the producer refreshes.',
      asof: typeof packet?.generated_at === 'string' ? packet.generated_at : 'unavailable'
    };
  }
  function render(element, packet) {
    if (!element) return;
    const state = describe(packet), doc = element.ownerDocument;
    element.replaceChildren(); element.className = 'jh-holdings-boundary';
    element.dataset.status = state.revised ? 'excluded' : 'unconfirmed';
    const title = doc.createElement('strong'); title.textContent = state.title;
    const text = doc.createElement('p'); text.textContent = state.text;
    const clock = doc.createElement('small'); clock.textContent = 'Displayed output: ' + state.asof + ' · ';
    const link = doc.createElement('a'); link.href = '/capital-flow.html'; link.textContent = 'Inspect the source research';
    clock.append(link); element.append(title, text, clock);
  }
  function narrativeRows(packet) {
    if (!describe(packet).revised || !Array.isArray(packet.quiet_accumulation)) return [];
    return packet.quiet_accumulation.filter(row => row && typeof row.ticker === 'string' && row.ticker
      && row.tape === 'Upstream dislocation screen: cheap & inflecting');
  }
  const api = { describe, render, narrativeRows };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.JHCapitalBoundary = api;
})(typeof window === 'object' ? window : globalThis);
