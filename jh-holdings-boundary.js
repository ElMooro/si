(function (root) {
  'use strict';
  function describe(packet) {
    const q = packet && packet.holdings_exclusions;
    const revised = q && q.basis === 'holdings-direct-and-cluster-excluded.v1';
    return {
      revised: Boolean(revised),
      title: revised ? '13F trading inferences excluded' : '13F contribution status unconfirmed',
      text: revised
        ? 'Direct holdings and smart-money cluster paths do not contribute to this calculation’s score, agreement count or weight denominator. Other indirect paths, input independence and predictive performance remain unverified.'
        : 'This displayed packet does not identify the revised calculation. Its scores may still include legacy holdings inferences.',
      asof: typeof packet?.generated_at === 'string' ? packet.generated_at : (typeof packet?.as_of === 'string' ? packet.as_of : 'unavailable')
    };
  }
  function render(element, packet) {
    if (!element) return;
    const state = describe(packet);
    element.replaceChildren();
    element.className = 'jh-holdings-boundary';
    element.dataset.status = state.revised ? 'excluded' : 'unconfirmed';
    const doc = element.ownerDocument;
    const title = doc.createElement('strong'); title.textContent = state.title;
    const text = doc.createElement('p'); text.textContent = state.text;
    const clock = doc.createElement('small'); clock.textContent = 'Displayed packet: ' + state.asof + ' · ';
    const link = doc.createElement('a'); link.href = '/holdings-research.html'; link.textContent = 'Inspect original disclosures';
    clock.append(link); element.append(title, text, clock);
  }
  const api = { describe, render };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.JHHoldingsBoundary = api;
})(typeof window === 'object' ? window : globalThis);
