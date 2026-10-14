#!/usr/bin/env python3
"""Show the horizon move when the cascade book has no scores.

Cascade Score was painting a dash and avg 0 because Number('') is 0 and
today's ranked tiers are empty. Fall back to the performance change, and
fill week/month/quarter/year for the whole board instead of only the
tiles already on screen.
"""
from pathlib import Path

PATH = Path("chart-pro.html")

PAINT_OLD = """    const perf = this.perfValue(metric);
    const score = this.cascadeScore(symbol);
    const sources = this.signalSources(symbol);
    const value = st.mode === 'perf' ? perf : st.mode === 'cascade' ? score : sources.length;
    cell.classList.remove('loading');
    cell.style.background = this.colorFor(st.mode, value, st.horizon);
    cell.dataset.value = Number.isFinite(value) ? String(value) : '';
    const ch = cell.querySelector('.hm-ch');
    if (st.mode === 'perf') ch.textContent = this.formatChange(perf);
    else if (st.mode === 'cascade') ch.textContent = Number.isFinite(score) ? score.toFixed(0) : '—';
    else ch.textContent = `${sources.length} signal${sources.length === 1 ? '' : 's'}`;
    ch.className = 'hm-ch ' + (st.mode === 'perf' ? (perf > 0 ? 'up' : perf < 0 ? 'down' : 'na') : 'neutral');"""

PAINT_NEW = """    const perf = this.perfValue(metric);
    const score = this.cascadeScore(symbol);
    const sources = this.signalSources(symbol);
    const useCascade = st.mode === 'cascade' && Number.isFinite(score);
    const useSignals = st.mode === 'signals' && sources.length > 0;
    const value = useCascade ? score : useSignals ? sources.length : perf;
    const paintMode = useCascade ? 'cascade' : useSignals ? 'signals' : 'perf';
    cell.classList.remove('loading');
    cell.style.background = this.colorFor(paintMode, value, st.horizon);
    cell.dataset.value = Number.isFinite(value) ? String(value) : '';
    cell.dataset.paintMode = paintMode;
    const ch = cell.querySelector('.hm-ch');
    if (useCascade) ch.textContent = score.toFixed(0);
    else if (useSignals) ch.textContent = `${sources.length} signal${sources.length === 1 ? '' : 's'}`;
    else ch.textContent = this.formatChange(perf);
    ch.className = 'hm-ch ' + (paintMode === 'perf' ? (perf > 0 ? 'up' : perf < 0 ? 'down' : 'na') : 'neutral');"""

AVG_OLD = "const values = [...grid.querySelectorAll('.hm-cell')].map(c => Number(c.dataset.value)).filter(v => Number.isFinite(v));"
AVG_NEW = """const cells = [...grid.querySelectorAll('.hm-cell')];
      const values = cells.map(c => {
        const raw = c.dataset.value;
        if (raw == null || raw === '') return NaN;
        return Number(raw);
      }).filter(v => Number.isFinite(v));
      const paintMode = (cells.find(c => c.dataset.paintMode) || {}).dataset?.paintMode || st.mode;"""

LABEL_OLD = "node.textContent = st.mode === 'perf' ? `avg ${this.formatChange(avg)}` : st.mode === 'cascade' ? `avg ${avg.toFixed(0)}` : `avg ${avg.toFixed(1)} signals`;"
LABEL_NEW = "node.textContent = paintMode === 'perf' ? `avg ${this.formatChange(avg)}` : paintMode === 'cascade' ? `avg ${avg.toFixed(0)}` : `avg ${avg.toFixed(1)} signals`;"

CLASS_OLD = "node.className = 'hm-sector-avg ' + (st.mode === 'perf' ? (avg > 0 ? 'up' : avg < 0 ? 'down' : '') : '');"
CLASS_NEW = "node.className = 'hm-sector-avg ' + (paintMode === 'perf' ? (avg > 0 ? 'up' : avg < 0 ? 'down' : '') : '');"

HIST_OLD = """    this.paintQuotes(market, token).then(() => {
      if (token !== this.renderToken) return;
      const missing = symbols.filter(symbol => !this.metricsBySymbol.get(symbol));
      if (market.length && missing.length > market.length * 0.8) return;
      missing.forEach(symbol => this.queueHistory(symbol, token));
    });"""
HIST_NEW = """    this.paintQuotes(market, token).then(() => {
      if (token !== this.renderToken) return;
      symbols.forEach(symbol => this.queueHistory(symbol, token));
    });"""

LEGEND_OLD = """? `<span class="hm-lg">0</span><span class="hm-lg-bar cascade"></span><span class="hm-lg">200</span><span class="hm-lg-note">cascade combined score</span>`"""
LEGEND_NEW = """? `<span class="hm-lg">0</span><span class="hm-lg-bar cascade"></span><span class="hm-lg">200</span><span class="hm-lg-note">cascade score, or ${st.horizon} change when today's book is empty</span>`"""


def replace_once(text, old, new, label):
    if new in text:
        print("already", label)
        return text
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label} needle count {n}")
    print("patched", label)
    return text.replace(old, new, 1)


def main():
    text = PATH.read_text(encoding="utf-8")
    text = replace_once(text, PAINT_OLD, PAINT_NEW, "paintCell fallback")
    text = replace_once(text, AVG_OLD, AVG_NEW, "average blanks")
    text = replace_once(text, LABEL_OLD, LABEL_NEW, "average label")
    text = replace_once(text, CLASS_OLD, CLASS_NEW, "average color")
    text = replace_once(text, HIST_OLD, HIST_NEW, "history for every tile")
    text = replace_once(text, LEGEND_OLD, LEGEND_NEW, "cascade legend")
    PATH.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
