// Search cache lifetime can shorten, but never extend, the native check window.
function integer(value) {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) return null;
  const n = Number(value);
  return Number.isSafeInteger(n) ? n : null;
}

export function searchDeadline(upstream, started, cap = 120) {
  if (!Number.isSafeInteger(started) || started < 0 || upstream.status !== 200 ||
      upstream.headers.has('Set-Cookie') || upstream.headers.get('Vary')?.trim() === '*') return started;
  const directives = (upstream.headers.get('Cache-Control') || '').toLowerCase().split(',').map(s => s.trim());
  if (directives.some(s => /^(?:private|no-store|no-cache)(?:\s*=|$)/.test(s))) return started;
  const limits = [];
  for (const name of ['max-age', 's-maxage']) {
    const matches = directives.filter(s => new RegExp('^' + name + '(?:\\s*=|$)').test(s));
    if (matches.length > 1 || (name === 'max-age' && matches.length !== 1)) return started;
    if (!matches.length) continue;
    const match = matches[0].match(new RegExp('^' + name + '\\s*=\\s*(?:"(\\d+)"|(\\d+))$'));
    const n = match && integer(match[1] || match[2]);
    if (n === null || n === false) return started;
    limits.push(n);
  }
  const ageHeader = upstream.headers.get('Age');
  const age = ageHeader === null ? 0 : integer(ageHeader);
  if (age === null) return started;
  return started + Math.max(0, Math.min(cap, ...limits) - age) * 1000;
}

export function searchCacheSeconds(response, now, cap = 120) {
  const started = integer(response.headers.get('X-Symdir-Cached-At'));
  const until = integer(response.headers.get('X-Symdir-Cache-Until'));
  if (!Number.isSafeInteger(now) || started === null || until === null ||
      started > now || until < started || until - started > cap * 1000) return 0;
  return Math.max(0, Math.min(cap, Math.floor((until - now) / 1000)));
}

export function searchCacheControl(seconds) {
  return seconds > 0 ? `public, max-age=${Math.min(seconds, 60)}, s-maxage=${seconds}` : 'no-store';
}
