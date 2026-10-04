// Preserve public object identity without decoding general URL escapes. Only
// the documented OECD dataflow separator is accepted in its public namespace.
export function publicObjectPath(pathname) {
  if (typeof pathname !== 'string') return null;
  const path = pathname.replace(/^\/+/, '');
  if (path.includes('..')) return null;
  if (/^[a-zA-Z0-9_\-./]+$/.test(path)) return path;
  if (/^data\/warm\/oecd\/data\/[A-Za-z0-9_-]+(?:@|%40)[A-Za-z0-9_-]+\.dat\.gz$/.test(path)) {
    return path.replace('%40', '@');
  }
  return null;
}
