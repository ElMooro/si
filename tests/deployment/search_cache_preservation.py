"""Exact reviewed search-cache delta for older whole-Worker preservation checks."""
from pathlib import Path
import hashlib
ROOT=Path(__file__).resolve().parents[2]
EXPECTED_ROUTE_SHA256='81e7bab568901ed2eff62550f2c86b7be211a6cf84eadd78812cf5926126d1c6'
START='    if (url.pathname === "/symsearch" || url.pathname === "/browse" || url.pathname === "/explorer" ||'
END='\n    if (url.pathname === "/tv-search")'
IMPORT="import { searchDeadline, searchCacheSeconds, searchCacheControl } from './symsearch-cache.js';\n"


def restore_reviewed_search_cache(current):
    predecessor=(ROOT/'tests/fixtures/symbol-directory/pre-resident-proxy.js.txt').read_bytes()
    assert hashlib.sha256(predecessor).hexdigest()=='2b79bc59d11d19a22db79416c9a5857be0fb1aefe73d642409a8bbd212cae71a'
    old=predecessor.decode('utf-8')
    assert current.count(IMPORT)==1
    a=current.index(START);b=current.index(END,a)
    assert hashlib.sha256(current[a:b].encode()).hexdigest()==EXPECTED_ROUTE_SHA256
    oa=old.index(START);ob=old.index(END,oa)
    restored=current[:a]+old[oa:ob]+current[b:]
    restored=restored.replace(IMPORT,'',1)
    comment='    //   /symsearch?q=   -> /search   (native-bounded edge cache, at most 120s)'
    assert restored.count(comment)==1
    return restored.replace(comment,'    //   /symsearch?q=   -> /search   (edge 120s)',1)
