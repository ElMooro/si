#!/usr/bin/env python3
"""Fail on parser errors in the current public HTML/JS graph, never a saved registry.

Uses the same parse-only Acorn source graph as page-data contracts. This catches
syntax errors; it does not replace browser execution or check provider data.
"""
import argparse
import sys
from pathlib import Path

from page_sources import scan_pages


def check(root):
    graphs = scan_pages(root)
    if not graphs:
        raise ValueError('No public HTML pages found; syntax check is incomplete')
    failures = sorted({(entry['source'], entry['error'])
                       for graph in graphs.values()
                       for entry in graph['script_parse_errors']})
    return len(graphs), failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', nargs='?', type=Path,
                        default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        count, failures = check(args.root.resolve())
    except Exception as error:
        print('PAGE SCRIPT CHECK INCOMPLETE:', error, file=sys.stderr)
        return 1
    for source, error in failures:
        print(f'{source}: {error}', file=sys.stderr)
    print(f'Parsed {count} public page graphs; {len(failures)} source syntax errors.')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
