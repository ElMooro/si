"""Invented successful empty declared collections, never real packet access."""
def empty_sources(module):
    objects = {}
    for key, path, _ in module.FEEDS.values():
        doc = objects.setdefault(key, {})
        parts = path.split('.')
        for part in parts[:-1]:
            doc = doc.setdefault(part, {})
        doc[parts[-1]] = []
    return objects
