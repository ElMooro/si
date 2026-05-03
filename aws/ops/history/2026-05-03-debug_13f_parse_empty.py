"""
Debug 13F parse_returned_empty failures.

The diagnose script confirmed BERKSHIRE's 50240.xml has has_infoTable=True
but the parser returns 0 positions. Let's see why.

Plan:
  1. Fetch BERKSHIRE's 50240.xml
  2. Show first 800 chars of raw + cleaned (post-namespace-strip) versions
  3. Try parsing with the current logic step by step
  4. Try alternate parsing approaches if step 3 fails
"""
import urllib.request
import xml.etree.ElementTree as ET
import re

from ops_report import report

USER_AGENT = "JustHodl Research raafouis@gmail.com"


def _fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("debug_13f_parse_empty") as r:
        r.heading("Why does parse_infotable return 0 positions for BERKSHIRE?")

        target = "https://www.sec.gov/Archives/edgar/data/1067983/000119312526054580/50240.xml"

        r.section("1. Fetch raw XML")
        try:
            raw = _fetch(target).decode("utf-8", errors="ignore")
        except Exception as e:
            r.fail(f"  fetch failed: {e}")
            return
        r.log(f"  size: {len(raw)} chars")
        r.log(f"\n  first 600 chars:")
        for line in raw[:600].split("\n")[:15]:
            r.log(f"    {line[:140]}")

        r.section("2. Strip namespaces (current logic)")
        cleaned = re.sub(r"<(/?)\w+:", r"<\1", raw)
        cleaned = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", cleaned)
        r.log(f"  cleaned size: {len(cleaned)} chars")
        r.log(f"\n  first 600 chars after cleaning:")
        for line in cleaned[:600].split("\n")[:15]:
            r.log(f"    {line[:140]}")

        r.section("3. ElementTree.fromstring + count infoTable elements")
        try:
            root = ET.fromstring(cleaned)
            r.log(f"  root tag: {root.tag}")
            r.log(f"  root attribs: {dict(list(root.attrib.items())[:5])}")
            r.log(f"  num direct children: {len(list(root))}")
            r.log(f"  child tags (first 10): {[c.tag for c in list(root)[:10]]}")
        except Exception as e:
            r.fail(f"  ET.fromstring failed: {e}")
            r.log(f"  trying without strip — raw parse")
            try:
                root_raw = ET.fromstring(raw)
                r.log(f"  raw root.tag: {root_raw.tag}")
            except Exception as e2:
                r.fail(f"  raw parse also fails: {e2}")
            return

        # Use root.iter to find ALL infoTable elements at any depth
        all_infotables = list(root.iter("infoTable"))
        r.log(f"\n  root.iter('infoTable'): {len(all_infotables)} matches")

        # Maybe the iter returned 0 because tags have different casing?
        all_tags = set()
        for elem in root.iter():
            all_tags.add(elem.tag)
        info_like = [t for t in all_tags if "info" in t.lower() or "table" in t.lower()]
        r.log(f"  tags with 'info' or 'table' substring: {info_like[:10]}")
        r.log(f"  all unique tags (first 20): {sorted(all_tags)[:20]}")

        # Try the iter API differently
        r.section("4. Alternative — find ALL elements containing <nameOfIssuer>")
        with_name = []
        for elem in root.iter():
            if elem.tag == "nameOfIssuer":
                with_name.append(elem)
        r.log(f"  found {len(with_name)} <nameOfIssuer> elements")
        if with_name:
            sample = with_name[0]
            r.log(f"  first nameOfIssuer text: {sample.text!r}")
            # Walk up to find parent
            for parent in root.iter():
                if sample in list(parent):
                    r.log(f"  parent of first nameOfIssuer: {parent.tag}")
                    break

        r.section("5. Try minimal regex extraction as fallback")
        names = re.findall(r"<nameOfIssuer>([^<]+)</nameOfIssuer>", cleaned)
        r.log(f"  regex matched {len(names)} names; first 5: {names[:5]}")

        if len(names) > 0 and len(all_infotables) == 0:
            r.log(f"\n  ⚠ DIAGNOSIS: tags exist, but ET.iter('infoTable') doesn't find them.")
            r.log(f"     Likely because root namespace was preserved on root element only.")
            r.log(f"     Fix: also strip xmlns from root, or use root.iter('{{*}}infoTable').")


if __name__ == "__main__":
    main()
