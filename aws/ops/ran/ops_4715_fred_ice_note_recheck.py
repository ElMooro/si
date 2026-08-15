"""ops 4715 — re-check with the CORRECT field name.

4714's own verification checked "coverage_note" (a DIFFERENT variable,
cov_note), while the actual note text lands under "catalog_note" (line
582 of provider-catalog). The "before" snapshot ALSO showed None under
the wrong field, which is strong evidence the note was present all
along under the right key. Confirming directly rather than assuming
either way.
"""
import json
import sys

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4715_fred_ice_note_recheck") as r:
        r.heading("ops 4715 — recheck with the correct field "
                  "(catalog_note, not coverage_note)")
        hub = json.loads(s3.get_object(
            Bucket=B,
            Key="data/provider-catalog.json")["Body"].read())
        fred = next((p for p in hub.get("providers") or []
                    if p.get("slug") == "fred"), {})
        r.log("  ALL fred fields: %s" % sorted(fred.keys()))
        cat_note = fred.get("catalog_note")
        cov_note = fred.get("coverage_note")
        r.log("  catalog_note (the real field): %r" % cat_note)
        r.log("  coverage_note (what 4714 wrongly checked): %r"
             % cov_note)

        misses = 0
        misses += contract(
            r, "original", cat_note and "scoped import" in
            str(cat_note),
            "original FRED note content present: %r" % cat_note)
        misses += contract(
            r, "additive", cat_note and "ICE BofA" in str(cat_note)
            and "cross-validated" in str(cat_note),
            "ICE-via-TE annotation present as an append: %r"
            % cat_note)
        misses += contract(
            r, "count_unchanged",
            fred.get("n_keys") == 279497,
            "FRED n_keys still 279497 (matches 4714's before/after "
            "proof, which WAS correct — count identity was verified "
            "on the right fields)")

        if misses:
            r.fail("recheck: %d red — the field-name theory was "
                  "wrong, something else is going on" % misses)
            sys.exit(1)
        r.ok("CONFIRMED: 4714's failure was its own wrong-field-name "
            "bug. The real catalog_note has BOTH the original FRED "
            "text and the additive ICE annotation, exactly as built: "
            "%r" % cat_note)


if __name__ == "__main__":
    main()
