# BIS policy-rate chart coverage

The chart adds the 98 exact daily/monthly WS_CBPOL series advertised by the
official BIS API across 49 economies. The existing BIS dataset catalogue is
preserved: choose WS_CBPOL, then select an exact country and frequency. Only
reviewed identities become chart actions. Thirteen formerly unrouted watchlist
policy-rate identifiers have explicitly unqualified BIS alternatives.

Every successful packet retains the full original CSV compressed losslessly,
its byte hashes, every row ordinal, original period, status/confidentiality flags,
unit and policy-instrument compilation notes. Duplicate periods, different
countries/frequencies/units, confidential or forecast values, malformed numbers,
incomplete responses and bounds failures cannot become successful histories.
Zero and negative rates remain valid observations. Monthly day 1 is a display
anchor, never a release timestamp. No missing dates are filled.

The official-source audit replayed all 98 responses. The initial Japan daily
response exceeded the decoded-byte limit because compilation text is repeated;
a 96 MB decoded cap (with the existing 4 MB wire and 40,000-row bounds) handles
the complete CSV. The original failure is retained in audit working evidence.
Lambda resources, triggers and provider entitlements are unchanged.

All 346 previously routed CFTC aliases were separately requested through the
live public application endpoint. 344 returned histories reproducible from the
original response receipts; two 11700-contract aliases returned no observations.
29 returned series contain 3,699 source rows without the selected field, retained
as missing values. This establishes delivered-observation coverage, not complete
upstream history, point-in-time vintages, freshness, trading edge or sizing.

Primary definitions: [BIS policy-rate methodology](https://data.bis.org/topics/CBPOL)
and [BIS public API](https://stats.bis.org/api-doc/v2/). Whole schema/directory
responses and their receipts are retained alongside this report. Candidate
status changes only after exact release receipt, native controls and served
page verification.

Incoming peer row-click handoff is retained through the shared qualified router.
Mouse/search selection and keyboard selection preserve the same requested ID;
there is no second regex-parsed route table or delayed click replay. The complete
peer module and prior source/fixture states are retained in the new preservation
layer. Peer changes to older fixtures are preserved byte for byte.
