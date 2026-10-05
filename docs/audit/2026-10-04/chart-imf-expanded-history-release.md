# IMF expanded live-history audit

Independent replay now verifies numeric histories for 8,670 of the 10,113 reviewed public definitions. Another 13 official source responses were successfully replayed but contain no accepted numeric observations; they are explicitly distinguished from chartable history. Every accepted packet is checked against its complete retained official response, including values, source decimal text, period anchors, status flags and original dimensions. All 76 histories accepted with the original release remain accepted. The earlier release audit is preserved.

One production-index definition, `imf:PI:IND.IND.IX.A`, returned an unavailable application packet with no observations and no source receipt. Its acquisition did not produce a verifiable source snapshot. It has not been retried; another 1,429 PI definitions remain unrequested pending source-definition review. The audit does not assert a particular upstream failure cause.

The other reviewed dataflows completed without an additional failure. This evidence adds verified provider histories, not new watchlist aliases. Routing counts are unchanged. Source snapshots are not publication-time vintages, full upstream coverage, proprietary-vendor equivalence, Calls eligibility or sizing authority. Mixed-access discovery archives remain excluded from publication.
