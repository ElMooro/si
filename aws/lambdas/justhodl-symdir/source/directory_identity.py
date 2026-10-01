"""Optional identifier evidence for a ticker directory, never routing authority.

This module does not acquire data or certify the upstream spine. A matching CIK
supports issuer consistency only, not a current security-to-identifier relation.
Stored values remain inspectable candidates; malformed optional rows cannot
remove a primary reference instrument from the index.
"""
import math,re

SOURCE='data/symbology/master.json'


def cik(value):
    if type(value) is int:
        value=str(value)
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{1,10}',value.strip()):
        return None
    value=value.strip()
    return value.zfill(10) if int(value)>0 else None


def population(document):
    return document['by_ticker'] if isinstance(document,dict) and isinstance(document.get('by_ticker'),dict) else None


def evidence(ticker,reference,document,market):
    rows=population(document)
    row=rows.get(ticker) if rows is not None and isinstance(ticker,str) else None
    row_valid=isinstance(row,dict)
    reference_ticker=reference.get('ticker') if isinstance(reference,dict) else None
    reference_consistent=reference_ticker==ticker
    ref_cik=cik(reference.get('cik')) if isinstance(reference,dict) else None
    spine_cik=cik(row.get('cik')) if row_valid else None
    same_issuer=ref_cik==spine_cik if ref_cik and spine_cik else None
    claimed_ticker=row.get('ticker') if row_valid else None
    ticker_consistent=claimed_ticker in (None,ticker) if isinstance(ticker,str) else False
    reported=row.get('isin') if row_valid else None
    reported_scalar=reported if type(reported) in (str,int,float,bool) else None
    if type(reported_scalar) is float and not math.isfinite(reported_scalar):reported_scalar=None
    if type(reported_scalar) is int and not -(2**63)<reported_scalar<2**63:reported_scalar=None
    # Keep selected scalar evidence bounded; upstream complete inputs stay upstream.
    if isinstance(reported_scalar,str) and len(reported_scalar)>128:reported_scalar=None
    value=reported.strip().upper() if isinstance(reported,str) and len(reported)<=128 else None
    character_pattern=bool(value and re.fullmatch(r'[A-Z]{2}[A-Z0-9]{9}[0-9]',value))
    if rows is None:status='source_unavailable_or_invalid'
    elif not isinstance(ticker,str) or ticker not in rows:status='ticker_not_in_received_population'
    elif not row_valid:status='invalid_optional_row'
    elif not reference_consistent:status='conflicting_reference_ticker'
    elif not ticker_consistent:status='conflicting_row_ticker'
    elif market!='stocks':status='market_relationship_unqualified'
    elif same_issuer is False:status='conflicting_issuer'
    elif same_issuer is None:status='issuer_identity_unverified'
    elif reported is None:status='identifier_not_reported'
    elif not character_pattern:status='invalid_identifier_character_pattern'
    else:status='candidate_only'
    clocks={k:document[k] for k in ('as_of','generated_at','updated_at')
            if isinstance(document,dict) and isinstance(document.get(k),str) and len(document[k])<=80}
    return {'isin':None,'identifier_evidence':{
        'contract':'directory-identifier-candidates.v1','status':status,'source_key':SOURCE,
        'ticker':ticker if isinstance(ticker,str) else None,'market':market if isinstance(market,str) else None,
        'reference_cik':ref_cik,'spine_cik':spine_cik,'issuer_ids_match':same_issuer,
        'reference_ticker_consistent':reference_consistent,
        'row_ticker_consistent':ticker_consistent,'reported_source_clocks':clocks,
        'reported_isin':reported_scalar,'reported_isin_type':type(reported).__name__,
        'candidate_isin':value if character_pattern else None,
        'character_pattern_matches':character_pattern,
        'source_claims_isin_qualified':row.get('isin_match_qualified') is True if row_valid else False,
        'checksum_verified':False,'current_security_relationship_verified':False,
        'independent_source_replay_verified':False,'automatic_identifier_routing_eligible':False}}
