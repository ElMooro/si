"""Independent Decimal checks against every original ACM workbook cell."""
from datetime import datetime,timezone,date
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,math
import xlrd

HEADERS=['DATE']+[p+str(i).zfill(2) for p in ('ACMY','ACMTP','ACMRNY') for i in range(1,11)]
SPECS={'ACM Daily':('D',7,(1,5,21,63,252)),'ACM Monthly':('M',70,(1,3,12))}
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified')


def numeric(cell):
    if cell.ctype in (0,6) or cell.ctype==1 and cell.value in ('','.'):return None
    assert cell.ctype==2 and type(cell.value) in (int,float) and math.isfinite(cell.value)
    return Decimal.from_float(cell.value) if isinstance(cell.value,float) else Decimal(cell.value)


def cell_date(cell,datemode):
    if cell.ctype==3:return date(*xlrd.xldate_as_tuple(cell.value,datemode)[:3]).isoformat()
    assert cell.ctype==1
    return datetime.strptime(cell.value,'%d-%b-%Y').date().isoformat()


def scalar(actual,expected):
    assert actual['value']==(float(expected) if expected is not None else None)
    assert (Decimal(actual['exact_decimal']) if actual['exact_decimal'] is not None else None)==expected


def verify(output,raw):
    assert output['contract']=='term-premium-candidate.v1' and output['candidate_only'] is True and output['publication_eligible'] is False
    assert all(output[k] is False for k in FLAGS) and output['signals']==[] and output['call'] is None
    assert output['decision']=={'verb':'WAIT','meaning':'abstain'} and output['portfolio_consequences']=={'status':'UNAVAILABLE','target_weights':None}
    source=output['source'];assert source['sha256']==hashlib.sha256(raw).hexdigest() and source['bytes']==len(raw)
    assert source['source_url']=='https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls'
    acquired=datetime.fromisoformat(source['acquired_at'].replace('Z','+00:00'))
    now=datetime.fromisoformat(output['generated_at'].replace('Z','+00:00'))
    assert acquired.tzinfo is not None and now.tzinfo is not None
    acquired=acquired.astimezone(timezone.utc);now=now.astimezone(timezone.utc);assert acquired<=now
    assert output['dependency_graph']['model_roots']==['NYFED:ACM'] and output['dependency_graph']['independent_votes']==0
    book=xlrd.open_workbook(file_contents=raw);assert set(book.sheet_names())==set(SPECS)
    assert set(output['tables'])==set(SPECS)
    expected_ids={frequency+':'+h for frequency,_,_ in SPECS.values() for h in HEADERS[1:]}
    assert set(output['series'])==expected_ids
    checks=0;rows=0;cells=0;current=0;comparisons=0;missing=0
    with localcontext() as precision:
        precision.prec=1100;precision.rounding=ROUND_HALF_EVEN
        for name,(frequency,age_limit,steps) in SPECS.items():
            sheet=book.sheet_by_name(name);table=output['tables'][name]
            assert sheet.row_values(0)==HEADERS==table['headers']
            assert table['header_cell_types']==[sheet.cell(0,c).ctype for c in range(31)]
            assert table['original_rows_including_header']==sheet.nrows and len(table['rows'])==sheet.nrows-1
            assert table['original_columns']==31 and table['datemode']==book.datemode and table['frequency']==frequency
            dates={r:cell_date(sheet.cell(r,0),book.datemode) for r in range(1,sheet.nrows)}
            assert len(set(dates.values()))==len(dates)
            for index,row in enumerate(table['rows'],1):
                assert row=={'original_row':index,'observation_date':dates[index],'cells':sheet.row_values(index),
                    'cell_types':[sheet.cell(index,c).ctype for c in range(31)]}
            rows+=sheet.nrows-1;cells+=sheet.nrows*sheet.ncols
            past=sorted((r for r in dates if dates[r]<=acquired.date().isoformat()),key=dates.get)
            last=past[-1] if past else None;identity_max=Decimal(0);identity_count=0;identity_missing=0
            for r in dates:
                for tenor in range(1,11):
                    y,tp,rn=(numeric(sheet.cell(r,offset+tenor)) for offset in (0,10,20))
                    if None in (y,tp,rn):identity_missing+=1;continue
                    difference=abs(y-tp-rn);assert difference<=Decimal('1e-10')
                    identity_max=max(identity_max,difference);identity_count+=1
            check=output['identities'][name];assert check['checked']==identity_count and check['unavailable']==identity_missing
            assert check['formula']=='ACMYxx - ACMTPxx - ACMRNYxx' and Decimal(check['tolerance_pct'])==Decimal('1e-10')
            scalar(check['maximum_absolute_residual_pct'],identity_max);checks+=identity_count;missing+=identity_missing
            for col,header in enumerate(HEADERS[1:],1):
                row=output['series'][frequency+':'+header]
                assert row['series_id']==frequency+':'+header and row['model']=='Adrian-Crump-Moench'
                assert row['column']==col and row['table']==name and row['native_header']==header
                assert row['unit']=='percent' and row['frequency']==frequency and row['tenor_years']==int(header[-2:])
                assert all(row[k] is False for k in FLAGS)
                family='risk_neutral_yield' if header.startswith('ACMRNY') else 'term_premium' if header.startswith('ACMTP') else 'fitted_yield'
                assert row['family']==family
                latest=numeric(sheet.cell(last,col)) if last else None
                def point(actual,index):
                    if index is None:assert actual is None;return
                    assert actual['original_row']==index and actual['observation_date']==dates[index]
                    scalar(actual,numeric(sheet.cell(index,col)))
                point(row['last_observed'],last)
                age=(now.date()-date.fromisoformat(dates[last])).days if last else None
                status=('unavailable' if latest is None else 'stale_observation' if age>age_limit else
                    'stale_acquisition' if (now-acquired).total_seconds()>26*3600 else 'within_age_ceiling')
                assert row['quality']['status']==status
                assert row['quality']['observation_age_days']==age and row['quality']['max_observation_age_days']==age_limit
                assert row['quality']['acquisition_age_seconds']==(now-acquired).total_seconds() and row['quality']['release_calendar_verified'] is False
                assert row['quality']['max_acquisition_age_seconds']==26*3600
                if status=='within_age_ceiling':point(row['current'],last);current+=1
                else:assert row['current'] is None
                valid=[r for r in past if numeric(sheet.cell(r,col)) is not None];end=valid[-1] if valid else None
                assert set(row['historical_comparisons'])=={str(s) for s in steps}
                for n in steps:
                    baseline=valid[-n-1] if len(valid)>n else None;c=row['historical_comparisons'][str(n)]
                    assert c['numeric_observation_steps']==n;point(c['current'],end);point(c['baseline'],baseline)
                    expected=100*(numeric(sheet.cell(end,col))-numeric(sheet.cell(baseline,col))) if baseline else None
                    scalar(c['change_bps'],expected)
                    elapsed=(date.fromisoformat(dates[end])-date.fromisoformat(dates[baseline])).days if baseline else None
                    gaps=sum(dates[baseline]<dates[r]<=dates[end] and numeric(sheet.cell(r,col)) is None for r in past) if baseline else None
                    assert c['elapsed_calendar_days']==elapsed and c['missing_observation_dates']==gaps;comparisons+=1
                assert row['current_comparisons']==(row['historical_comparisons'] if status=='within_age_ceiling' else None)
    assert output['quality']['requested_series']==60 and output['quality']['current_series']==current
    assert output['quality']['status']==('within_age_ceiling' if current==60 else 'degraded' if current else 'unavailable')
    return {'original_rows':rows,'original_cells_including_headers':cells,'requested_series':60,'current_series':current,
        'model_identity_checks':checks,'unavailable_model_identities':missing,'observation_comparisons':comparisons,
        'all_returned_cells_retained':True,'forecast_qualified':False,'sizing_qualified':False}
