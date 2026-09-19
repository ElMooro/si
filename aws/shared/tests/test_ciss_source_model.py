from copy import deepcopy
import csv
import io
import unittest

import ciss_source_model as model

NOW='2026-09-18T22:00:00+00:00'


def csv_bytes(key,points,unit='PURE_NUMB'):
    columns=['KEY',*model.DIMENSIONS,'TIME_PERIOD','OBS_VALUE','OBS_STATUS','UNIT','UNIT_MULT','TITLE','TITLE_COMPL','OBS_COM']
    stream=io.StringIO();writer=csv.DictWriter(stream,fieldnames=columns);writer.writeheader()
    for day,value,status in points:
        writer.writerow({'KEY':key,**dict(zip(model.DIMENSIONS,key.split('.')[1:])),
            'TIME_PERIOD':day,'OBS_VALUE':value,'OBS_STATUS':status,'UNIT':unit,'UNIT_MULT':'0',
            'TITLE':'Fixture, stress measure','TITLE_COMPL':'Test-only exact definition','OBS_COM':''})
    return stream.getvalue().encode()


def item(raw):
    return {'raw':raw,'acquired_at':NOW,'evidence':{'first_received_at':NOW,'fixture':'SYNTHETIC'}}


def packet_inputs():
    keys={model.HEAD:'0.03',**{'CISS.D.U2.Z0Z.4F.EC.'+c+'.CON':('-0.12' if c=='SS_CON' else '0.03') for c in model.CONTRIBUTIONS}}
    histories={key:item(csv_bytes(key,[('2025-09-15','0.01','A'),('2026-09-15',value,'A')])) for key,value in keys.items()}
    discoveries=b'\n'.join([csv_bytes(key,[('2026-09-15',value,'A')]) if n==0 else csv_bytes(key,[('2026-09-15',value,'A')]).split(b'\n',1)[1] for n,(key,value) in enumerate(keys.items())])
    return {'CISS':item(discoveries)},histories


class CissSourceTests(unittest.TestCase):
    def test_missing_latest_discovery_preserves_country_without_carrying_prior_value(self):
        key='CISS.D.DE.Z0Z.4F.EC.SS_CIN.IDX'
        raw=csv_bytes(key,[('2026-09-15','0.02','A'),('2026-09-16','','M')])
        data=model.csv_series(raw,key)[key];self.assertEqual(len(data['rows']),2)
        out=model.summarize(key,data,item(raw)['evidence'],NOW,NOW)
        self.assertEqual(out['latest_date'],'2026-09-16');self.assertIsNone(out['latest'])
        self.assertEqual(out['last_observed_value'],.02);self.assertEqual(out['quality']['status'],'missing')
        self.assertFalse(out['ranking_eligible']);self.assertIsNone(out['percentile_3y'])
        discovery={'CISS':item(csv_bytes(key,[('2026-09-16','','M')]))}
        packet=model.build(discovery,{key:item(raw)},NOW)
        self.assertIn(key,packet['catalog']);self.assertEqual(packet['series'][0]['latest_date'],'2026-09-16')

    def test_precise_signed_contributions_reconcile_without_crisis_verb(self):
        discovery,histories=packet_inputs();before=deepcopy((discovery,histories))
        out=model.build(discovery,histories,NOW)
        self.assertEqual(out['headline_reconciliation']['status'],'matched')
        self.assertEqual(out['headline_reconciliation']['residual_decimal'],'0.00')
        self.assertEqual(out['ea_composite'],.03);self.assertIsNone(out['ea_regime'])
        self.assertFalse(out['calls_eligible']);self.assertEqual((discovery,histories),before)
        key='CISS.D.U2.Z0Z.4F.EC.SS_CON.CON'
        self.assertEqual(next(r for r in out['series'] if r['key']==key)['unit'],'dimensionless_contribution')
        histories[key]=item(csv_bytes(key,[('2026-09-15','-0.11','A')]))
        bad=model.build(discovery,histories,NOW)
        self.assertEqual(bad['headline_reconciliation']['status'],'mismatch');self.assertIsNone(bad['ea_composite'])
        self.assertEqual(bad['quality']['status'],'invalid')
        self.assertIsNone(next(r for r in bad['series'] if r['key']==model.HEAD)['latest'])

    def test_week_chart_uses_last_row_and_iso_year_including_missing_friday(self):
        key=model.HEAD;raw=csv_bytes(key,[('2020-12-31','.1','A'),('2021-01-01','.2','A'),('2021-01-07','.3','A'),('2021-01-08','','M')])
        points=model.csv_series(raw,key)[key]['rows']
        self.assertEqual(model.downsample(points,'D'),[['2021-01-01',.2],['2021-01-08',None]])

    def test_monthly_period_end_calendar_annual_comparison_and_zero_variance(self):
        key='CLIFS.M.AT._Z.4F.EC.CLIFS_CI.IDX'
        raw=csv_bytes(key,[('2025-07','0','A'),('2026-07','0','A')])
        out=model.summarize(key,model.csv_series(raw,key)[key],item(raw)['evidence'],NOW,NOW)
        self.assertEqual(out['observation_period_end'],'2026-07-31');self.assertEqual(out['quality']['observation_age_days'],49)
        self.assertEqual(out['latest'],0);self.assertEqual(out['chg_1y'],0);self.assertIsNone(out['zscore'])
        self.assertEqual(out['annual_comparison']['baseline_period'],'2025-07')
        # A missing latest/baseline is never replaced with the previous valid row.
        raw=csv_bytes(key,[('2025-06','.1','A'),('2025-07','','M'),('2026-07','.2','A')])
        out=model.summarize(key,model.csv_series(raw,key)[key],item(raw)['evidence'],NOW,NOW)
        self.assertIsNone(out['chg_1y'])

    def test_invalid_dimensions_units_decimals_and_conflicting_duplicates_rejected(self):
        key=model.HEAD
        for raw in [csv_bytes(key,[('2026-09-15','NaN','A')]),csv_bytes(key,[('2026-09-15','.2','A')],unit='EUR'),
                    csv_bytes(key,[('2026-09-15','.2','A'),('2026-09-15','.3','A')]),
                    csv_bytes(key,[('2026-09-15','1.1','A')]),csv_bytes(key,[('2026-09-15','.2','A')]).replace(b',U2,',b',US,')]:
            with self.assertRaises(ValueError):model.csv_series(raw,key)

    def test_newer_missing_discovery_and_future_rows_never_become_current(self):
        key=model.HEAD
        discovery={'CISS':item(csv_bytes(key,[('2026-09-17','','M')]))}
        history=item(csv_bytes(key,[('2026-09-15','.1','A'),('2026-10-01','.9','A')]))
        row=model.build(discovery,{key:history},NOW)['series'][0]
        self.assertIsNone(row['latest']);self.assertEqual(row['quality']['status'],'incomplete')
        self.assertEqual(row['quality']['excluded_future_rows'],1)
        self.assertIsNone(row['annual_comparison']['value'])

    def test_commentary_is_dated_source_bound_and_expires(self):
        discovery,histories=packet_inputs();p=model.build(discovery,histories,NOW);p['replay']={'manifest_key':'fixture'}
        narrative=model.commentary(p,NOW)
        self.assertEqual(len(narrative['claims']),7);self.assertIn('2026-09-15',narrative['interpretation']['headline'])
        self.assertIsNone(narrative['ea_regime']);self.assertFalse(narrative['sizing_eligible'])
        self.assertEqual(model.commentary(p,'2026-09-23T22:00:00Z')['claims'],[])
        # A still-recent packet can contain an observation which has just expired.
        head=next(row for row in p['series'] if row['key']==model.HEAD)
        head['observation_period_end']='2026-09-04'
        self.assertEqual(model.commentary(p,'2026-09-19T00:00:00Z')['claims'],[])

    def test_calendar_comparison_keeps_missing_baseline_and_leap_dates(self):
        key=model.HEAD
        raw=csv_bytes(key,[('2026-08-14','.1','A'),('2026-08-15','','M'),('2026-09-15','.2','A')])
        row=model.summarize(key,model.csv_series(raw,key)[key],item(raw)['evidence'],NOW,NOW)
        self.assertIsNone(row['comparisons']['1m']['value'])
        self.assertEqual(row['comparisons']['1m']['baseline_period'],'2026-08-15')
        self.assertEqual(row['comparisons']['1m']['baseline_source_row'],1)
        raw=csv_bytes(key,[('2023-02-28','.1','A'),('2024-02-29','.2','A')])
        rows=model.csv_series(raw,key)[key]['rows']
        comparison=model.calendar_comparison(rows,rows[-1],'D',months=12)
        self.assertEqual(comparison['target_date'],'2023-02-28');self.assertEqual(comparison['value'],.1)


if __name__=='__main__':unittest.main()
