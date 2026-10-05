from pathlib import Path
import gzip,io,json,unittest,zipfile
import regional_fed_parser as m
F=Path(__file__).resolve().parents[4]/'tests/fixtures/chart-regional-fed/data'
load=lambda n:gzip.decompress((F/(n+'.gz')).read_bytes())
CSV=load('cfnaiDataSeriesCsvCsv.csv');XLSX=load('kc-monthly.xlsx');PAGE=load('kc-manufacturing.html')
def changed_zip(name,transform):
 source=zipfile.ZipFile(io.BytesIO(XLSX));out=io.BytesIO()
 with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as z:
  for n in source.namelist():z.writestr(n,transform(source.read(n)) if n==name else source.read(n))
 return out.getvalue()
class Tests(unittest.TestCase):
 def test_official_complete_sources_match_independent_replay_inventory(self):
  packets={**{'chicago-cfnai:'+k:v for k,v in m.parse_cfnai(CSV).items()},**{'kc-manufacturing:'+k:v for k,v in m.parse_kc(XLSX).items()}}
  review=json.loads((F/'source-review.json').read_bytes());self.assertEqual(len(packets),77)
  for row in review['results']:
   records=packets[row['id']];self.assertEqual(len(records),row['observations']);self.assertEqual(sum(r['value'] is not None for r in records),row['numeric']);self.assertEqual(records[0]['period'],row['first']);self.assertEqual(records[-1]['period'],row['last'])
  self.assertEqual(sum(len(v) for v in packets.values()),26208);self.assertEqual(sum(r['value'] is not None for v in packets.values() for r in v),25598)
 def test_original_csv_numbers_missingness_and_component_identity_are_distinct(self):
  p=m.parse_cfnai(CSV);self.assertEqual(set(p),set(m.CFNAI_COLUMNS[1:]));self.assertEqual(p['CFNAI'][0]['value'],-.35);self.assertIsNone(p['CFNAI_MA3'][0]['value']);self.assertEqual(p['CFNAI_MA3'][0]['original_value'],'NaN');self.assertEqual(p['SO_I'][-1]['value'],0)
  self.assertEqual(p['CFNAI'][0]['source_location'],{'row':2,'column':'CFNAI'});self.assertEqual(p['CFNAI'][0]['anchor'],'1967-03-01')
 def test_csv_duplicate_month_shape_unknown_header_or_invalid_date_are_rejected(self):
  lines=CSV.decode().splitlines()
  for raw in [CSV.replace(b'P_I',b'UNKNOWN',1),('\n'.join(lines+[lines[1]])).encode(),CSV.replace(b'1967/03',b'1967/13',1),CSV.replace(b'1967/03',b'1967/03,extra',1),b'']:
   with self.assertRaises(ValueError):m.parse_cfnai(raw)
 def test_csv_invalid_numbers_are_preserved_and_withheld_never_zero_filled(self):
  raw=b'Date,P_I,EU_H,C_H,SO_I,CFNAI,CFNAI_MA3,DIFFUSION\n2026/01,NaN,,true,1e1000,0,-0,2.5\n';p=m.parse_cfnai(raw)
  for k in ['P_I','EU_H','C_H','SO_I']:self.assertIsNone(p[k][0]['value'])
  self.assertEqual(p['C_H'][0]['original_value'],'true');self.assertEqual(p['CFNAI'][0]['value'],0);self.assertEqual(p['CFNAI_MA3'][0]['original_value'],'-0')
 def test_diffusion_comparisons_and_seasonal_adjustments_do_not_merge(self):
  p=m.parse_kc(XLSX);self.assertEqual(len(p),70);self.assertEqual(p['month-sa:composite'][-1]['value'],14);self.assertEqual(p['six-month-expectation-nsa:composite'][-1]['value'],18)
  self.assertEqual(p['month-sa:composite'][-1]['period'],'2026-09');self.assertEqual(p['month-sa:composite'][-1]['source_location']['original_excel_date'],'2026-09-26');self.assertEqual(p['month-sa:composite'][-1]['anchor'],'2026-09-01')
 def test_seasonal_adjustment_is_not_constrained_to_unadjusted_percentage_bounds(self):
  def transform(raw):
   root=m.ET.fromstring(raw)
   for cell in root.findall('s:sheetData/s:row/s:c',m.NS):
    if cell.get('r') in ('B6','B22'):cell.find('s:v',m.NS).text='101'
   return m.ET.tostring(root)
  p=m.parse_kc(changed_zip('xl/worksheets/sheet1.xml',transform));self.assertEqual(p['month-sa:composite'][0]['value'],101);self.assertEqual(p['month-sa:composite'][0]['source_flags'],['seasonal_adjustment_can_exceed_unadjusted_diffusion_bounds']);self.assertIsNone(p['month-nsa:composite'][0]['value']);self.assertEqual(p['month-nsa:composite'][0]['original_value'],'101')
 def test_workbook_date_system_duplicate_cell_or_external_sheet_cannot_be_used(self):
  inputs=[changed_zip('xl/workbook.xml',lambda b:b.replace(b'<workbookPr',b'<workbookPr date1904="1"',1)),changed_zip('xl/worksheets/sheet1.xml',lambda b:b.replace(b'r="B3"',b'r="A3"',1)),changed_zip('xl/_rels/workbook.xml.rels',lambda b:b.replace(b'Target="worksheets/sheet1.xml"',b'TargetMode="External" Target="https://evil.invalid/x"',1))]
  for raw in inputs:
   self.assertNotEqual(raw,XLSX)
   with self.assertRaises(ValueError):m.parse_kc(raw)
 def test_unsafe_xml_and_archive_shapes_are_rejected_without_external_access(self):
  with self.assertRaises(ValueError):m.xml(b'<!DOCTYPE x [<!ENTITY y "z">]><x>&y;</x>')
  out=io.BytesIO()
  with zipfile.ZipFile(out,'w') as z:z.writestr('../evil.xml','x')
  with self.assertRaises(ValueError):m.parse_kc(out.getvalue())
 def test_only_exact_public_monthly_link_is_followed(self):
  self.assertEqual(m.kc_workbook_url(PAGE),json.loads(load('kc-monthly-receipt.json'))['url'])
  for href in ['https://evil.invalid/a.xlsx','/documents/1/../a.xlsx','/documents/1/a.xls','/documents/1/a.xlsx?token=x']:
   with self.assertRaises(ValueError):m.kc_workbook_url(('<a href="'+href+'">Historical Monthly Data</a>').encode())
  with self.assertRaises(ValueError):m.kc_workbook_url(b'<a href="/documents/1/a.xlsx">Historical Monthly Data</a><a href="/documents/2/b.xlsx">Historical Monthly Data</a>')
 def test_finite_decimal_representation_never_accepts_bool_comma_or_underflow(self):
  for x in [True,'true','1,000','1e1000','1e-1000','NaN',None]:self.assertIsNone(m.number(x))
  self.assertEqual(m.number('-10.25'),-10.25)
if __name__=='__main__':unittest.main(verbosity=2)
