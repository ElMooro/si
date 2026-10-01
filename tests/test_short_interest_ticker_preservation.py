from pathlib import Path
import hashlib,json,unittest
R=Path(__file__).resolve().parents[1]
class Preservation(unittest.TestCase):
 def test_complete_predecessors_and_exact_reviewed_edits(self):
  manifest=json.loads((R/'tests/fixtures/short-interest-ticker-projection/preservation.json').read_bytes())
  for path,item in manifest['files'].items():
   raw=(R/item['predecessor']).read_bytes();self.assertEqual(len(raw),item['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),item['sha256'])
   text=raw.decode('utf-8')
   for a,b in item['edits']:self.assertEqual(text.count(a),1,path);text=text.replace(a,b)
   self.assertEqual((R/path).read_text(encoding='utf-8'),text,path)
if __name__=='__main__':unittest.main()
