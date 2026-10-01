from pathlib import Path
import hashlib,json,unittest
R=Path(__file__).resolve().parents[1]
class Preservation(unittest.TestCase):
 def test_whole_predecessors_and_exact_changes_preserved(self):
  spec=json.loads((R/'tests/fixtures/squeeze-research/preservation.json').read_bytes())
  for path,item in spec['files'].items():
   raw=(R/item['predecessor']).read_bytes();self.assertEqual(len(raw),item['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),item['sha256'])
   text=raw.decode('utf-8')
   for a,b in item['edits']:self.assertEqual(text.count(a),1,path);text=text.replace(a,b)
   self.assertEqual(item['prefix']+text+item['suffix'],(R/path).read_text(encoding='utf-8'),path)
   if path.endswith('/config.json'):
    old=json.loads(raw);current=json.loads(text);old.pop('description');current.pop('description');self.assertEqual(old,current)
if __name__=='__main__':unittest.main()
