"""Complete candidate parity and interrupted/corrupt file boundaries."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate','aws/ops/checks','aws/shared','tests')]
import genealogy_public_archive as archive
import genealogy_research_model as predecessor
import genealogy_streamed_pipeline as streamed
import test_genealogy_public_archive as fixtures


class Streamed(unittest.TestCase):
    def test_whole_input_output_membership_and_head_equal_the_accepted_pipeline(self):
        for source in (None, 'data/prospective-research.json', 'data/crypto-signals.json'):
            store, capture, _ = fixtures.fixture(source)
            for n in range(1, 12):
                repeat = deepcopy(capture)
                repeat['generated_at'] = (fixtures.NOW+timedelta(minutes=n)).isoformat()
                for ref in repeat['records']:
                    ref['created'] = False
                fixtures.add_capture(store, repeat)
            inventory = store.inventories()
            inputs, output = predecessor.collect(store, inventory)
            with tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)/'new'
                proof = streamed.collect(store, inventory, directory)
                expected = {'inputs':inputs, 'output':output, 'membership':output['membership'], 'head':predecessor.summary(output)}
                for key, document in expected.items():
                    raw = (directory/proof['files'][key]['file']).read_bytes()
                    self.assertEqual(raw, archive.canonical(document), (source,key))
                    self.assertEqual(hashlib.sha256(raw).hexdigest(), proof['files'][key]['sha256'])
                self.assertEqual(predecessor.compile_frozen(json.loads((directory/'inputs.json').read_bytes())), output)
                self.assertTrue(streamed.verify_files(directory, proof))
                self.assertEqual(json.loads((directory/'READY.json').read_bytes()), proof)
            self.assertTrue(all(s.closed for s in store.streams))

    def test_failed_original_never_yields_a_complete_output_or_ready_marker(self):
        store, _, _ = fixtures.fixture(); inventory = store.inventories()
        key = next(k for k in store.objects if '/records/' in k)
        raw, stamp = store.objects[key]; store.objects[key] = (raw[:-1], stamp)
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/'failed'
            with self.assertRaises(ValueError):
                streamed.collect(store, inventory, directory)
            self.assertFalse((directory/'READY.json').exists())
            self.assertFalse((directory/'output.json').exists())

    def test_complete_artifact_budget_cannot_turn_into_sampled_success(self):
        store, _, _ = fixtures.fixture()
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/'limited'
            with self.assertRaisesRegex(ValueError, 'complete_artifact_exceeds_budget'):
                streamed.collect(store, store.inventories(), directory, artifact_limit=256)
            self.assertFalse((directory/'READY.json').exists())
            with self.assertRaisesRegex(ValueError, 'fresh_pipeline_directory_required'):
                streamed.collect(store, store.inventories(), directory)

    def test_corruption_truncation_and_changed_proof_cannot_pass_readback(self):
        store, _, _ = fixtures.fixture()
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/'result'
            proof = streamed.collect(store, store.inventories(), directory)
            for name, ref in proof['files'].items():
                path = directory/ref['file']; original = path.read_bytes()
                for changed in (original[:-1], original+b' ', original.replace(b'{',b'[',1)):
                    path.write_bytes(changed)
                    with self.assertRaises(ValueError, msg=name):
                        streamed.verify_files(directory, proof)
                path.write_bytes(original)
            changed = deepcopy(proof); changed['files']['inputs']['bytes'] = True
            with self.assertRaises(ValueError):streamed.verify_files(directory, changed)
            changed = deepcopy(proof); changed['files']['inputs']['file'] = '../private.json'
            with self.assertRaises(ValueError):streamed.verify_files(directory, changed)
            changed = deepcopy(proof); del changed['files']['head']
            with self.assertRaises(ValueError):streamed.verify_files(directory, changed)

    def test_existing_directory_and_bad_inventory_fail_before_original_reads(self):
        store, _, _ = fixtures.fixture(); inventory = store.inventories()
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, 'fresh_pipeline_directory_required'):
                streamed.collect(store, inventory, Path(temp))
            self.assertEqual(store.reads, [])
            inventory[archive.PREFIX+'captures/']['listing_complete'] = False
            with self.assertRaises(ValueError):
                streamed.collect(store, inventory, Path(temp)/'uncreated')
            self.assertFalse((Path(temp)/'uncreated').exists())
            self.assertEqual(store.reads, [])


if __name__ == '__main__':
    unittest.main()
