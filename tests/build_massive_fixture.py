"""Rebuild the synthetic composition/browser fixture, without network access."""
from pathlib import Path
from test_massive_research import fixture, model, store
ROOT = Path(__file__).resolve().parents[1]


def build():
    memory, inputs, _ = fixture(); read = store.reader(memory, 'synthetic')
    output = model.build(inputs, read)
    identity = store.retain(memory, 'synthetic', inputs, output, read, lambda **kw: None)
    packet = {**output, 'replay': identity}
    artifacts = {key: raw.decode('utf-8') for key, raw in memory.data.items()
        if key.startswith('data/') and ('/runs/' in key or '/outputs/' in key)}
    artifacts[model.CURRENT] = model.encoded(packet).decode('utf-8')
    path = ROOT/'tests/fixtures/massive-native.json'; path.write_bytes(model.encoded({'publication': packet, 'artifacts': artifacts})+b'\n')
    print(path.name, path.stat().st_size)


if __name__ == '__main__': build()
