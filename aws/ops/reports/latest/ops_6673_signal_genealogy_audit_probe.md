# ops 6673 - signal-genealogy: the archive audit on the runner (read-only)

**Status:** success  
**Duration:** 121.6s  
**Finished:** 2026-10-26T15:12:16+00:00  

## Log
- `15:10:17` discover: 6157 original objects, 329793622 bytes, prefixes {'data/research-forecasts/captures/': 681, 'data/research-forecasts/records/': 5475} in 2.7s
- `15:12:16` audit in 118.9s: listed 6156, validated captures 476, records 5475, original bytes 237508871, complete False
## failures

- `15:12:16` failures: 205; reasons {'record_ref_schema': 205}
- `15:12:16`    data/research-forecasts/captures/068b8058c871bb41625b5da5c6d2224f19c951635f86e838f0a7eae1fad8818d.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/08b9725f88696f38065cd2f535ea972b78e2aafe95b9916a64875c5c312c42ef.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/0b845a243f38e27074f30d6c67b86fd079f6ab128451d4bd845ed3817a47d4ef.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/0e83e1586787dd59859b2a61e8e941053bb989c102861e28f57beeb73e901e15.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/0f4be2f9fec59bc801c7c41352418dd86835f25f46026171d83018158d0e520a.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1038e91366098705d019c182355e25cad9112dd6a3002dcca25a1adc5b06061a.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/10468f7f501c1a910da47ee37ebb084f90163c5b41c16ca18d4065ee4c4c458c.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/11bc0afddd5316d0ec92836633570bccb8b5388d40dee4a073de05e6bf2b83e8.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/11c4345feb61f40bff6d286e312bff851d4c7e762ac2336455a11eb066562009.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/140487d48bf9b30ee07d24e54ca087ecef570de1602b29f1a06dfb8f24231901.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1408b32d3c510e546f6aed8e7a140a78d151034d0778c5040f064cd573fc9284.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1556de11d12b7303faef21d3e375a0623c64a3020ae88cdff6898cece37db553.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/160cd245cfff2a9b9bac42ae93485db814e6c6abf608b21b5f98c5a6828fb4de.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/168af5bf2b18de8df60463d21d350613e7bdef5b590105e856bb81ba9509c25b.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1722a4f0a021edb837f8b29ce1af20129de2874b864bdac7008702c1860b538c.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/18f20035d0cf8e441a6ffa8ce76d4527eedf411f15ce07e0df65d04591d4e8b5.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/19bd986ce982a2fc5672942e1ba5d833d735ecc7530f66e9ddf55fbb2de7a963.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1a304f9487a24ee31168c3fceeeef4430a67c9f3ebae192389dff7441168f626.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1b5ed060a0f8beb642d72fe517096542f24cf12f2cf64b912834dafc1e8d1dc9.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1bfc24005542f5b3067c919b7acff82c968421662c6b8f8925152b1e61a75e30.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1c4d4f600bfc7a7a4ed4bb0b9b99fde69954cb8aa6751c8715dceb00dca25c64.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/1f8627333a7d4ecda74b969d04cce5aa9bf7978bd91a737c7f546cf3a242145b.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/214e56a756c9d5339edc6cc22d06ff501a53ed5da3544d764753ab9d4156c317.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/23832eb36578c14593b9a453ab2bd94e4779245386d842c8c2d7f224d9d0894c.json -> record_ref_schema
- `15:12:16`    data/research-forecasts/captures/2445b5980fe47ac0736d608189e9c6316ced29659bff60bbb8c7fb5f8f9f516b.json -> record_ref_schema
## reference errors / conflicts / unreferenced

- `15:12:16` reference_errors: 0; reasons {}
- `15:12:16` reference_conflicts: 0
- `15:12:16` unreferenced records: 1194; capture scans complete: 190; registrations by day (last 7): {'2026-10-16': 118, '2026-10-21': 45, '2026-10-22': 69, '2026-10-23': 85, '2026-10-24': 85, '2026-10-25': 60, '2026-10-26': 34}
## verdict

- `15:12:16` ⚠ the archive does NOT audit clean: 205 failures, 0 reference errors, 0 conflicts (above) - the engine refuses to publish by contract until these are repaired
- `15:12:16` ✅ ops 6673 complete (read-only)
