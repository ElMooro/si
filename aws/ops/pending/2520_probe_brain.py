import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
br=g("data/brain.json")
print("=== brain.json top keys ===")
print(list(br.keys()))
print("\ngenerated/updated:", br.get("generated_at") or br.get("updated_at") or br.get("as_of"))
for f in ["n_notes","note_count","count","schema_version","version"]:
    if br.get(f) is not None: print(f"{f}: {br.get(f)}")
print("\n=== brain_directive / regime read ===")
for f in ["directive","brain_directive","regime","regime_read","macro_regime","posture","stance"]:
    v=br.get(f)
    if v is not None: print(f"  .{f}: {json.dumps(v)[:300]}")
print("\n=== tilts / tickers (structured outputs) ===")
for f in ["tilts","brain_tilts","tickers","brain_tickers","pinned","brain_pinned","sector_tilts","aligned"]:
    v=br.get(f)
    if v is not None: print(f"  .{f}: {json.dumps(v)[:300]}")
print("\n=== prompt_block (ready-to-inject) preview ===")
pb=br.get("prompt_block") or br.get("brain_prompt_block") or br.get("prompt")
if pb: print(str(pb)[:600])
print("\n=== sample NOTES ===")
notes=br.get("notes") or br.get("pinned") or br.get("entries") or []
if isinstance(notes,dict): notes=list(notes.values())
print("n notes in this object:",len(notes) if isinstance(notes,list) else "n/a")
for n in (notes[:5] if isinstance(notes,list) else []):
    print("  -",json.dumps(n)[:240])
print("DONE 2520")
