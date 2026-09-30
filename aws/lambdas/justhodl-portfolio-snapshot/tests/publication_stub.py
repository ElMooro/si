"""Publication replacement for measurement-only tests; no ordering acceptance."""
def isolate(mod):
    frame={'schema_version':'portfolio-snapshot-publication.v1','revision':1,'started_at':'2026-09-30T15:00:00Z'}
    mod.begin_snapshot_publication=lambda *args:{'frame':dict(frame),'token':'invented-unused'}
    def finish(client,validate,attempt,raw,identity,context=None):
        # Existing measurement assertions observe both complete representations.
        # These callbacks deliberately do not exercise real storage or ordering.
        mod.publish_snapshot(raw,identity,context)
        mod.s3.put_object(Bucket=mod.S3_BUCKET,Key=mod.SNAPSHOT_KEY,Body=raw,ContentType='application/json',CacheControl='private, no-store')
        return {'published':True,'status':'published','revision':1}
    mod.finish_snapshot_publication=finish
