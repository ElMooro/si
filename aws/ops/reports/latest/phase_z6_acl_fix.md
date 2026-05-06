# Phase Z6 — Fix intel HTML 403

  invoking acl fix...
  status: 200

## Bucket diagnostics:
  policy: {"Version":"2012-10-17","Statement":[{"Sid":"PublicReadDataDir","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::justhodl-dashboard-live/data/*"},{"Sid":"PublicReadScr

## ACL details:
  acl_data/universe.json:
    - FULL_CONTROL for CanonicalUser
  acl_intel/index.html:
    - FULL_CONTROL for CanonicalUser
  acl_intel.html:
    - FULL_CONTROL for CanonicalUser
  acl_index.html:
    - FULL_CONTROL for CanonicalUser

## Steps applied:

## Fetch tests:
  https://justhodl-dashboard-live.s3.amazonaws.com/intel/index.html — {"status": 403, "err": "HTTP 403"}
  https://justhodl-dashboard-live.s3.amazonaws.com/intel.html — {"status": 403, "err": "HTTP 403"}

  ✓ patcher cleaned up