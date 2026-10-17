# Exact history-expiry containment

```json
{
  "checked_at": "2026-10-17T03:03:52.448059+00:00",
  "failure_code": null,
  "op": 6544,
  "other_rules_and_fields_unchanged": true,
  "put_attempts": 1,
  "reads": 4,
  "remaining_enabled_expirations": [
    {
      "expiration": {
        "Expiration": {
          "Days": 30
        }
      },
      "id": "WITHHELD",
      "prefix": "WITHHELD",
      "rule_index": 2,
      "tag_or_size_qualified": false
    },
    {
      "expiration": {
        "Expiration": {
          "ExpiredObjectDeleteMarker": true
        },
        "NoncurrentVersionExpiration": {
          "NoncurrentDays": 1
        }
      },
      "id": "WITHHELD",
      "prefix": "WITHHELD",
      "rule_index": 7,
      "tag_or_size_qualified": false
    },
    {
      "expiration": {
        "Expiration": {
          "ExpiredObjectDeleteMarker": true
        },
        "NoncurrentVersionExpiration": {
          "NoncurrentDays": 1
        }
      },
      "id": "WITHHELD",
      "prefix": "data/",
      "rule_index": 8,
      "tag_or_size_qualified": false
    },
    {
      "expiration": {
        "Expiration": {
          "ExpiredObjectDeleteMarker": true
        },
        "NoncurrentVersionExpiration": {
          "NoncurrentDays": 1
        }
      },
      "id": "ops6520-purge-dead-versions-system-events",
      "prefix": "system-events/",
      "rule_index": 9,
      "tag_or_size_qualified": false
    }
  ],
  "schema": "retention-containment.v1",
  "source_sha": "48449c14ae7d3563b752e8bfc57b0c8caf7831a3",
  "status": "VERIFIED_DISABLED",
  "target_after": {
    "Filter": {
      "Prefix": ""
    },
    "ID": "ops6541-bound-noncurrent-versions",
    "NoncurrentVersionExpiration": {
      "NewerNoncurrentVersions": 10,
      "NoncurrentDays": 1
    },
    "Status": "Disabled"
  },
  "target_before": {
    "Filter": {
      "Prefix": ""
    },
    "ID": "ops6541-bound-noncurrent-versions",
    "NoncurrentVersionExpiration": {
      "NewerNoncurrentVersions": 10,
      "NoncurrentDays": 1
    },
    "Status": "Enabled"
  },
  "transition_default_after": "all_storage_classes_128K",
  "transition_default_before": "all_storage_classes_128K",
  "verified": true,
  "verify_only": false
}
```
