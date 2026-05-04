# Create justhodl-short-interest Lambda + schedule

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-05-04T00:03:34+00:00  

## Log
## 1. Build deployment zip

- `00:03:32`   zip size: 4,654b
## 2. Create or update Lambda

- `00:03:32`   function does not exist — creating
- `00:03:33` ✅   ✓ created
## 3. EventBridge 6h schedule

- `00:03:33`   rule: justhodl-short-interest-6h
- `00:03:34` ✅   ✓ schedule wired
## 4. Smoke test

- `00:03:34` ✗   ✗ An error occurred (ResourceConflictException) when calling the Invoke operation: The operation cannot be performed at this time. The function is currently in the following state: Pending
