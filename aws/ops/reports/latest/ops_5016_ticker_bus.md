# ops 5016 -- ticker bus: layers follow the desk's navigation

**Status:** success  
**Duration:** 152.4s  
**Finished:** 2026-08-27T16:43:08+00:00  

## Data

| page_kb |
|---|
| 359 |

## Log
## G1 repo file carries the bus + rewires

- `16:40:36` ✅ bus block present
- `16:40:36` ✅ bus singleton
- `16:40:36` ✅ history hooks
- `16:40:36` ✅ fetch interception
- `16:40:36` ✅ four subscriptions
- `16:40:36` ✅ four jhStart entrypoints
- `16:40:36` ✅ generation checks >=20
- `16:40:36` ✅ bus precedes OPS5010
## G2 served page carries it

- `16:40:36` waiting for site sync
- `16:40:51` waiting for site sync
- `16:41:07` waiting for site sync
- `16:41:22` waiting for site sync
- `16:41:37` waiting for site sync
- `16:41:52` waiting for site sync
- `16:42:07` waiting for site sync
- `16:42:23` waiting for site sync
- `16:42:38` waiting for site sync
- `16:42:53` waiting for site sync
- `16:43:08` ✅ bus + all four subscribed layers on the served page
- `16:43:08` ✅ OPS 5016 PASS -- every research layer arms from any navigation path and re-renders on in-page ticker switches
