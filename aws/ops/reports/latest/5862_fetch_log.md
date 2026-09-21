# ops 5862 -- newest supply fetch log tails

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-09-21T01:09:21+00:00  

## Log
- `01:09:21` == 35549816687-fetch-tail.log (09-21 01:07)
- `01:09:21`    {"apps_loader": "tarball", "problems": 5000}
- `01:09:21`    Traceback (most recent call last):
- `01:09:21`      File "/home/runner/work/si/si/scripts/factory_oss_curriculum.py", line 371, in <module>
- `01:09:21`        sys.exit(main())
- `01:09:21`                 ^^^^^^
- `01:09:21`      File "/home/runner/work/si/si/scripts/factory_oss_curriculum.py", line 366, in main
- `01:09:21`        {"fetch": cmd_fetch, "exam": cmd_exam, "write": cmd_write}[args.cmd](args)
- `01:09:21`      File "/home/runner/work/si/si/scripts/factory_oss_curriculum.py", line 279, in cmd_fetch
- `01:09:21`        f.write(json.dumps({"_fetch": {**LOADER_LOG, "counts": n}}) + "\n")     # loader provenance rides with the candidates into the run summary
- `01:09:21`                                         ^^^^^^^^^^
- `01:09:21`    NameError: name 'LOADER_LOG' is not defined
- `01:09:21` ✅ done
