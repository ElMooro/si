# ops 5175 -- Heatmap scope probe

**Status:** failure  
**Duration:** 19.3s  
**Finished:** 2026-09-03T22:33:47+00:00  

## Error

```
SystemExit: 1
```

## Log
- `22:33:28`    live html bytes=672380 has_marker=True has_class=True
- `22:33:28`    inline script 0: 477731 chars -> SYNTAX ERROR /tmp/live_s0.js:7129
    if (mode === 'cascade') return `rgba(201,148,46,${(0.08 + Math.min(1) / 200) * 0.55).toFixed(3)})`;
                                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SyntaxError: Missing } in template expression
    at wrapSafe (node:internal/modules/c
- `22:33:28`    inline script 1: 102 chars -> OK 
- `22:33:28`    inline script 2: 21980 chars -> OK 
- `22:33:28`    inline script 3: 8964 chars -> OK 
- `22:33:28`    inline script 4: 930 chars -> OK 
- `22:33:47`    typeof -> {'Heatmap': 'undefined', 'MacroData': 'undefined', 'UI': 'undefined', 'State': 'undefined', 'PROXY': 'undefined', 'btn': True, 'modal': True, 'editor': True}
- `22:33:47` ⚠    pageerror: Missing } in template expression
- `22:33:47` ⚠    pageerror: Missing } in template expression
- `22:33:47` ⚠    console: error: Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fchart-pro.html&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by COR
- `22:33:47` ⚠    console: error: Failed to load resource: net::ERR_FAILED
- `22:33:47` ⚠    console: error: Failed to load resource: the server responded with a status of 404 ()
- `22:33:47` ⚠    console: error: Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=chartpro&lists=492&err=&sw=none&drawer=c3efc5d3&favs=0&lwc=true' from origin 'https://justhodl.ai'
- `22:33:47` ⚠    console: error: Failed to load resource: net::ERR_FAILED
