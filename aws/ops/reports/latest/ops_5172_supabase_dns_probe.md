# ops 5172 -- Supabase hostname probe

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-09-03T22:02:34+00:00  

## Log
- `22:02:33`    dig bdmjenqcyvzouusfcgow.supabase.co (system) -> (no answer)
- `22:02:33`    dig bdmjenqcyvzouusfcgow.supabase.co @1.1.1.1 -> (no answer)
- `22:02:33`    dig bdmjenqcyvzouusfcgow.supabase.co @8.8.8.8 -> (no answer)
- `22:02:33`    dig status: status: NXDOMAIN
- `22:02:33`    dig supabase.co (system) -> 76.76.21.21
- `22:02:33`    dig supabase.co @1.1.1.1 -> 76.76.21.21
- `22:02:33`    dig supabase.co @8.8.8.8 -> 76.76.21.21
- `22:02:33`    dig status: status: NOERROR
- `22:02:33`    dig api.supabase.com (system) -> 172.64.145.26 | 104.18.42.230
- `22:02:33`    dig api.supabase.com @1.1.1.1 -> 104.18.42.230 | 172.64.145.26
- `22:02:33`    dig api.supabase.com @8.8.8.8 -> 172.64.145.26 | 104.18.42.230
- `22:02:33`    dig status: status: NOERROR
- `22:02:33`    dig justhodl.ai (system) -> 104.21.71.253 | 172.67.173.30
- `22:02:34`    dig justhodl.ai @1.1.1.1 -> 104.21.71.253 | 172.67.173.30
- `22:02:34`    dig justhodl.ai @8.8.8.8 -> 172.67.173.30 | 104.21.71.253
- `22:02:34`    dig status: status: NOERROR
- `22:02:34`    GET https://api.supabase.com/v1/projects -> HTTP Error 401: Unauthorized
- `22:02:34`    GET https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/health -> <urlopen error [Errno -2] Name or service not known>
- `22:02:34` ✅ probe complete
