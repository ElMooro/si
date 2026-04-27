# Grant lambda-execution-role describe permissions

**Status:** success  
**Duration:** 2.6s  
**Finished:** 2026-04-27T22:25:15+00:00  

## Log
## 1. Inspect current inline policies

- `22:25:13`   current inline policies: ['HealthMonitorEventBridgeRead', 'HealthMonitorTelegramSSM', 'SESPolicy', 'ssm-ai-chat-auth-read']
## 2. Apply inline policy

- `22:25:13` ✅   ✓ put_role_policy lambda-describe-self-and-peers on lambda-execution-role
## 3. Verify

- `22:25:15` ✅   ✓ verified — actions granted: ['lambda:GetFunctionConfiguration', 'lambda:GetFunction', 'lambda:ListFunctions']
