# Diagnose AI Lambda 'Failed to fetch'

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-04-25T23:49:36+00:00  

## Log
## === justhodl-stock-ai-research ===

- `23:49:36`   FunctionArn: arn:aws:lambda:us-east-1:857687956942:function:justhodl-stock-ai-research
- `23:49:36`   Runtime: python3.11, Handler: lambda_function.lambda_handler
- `23:49:36`   LastModified: 2026-04-25T23:34:39
- `23:49:36` 
  Function URL:
- `23:49:36`     URL:      https://obcsgkzlvicwc6htdmj5wg6yae0tfmya.lambda-url.us-east-1.on.aws/
- `23:49:36`     AuthType: NONE
- `23:49:36`     CORS:
- `23:49:36`       AllowOrigins:     ['*']
- `23:49:36`       AllowMethods:     ['*']
- `23:49:36`       AllowHeaders:     ['*']
- `23:49:36`       ExposeHeaders:    []
- `23:49:36`       AllowCredentials: False
- `23:49:36`       MaxAge:           86400
- `23:49:36` 
  Resource policy (1 statements):
- `23:49:36`     PublicFunctionUrlInvoke:
- `23:49:36`       Principal: *
- `23:49:36`       Action:    lambda:InvokeFunctionUrl
- `23:49:36`       Condition: {"StringEquals": {"lambda:FunctionUrlAuthType": "NONE"}}
## === justhodl-stock-screener ===

- `23:49:36`   FunctionArn: arn:aws:lambda:us-east-1:857687956942:function:justhodl-stock-screener
- `23:49:36`   Runtime: python3.12, Handler: lambda_function.lambda_handler
- `23:49:36`   LastModified: 2026-04-25T23:34:47
- `23:49:36` 
  Function URL:
- `23:49:36`     URL:      https://sajhxaiui7tijd54pbqzeibzee0xgxho.lambda-url.us-east-1.on.aws/
- `23:49:36`     AuthType: NONE
- `23:49:36`     CORS:
- `23:49:36`       AllowOrigins:     ['*']
- `23:49:36`       AllowMethods:     ['*']
- `23:49:36`       AllowHeaders:     ['*']
- `23:49:36`       ExposeHeaders:    []
- `23:49:36`       AllowCredentials: False
- `23:49:36`       MaxAge:           86400
- `23:49:36` 
  Resource policy (2 statements):
- `23:49:36`     FunctionURLAllowPublicAccess:
- `23:49:36`       Principal: *
- `23:49:36`       Action:    lambda:InvokeFunctionUrl
- `23:49:36`       Condition: {"StringEquals": {"lambda:FunctionUrlAuthType": "NONE"}}
- `23:49:36`     EventBridgeInvoke:
- `23:49:36`       Principal: {'Service': 'events.amazonaws.com'}
- `23:49:36`       Action:    lambda:InvokeFunction
- `23:49:36`       Condition: {"ArnLike": {"AWS:SourceArn": "arn:aws:events:us-east-1:857687956942:rule/justhodl-stock-screener-4h"}}
## Diagnosis

- `23:49:36` 
  AI resource policy:      present
- `23:49:36`   Working resource policy: present
- `23:49:36` Done
