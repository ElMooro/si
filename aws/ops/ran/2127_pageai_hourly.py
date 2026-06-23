import boto3
ev=boto3.client("events","us-east-1")
ev.put_rule(Name="justhodl-page-ai-wave",ScheduleExpression="cron(15 * * * ? *)",State="ENABLED")
print("page-ai schedule -> hourly (cron 15 * * * ? *); ~37 pages/wave x 24 = full 331 coverage in <9h then steady refresh")
print("DONE 2127")
