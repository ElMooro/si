import boto3, json, time
ses=boto3.client("ses","us-east-1"); lam=boto3.client("lambda","us-east-1")
GMAIL="raafouis@gmail.com"; FN="justhodl-hot-stocks-digest"
# 1. current verification status
try:
    attrs=ses.get_identity_verification_attributes(Identities=[GMAIL])["VerificationAttributes"]
    status=attrs.get(GMAIL,{}).get("VerificationStatus","NotStarted")
except Exception as e: status="check_failed:"+str(e)[:40]
print("gmail SES status BEFORE:",status)
# 2. trigger verification email (AWS emails the clickable link to the gmail)
if status not in ("Success",):
    ses.verify_email_identity(EmailAddress=GMAIL)
    print("-> verification email sent to",GMAIL,"(AWS sender: no-reply-aws@amazon.com, subject 'Amazon SES Address Verification Request')")
else:
    print("-> already verified")
# 3. set SES_DIGEST_TO env (merge-safe) so it routes to gmail once verified; falls back to reports@ until then
cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
cur["SES_DIGEST_TO"]=GMAIL
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur})
print("-> SES_DIGEST_TO env set to",GMAIL,"on",FN)
# 4. account sending mode (sandbox vs production)
try:
    acct=ses.get_account_sending_enabled(); 
    snd=ses.describe_configuration_set if False else None
except Exception: pass
try:
    q=boto3.client("sesv2","us-east-1").get_account()
    print("SES account: production_access=",q.get("ProductionAccessEnabled"),"| sending_enabled=",q.get("SendingEnabled"))
except Exception as e:
    print("sesv2 account check:",str(e)[:50])
print("VERIFIED IDENTITIES:", ses.list_verified_email_addresses().get("VerifiedEmailAddresses"))
print("DONE 2141")
