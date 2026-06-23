import boto3
lam=boto3.client("lambda","us-east-1"); FN="justhodl-hot-stocks-digest"; GMAIL="raafouis@gmail.com"
cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
cur["SES_DIGEST_TO"]=GMAIL
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur})
import time; time.sleep(4)
back=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
print("SES_DIGEST_TO now =",back.get("SES_DIGEST_TO"))
print("-> until you verify the address, send to gmail fails in sandbox and auto-falls-back to reports@justhodl.ai")
print("-> the moment you verify, the morning brief routes to your gmail automatically")
print("DONE 2142")
