import json
import os
import urllib.request


def lambda_handler(event, context):

    """ Lambda function for failed jobs monitoring """

    print(f"Received failure event: {json.dumps(event)}")

    # extract basic info from lambda async failure payload
    function_arn = event.get("requestContext", {}).get("functionArn", "")
    condition = event.get("requestContext", {}).get("condition", "Unknown")
    error_message = event.get("responsePayload", {}).get("errorMessage", "No error message available")
    error_type = event.get("responsePayload", {}).get("errorType", "Unknown")

    # parse function name from ARN
    function_name = function_arn.split(":")[-2] if ":" in function_arn else function_arn

    # send formatted message to Discord
    send_discord_message(function_name, condition, error_type, error_message)

    return {"statusCode": 200}


def send_discord_message(function_name: str, condition: str, error_type: str, error_message: str):

    """ Sends HTTP request with formated discord message """
    
    # get discord webhook URL from env
    DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]

    # build discord embed message
    embed = {
        "title": "Lambda Job Failed",
        "color": 15158332,  
        "fields": [
            {"name": "Function", "value": function_name, "inline": False},
            {"name": "Condition", "value": condition, "inline": True},
            {"name": "Error type", "value": error_type, "inline": True},
            {"name": "Error message", "value": f"```{error_message[:1000]}```", "inline": False},
        ],
    }

    # prepare HTTP request payload
    payload = {"embeds": [embed]}
    body = json.dumps(payload).encode("utf-8")

    # create POST request to discord webhook
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; DiscordBot/1.0)",
        },
        method="POST",
    )

    # send request to Discord
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Discord response status: {resp.status}")
    except Exception as e:
        print(f"Failed to send Discord notification: {e}")