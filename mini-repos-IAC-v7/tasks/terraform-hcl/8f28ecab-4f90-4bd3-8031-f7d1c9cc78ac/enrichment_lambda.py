import json


def handler(event, context):
    try:
        message = json.loads(event["body"])
        return {
            "statusCode": 200,
            "body": json.dumps(message),
        }
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        return {
            "statusCode": 400,
            "body": str(exc),
        }
