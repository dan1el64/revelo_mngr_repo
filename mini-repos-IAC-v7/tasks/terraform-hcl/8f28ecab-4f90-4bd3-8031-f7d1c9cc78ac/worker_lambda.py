import json
import os

import boto3


def get_db_connection(secret, db_endpoint):
    import psycopg2

    return psycopg2.connect(
        host=db_endpoint,
        port=5432,
        user=secret["username"],
        password=secret["password"],
        dbname="postgres",
    )


def handler(event, context):
    secrets_client = boto3.client("secretsmanager")
    secret_arn = os.environ.get("SECRET_ARN", event["SECRET_ARN"])
    secret_response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(secret_response["SecretString"])

    db_endpoint = os.environ.get("DB_ENDPOINT", event["DB_ENDPOINT"])
    conn = get_db_connection(secret, db_endpoint)

    try:
        with conn.cursor() as cur:
            message = json.loads(event["body"])
            cur.execute(
                "INSERT INTO orders (id, data) VALUES (%s, %s)",
                (message["order_id"], json.dumps(message)),
            )
            conn.commit()
        return {
            "statusCode": 200,
            "body": "Success",
        }
    except Exception as exc:
        conn.rollback()
        return {
            "statusCode": 500,
            "body": str(exc),
        }
    finally:
        conn.close()
