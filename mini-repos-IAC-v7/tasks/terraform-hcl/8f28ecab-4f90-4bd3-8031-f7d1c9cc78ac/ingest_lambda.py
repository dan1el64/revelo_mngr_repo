import os

import boto3


def handler(event, context):
    client = boto3.client("events")
    event_bus_name = os.environ.get("EVENTBUS_NAME", event.get("EVENTBUS_NAME", ""))

    response = client.put_events(
        Entries=[
            {
                "Source": "app.orders",
                "DetailType": "OrderCreated",
                "Detail": str(event),
                "EventBusName": event_bus_name,
            }
        ]
    )
    return response
