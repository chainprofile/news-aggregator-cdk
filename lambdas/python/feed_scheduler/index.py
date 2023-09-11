""" Lambda handler for feed_scheduler """
# pylint: disable=unused-argument

from datetime import datetime, timedelta
import os
import json

from aws_lambda_powertools.utilities.data_classes import event_source, EventBridgeEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import boto3


# Initialize DynamoDB and SQS clients
dynamodb = boto3.resource("dynamodb")
sqs = boto3.resource("sqs")

# References to the DynamoDB table and SQS queue
TABLE_NAME = os.environ["TABLE_NAME"]
QUEUE_URL = os.environ["QUEUE_URL"]

PERIOD_TO_SECONDS = {
    "hourly": 3600,
    "daily": 86400,
    "weekly": 604800,
    "monthly": 2592000,
}


# pylint: disable=no-value-for-parameter
@event_source(data_class=EventBridgeEvent)
def handler(event: EventBridgeEvent, context: LambdaContext):
    """Lambda handler for feed_scheduler"""

    # Reference to the DynamoDB table
    table = dynamodb.Table(TABLE_NAME)

    # Reference to the SQS queue using Queue URL
    queue = sqs.Queue(QUEUE_URL)

    # Scan the DynamoDB table for records matching the keys pattern
    response = table.scan(
        FilterExpression="begins_with(#pk, :pk) AND begins_with(#sk, :sk)",
        ExpressionAttributeNames={"#pk": "PK", "#sk": "SK"},
        ExpressionAttributeValues={":pk": "FEED#", ":sk": "SCHEDULE#"},
    )

    # Iterate over the records
    for item in response["Items"]:
        feed_id = item["PK"].split("#")[1]
        feed_url = item["feed_url"]

        last_polled = datetime.strptime(item["last_polled"], "%Y-%m-%d %H:%M:%S")

        # Get the update_period and update_frequency values
        update_period = item.get(
            "update_period", "hourly"
        )  # Default to 'daily' if not present
        update_frequency = int(
            item.get("update_frequency", 1)
        )  # Default to 1 if not present

        # Calculate polling interval in seconds
        polling_interval = PERIOD_TO_SECONDS.get(update_period, 3600) * update_frequency

        # Calculate the next polling time
        next_polled = last_polled + timedelta(seconds=polling_interval)

        # Get the current time
        current_time = datetime.utcnow()

        # If the next polling time is in the past, add the feed to the SQS queue
        if current_time >= next_polled:
            message_body = json.dumps(
                {
                    "feed_id": feed_id,
                    "feed_url": feed_url,
                }
            )
            queue.send_message(MessageBody=message_body)
            print(f"Added {feed_id} to SQS queue for fetching feed items")
