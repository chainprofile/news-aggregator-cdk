""" Lambda function to establish web sub subscription """

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBStreamEvent,
    DynamoDBRecordEventName,
)


def handler(event: DynamoDBStreamEvent, context: LambdaContext):
    """Lambda handler"""
    event: DynamoDBStreamEvent = DynamoDBStreamEvent(event)

    print(f"Received event: {event}")
    print(f"Received context: {context}")

    # Multiple records can be delivered in a single event
    for record in event.records:
        print(f"Event name: {record.event_name}")
        if record.event_name == DynamoDBRecordEventName.INSERT:
            primary_key, _ = record.dynamodb.keys
            is_feed_meta = primary_key.startswith("FEED#")
            is_push_supported = (
                is_feed_meta and record.dynamodb.new_image["push_supported"]
            )

            # We only want the feed records that support push
            if not is_push_supported:
                continue

            print(f"Feed has push support: {is_push_supported}")
