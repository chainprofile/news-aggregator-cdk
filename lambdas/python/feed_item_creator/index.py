""" Lambda function to fetch feed items """
# pylint: disable=unused-argument

import os
import json

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBStreamEvent,
    DynamoDBRecordEventName,
)
from aws_lambda_powertools.utilities.parser.pydantic import (
    ValidationError as PydanticValidationError,
)
import boto3
from botocore.exceptions import ClientError
import feedparser

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TABLE_NAME"]
feed_table = dynamodb.Table(TABLE_NAME)

dynamodb_client = boto3.client("dynamodb")


class FeedValidationError(Exception):
    """Raised when feed validation fails"""


def prepare_item(key_type, value):
    """Prepare an item for DynamoDB"""
    if key_type == "S" and value:
        return {key_type: str(value)}
    if key_type == "N" and value is not None:
        return {key_type: str(value)}
    if key_type == "BOOL":
        return {key_type: bool(value)}
    if key_type == "SS" and value:
        return {key_type: value}
    return None


def chunk(items, batch_size):
    """Yield successive batch_size-sized chunks from items."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def store_feed_items(feed_id: str, feed_data: feedparser.FeedParserDict):
    """Store feed items in DynamoDB"""

    feed_items = []
    items_to_check_keys = []

    for entry in feed_data.entries:
        item_pk = {"S": f"FEED#{feed_id}"}

        if "guid" in entry:
            item_sk = {"S": f"ITEM#{entry.guid}"}
        elif "id" in entry:
            item_sk = {"S": f"ITEM#{entry.id}"}
        elif "link" in entry:
            item_sk = {"S": f"ITEM#{entry.link}"}
        else:
            continue

        # Extract categories or tags
        categories = list({tag.term for tag in entry.get("tags", [])})

        # Extract comments link
        comments_link = entry.get("comments", None)
        if not comments_link:
            comments_links = [
                link.href for link in entry.get("links", []) if link.rel == "replies"
            ]
            comments_link = comments_links[0] if comments_links else None

        item_data = {
            "title": prepare_item("S", entry.get("title")),
            "link": prepare_item("S", entry.get("link")),
            "description": prepare_item("S", entry.get("description")),
            "author": prepare_item("S", entry.get("author")),
            "published": prepare_item("S", entry.get("published")),
            "updated": prepare_item("S", entry.get("updated")),
            "content": prepare_item("S", entry.get("content")),
            "categories": prepare_item("SS", categories),
            "comments_link": prepare_item("S", comments_link),
        }

        # Remove keys with empty dictionary values
        item_data = {k: v for k, v in item_data.items() if v}

        feed_item = {
            "PK": item_pk,
            "SK": item_sk,
            **item_data,
        }
        feed_items.append(feed_item)

        # Add the item to the list of items to check for keys
        items_to_check_keys.append({"PK": item_pk, "SK": item_sk})

    # Batch get existing feed items from DynamoDB for comparison using the client interface
    fetched_items_dict = {}
    batch_keys = [{"PK": key["PK"], "SK": key["SK"]} for key in items_to_check_keys]
    response = dynamodb_client.batch_get_item(
        RequestItems={TABLE_NAME: {"Keys": batch_keys}}
    )

    fetched_items = response["Responses"].get(TABLE_NAME, [])
    for item in fetched_items:
        fetched_items_dict[(item["PK"]["S"], item["SK"]["S"])] = item

    print(f"existing items: {fetched_items_dict}")

    # Filter items to only those that are new or have changed
    items_to_write = []
    for item in feed_items:
        key = (item["PK"]["S"], item["SK"]["S"])
        existing_item = fetched_items_dict.get(key, None)
        if not existing_item or existing_item != item:
            items_to_write.append(item)

    print(f"Items to write: {items_to_write}")

    # Batch write the new or changed items to DynamoDB using the client API
    for batch in chunk(items_to_write, 25):  # DynamoDB allows up to 25 items in a batch
        write_requests = [{"PutRequest": {"Item": item}} for item in batch]
        dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: write_requests})


def stream_handler(event: DynamoDBStreamEvent, context: LambdaContext):
    """Lambda handler"""
    event: DynamoDBStreamEvent = DynamoDBStreamEvent(event)

    # Multiple records can be delivered in a single event
    for record in event.records:
        print(f"Event name: {record.event_name}")
        if record.event_name == DynamoDBRecordEventName.INSERT:
            primary_key = record.dynamodb.keys["PK"]
            sort_key = record.dynamodb.keys["SK"]
            if not (primary_key.startswith("FEED#") and sort_key.startswith("META#")):
                continue

            feed_id = record.dynamodb.new_image["PK"].split("#")[1]
            feed_url = record.dynamodb.new_image["feed_url"]
            feed_data = feedparser.parse(feed_url)

            if feed_data.bozo:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid feed URL"}),
                }

            # Extract necessary metadata from the parsed feed
            try:
                store_feed_items(feed_id, feed_data)
            except PydanticValidationError as exc:
                raise FeedValidationError(exc.errors()) from exc
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "TransactionCanceledException":
                    # One of the conditions failed (likely the uniqueness check)
                    return {
                        "statusCode": 400,
                        "body": json.dumps(
                            {
                                "message": f"A feed with URL {feed_url} already exists or another condition failed!"
                            }
                        ),
                    }
                print(exc)

    return {"statusCode": 200, "body": json.dumps({"message": "Success"})}
