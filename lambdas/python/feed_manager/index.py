""" Lambda function to add a feed to the database"""
# pylint: disable=unused-argument, import-error

import os
import json
import uuid

import boto3
from botocore.exceptions import ClientError

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.parser.pydantic import (
    ValidationError as PydanticValidationError,
)

import feedparser

from models import CreateFeedInput, FeedStatus

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TABLE_NAME"]
feed_table = dynamodb.Table(TABLE_NAME)

dynamodb_client = boto3.client("dynamodb")

logger = Logger()
tracer = Tracer()
metrics = Metrics()

app = APIGatewayRestResolver()


class InputValidationError(Exception):
    """Raised when input validation fails"""


class FeedValidationError(Exception):
    """Raised when feed validation fails"""


@app.exception_handler([InputValidationError, FeedValidationError])
def handle_invalid_input(exc: Exception):
    """Handle validation errors"""

    # Make the Pydantic error messages more readable
    readable_errors = [
        {
            "field": exc["loc"][0],
            "message": exc["msg"],
        }
    ]

    return {
        "statusCode": 400,
        "body": {
            "message": "Invalid validation error",
            "errors": readable_errors,
        },
    }


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


def store_feed_metadata(feed_url: str, feed_data: feedparser.FeedParserDict):
    """Store feed metadata in DynamoDB"""

    feed_id = str(uuid.uuid4())

    # Construct the PK and SK for the actual feed
    pk_value = {"S": f"FEED#{feed_id}"}
    sk_value = {"S": f"META#{feed_id}"}

    # Construct the PK and SK for the uniqueness check
    unique_pk = {"S": f"UNIQUE#FEED_URL#{feed_url}"}
    unique_sk = {"S": f"UNIQUE#FEED_URL#{feed_url}"}

    hub_links = [
        link for link in feed_data.feed.get("links", []) if link.get("rel") == "hub"
    ]
    topic_links = [
        link for link in feed_data.feed.get("links", []) if link.get("rel") == "self"
    ]

    push_hub_url = hub_links[0].href if hub_links else None
    push_topic_url = topic_links[0].href if topic_links else None

    # Extract categories or tags
    categories = [tag.term for tag in feed_data.feed.get("tags", [])]

    feed_metadata = {
        "feed_url": prepare_item("S", feed_url),
        "feed_atom_id": prepare_item("S", feed_data.feed.get("id")),
        "feed_title": prepare_item("S", feed_data.feed.get("title")),
        "feed_link": prepare_item("S", feed_data.feed.get("link")),
        "feed_description": prepare_item("S", feed_data.feed.get("description")),
        "feed_author": prepare_item("S", feed_data.feed.get("author")),
        "feed_language": prepare_item("S", feed_data.feed.get("language")),
        "feed_pub_date": prepare_item("S", feed_data.feed.get("pubDate")),
        "feed_last_build_date": prepare_item("S", feed_data.feed.get("lastBuildDate")),
        "feed_updated": prepare_item("S", feed_data.feed.get("updated")),
        "feed_ttl": prepare_item("S", feed_data.feed.get("ttl")),
        "feed_image": prepare_item("S", feed_data.feed.get("image")),
        "last_polled": prepare_item("S", ""),
        "update_period": prepare_item(
            "S", feed_data.feed.get("sy_updateperiod", "hourly")
        ),
        "update_frequency": prepare_item(
            "S", feed_data.feed.get("sy_updatefrequency", "1")
        ),
        "status": prepare_item("S", FeedStatus.ACTIVE.value),
        "error_count": prepare_item("N", 0),
        "last_error_message": prepare_item("S", ""),
        "push_supported": prepare_item("BOOL", "hub" in feed_data.feed),
        "push_hub_url": prepare_item("S", push_hub_url),
        "push_topic_url": prepare_item("S", push_topic_url),
        "push_last_subscription": prepare_item("S", ""),
        "categories": prepare_item("SS", categories),
        "version": prepare_item("S", feed_data.version),
    }

    # Filter out None values
    feed_metadata = {k: v for k, v in feed_metadata.items() if v}

    transact_items = [
        {
            "Put": {
                "TableName": TABLE_NAME,
                "Item": {
                    "PK": unique_pk,
                    "SK": unique_sk,
                },
                "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
            }
        },
        {
            "Put": {
                "TableName": TABLE_NAME,
                "Item": {
                    "PK": pk_value,
                    "SK": sk_value,
                    **feed_metadata,
                },
            }
        },
    ]

    dynamodb_client.transact_write_items(TransactItems=transact_items)


# def store_feed_items(feed_url: str, feed_data: feedparser.FeedParserDict):
#     """Store feed items in DynamoDB"""

#     feed_items = []
#     items_to_check_keys = []

#     for entry in feed_data.entries:
#         item_pk = f"FEED#{feed_url}"

#         if "guid" in entry:
#             item_sk = f"ITEM#{entry.guid}"
#         elif "id" in entry:
#             item_sk = f"ITEM#{entry.id}"
#         elif "link" in entry:
#             item_sk = f"ITEM#{entry.link}"
#         else:
#             continue

#         # Extract categories or tags
#         categories = [tag.term for tag in entry.get("tags", [])]

#         # Extract comments link
#         comments_link = entry.get("comments", None)
#         if not comments_link:
#             comments_links = [
#                 link.href for link in entry.get("links", []) if link.rel == "replies"
#             ]
#             comments_link = comments_links[0] if comments_links else None

#         item_data = {
#             "title": entry.get("title"),
#             "link": entry.get("link"),
#             "description": entry.get("description"),
#             "author": entry.get("author"),
#             "published": entry.get("published"),
#             "updated": entry.get("updated"),
#             "content": entry.get("content"),
#             "categories": categories or None,
#             "comments_link": comments_link,
#         }

#         feed_item = {
#             "PK": item_pk,
#             "SK": item_sk,
#             **item_data,
#         }
#         feed_items.append(feed_item)

#         # Add the item to the list of items to check for keys
#         items_to_check_keys.append({"PK": item_pk, "SK": item_sk})

#     # Batch get existing feed items from DynamoDB for comparison using the client interface
#     fetched_items_dict = {}
#     batch_keys = [{"PK": key["PK"], "SK": key["SK"]} for key in items_to_check_keys]
#     response = dynamodb_client.batch_get_item(
#         RequestItems={TABLE_NAME: {"Keys": batch_keys}}
#     )

#     fetched_items = response["Responses"].get(TABLE_NAME, [])
#     for item in fetched_items:
#         fetched_items_dict[(item["PK"], item["SK"])] = item

#     # Filter items to only those that are new or have changed
#     items_to_write = []
#     for item in feed_items:
#         key = (item["PK"], item["SK"])
#         existing_item = fetched_items_dict.get(key, None)
#         if not existing_item or existing_item != item:
#             items_to_write.append(item)

#     # Batch write the new or changed items to DynamoDB
#     with feed_table.batch_writer() as batch:
#         for item in items_to_write:
#             batch.put_item(Item=item)


@app.post("/feeds")
@tracer.capture_method
def create_feed() -> dict:
    """Add a feed to the database"""
    try:
        feed_input = CreateFeedInput(**app.current_event.json_body)
    except PydanticValidationError as exc:
        raise InputValidationError(exc.errors()) from exc

    feed_url = str(feed_input.feed_url)
    feed_data = feedparser.parse(feed_url)

    if feed_data.bozo:
        return {"statusCode": 400, "body": json.dumps({"message": "Invalid feed URL"})}

    # Extract necessary metadata from the parsed feed
    try:
        store_feed_metadata(feed_url, feed_data)
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

        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"}),
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Feed added successfully"}),
    }


@app.delete("/feeds/<feed_id>")
def delete_feed(feed_id: str):
    """Delete a feed from the database"""

    print(f"Deleting feed with ID {feed_id}")

    try:
        # Construct the PK and SK for the feed metadata
        pk_value = f"FEED#{feed_id}"
        sk_value = f"META#{feed_id}"

        # Update the feed's status to 'MARKED_FOR_DELETION'
        feed_table.update_item(
            Key={"PK": pk_value, "SK": sk_value},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": FeedStatus.INACTIVE},
        )
    except ClientError as exc:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"An error occurred: {exc}"}),
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Feed deleted successfully"}),
    }


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event, context: LambdaContext):
    """Lambda function to add a feed to the database"""
    print(f"Received event: {json.dumps(event)}")
    print(f"Received context: {vars(context)}")
    return app.resolve(event, context)
