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

from models import CreateFeedInput, FeedStatus, Feed


ITEMS_PER_PAGE = 20

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

    push_supported = bool(push_hub_url)

    # Extract categories or tags
    categories = [tag.term for tag in feed_data.feed.get("tags", [])]

    feed_image = feed_data.feed.get("image", {}).get("href")

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
        "feed_image": prepare_item("S", feed_image),
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
        "push_supported": prepare_item("BOOL", push_supported),
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


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def handler(event, context: LambdaContext):
    """Lambda function to add a feed to the database"""
    return app.resolve(event, context)
