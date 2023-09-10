""" Model for add_feed lambda function """

from enum import Enum
from typing import Optional
from aws_lambda_powertools.utilities.parser.pydantic import (
    BaseModel,
    ConfigDict,
    HttpUrl,
    field_serializer,
)


class FeedStatus(str, Enum):
    """Feed status"""

    ACTIVE = "active"
    INACTIVE = "inactive"


class Feed(BaseModel):
    """Feed model"""

    model_config = ConfigDict()

    feed_url: HttpUrl
    feed_atom_id: Optional[str]  # Atom's feed id
    feed_title: str
    feed_link: Optional[HttpUrl]
    feed_description: Optional[str]
    feed_author: Optional[str]
    feed_language: Optional[str]
    feed_pub_date: Optional[str]  # RSS's pubDate
    feed_last_build_date: Optional[str]  # RSS's lastBuildDate
    feed_updated: Optional[str]  # Atom's updated field
    feed_ttl: Optional[int]
    feed_image: Optional[HttpUrl]
    last_polled: str
    update_period: str
    update_frequency: str
    status: FeedStatus
    error_count: int
    last_error_message: Optional[str]
    push_supported: bool
    push_hub_url: Optional[HttpUrl]
    push_topic_url: Optional[HttpUrl]
    push_last_subscription: Optional[str]
    categories: Optional[list[str]]  # Categories or tags
    version: str  # Directly storing the feed version provided by feedparser

    @field_serializer(
        "feed_url",
        "feed_link",
        "feed_image",
        "push_hub_url",
        "push_topic_url",
    )
    def serialize_url(self, value: HttpUrl) -> str:
        """Convert url to string."""
        return str(value) if value else value


class FeedItem(BaseModel):
    """Feed item model"""

    model_config = ConfigDict()

    item_id: str  # This will be the GUID (RSS), ID (Atom), or LINK
    title: str
    description: Optional[str]
    link: HttpUrl
    author: Optional[str]
    published: Optional[str]
    updated: Optional[str]
    content: Optional[str]
    categories: Optional[list[str]]
    comments_link: Optional[HttpUrl]

    @field_serializer(
        "link",
        "comments_link",
    )
    def serialize_url(self, value: HttpUrl) -> str:
        """Convert url to string."""
        return str(value) if value else value


class CreateFeedInput(BaseModel):
    """Input for adding a feed"""

    feed_url: HttpUrl

    @field_serializer("feed_url")
    def serialize_url(self, value: HttpUrl) -> str:
        """Convert url to string."""
        return str(value) if value else value
