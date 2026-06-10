import logging
from typing import Optional
from datetime import datetime, UTC

import re

from config import CHANNEL_TABLE_PREFIX

logger = logging.getLogger(__name__)


def sanitize_channel_name(name: str) -> str:
    """
    Normalizes channel names for consistent storage.
    Converts to lowercase and strips whitespace.

    Args:
        name (str): The raw channel name

    Returns:
        str: The normalized channel name
    """
    # Convert to lowercase for consistency (xQc = xqc), but keep the name
    # otherwise untouched: kick slugs can contain dashes, and mangling them
    # here used to break the kick api lookup when channels were restarted
    return name.lower().strip()


def get_channel_table_name(channel_name: str) -> str:
    """
    Gets the table name for a channel.

    Args:
        channel_name (str): The name of the channel

    Returns:
        str: The table name for the channel, prefixed with the CHANNEL_TABLE_PREFIX constant
    """
    normalized = sanitize_channel_name(channel_name)

    # Replace non-alphanumeric characters with underscores for safe table naming
    # Note: This means 'a-b' and 'a_b' share a table, but collisions like that
    # are rare enough that this is acceptable
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", normalized)

    return f"{CHANNEL_TABLE_PREFIX}{sanitized}"


def normalize_timestamp(timestamp) -> Optional[str]:
    """
    Normalizes timestamps to ISO format with UTC timezone.
    Handles datetime objects, string timestamps, and unix timestamps.

    Args:
        timestamp (datetime, str, int, float): The timestamp to normalize

    Returns:
        Optional[str]: The normalized timestamp, or None if the timestamp is invalid
    """
    if not timestamp:
        return None

    try:
        if isinstance(timestamp, datetime):
            # Handle datetime objects directly, naive ones are assumed UTC
            # isoformat() already includes +00:00, so no "Z" suffix is needed
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            return timestamp.astimezone(UTC).isoformat()
        elif isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp, UTC).isoformat()
        elif isinstance(timestamp, str):
            # already a string, assume it's properly formatted
            return timestamp
        else:
            logger.warning("Unknown timestamp format: %s", type(timestamp))
            return None
    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize timestamp %s: %s", timestamp, e)
        return None
