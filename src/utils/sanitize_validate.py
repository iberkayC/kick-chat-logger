import logging
from typing import Optional
from datetime import datetime, UTC

import re

from config import CHANNEL_TABLE_PREFIX

logger = logging.getLogger(__name__)


def sanitize_channel_name(name: str) -> str:
    """
    Normalizes and sanitizes channel names for consistent storage and table naming.
    Converts to lowercase and replaces invalid characters with underscores.

    Args:
        name (str): The raw channel name

    Returns:
        str: The normalized channel name
    """
    # Convert to lowercase for consistency (xQc = xqc)
    normalized = name.lower().strip()

    # Replace non-alphanumeric characters with underscores for safe table naming
    # Note: This means 'xqc.' and 'xqc_' become the same, but Kick channel names
    # typically don't contain special characters, so this is acceptable
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", normalized)

    return sanitized


def get_channel_table_name(channel_name: str) -> str:
    """
    Gets the table name for a channel.

    Args:
        channel_name (str): The name of the channel

    Returns:
        str: The table name for the channel, prefixed with the CHANNEL_TABLE_PREFIX constant
    """
    sanitized = sanitize_channel_name(channel_name)
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
            # Handle datetime objects directly
            return timestamp.isoformat() + "Z"
        elif isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp, UTC).isoformat() + "Z"
        elif isinstance(timestamp, str):
            # already a string, assume it's properly formatted
            return timestamp
        else:
            logger.warning("Unknown timestamp format: %s", type(timestamp))
            return None
    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize timestamp %s: %s", timestamp, e)
        return None
