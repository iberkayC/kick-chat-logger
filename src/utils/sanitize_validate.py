"""Helpers for channel-name sanitization and timestamp parsing."""

import logging
import re
from datetime import UTC, datetime

from config import CHANNEL_TABLE_PREFIX

logger = logging.getLogger(__name__)


def sanitize_channel_name(name: str) -> str:
    """Normalize channel names for consistent storage.

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
    """Get the table name for a channel.

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


def parse_timestamp(timestamp) -> datetime | None:
    """Parse a timestamp into an aware UTC datetime.

    Handles datetime objects, ISO 8601 strings (including a Z suffix),
    and unix timestamps. Naive inputs are assumed UTC.

    Args:
        timestamp (datetime, str, int, float): The timestamp to parse

    Returns:
        Optional[datetime]: The parsed datetime, or None if the timestamp is invalid

    """
    if not timestamp:
        return None

    try:
        if isinstance(timestamp, datetime):
            parsed = timestamp
        elif isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp, UTC)
        elif isinstance(timestamp, str):
            parsed = datetime.fromisoformat(timestamp)
        else:
            logger.warning("Unknown timestamp format: %s", type(timestamp))
            return None
    except (ValueError, TypeError) as e:
        logger.warning("Failed to parse timestamp %s: %s", timestamp, e)
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
