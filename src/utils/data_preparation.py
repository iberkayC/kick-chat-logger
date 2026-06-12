"""Turn parsed Kick events into typed PreparedEvent objects for storage."""

from dataclasses import dataclass
from datetime import datetime

from config import (
    CHAT_MESSAGE_EVENT,
    CHAT_MESSAGE_SENT_EVENT,
    CHATROOM_CLEAR_EVENT,
    CHATROOM_CLEAR_TEMPLATE,
    CHATROOM_UPDATED_EVENT,
    CHATROOM_UPDATED_TEMPLATE,
    MESSAGE_DELETED_AI_TEMPLATE,
    MESSAGE_DELETED_EVENT,
    MESSAGE_DELETED_MANUAL_TEMPLATE,
    MESSAGE_PINNED_TEMPLATE,
    MESSAGE_SENT_TEMPLATE,
    PINNED_MESSAGE_CREATED_EVENT,
    PINNED_MESSAGE_DELETED_EVENT,
    PINNED_MESSAGE_DELETED_TEMPLATE,
    STREAM_HOST_EVENT,
    STREAM_HOST_TEMPLATE,
    SUBSCRIPTION_CONTENT_TEMPLATE,
    SUBSCRIPTION_EVENT,
    USER_BANNED_EVENT,
    USER_BANNED_PERMANENT_TEMPLATE,
    USER_BANNED_TEMPORARY_TEMPLATE,
    USER_UNBANNED_EVENT,
    USER_UNBANNED_TEMPLATE,
)
from kick_event import KickEvent
from utils.sanitize_validate import parse_timestamp


@dataclass(frozen=True)
class PreparedEvent:
    """One event in storage-ready form, with native Python types.

    Backends serialize these fields to their own column types; nothing
    here is pre-stringified.
    """

    event_type: str
    event_id: str | None  # uuid for chat messages, stringified int elsewhere
    chatroom_id: int | None
    timestamp: datetime | None
    user_id: int | None
    username: str | None
    content: str | None
    sender_data: dict | None
    metadata: dict | None
    raw_payload: dict


def _as_str(value) -> str | None:
    return str(value) if value is not None else None


def _as_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def prepare_event_data(kick_event: KickEvent) -> PreparedEvent:
    """Prepare event data for database insertion.

    Args:
        kick_event (KickEvent): The KickEvent dataclass instance

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    event_data_dict = kick_event.data
    event_type = kick_event.event

    prepare_methods = {
        CHAT_MESSAGE_EVENT: _prepare_chat_message_data,
        SUBSCRIPTION_EVENT: _prepare_subscription_data,
        USER_BANNED_EVENT: _prepare_user_banned_data,
        USER_UNBANNED_EVENT: _prepare_user_unbanned_data,
        MESSAGE_DELETED_EVENT: _prepare_message_deleted_data,
        PINNED_MESSAGE_CREATED_EVENT: _prepare_pinned_message_data,
        CHAT_MESSAGE_SENT_EVENT: _prepare_chat_message_sent_data,
        CHATROOM_UPDATED_EVENT: _prepare_chatroom_updated_data,
        STREAM_HOST_EVENT: _prepare_stream_host_data,
        PINNED_MESSAGE_DELETED_EVENT: _prepare_pinned_message_deleted_data,
        CHATROOM_CLEAR_EVENT: _prepare_chatroom_clear_data,
    }

    prepare_method = prepare_methods.get(event_type, _prepare_generic_data)
    return prepare_method(event_data_dict, event_type)


def _prepare_chat_message_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare chat message event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    sender_dict = event_data_dict.get("sender", {})

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=_as_int(event_data_dict.get("chatroom_id")),
        timestamp=parse_timestamp(event_data_dict.get("created_at")),
        user_id=_as_int(sender_dict.get("id")),
        username=sender_dict.get("username"),
        content=event_data_dict.get("content"),
        sender_data=event_data_dict.get("sender") or None,
        metadata=event_data_dict.get("metadata") or None,
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_subscription_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare subscription event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    username = event_data_dict.get("username", "Unknown")
    months = event_data_dict.get("months", 0)
    content = SUBSCRIPTION_CONTENT_TEMPLATE.format(username=username, months=months)

    return PreparedEvent(
        event_type=event_type,
        event_id=None,
        chatroom_id=_as_int(event_data_dict.get("chatroom_id")),
        timestamp=None,
        user_id=None,
        username=username,
        content=content,
        sender_data={"username": username, "months": months},
        metadata={"months": months},
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_user_banned_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare user banned event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    user_dict = event_data_dict.get("user", {})
    banned_by_dict = event_data_dict.get("banned_by", {})
    username = user_dict.get("username", "Unknown")
    banned_by = banned_by_dict.get("username", "Unknown")

    if event_data_dict.get("permanent"):
        content = USER_BANNED_PERMANENT_TEMPLATE.format(
            username=username,
            banned_by=banned_by,
        )
    else:
        duration = event_data_dict.get("duration", 0)
        content = USER_BANNED_TEMPORARY_TEMPLATE.format(
            username=username,
            duration=duration,
            banned_by=banned_by,
        )

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=None,
        timestamp=parse_timestamp(event_data_dict.get("expires_at")),
        user_id=_as_int(user_dict.get("id")),
        username=username,
        content=content,
        sender_data=user_dict,
        metadata={
            "banned_by": banned_by_dict,
            "banned_by_username": banned_by_dict.get("username"),
            "permanent": event_data_dict.get("permanent"),
            "duration": event_data_dict.get("duration"),
            "expires_at": event_data_dict.get("expires_at"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_user_unbanned_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare user unbanned event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    user_dict = event_data_dict.get("user", {})
    unbanned_by_dict = event_data_dict.get("unbanned_by", {})
    username = user_dict.get("username", "Unknown")
    unbanned_by = unbanned_by_dict.get("username", "Unknown")

    content = USER_UNBANNED_TEMPLATE.format(username=username, unbanned_by=unbanned_by)

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=None,
        timestamp=None,
        user_id=_as_int(user_dict.get("id")),
        username=username,
        content=content,
        sender_data=user_dict,
        metadata={
            "unbanned_by": unbanned_by_dict,
            "unbanned_by_username": unbanned_by_dict.get("username"),
            "permanent": event_data_dict.get("permanent"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_message_deleted_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare message deleted event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    message_dict = event_data_dict.get("message", {})
    ai_moderated = event_data_dict.get("aiModerated", False)

    content = (
        MESSAGE_DELETED_AI_TEMPLATE if ai_moderated else MESSAGE_DELETED_MANUAL_TEMPLATE
    )

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=None,
        timestamp=None,
        user_id=None,
        username=None,
        content=content,
        sender_data=None,
        metadata={
            "deleted_message_id": message_dict.get("id"),
            "aiModerated": ai_moderated,
            "violatedRules": event_data_dict.get("violatedRules", []),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_pinned_message_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare pinned message created event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    message_dict = event_data_dict.get("message", {})
    pinned_by_dict = event_data_dict.get("pinnedBy", {})
    sender_dict = message_dict.get("sender", {})

    message_content = message_dict.get("content", "")
    content = MESSAGE_PINNED_TEMPLATE.format(content=message_content)

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(message_dict.get("id")),
        chatroom_id=_as_int(message_dict.get("chatroom_id")),
        timestamp=parse_timestamp(message_dict.get("created_at")),
        user_id=_as_int(sender_dict.get("id")),
        username=sender_dict.get("username"),
        content=content,
        sender_data=sender_dict,
        metadata={
            "duration": event_data_dict.get("duration"),
            "pinnedBy": pinned_by_dict,
            "pinned_by_username": pinned_by_dict.get("username"),
            "original_metadata": message_dict.get("metadata"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_chat_message_sent_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare chat message sent event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    message_dict = event_data_dict.get("message", {})
    user_dict = event_data_dict.get("user", {})

    action = message_dict.get("action", "")
    message_type = message_dict.get("type", "")
    content = MESSAGE_SENT_TEMPLATE.format(message_type=message_type, action=action)

    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(message_dict.get("id")),
        chatroom_id=_as_int(message_dict.get("chatroom_id")),
        timestamp=parse_timestamp(message_dict.get("created_at")),
        user_id=_as_int(user_dict.get("id")),
        username=user_dict.get("username"),
        content=content,
        sender_data=user_dict,
        metadata={
            "message_info": message_dict,
            "months_subscribed": message_dict.get("months_subscribed"),
            "subscriptions_count": message_dict.get("subscriptions_count"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_chatroom_updated_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare chatroom updated event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=None,
        timestamp=None,
        user_id=None,
        username=None,
        content=CHATROOM_UPDATED_TEMPLATE,
        sender_data=None,
        metadata={
            "slow_mode": event_data_dict.get("slow_mode"),
            "subscribers_mode": event_data_dict.get("subscribers_mode"),
            "followers_mode": event_data_dict.get("followers_mode"),
            "emotes_mode": event_data_dict.get("emotes_mode"),
            "advanced_bot_protection": event_data_dict.get(
                "advanced_bot_protection",
            ),
            "account_age": event_data_dict.get("account_age"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_stream_host_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare stream host event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    host_username = event_data_dict.get("host_username", "Unknown")
    number_viewers = event_data_dict.get("number_viewers", 0)
    content = STREAM_HOST_TEMPLATE.format(
        host_username=host_username,
        number_viewers=number_viewers,
    )

    return PreparedEvent(
        event_type=event_type,
        event_id=None,
        chatroom_id=_as_int(event_data_dict.get("chatroom_id")),
        timestamp=None,
        user_id=None,
        username=host_username,
        content=content,
        sender_data=None,
        metadata={
            "host_username": host_username,
            "number_viewers": number_viewers,
            "optional_message": event_data_dict.get("optional_message"),
        },
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_pinned_message_deleted_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare pinned message deleted event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    return PreparedEvent(
        event_type=event_type,
        event_id=None,
        chatroom_id=None,
        timestamp=None,
        user_id=None,
        username=None,
        content=PINNED_MESSAGE_DELETED_TEMPLATE,
        sender_data=None,
        metadata={},  # Empty data array from API
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_chatroom_clear_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare chatroom clear event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=None,
        timestamp=None,
        user_id=None,
        username=None,
        content=CHATROOM_CLEAR_TEMPLATE,
        sender_data=None,
        metadata={"clear_id": event_data_dict.get("id")},
        raw_payload={"event": event_type, "data": event_data_dict},
    )


def _prepare_generic_data(
    event_data_dict: dict,
    event_type: str,
) -> PreparedEvent:
    """Prepare generic event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        PreparedEvent: Typed event data ready for a storage backend

    """
    return PreparedEvent(
        event_type=event_type,
        event_id=_as_str(event_data_dict.get("id")),
        chatroom_id=_as_int(event_data_dict.get("chatroom_id")),
        timestamp=parse_timestamp(event_data_dict.get("created_at")),
        user_id=None,
        username=event_data_dict.get("username"),
        content=str(event_data_dict),  # Stringify the data for unknown event types
        sender_data=None,
        metadata=event_data_dict,
        raw_payload={"event": event_type, "data": event_data_dict},
    )
