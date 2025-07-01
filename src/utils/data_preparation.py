import json
from typing import Tuple

from kick_event import KickEvent
from config import (
    CHAT_MESSAGE_EVENT,
    SUBSCRIPTION_EVENT,
    USER_BANNED_EVENT,
    USER_UNBANNED_EVENT,
    MESSAGE_DELETED_EVENT,
    PINNED_MESSAGE_CREATED_EVENT,
    CHAT_MESSAGE_SENT_EVENT,
    CHATROOM_UPDATED_EVENT,
    STREAM_HOST_EVENT,
    PINNED_MESSAGE_DELETED_EVENT,
    CHATROOM_CLEAR_EVENT,
    SUBSCRIPTION_CONTENT_TEMPLATE,
    USER_BANNED_PERMANENT_TEMPLATE,
    USER_BANNED_TEMPORARY_TEMPLATE,
    USER_UNBANNED_TEMPLATE,
    MESSAGE_DELETED_AI_TEMPLATE,
    MESSAGE_DELETED_MANUAL_TEMPLATE,
    MESSAGE_PINNED_TEMPLATE,
    MESSAGE_SENT_TEMPLATE,
    CHATROOM_UPDATED_TEMPLATE,
    STREAM_HOST_TEMPLATE,
    PINNED_MESSAGE_DELETED_TEMPLATE,
    CHATROOM_CLEAR_TEMPLATE,
)
from utils.sanitize_validate import normalize_timestamp


def prepare_event_data(kick_event: KickEvent) -> Tuple:
    """
    Prepares event data for database insertion.

    Args:
        kick_event (KickEvent): The KickEvent dataclass instance

    Returns:
        Tuple: Prepared data for database insertion
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


def _prepare_chat_message_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare chat message event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    sender_dict = event_data_dict.get("sender", {})

    sender_data_str = (
        json.dumps(event_data_dict.get("sender"))
        if event_data_dict.get("sender")
        else None
    )
    metadata_str = (
        json.dumps(event_data_dict.get("metadata"))
        if event_data_dict.get("metadata")
        else None
    )
    raw_payload_str = json.dumps({"event": event_type, "data": event_data_dict})

    return (
        event_type,
        event_data_dict.get("id"),
        event_data_dict.get("chatroom_id"),
        event_data_dict.get("created_at"),
        sender_dict.get("id"),
        sender_dict.get("username"),
        event_data_dict.get("content"),
        sender_data_str,
        metadata_str,
        raw_payload_str,
    )


def _prepare_subscription_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare subscription event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    username = event_data_dict.get("username", "Unknown")
    months = event_data_dict.get("months", 0)
    content = SUBSCRIPTION_CONTENT_TEMPLATE.format(username=username, months=months)

    return (
        event_type,
        None,
        event_data_dict.get("chatroom_id"),
        None,
        None,
        username,
        content,
        json.dumps({"username": username, "months": months}),
        json.dumps({"months": months}),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_user_banned_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare user banned event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    user_dict = event_data_dict.get("user", {})
    banned_by_dict = event_data_dict.get("banned_by", {})
    username = user_dict.get("username", "Unknown")
    banned_by = banned_by_dict.get("username", "Unknown")

    if event_data_dict.get("permanent"):
        content = USER_BANNED_PERMANENT_TEMPLATE.format(
            username=username, banned_by=banned_by
        )
    else:
        duration = event_data_dict.get("duration", 0)
        content = USER_BANNED_TEMPORARY_TEMPLATE.format(
            username=username, duration=duration, banned_by=banned_by
        )

    return (
        event_type,
        event_data_dict.get("id"),
        None,
        event_data_dict.get("expires_at"),
        user_dict.get("id"),
        username,
        content,
        json.dumps(user_dict),
        json.dumps(
            {
                "banned_by": banned_by_dict,
                "banned_by_username": banned_by_dict.get("username"),
                "permanent": event_data_dict.get("permanent"),
                "duration": event_data_dict.get("duration"),
                "expires_at": event_data_dict.get("expires_at"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_user_unbanned_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare user unbanned event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    user_dict = event_data_dict.get("user", {})
    unbanned_by_dict = event_data_dict.get("unbanned_by", {})
    username = user_dict.get("username", "Unknown")
    unbanned_by = unbanned_by_dict.get("username", "Unknown")

    content = USER_UNBANNED_TEMPLATE.format(username=username, unbanned_by=unbanned_by)

    return (
        event_type,
        event_data_dict.get("id"),
        None,
        None,
        user_dict.get("id"),
        username,
        content,
        json.dumps(user_dict),
        json.dumps(
            {
                "unbanned_by": unbanned_by_dict,
                "unbanned_by_username": unbanned_by_dict.get("username"),
                "permanent": event_data_dict.get("permanent"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_message_deleted_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare message deleted event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    message_dict = event_data_dict.get("message", {})
    ai_moderated = event_data_dict.get("aiModerated", False)

    content = (
        MESSAGE_DELETED_AI_TEMPLATE if ai_moderated else MESSAGE_DELETED_MANUAL_TEMPLATE
    )

    return (
        event_type,
        event_data_dict.get("id"),
        None,
        None,
        None,
        None,
        content,
        None,
        json.dumps(
            {
                "deleted_message_id": message_dict.get("id"),
                "aiModerated": ai_moderated,
                "violatedRules": event_data_dict.get("violatedRules", []),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_pinned_message_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare pinned message created event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    message_dict = event_data_dict.get("message", {})
    pinned_by_dict = event_data_dict.get("pinnedBy", {})
    sender_dict = message_dict.get("sender", {})

    message_content = message_dict.get("content", "")
    content = MESSAGE_PINNED_TEMPLATE.format(content=message_content)

    return (
        event_type,
        message_dict.get("id"),
        message_dict.get("chatroom_id"),
        message_dict.get("created_at"),
        sender_dict.get("id"),
        sender_dict.get("username"),
        content,
        json.dumps(sender_dict),
        json.dumps(
            {
                "duration": event_data_dict.get("duration"),
                "pinnedBy": pinned_by_dict,
                "pinned_by_username": pinned_by_dict.get("username"),
                "original_metadata": message_dict.get("metadata"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_chat_message_sent_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare chat message sent event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    message_dict = event_data_dict.get("message", {})
    user_dict = event_data_dict.get("user", {})

    action = message_dict.get("action", "")
    message_type = message_dict.get("type", "")
    content = MESSAGE_SENT_TEMPLATE.format(message_type=message_type, action=action)

    timestamp = normalize_timestamp(message_dict.get("created_at"))

    return (
        event_type,
        message_dict.get("id"),
        message_dict.get("chatroom_id"),
        timestamp,
        user_dict.get("id"),
        user_dict.get("username"),
        content,
        json.dumps(user_dict),
        json.dumps(
            {
                "message_info": message_dict,
                "months_subscribed": message_dict.get("months_subscribed"),
                "subscriptions_count": message_dict.get("subscriptions_count"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_chatroom_updated_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare chatroom updated event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    content = CHATROOM_UPDATED_TEMPLATE

    return (
        event_type,
        event_data_dict.get("id"),
        None,
        None,
        None,
        None,
        content,
        None,
        json.dumps(
            {
                "slow_mode": event_data_dict.get("slow_mode"),
                "subscribers_mode": event_data_dict.get("subscribers_mode"),
                "followers_mode": event_data_dict.get("followers_mode"),
                "emotes_mode": event_data_dict.get("emotes_mode"),
                "advanced_bot_protection": event_data_dict.get(
                    "advanced_bot_protection"
                ),
                "account_age": event_data_dict.get("account_age"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_stream_host_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare stream host event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    host_username = event_data_dict.get("host_username", "Unknown")
    number_viewers = event_data_dict.get("number_viewers", 0)
    content = STREAM_HOST_TEMPLATE.format(
        host_username=host_username, number_viewers=number_viewers
    )

    return (
        event_type,
        None,
        event_data_dict.get("chatroom_id"),
        None,
        None,
        host_username,
        content,
        None,
        json.dumps(
            {
                "host_username": host_username,
                "number_viewers": number_viewers,
                "optional_message": event_data_dict.get("optional_message"),
            }
        ),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_pinned_message_deleted_data(
    event_data_dict: dict, event_type: str
) -> Tuple:
    """
    Prepare pinned message deleted event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    content = PINNED_MESSAGE_DELETED_TEMPLATE

    return (
        event_type,
        None,
        None,
        None,
        None,
        None,
        content,
        None,
        json.dumps({}),  # Empty data array from API
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_chatroom_clear_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare chatroom clear event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    content = CHATROOM_CLEAR_TEMPLATE

    return (
        event_type,
        event_data_dict.get("id"),
        None,
        None,
        None,
        None,
        content,
        None,
        json.dumps({"clear_id": event_data_dict.get("id")}),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )


def _prepare_generic_data(event_data_dict: dict, event_type: str) -> Tuple:
    """
    Prepare generic event data for database insertion.

    Args:
        event_data_dict (dict): The event data dictionary
        event_type (str): The event type

    Returns:
        Tuple: Prepared data for database insertion
    """
    return (
        event_type,
        event_data_dict.get("id"),
        event_data_dict.get("chatroom_id"),
        event_data_dict.get("created_at"),
        None,
        event_data_dict.get("username"),
        str(event_data_dict),  # Stringify the data for unknown event types
        None,
        json.dumps(event_data_dict),
        json.dumps({"event": event_type, "data": event_data_dict}),
    )
