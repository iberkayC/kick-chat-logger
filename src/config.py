"""
Configuration settings for the Kick chat logger.
"""

#### kick_chat_listener.py
# WebSocket configuration
WEBSOCKET_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?"
    "protocol=7&client=js&version=7.6.0&flash=false"
)

# Event type constants
# adding an event here will only cause the scraper to not
# put it inside the unhandles messages file. it will still be
# logged as a generic message to the database. if one wants to
# add new event types, they need to add a prepare function to
# storage.py, and add it to the HANDLED_EVENTS set.
CHAT_MESSAGE_EVENT = r"App\Events\ChatMessageEvent"
SUBSCRIPTION_EVENT = r"App\Events\SubscriptionEvent"
USER_BANNED_EVENT = r"App\Events\UserBannedEvent"
USER_UNBANNED_EVENT = r"App\Events\UserUnbannedEvent"
MESSAGE_DELETED_EVENT = r"App\Events\MessageDeletedEvent"
PINNED_MESSAGE_CREATED_EVENT = r"App\Events\PinnedMessageCreatedEvent"
CHAT_MESSAGE_SENT_EVENT = r"App\Events\ChatMessageSentEvent"
CHATROOM_UPDATED_EVENT = r"App\Events\ChatroomUpdatedEvent"
STREAM_HOST_EVENT = r"App\Events\StreamHostEvent"
PINNED_MESSAGE_DELETED_EVENT = r"App\Events\PinnedMessageDeletedEvent"
CHATROOM_CLEAR_EVENT = r"App\Events\ChatroomClearEvent"
POLL_UPDATE_EVENT = r"App\Events\PollUpdateEvent"
POLL_DELETE_EVENT = r"App\Events\PollDeleteEvent"
PUSHER_CONNECTION_ESTABLISHED_EVENT = "pusher:connection_established"
PUSHER_INTERNAL_SUBSCRIPTION_SUCCEEDED_EVENT = "pusher_internal:subscription_succeeded"

HANDLED_EVENTS = {
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
}

IGNORED_EVENTS = {
    PUSHER_CONNECTION_ESTABLISHED_EVENT,
    PUSHER_INTERNAL_SUBSCRIPTION_SUCCEEDED_EVENT,
    POLL_UPDATE_EVENT,
    POLL_DELETE_EVENT,
}

UNHANDLED_MESSAGES_FILE = "unhandled_messages.txt"

PING_INTERVAL_MINUTES = 60

#### kick_api.py
KICK_API_V2_URL = "https://kick.com/api/v2/channels/"

#### storage.py
# Database configuration
DEFAULT_DB_PATH = "database/kick_scraper.db"
CHANNEL_TABLE_PREFIX = "kickchat_"

# Content message templates
SUBSCRIPTION_CONTENT_TEMPLATE = "{username} subscribed for {months} months"
USER_BANNED_PERMANENT_TEMPLATE = "{username} was banned permanently by {banned_by}"
USER_BANNED_TEMPORARY_TEMPLATE = (
    "{username} was banned for {duration} seconds by {banned_by}"
)
USER_UNBANNED_TEMPLATE = "{username} was unbanned by {unbanned_by}"
MESSAGE_DELETED_AI_TEMPLATE = "Message deleted (AI moderated)"
MESSAGE_DELETED_MANUAL_TEMPLATE = "Message deleted (manually moderated)"
MESSAGE_PINNED_TEMPLATE = "Message pinned: {content}"
MESSAGE_SENT_TEMPLATE = "Message sent ({message_type}): {action}"
# we could add more info here like slow mode settings, etc.
# but it's not worth it, not very interesting.
CHATROOM_UPDATED_TEMPLATE = "Chatroom settings updated"
STREAM_HOST_TEMPLATE = (
    "{host_username} is hosting the stream with {number_viewers} viewers"
)
PINNED_MESSAGE_DELETED_TEMPLATE = "Pinned message deleted"
CHATROOM_CLEAR_TEMPLATE = "Chatroom cleared"
