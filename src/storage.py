"""
This module handles database storage for Kick chat events.
It manages SQLite database operations for storing chat messages, subscriptions,
bans, and other events from Kick chat streams.
"""

import logging
import re
import os
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional, Tuple
import json
import sqlite3

import aiosqlite

from kick_event import KickEvent
from config import (
    DEFAULT_DB_PATH,
    CHANNEL_TABLE_PREFIX,
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


logger = logging.getLogger(__name__)


class KickChatStorage:
    """
    Handles database storage for Kick chat events.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path

    async def initialize(self) -> bool:
        """
        Initializes the database and creates the channels table if it doesn't exist.

        Returns:
            bool: True if initialized successfully, False otherwise
        """
        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                logger.info("Created database directory: %s", db_dir)

            async with aiosqlite.connect(self.db_path) as db:
                await self._enable_wal_mode(db)
                await self._create_channels_table(db)

            logger.info("Database initialized successfully")
            return True
        except (sqlite3.Error, OSError) as e:
            logger.error("Failed to initialize database: %s", e)
            return False

    async def _enable_wal_mode(self, db: aiosqlite.Connection) -> bool:
        """
        Enables WAL (Write-Ahead Logging) mode for better concurrent access.
        
        Args:
            db (aiosqlite.Connection): The database connection
            
        Returns:
            bool: True if WAL mode was enabled successfully, False otherwise
        """
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute("PRAGMA cache_size=10000")
            await db.execute("PRAGMA temp_store=memory")
            await db.commit()
            logger.info("WAL mode and performance optimizations enabled")
            return True
        except sqlite3.Error as e:
            logger.error("Failed to enable WAL mode: %s", e)
            return False

    async def _create_channels_table(self, db: aiosqlite.Connection) -> bool:
        """
        Creates the channels table if it doesn't exist.

        Args:
            db (aiosqlite.Connection): The database connection

        Returns:
            bool: True if created successfully, False otherwise
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            added_at TEXT NOT NULL,
            paused BOOLEAN DEFAULT FALSE,
            paused_at TEXT
        );
        """
        try:
            await db.execute(create_table_sql)
            await db.commit()
            logger.info("Channels table created successfully")
            return True

        except sqlite3.Error as e:
            logger.error("Failed to create channels table: %s", e)
            return False

    def sanitize_channel_name(self, name: str) -> str:
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

    def _get_channel_table_name(self, channel_name: str) -> str:
        """
        Gets the table name for a channel.

        Args:
            channel_name (str): The name of the channel

        Returns:
            str: The table name for the channel, prefixed with the CHANNEL_TABLE_PREFIX constant
        """
        sanitized = self.sanitize_channel_name(channel_name)
        return f"{CHANNEL_TABLE_PREFIX}{sanitized}"

    def _normalize_timestamp(self, timestamp) -> Optional[str]:
        """
        Normalizes timestamps to ISO format with UTC timezone.
        Handles both string timestamps and unix timestamps.

        Args:
            timestamp (str): The timestamp to normalize

        Returns:
            Optional[str]: The normalized timestamp, or None if the timestamp is invalid
        """
        if not timestamp:
            return None

        try:
            if isinstance(timestamp, (int, float)):
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

    async def _create_channel_chat_table(
        self, db: aiosqlite.Connection, channel_name: str
    ) -> bool:
        """
        Creates the channel chat table if it doesn't exist.

        Args:
            db (aiosqlite.Connection): The database connection
            channel_name (str): The name of the channel

        Returns:
            bool: True if created successfully, False otherwise
        """
        table_name = self._get_channel_table_name(channel_name)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            event_id TEXT,
            chatroom_id TEXT,
            timestamp TEXT,
            user_id TEXT,
            username TEXT,
            content TEXT,
            sender_data TEXT,
            metadata TEXT,
            raw_payload TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """

        try:
            await db.execute(create_table_sql)
            logger.info("Channel chat table %s created successfully", table_name)
            return True
        except sqlite3.Error as e:
            logger.error("Failed to create channel chat table %s: %s", table_name, e)
            return False

    async def add_channel(self, channel_name: str) -> bool:
        """
        Adds a channel to the database and creates its chat table.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if added successfully, False otherwise
        """
        normalized_name = self.sanitize_channel_name(channel_name)
        timestamp = datetime.now(UTC).isoformat() + "Z"  # UTC timestamp

        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO channels (name, added_at) VALUES (?, ?)",
                    (normalized_name, timestamp),
                )

                table_created = await self._create_channel_chat_table(
                    db, normalized_name
                )

                if table_created:
                    await db.commit()
                    logger.info(
                        "Channel %s (normalized: %s) added successfully",
                        channel_name,
                        normalized_name,
                    )
                    return True
                return False

        except sqlite3.Error as e:
            logger.error("Failed to add channel %s: %s", channel_name, e)
            return False

    async def list_all_channels(self) -> List[Dict[str, Any]]:
        """
        Lists all channels in the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing channel info
            (name, added_at, paused, paused_at) or an empty list if the database is not initialized
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT name, added_at, paused, paused_at FROM channels"
                ) as cursor:
                    rows = await cursor.fetchall()

                channels = []
                for row in rows:
                    channels.append(
                        {
                            "name": row[0],
                            "added_at": row[1],
                            "paused": bool(row[2]),
                            "paused_at": row[3],
                        }
                    )

                return channels

        except sqlite3.Error as e:
            logger.error("Failed to list channels: %s", e)
            return []

    async def channel_exists(self, channel_name: str) -> bool:
        """
        Checks if a channel exists in the database.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if the channel exists, False otherwise
        """
        normalized_name = self.sanitize_channel_name(channel_name)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT 1 FROM channels WHERE name = ?", (normalized_name,)
                ) as cursor:
                    result = await cursor.fetchone()
                    return result is not None

        except sqlite3.Error as e:
            logger.error("Failed to check if channel exists: %s", e)
            return False

    async def _validate_and_normalize_channel(self, channel_name: str) -> Optional[str]:
        """
        Validates and normalizes a channel name.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Optional[str]: The normalized channel name, or None if the channel does not exist
        """
        normalized_name = self.sanitize_channel_name(channel_name)

        if not await self.channel_exists(normalized_name):
            logger.error("Channel does not exist: %s", normalized_name)
            return None

        return normalized_name

    def _prepare_event_data(self, kick_event: KickEvent) -> Tuple:
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
            CHAT_MESSAGE_EVENT: self._prepare_chat_message_data,
            SUBSCRIPTION_EVENT: self._prepare_subscription_data,
            USER_BANNED_EVENT: self._prepare_user_banned_data,
            USER_UNBANNED_EVENT: self._prepare_user_unbanned_data,
            MESSAGE_DELETED_EVENT: self._prepare_message_deleted_data,
            PINNED_MESSAGE_CREATED_EVENT: self._prepare_pinned_message_data,
            CHAT_MESSAGE_SENT_EVENT: self._prepare_chat_message_sent_data,
            CHATROOM_UPDATED_EVENT: self._prepare_chatroom_updated_data,
            STREAM_HOST_EVENT: self._prepare_stream_host_data,
            PINNED_MESSAGE_DELETED_EVENT: self._prepare_pinned_message_deleted_data,
            CHATROOM_CLEAR_EVENT: self._prepare_chatroom_clear_data,
        }

        prepare_method = prepare_methods.get(event_type, self._prepare_generic_data)
        return prepare_method(event_data_dict, event_type)

    def _prepare_chat_message_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_subscription_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_user_banned_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_user_unbanned_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

        content = USER_UNBANNED_TEMPLATE.format(
            username=username, unbanned_by=unbanned_by
        )

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

    def _prepare_message_deleted_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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
            MESSAGE_DELETED_AI_TEMPLATE
            if ai_moderated
            else MESSAGE_DELETED_MANUAL_TEMPLATE
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

    def _prepare_pinned_message_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_chat_message_sent_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

        timestamp = self._normalize_timestamp(message_dict.get("created_at"))

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

    def _prepare_chatroom_updated_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_stream_host_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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
        self, event_data_dict: dict, event_type: str
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

    def _prepare_chatroom_clear_data(
        self, event_data_dict: dict, event_type: str
    ) -> Tuple:
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

    def _prepare_generic_data(self, event_data_dict: dict, event_type: str) -> Tuple:
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

    async def store_event(self, channel_name: str, kick_event: KickEvent) -> bool:
        """
        Stores any event to the channel's chat table.

        Args:
            channel_name (str): The name of the channel
            kick_event (KickEvent): The KickEvent dataclass instance containing event information

        Returns:
            bool: True if stored successfully, False otherwise
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False

        table_name = self._get_channel_table_name(normalized_name)
        created_at_db = datetime.now(UTC).isoformat() + "Z"

        # Prepare all event data for insertion
        prepared_data = self._prepare_event_data(kick_event)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                insert_sql = f"""
                INSERT INTO {table_name} (
                    event_type, event_id, chatroom_id, timestamp, user_id, username,
                    content, sender_data, metadata, raw_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                await db.execute(insert_sql, prepared_data + (created_at_db,))
                await db.commit()

            logger.debug(
                "Stored event for channel %s: %s",
                normalized_name,
                prepared_data[0],
            )
            return True

        except sqlite3.Error as e:
            logger.error("Failed to store event for channel %s: %s", normalized_name, e)
            return False

    async def pause_channel(self, channel_name: str) -> bool:
        """
        Pauses a channel (sets paused = True and updates paused_at).

        Args:
            channel_name (str): The name of the channel to pause

        Returns:
            bool: True if paused successfully, False otherwise
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False

        try:
            async with aiosqlite.connect(self.db_path) as db:
                timestamp = datetime.now(UTC).isoformat() + "Z"
                await db.execute(
                    "UPDATE channels SET paused = ?, paused_at = ? WHERE name = ?",
                    (True, timestamp, normalized_name),
                )
                await db.commit()

            logger.info("Channel %s paused successfully", normalized_name)
            return True

        except sqlite3.Error as e:
            logger.error("Failed to pause channel %s: %s", normalized_name, e)
            return False

    async def resume_channel(self, channel_name: str) -> bool:
        """
        Resumes a channel (sets paused = False, keeps paused_at for history).

        Args:
            channel_name (str): The name of the channel to resume

        Returns:
            bool: True if resumed successfully, False otherwise
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False

        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE channels SET paused = ? WHERE name = ?",
                    (False, normalized_name),
                )
                await db.commit()

            logger.info("Channel %s unpaused successfully", normalized_name)
            return True

        except sqlite3.Error as e:
            logger.error("Failed to unpause channel %s: %s", normalized_name, e)
            return False

    async def get_active_channels(self) -> List[str]:
        """
        Returns a list of all active (unpaused) channels.

        Returns:
            List[str]: List of active channel names
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT name FROM channels WHERE paused = 0"
                ) as cursor:
                    result = await cursor.fetchall()
                    return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get active channels: %s", e)
            return []

    async def get_paused_channels(self) -> List[str]:
        """
        Returns a list of all paused channels.

        Returns:
            List[str]: List of paused channel names
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT name FROM channels WHERE paused = 1"
                ) as cursor:
                    result = await cursor.fetchall()
                    return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get paused channels: %s", e)
            return []

    async def get_all_channels(self) -> List[str]:
        """
        Returns a list of all channels.
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("SELECT name FROM channels") as cursor:
                    result = await cursor.fetchall()
                    return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get all channels: %s", e)
            return []

    async def get_channel_stats(self, channel_name: str) -> Dict[str, Any]:
        """
        Gets the stats for a channel.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Dict[str, Any]: Dictionary containing channel statistics:
                - total_messages: Total number of events/messages
                - message_counts: Event type counts
                - date_range: Tuple of (min_timestamp, max_timestamp)
                - unique_users: Number of unique users
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return {}

        table_name = self._get_channel_table_name(normalized_name)

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Message counts by event type
                async with db.execute(
                    f"""
                   SELECT event_type, COUNT(*) as count
                   FROM {table_name}
                   GROUP BY event_type                                    
                """
                ) as cursor:
                    event_counts = dict(await cursor.fetchall())

                # Total number of messages
                async with db.execute(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                    total_messages = (await cursor.fetchone())[0]

                # Date range
                async with db.execute(
                    f"SELECT MIN(created_at), MAX(created_at) FROM {table_name}"
                ) as cursor:
                    date_range = await cursor.fetchone()

                # Number of unique users
                async with db.execute(
                    f"SELECT COUNT(DISTINCT user_id) FROM {table_name}"
                ) as cursor:
                    unique_users = (await cursor.fetchone())[0]

                return {
                    "total_messages": total_messages,
                    "message_counts": event_counts,
                    "date_range": date_range,
                    "unique_users": unique_users,
                }

        except sqlite3.Error as e:
            logger.error("Failed to get channel stats for %s: %s", channel_name, e)
            return {}
