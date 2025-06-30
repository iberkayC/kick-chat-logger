"""
This module handles database storage for Kick chat events.
It manages SQLite database operations for storing chat messages, subscriptions,
bans, and other events from Kick chat streams.
"""
import logging
import os
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional
import sqlite3

import aiosqlite

from storage.storage_interface import StorageInterface
from utils.sanitize_validate import (
    sanitize_channel_name,
    get_channel_table_name,
    normalize_timestamp,
)
from kick_event import KickEvent
from config import DEFAULT_DB_PATH
from utils.data_preparation import prepare_event_data


logger = logging.getLogger(__name__)


class KickChatStorage(StorageInterface):
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
        Enables WAL mode for better concurrent access.
        
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
        table_name = get_channel_table_name(channel_name)

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
        normalized_name = sanitize_channel_name(channel_name)
        timestamp = normalize_timestamp(datetime.now(UTC))

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
        normalized_name = sanitize_channel_name(channel_name)

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
        normalized_name = sanitize_channel_name(channel_name)

        if not await self.channel_exists(normalized_name):
            logger.error("Channel does not exist: %s", normalized_name)
            return None

        return normalized_name

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

        table_name = get_channel_table_name(normalized_name)
        created_at_db = normalize_timestamp(datetime.now(UTC))

        # Prepare all event data for insertion
        prepared_data = prepare_event_data(kick_event)

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
                timestamp = normalize_timestamp(datetime.now(UTC))
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

        table_name = get_channel_table_name(normalized_name)

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
                    rows = await cursor.fetchall()
                    event_counts = dict((row[0], row[1]) for row in rows)

                # Total number of messages
                async with db.execute(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                    result = await cursor.fetchone()
                    total_messages = result[0] if result else 0

                # Date range
                async with db.execute(
                    f"SELECT MIN(created_at), MAX(created_at) FROM {table_name}"
                ) as cursor:
                    date_range = await cursor.fetchone()

                # Number of unique users
                async with db.execute(
                    f"SELECT COUNT(DISTINCT user_id) FROM {table_name}"
                ) as cursor:
                    result = await cursor.fetchone()
                    unique_users = result[0] if result else 0

                return {
                    "total_messages": total_messages,
                    "message_counts": event_counts,
                    "date_range": date_range,
                    "unique_users": unique_users,
                }

        except sqlite3.Error as e:
            logger.error("Failed to get channel stats for %s: %s", channel_name, e)
            return {}
