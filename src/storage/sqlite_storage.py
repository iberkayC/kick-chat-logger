"""Database storage for Kick chat events using SQLite.

Manages SQLite database operations for storing chat messages,
subscriptions, bans, and other events from Kick chat streams.

All operations share one long-lived connection, serialized by a lock.
"""

import asyncio
import contextlib
import logging
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any

import aiosqlite

from config import DEFAULT_DB_PATH
from kick_event import KickEvent
from storage.storage_interface import StorageInterface
from utils.data_preparation import prepare_event_data
from utils.sanitize_validate import (
    get_channel_table_name,
    normalize_timestamp,
    sanitize_channel_name,
)

logger = logging.getLogger(__name__)

# store_event batches inserts into one commit, so a crash can lose at most
# COMMIT_BATCH_SIZE events or COMMIT_INTERVAL_SECONDS worth of chat
COMMIT_BATCH_SIZE = 100
COMMIT_INTERVAL_SECONDS = 1.0


class SQLiteStorage(StorageInterface):
    """Handles database storage for Kick chat events."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """Initialize the SQLite storage.

        Args:
            db_path (str): Path to the database file

        """
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._pending = 0  # inserts awaiting commit
        self._flush_task: asyncio.Task | None = None
        self._known_channels: set[str] = set()  # names validated against the db

    @property
    def _connection(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Storage not initialized. Call initialize() first.")
        return self._db

    async def initialize(self) -> bool:
        """Open the shared database connection and create the channels table.

        Returns:
            bool: True if initialized successfully, False otherwise

        """
        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                logger.info("Created database directory: %s", db_dir)

            self._db = await aiosqlite.connect(self.db_path)
            await self._enable_wal_mode(self._db)
            await self._create_channels_table(self._db)

            logger.info("Database initialized successfully")
            return True
        except (sqlite3.Error, OSError) as e:
            logger.error("Failed to initialize database: %s", e)
            if self._db is not None:
                with contextlib.suppress(sqlite3.Error):
                    await self._db.close()
                self._db = None
            return False

    async def close(self) -> None:
        """Commit any batched inserts and close the database connection.

        Returns:
            None

        """
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
            self._flush_task = None

        if self._db is None:
            return
        try:
            async with self._lock:
                await self._commit()
        except sqlite3.Error as e:
            logger.error("Failed to commit batched events on close: %s", e)
        with contextlib.suppress(sqlite3.Error):
            await self._db.close()
        self._db = None
        logger.info("Database connection closed")

    async def _commit(self) -> None:
        """Commit the open transaction, including any batched inserts.

        Caller must hold self._lock.

        Returns:
            None

        """
        await self._connection.commit()
        self._pending = 0

    async def _flush_later(self) -> None:
        """Commit batched inserts once COMMIT_INTERVAL_SECONDS have passed.

        Returns:
            None

        """
        await asyncio.sleep(COMMIT_INTERVAL_SECONDS)
        try:
            async with self._lock:
                if self._pending:
                    await self._commit()
        except sqlite3.Error as e:
            logger.error("Failed to commit batched events: %s", e)

    async def _enable_wal_mode(self, db: aiosqlite.Connection) -> bool:
        """Enable WAL mode and performance pragmas on a connection.

        journal_mode persists in the database file, but the other pragmas
        are per-connection and only hold while that connection is open.

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
        """Create the channels table if it doesn't exist.

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
            paused_at TEXT,
            chatroom_id INTEGER
        );
        """
        try:
            await db.execute(create_table_sql)
            # databases from before the chatroom_id column get it added here
            async with db.execute("PRAGMA table_info(channels)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
            if "chatroom_id" not in columns:
                await db.execute("ALTER TABLE channels ADD COLUMN chatroom_id INTEGER")
            await db.commit()
            logger.info("Channels table created successfully")
            return True

        except sqlite3.Error as e:
            logger.error("Failed to create channels table: %s", e)
            return False

    async def _create_channel_chat_table(
        self,
        db: aiosqlite.Connection,
        channel_name: str,
    ) -> bool:
        """Create the channel chat table if it doesn't exist.

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
        """Add a channel to the database and create its chat table.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if added successfully, False otherwise

        """
        normalized_name = sanitize_channel_name(channel_name)
        timestamp = normalize_timestamp(datetime.now(UTC))

        try:
            async with self._lock:
                db = self._connection
                # flush batched inserts first so the rollback below can
                # only ever discard this channel's half-created state
                await self._commit()
                try:
                    await db.execute(
                        "INSERT INTO channels (name, added_at) VALUES (?, ?)",
                        (normalized_name, timestamp),
                    )

                    table_created = await self._create_channel_chat_table(
                        db,
                        normalized_name,
                    )

                    if not table_created:
                        await db.rollback()
                        return False
                    await self._commit()
                except sqlite3.Error:
                    await db.rollback()
                    raise

            self._known_channels.add(normalized_name)
            logger.info(
                "Channel %s (normalized: %s) added successfully",
                channel_name,
                normalized_name,
            )
            return True

        except sqlite3.Error as e:
            logger.error("Failed to add channel %s: %s", channel_name, e)
            return False

    async def list_all_channels(self) -> list[dict[str, Any]]:
        """List all channels in the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing channel info
            (name, added_at, paused, paused_at) or an empty list if the database is not initialized

        """
        try:
            async with (
                self._lock,
                self._connection.execute(
                    "SELECT name, added_at, paused, paused_at FROM channels",
                ) as cursor,
            ):
                rows = await cursor.fetchall()

            return [
                {
                    "name": row[0],
                    "added_at": row[1],
                    "paused": bool(row[2]),
                    "paused_at": row[3],
                }
                for row in rows
            ]

        except sqlite3.Error as e:
            logger.error("Failed to list channels: %s", e)
            return []

    async def channel_exists(self, channel_name: str) -> bool:
        """Check if a channel exists in the database.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if the channel exists, False otherwise

        """
        normalized_name = sanitize_channel_name(channel_name)

        try:
            async with (
                self._lock,
                self._connection.execute(
                    "SELECT 1 FROM channels WHERE name = ?",
                    (normalized_name,),
                ) as cursor,
            ):
                result = await cursor.fetchone()
                return result is not None

        except sqlite3.Error as e:
            logger.error("Failed to check if channel exists: %s", e)
            return False

    async def _validate_and_normalize_channel(self, channel_name: str) -> str | None:
        """Validate and normalize a channel name.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Optional[str]: The normalized channel name, or None if the channel does not exist

        """
        normalized_name = sanitize_channel_name(channel_name)

        # channels are never deleted, so a name that validated once stays
        # valid and the cache only grows
        if normalized_name in self._known_channels:
            return normalized_name

        if not await self.channel_exists(normalized_name):
            logger.error("Channel does not exist: %s", normalized_name)
            return None

        self._known_channels.add(normalized_name)
        return normalized_name

    async def store_event(self, channel_name: str, kick_event: KickEvent) -> bool:
        """Store any event to the channel's chat table.

        The insert lands in the shared transaction and is committed in
        batches, so a stored event may not be durable until the next flush.

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

        insert_sql = f"""
        INSERT INTO {table_name} (
            event_type, event_id, chatroom_id, timestamp, user_id, username,
            content, sender_data, metadata, raw_payload, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            async with self._lock:
                await self._connection.execute(
                    insert_sql,
                    prepared_data + (created_at_db,),
                )
                self._pending += 1
                if self._pending >= COMMIT_BATCH_SIZE:
                    await self._commit()
                elif self._flush_task is None or self._flush_task.done():
                    self._flush_task = asyncio.create_task(self._flush_later())

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
        """Pause a channel (sets paused = True and updates paused_at).

        Args:
            channel_name (str): The name of the channel to pause

        Returns:
            bool: True if paused successfully, False otherwise

        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False

        try:
            async with self._lock:
                timestamp = normalize_timestamp(datetime.now(UTC))
                await self._connection.execute(
                    "UPDATE channels SET paused = ?, paused_at = ? WHERE name = ?",
                    (True, timestamp, normalized_name),
                )
                await self._commit()

            logger.info("Channel %s paused successfully", normalized_name)
            return True

        except sqlite3.Error as e:
            logger.error("Failed to pause channel %s: %s", normalized_name, e)
            return False

    async def resume_channel(self, channel_name: str) -> bool:
        """Resume a channel (sets paused = False, keeps paused_at for history).

        Args:
            channel_name (str): The name of the channel to resume

        Returns:
            bool: True if resumed successfully, False otherwise

        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False

        try:
            async with self._lock:
                await self._connection.execute(
                    "UPDATE channels SET paused = ? WHERE name = ?",
                    (False, normalized_name),
                )
                await self._commit()

            logger.info("Channel %s unpaused successfully", normalized_name)
            return True

        except sqlite3.Error as e:
            logger.error("Failed to unpause channel %s: %s", normalized_name, e)
            return False

    async def get_chatroom_id(self, channel_name: str) -> int | None:
        """Return the stored chatroom ID for a channel, if known.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Optional[int]: The chatroom ID, or None if not stored

        """
        normalized_name = sanitize_channel_name(channel_name)

        try:
            async with (
                self._lock,
                self._connection.execute(
                    "SELECT chatroom_id FROM channels WHERE name = ?",
                    (normalized_name,),
                ) as cursor,
            ):
                row = await cursor.fetchone()
                return row[0] if row else None

        except sqlite3.Error as e:
            logger.error("Failed to get chatroom ID for %s: %s", normalized_name, e)
            return None

    async def set_chatroom_id(self, channel_name: str, chatroom_id: int) -> bool:
        """Store the chatroom ID for a channel.

        Args:
            channel_name (str): The name of the channel
            chatroom_id (int): The chatroom ID to store

        Returns:
            bool: True if stored successfully, False otherwise

        """
        normalized_name = sanitize_channel_name(channel_name)

        try:
            async with self._lock:
                await self._connection.execute(
                    "UPDATE channels SET chatroom_id = ? WHERE name = ?",
                    (chatroom_id, normalized_name),
                )
                await self._commit()

            logger.debug(
                "Stored chatroom ID %s for channel %s",
                chatroom_id,
                normalized_name,
            )
            return True

        except sqlite3.Error as e:
            logger.error("Failed to set chatroom ID for %s: %s", normalized_name, e)
            return False

    async def get_active_channels(self) -> list[str]:
        """Return a list of all active (unpaused) channels.

        Returns:
            List[str]: List of active channel names

        """
        try:
            async with (
                self._lock,
                self._connection.execute(
                    "SELECT name FROM channels WHERE paused = 0",
                ) as cursor,
            ):
                result = await cursor.fetchall()
                return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get active channels: %s", e)
            return []

    async def get_paused_channels(self) -> list[str]:
        """Return a list of all paused channels.

        Returns:
            List[str]: List of paused channel names

        """
        try:
            async with (
                self._lock,
                self._connection.execute(
                    "SELECT name FROM channels WHERE paused = 1",
                ) as cursor,
            ):
                result = await cursor.fetchall()
                return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get paused channels: %s", e)
            return []

    async def get_all_channels(self) -> list[str]:
        """Return a list of all channels."""
        try:
            async with (
                self._lock,
                self._connection.execute("SELECT name FROM channels") as cursor,
            ):
                result = await cursor.fetchall()
                return [row[0] for row in result]
        except sqlite3.Error as e:
            logger.error("Failed to get all channels: %s", e)
            return []

    async def get_channel_stats(self, channel_name: str) -> dict[str, Any]:
        """Get the stats for a channel.

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
            async with self._lock:
                db = self._connection

                # Message counts by event type
                async with db.execute(
                    f"""
                   SELECT event_type, COUNT(*) as count
                   FROM {table_name}
                   GROUP BY event_type
                """,
                ) as cursor:
                    rows = await cursor.fetchall()
                    event_counts = dict((row[0], row[1]) for row in rows)

                # Total number of messages
                async with db.execute(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                    result = await cursor.fetchone()
                    total_messages = result[0] if result else 0

                # Date range
                async with db.execute(
                    f"SELECT MIN(created_at), MAX(created_at) FROM {table_name}",
                ) as cursor:
                    date_range = await cursor.fetchone()

                # Number of unique users
                async with db.execute(
                    f"SELECT COUNT(DISTINCT user_id) FROM {table_name}",
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
