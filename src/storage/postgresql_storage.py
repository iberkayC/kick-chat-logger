"""
This module handles database storage for Kick chat events using PostgreSQL.
It manages PostgreSQL database operations for storing chat messages, subscriptions,
bans, and other events from Kick chat streams.
"""
import logging
from datetime import datetime, UTC
import asyncpg
from typing import Dict, List, Any, Optional

from storage.storage_interface import StorageInterface
from utils.sanitize_validate import (
    sanitize_channel_name,
    get_channel_table_name,
)
from kick_event import KickEvent
from utils.data_preparation import prepare_event_data
from config import (
    DEFAULT_PG_HOST,
    DEFAULT_PG_PORT,
    DEFAULT_PG_DB,
    DEFAULT_PG_USER,
    DEFAULT_PG_PASSWORD,
)

logger = logging.getLogger(__name__)

class PostgreSQLStorage(StorageInterface):
    """
    Handles database storage for Kick chat events using PostgreSQL.
    """
    
    def __init__(
        self,
        host: str = DEFAULT_PG_HOST,
        port: int = DEFAULT_PG_PORT,
        database: str = DEFAULT_PG_DB,
        user: str = DEFAULT_PG_USER,
        password: str = DEFAULT_PG_PASSWORD,
        min_connections: int = 10,
        max_connections: int = 50,
        command_timeout: int = 30,
    ) -> None:
        """
        Initializes the PostgreSQL storage with connection parameters.

        Args:
            host (str): PostgreSQL host address
            port (int): PostgreSQL port number
            database (str): Database name
            user (str): Database username
            password (str): Database password
            min_connections (int): Minimum connections in the pool
            max_connections (int): Maximum connections in the pool
            command_timeout (int): Command timeout in seconds
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.command_timeout = command_timeout
        self.pool: Optional[asyncpg.Pool] = None
        
    async def initialize(self) -> bool:
        """
        Initializes the database connection pool and creates the channels table if it doesn't exist.

        Returns:
            bool: True if initialized successfully, False otherwise
        """
        
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=self.command_timeout,
            )
            
            async with self.pool.acquire() as connection:
                await self._create_channels_table(connection)
                
            logger.info("PostgreSQL database initialized successfully")
            return True
            
        except (asyncpg.PostgresError, OSError) as e:
            logger.error(f"Failed to initialize PostgreSQL database: {e}")
            return False
        
    async def _create_channels_table(self, connection: asyncpg.Connection) -> bool:
        """
        Creates the channels table if it doesn't exist.

        Args:
            connection (asyncpg.Connection): The database connection

        Returns:
            bool: True if created successfully, False otherwise
        """
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            added_at TIMESTAMPTZ NOT NULL,
            paused BOOLEAN DEFAULT FALSE,
            paused_at TIMESTAMPTZ
        );
        """
        
        try:
            await connection.execute(create_table_sql)
            logger.info("Channels table created successfully")
            return True
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to create channels table: {e}")
            return False
            
    async def _create_channel_chat_table(
        self, connection: asyncpg.Connection, channel_name: str
    ) -> bool:
        """
        Creates the channel chat table if it doesn't exist.

        Args:
            connection (asyncpg.Connection): The database connection
            channel_name (str): The name of the channel

        Returns:
            bool: True if created successfully, False otherwise
        """
        table_name = get_channel_table_name(channel_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            event_id TEXT,
            chatroom_id TEXT,
            timestamp TIMESTAMPTZ,
            user_id TEXT,
            username TEXT,
            content TEXT,
            sender_data JSONB,
            metadata JSONB,
            raw_payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        
        try:
            await connection.execute(create_table_sql)
            logger.info(f"Channel chat table for {channel_name} created successfully")
            return True
        
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to create channel chat table for {channel_name}: {e}")
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
        timestamp = datetime.now(UTC)
        
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    "INSERT INTO channels (name, added_at) VALUES ($1, $2)",
                    normalized_name,
                    timestamp
                )
                
                table_created = await self._create_channel_chat_table(connection, normalized_name)
                
                if table_created:
                    logger.info("Channel %s (normalized: %s) added successfully", channel_name, normalized_name)
                    return True
                return False
        
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to add channel {channel_name}: {e}")
            return False
        
    async def list_all_channels(self) -> List[Dict[str, Any]]:
        """
        Lists all channels in the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing channel info
            (name, added_at, paused, paused_at) or an empty list if the database is not initialized
        """
        
        try:
            async with self.pool.acquire() as connection:
                rows = await connection.fetch("SELECT name, added_at, paused, paused_at FROM channels ORDER BY added_at DESC")
                channels = []
                for row in rows:
                    channels.append({
                        "name": row["name"],
                        "added_at": row["added_at"],
                        "paused": row["paused"],
                        "paused_at": row["paused_at"]
                    })
                return channels
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to list channels: {e}")
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
            async with self.pool.acquire() as connection:
                result = await connection.fetchval(
                    "SELECT 1 FROM channels WHERE name = $1", normalized_name
                )
                return result is not None
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to check if channel exists: {e}")
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
    
    async def store_event(self, channel_name: str, event: KickEvent) -> bool:
        """
        Stores any event to the channel's chat table.

        Args:
            channel_name (str): The name of the channel
            event (KickEvent): The KickEvent dataclass instance containing event information

        Returns:
            bool: True if stored successfully, False otherwise
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return False
        
        table_name = get_channel_table_name(normalized_name)
        created_at_db = datetime.now(UTC)
        
        prepared_data = prepare_event_data(event)
        
        try:
            async with self.pool.acquire() as connection:
                insert_sql = f"""
                INSERT INTO {table_name} (
                    event_type, event_id, chatroom_id, timestamp, user_id, username,
                    content, sender_data, metadata, raw_payload, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """
                
                sender_data = prepared_data[7]
                metadata = prepared_data[8]
                raw_payload = prepared_data[9]
                
                timestamp = None
                if prepared_data[3]:
                    try:
                        # Have to replace Z with +00:00, temporary solution for now
                        timestamp = datetime.fromisoformat(prepared_data[3].replace("Z", "+00:00"))
                    except ValueError:
                        logger.warning("Invalid timestamp format: %s", prepared_data[3])
                
                if not timestamp:
                    timestamp = created_at_db
                
                # keeping these as strings for now
                chatroom_id = str(prepared_data[2]) if prepared_data[2] is not None else None
                user_id = str(prepared_data[4]) if prepared_data[4] is not None else None
                event_id = str(prepared_data[1]) if prepared_data[1] is not None else None
                
                await connection.execute(
                    insert_sql,
                    prepared_data[0],
                    event_id,
                    chatroom_id,
                    timestamp,
                    user_id,
                    prepared_data[5],
                    prepared_data[6],
                    sender_data,
                    metadata,
                    raw_payload,
                    created_at_db,
                )
            
            logger.debug("Stored event for channel %s: %s", normalized_name, prepared_data[0])
            return True
    
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to store event for channel {normalized_name}: {e}")
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
            async with self.pool.acquire() as connection:
                timestamp = datetime.now(UTC)
                await connection.execute(
                    "UPDATE channels SET paused = $1, paused_at = $2 WHERE name = $3",
                    True,
                    timestamp,
                    normalized_name
                )
                logger.info("Channel %s paused at %s", normalized_name, timestamp)
                return True
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to pause channel {normalized_name}: {e}")
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
            async with self.pool.acquire() as connection:
                timestamp = datetime.now(UTC)
                await connection.execute(
                    "UPDATE channels SET paused = $1, paused_at = $2 WHERE name = $3",
                    False,
                    timestamp,
                    normalized_name
                )
                logger.info("Channel %s resumed at %s", normalized_name, timestamp)
                return True
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to resume channel {normalized_name}: {e}")
            return False
            
    async def get_active_channels(self) -> List[str]:
        """
        Returns a list of all active (unpaused) channels.

        Returns:
            List[str]: List of active channel names
        """
        try:
            async with self.pool.acquire() as connection:
                rows = await connection.fetch("SELECT name FROM channels WHERE paused = FALSE ORDER BY added_at DESC")
                return [row["name"] for row in rows]
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to get active channels: {e}")
            return []
        
    async def get_paused_channels(self) -> List[str]:
        """
        Returns a list of all paused channels.

        Returns:
            List[str]: List of paused channel names
        """
        try:
            async with self.pool.acquire() as connection:
                rows = await connection.fetch("SELECT name FROM channels WHERE paused = TRUE ORDER BY paused_at DESC")
                return [row["name"] for row in rows]
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to get paused channels: {e}")
            return []
        
    async def get_all_channels(self) -> List[str]:
        """
        Returns a list of all channels.

        Returns:
            List[str]: List of all channel names
        """
        try:
            async with self.pool.acquire() as connection:
                rows = await connection.fetch("SELECT name FROM channels ORDER BY added_at DESC")
                return [row["name"] for row in rows]
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to get all channels: {e}")
            return []
        
    async def get_channel_stats(self, channel_name: str) -> Dict[str, Any]:
        """
        Gets the stats for a channel.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Dict[str, Any]: Dictionary containing channel statistics:
                - event_counts: Event type counts
                - total_messages: Total number of events/messages
                - date_range: Tuple of (min_timestamp, max_timestamp)
                - unique_users: Number of unique users
        """
        normalized_name = await self._validate_and_normalize_channel(channel_name)
        if not normalized_name:
            return {}
        
        table_name = get_channel_table_name(normalized_name)
        
        try:
            async with self.pool.acquire() as connection:
                event_counts_rows = await connection.fetch(
                    f"""
                    SELECT event_type, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY event_type
                    ORDER BY count DESC
                    """
                )
                
                event_counts = {row["event_type"]: row["count"] for row in event_counts_rows}
                
                total_messages = await connection.fetchval(
                    f"SELECT COUNT(*) FROM {table_name}"
                )
                
                date_range = await connection.fetch(
                    f"SELECT MIN(created_at) as min_date, MAX(created_at) as max_date FROM {table_name}"
                )
                
                unique_users = await connection.fetchval(
                    f"SELECT COUNT(DISTINCT user_id) FROM {table_name} WHERE user_id IS NOT NULL"
                )
                
                return {
                    "event_counts": event_counts,
                    "total_messages": total_messages,
                    "date_range": (
                        date_range["min_date"].isoformat() if date_range["min_date"] else None,
                        date_range["max_date"].isoformat() if date_range["max_date"] else None
                    ) if date_range else (None, None),
                    "unique_users": unique_users
                }
            
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to get channel stats for {normalized_name}: {e}")
            return {}

    async def close(self) -> None:
        """
        Closes the database connection pool.
        """
        await self.pool.close()
        logger.info("PostgreSQL connection pool closed")
