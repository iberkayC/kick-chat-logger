from abc import ABC, abstractmethod
from typing import List, Dict, Any
from kick_event import KickEvent


class StorageInterface(ABC):

    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initializes the database and creates the necessary tables if they don't exist.

        Returns:
            bool: True if initialized successfully, False otherwise
        """

    @abstractmethod
    async def add_channel(self, channel_name: str) -> bool:
        """
        Adds a channel to the database and creates its chat table.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if added successfully, False otherwise
        """

    @abstractmethod
    async def list_all_channels(self) -> List[Dict[str, Any]]:
        """
        Lists all channels in the database.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing channel info
            (name, added_at, paused, paused_at) or an empty list if the database is not initialized
        """

    @abstractmethod
    async def channel_exists(self, channel_name: str) -> bool:
        """
        Checks if a channel exists in the database.

        Args:
            channel_name (str): The name of the channel

        Returns:
            bool: True if the channel exists, False otherwise
        """

    @abstractmethod
    async def store_event(self, channel_name: str, kick_event: KickEvent) -> bool:
        """
        Stores any event to the channel's chat table.

        Args:
            channel_name (str): The name of the channel
            kick_event (KickEvent): The KickEvent dataclass instance containing event information

        Returns:
            bool: True if stored successfully, False otherwise
        """

    @abstractmethod
    async def pause_channel(self, channel_name: str) -> bool:
        """
        Pauses a channel (sets paused = True, and updates paused_at).

        Args:
            channel_name (str): The name of the channel to pause

        Returns:
            bool: True if paused successfully, False otherwise
        """

    @abstractmethod
    async def resume_channel(self, channel_name: str) -> bool:
        """
        Resumes a channel (sets paused = False, keeps paused_at for history).

        Args:
            channel_name (str): The name of the channel to resume

        Returns:
            bool: True if resumed successfully, False otherwise
        """

    @abstractmethod
    async def get_active_channels(self) -> List[str]:
        """
        Returns a list of all active (unpaused) channels.

        Returns:
            List[str]: List of active channel names
        """

    @abstractmethod
    async def get_paused_channels(self) -> List[str]:
        """
        Returns a list of all paused channels.

        Returns:
            List[str]: List of paused channel names
        """

    @abstractmethod
    async def get_all_channels(self) -> List[str]:
        """
        Returns a list of all channels.

        Returns:
            List[str]: List of all channel names
        """

    @abstractmethod
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
