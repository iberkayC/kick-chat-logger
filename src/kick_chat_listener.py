"""
This module handles the websocket connection to the Kick chat.
It also handles the messages from the websocket.
"""

import json
import logging
import time
import asyncio
from typing import Dict, Any, Optional
import websockets
from websockets import Close
import aiofiles


from kick_api import get_channel_info
from storage.sqlite_storage import SQLiteStorage
from kick_event import KickEvent
from config import (
    WEBSOCKET_URL,
    HANDLED_EVENTS,
    IGNORED_EVENTS,
    UNHANDLED_MESSAGES_FILE,
    PING_INTERVAL_MINUTES,
)


logger = logging.getLogger(__name__)


async def listen_to_chat(
    channel_name: str,
    storage: SQLiteStorage,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """
    Connects to Kick's WebSocket and listens for chat messages.

    Args:
        channel_name (str): The name of the channel to listen to.
        storage (KickChatStorage): Storage instance for saving events.
        stop_event (Optional[asyncio.Event]): Event to signal when to stop listening.
    """

    chatroom_id = await get_chatroom_id(channel_name)

    last_ping_time = time.time()
    ping_interval = PING_INTERVAL_MINUTES * 20
    ping_timeout = 30  # seconds to wait for pong response
    consecutive_ping_failures = 0
    max_ping_failures = 3

    if not chatroom_id:
        logger.error("Failed to get chatroom ID for %s", channel_name)
        return
    logger.info("Chatroom ID for %s: %s", channel_name, chatroom_id)

    async with websockets.connect(WEBSOCKET_URL) as ws:
        logger.info("Connected to Kick WebSocket for channel %s", channel_name)

        await subscribe_to_chatroom(ws, chatroom_id)

        try:
            while stop_event is None or not stop_event.is_set():
                # Use asyncio.wait_for with timeout to make recv() cancellable
                try:
                    current_time = time.time()
                    if current_time - last_ping_time > ping_interval:
                        try:
                            # Send ping and wait for pong with timeout
                            pong_waiter = await ws.ping()
                            await asyncio.wait_for(pong_waiter, timeout=ping_timeout)
                            last_ping_time = current_time
                            consecutive_ping_failures = 0
                            logger.debug("Ping/pong successful for %s", channel_name)
                        except asyncio.TimeoutError:
                            consecutive_ping_failures += 1
                            logger.warning(
                                "Ping timeout for %s (failure %d/%d)",
                                channel_name,
                                consecutive_ping_failures,
                                max_ping_failures,
                            )
                            if consecutive_ping_failures >= max_ping_failures:
                                logger.error(
                                    "Max ping failures reached for %s, closing connection",
                                    channel_name,
                                )
                                raise websockets.exceptions.ConnectionClosed(
                                    sent=Close(1011, "Ping timeout"), rcvd=None
                                )
                            last_ping_time = (
                                current_time  # Reset to avoid immediate retry
                            )
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(
                                "Connection lost during ping for %s", channel_name
                            )
                            raise

                    message_raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    message = json.loads(message_raw)
                    kick_event = await handle_websocket_message(message)

                    if kick_event:
                        success = await storage.store_event(channel_name, kick_event)
                        if not success:
                            logger.error("Failed to store event for %s", channel_name)

                except asyncio.TimeoutError:
                    # Didn't receive a message in 1 second, so we continue
                    # looking for a message.
                    continue

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning("WebSocket connection closed for %s: %s", channel_name, e)
            raise e
        finally:
            logger.info("Disconnected from Kick WebSocket for %s", channel_name)


async def subscribe_to_chatroom(
    ws,
    chatroom_id: str,
) -> None:
    """
    Sends a subscription message to the Kick chatroom.

    Args:
        ws: The WebSocket connection
        chatroom_id (str): The ID of the chatroom to subscribe to
    """
    subscribe_message = {
        "event": "pusher:subscribe",
        "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
    }
    await ws.send(json.dumps(subscribe_message))
    logger.info("Subscribed to chatroom %s", chatroom_id)


async def get_chatroom_id(channel_name: str) -> Optional[str]:
    """
    Fetches and returns the chatroom ID for a given channel name.

    Args:
        channel_name (str): The name of the channel to get the chatroom ID for

    Returns:
        Optional[str]: The chatroom ID for the given channel name
    """
    channel_info_result = get_channel_info(channel_name)
    if not channel_info_result.success or not channel_info_result.data:
        logger.error(
            "Failed to get channel info for %s: %s",
            channel_name,
            channel_info_result.error,
        )
        return None

    if isinstance(channel_info_result.data, dict):
        return channel_info_result.data.get("chatroom", {}).get("id")
    return None


async def parse_event(message: Dict[str, Any]) -> KickEvent:
    """
    Parses all events from the websocket.
    Supports chat messages, subscriptions, bans, deletions, pins, and sent messages.

    Args:
        message (Dict[str, Any]): The message from the Kick WebSocket

    Returns:
        KickEvent: Parsed event data
    """
    event_type = message.get("event")
    if not isinstance(event_type, str):
        raise ValueError(f"Invalid event type: {event_type}")
    
    data_str = message.get("data", "{}")
    if isinstance(data_str, str):
        parsed_data = json.loads(data_str)
    else:
        parsed_data = data_str
    kick_event = KickEvent(event=event_type, data=parsed_data)
    return kick_event


async def _log_unhandled_message(message: Dict[str, Any]) -> None:
    """
    Logs unhandled messages to a file for analysis.

    Args:
        message (Dict[str, Any]): The unhandled message from the WebSocket
    """
    async with aiofiles.open(UNHANDLED_MESSAGES_FILE, "a", encoding="utf-8") as f:
        await f.write(json.dumps(message) + "\n")


async def handle_websocket_message(message: Dict[str, Any]) -> Optional[KickEvent]:
    """
    Handles messages from the Kick WebSocket, parsing handled events
    and logging unhandled ones.

    Args:
        message (Dict[str, Any]): The message from the Kick WebSocket

    Returns:
        Optional[KickEvent]: The parsed event if it is a handled event type, otherwise None
    """
    event_type = message.get("event")

    # we don't handle websocket events, only messages
    # kick_event can be None. we don't care about websocket events
    # and, there are probably more events that we don't handle
    # that we might want to care about. so, we are saving them
    # in a file.
    if event_type in IGNORED_EVENTS:
        return None

    if event_type in HANDLED_EVENTS:
        return await parse_event(message)

    logger.debug("Received unhandled message: %s", message)
    await _log_unhandled_message(message)
    return None
