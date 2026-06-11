"""
This module handles the websocket connection to the Kick chat.
All channels share a single Pusher connection owned by
ChatConnectionManager, with a queue per channel so one slow
channel can't stall the others.
It also handles the messages from the websocket.
"""

import json
import logging
import random
import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import websockets
import aiofiles


from kick_api import get_channel_info
from storage.storage_interface import StorageInterface
from kick_event import KickEvent
from config import (
    WEBSOCKET_URL,
    HANDLED_EVENTS,
    IGNORED_EVENTS,
    UNHANDLED_MESSAGES_FILE,
    CHANNEL_LOOKUP_CONCURRENCY,
    PING_INTERVAL_SECONDS,
    PING_TIMEOUT_SECONDS,
    PUSHER_INTERNAL_SUBSCRIPTION_SUCCEEDED_EVENT,
    PUSHER_PING_EVENT,
    PUSHER_PONG_EVENT,
)


logger = logging.getLogger(__name__)

# subscription states for a channel on the shared connection
PENDING = "pending"  # registered, no ack from pusher yet
SUBSCRIBED = "subscribed"  # pusher acked the subscription
ERRORED = "errored"  # gave up on this channel, resume retries it

SUBSCRIBE_ACK_TIMEOUT = 5
SUBSCRIBE_ACK_RETRIES = 2
QUEUE_MAXSIZE = 1000
DRAIN_TIMEOUT = 5

# reconnect backoff, the runner retries forever
CONNECTION_BASE_DELAY = 1  # pusher's reconnect-immediately class (4200-4299, 1011)
BASE_DELAY = 2  # everything else
MAX_BACKOFF_DELAY = 30


class ChannelNotFoundError(Exception):
    """
    Raised when a channel does not exist on Kick (404). Not retryable.
    """


class ChatroomIdError(Exception):
    """
    Raised when the chatroom ID for a channel could not be fetched. Retryable.
    """


@dataclass
class ChannelSub:
    """
    Bookkeeping for one channel subscription on the shared connection.
    """

    channel_name: str
    chatroom_id: int
    pusher_channel: str
    queue: asyncio.Queue
    state: str = PENDING
    ack: asyncio.Event = field(default_factory=asyncio.Event)
    consumer_task: Optional[asyncio.Task] = None
    ack_task: Optional[asyncio.Task] = None


class ChatConnectionManager:
    """
    Owns the single shared Pusher websocket and all channel subscriptions.

    Each subscribed channel gets a pusher:subscribe frame on the shared
    socket, a queue for its incoming messages, and a consumer task that
    stores them. One runner task holds the connection and reconnects with
    backoff for everyone at once.
    """

    def __init__(self, storage: StorageInterface):
        self.storage = storage
        self._channels: Dict[str, ChannelSub] = {}  # kick name -> sub
        self._routes: Dict[str, str] = {}  # pusher channel -> kick name
        self._ws = None
        self._connected = False
        self._closing = False
        self._runner: Optional[asyncio.Task] = None
        # every chatroom-id lookup goes through this cap, otherwise a mass
        # ack-timeout after a reconnect could burst thousands of concurrent
        # requests at the kick api
        self._lookup_sem = asyncio.Semaphore(CHANNEL_LOOKUP_CONCURRENCY)

    async def subscribe(self, channel_name: str, refresh: bool = False) -> bool:
        """
        Subscribe a channel on the shared connection and start logging it.

        Args:
            channel_name (str): The name of the channel to subscribe
            refresh (bool): Skip the stored chatroom ID and look it up fresh

        Returns:
            bool: True if the channel is subscribed (or already was)

        Raises:
            ChannelNotFoundError: If the channel does not exist on Kick (404).
            ChatroomIdError: If the chatroom ID could not be fetched (retryable).
        """
        sub = self._channels.get(channel_name)
        if sub is not None:
            if sub.state == ERRORED:
                # tear the failed sub down and redo it from scratch,
                # distrusting the stored chatroom id
                await self.unsubscribe(channel_name)
                refresh = True
            else:
                logger.warning("Channel %s is already subscribed", channel_name)
                return True

        chatroom_id = None
        if not refresh:
            chatroom_id = await self.storage.get_chatroom_id(channel_name)
        if chatroom_id is None:
            async with self._lookup_sem:
                chatroom_id = await get_chatroom_id(channel_name)
            await self.storage.set_chatroom_id(channel_name, chatroom_id)
        logger.info("Chatroom ID for %s: %s", channel_name, chatroom_id)

        sub = ChannelSub(
            channel_name=channel_name,
            chatroom_id=chatroom_id,
            pusher_channel=f"chatrooms.{chatroom_id}.v2",
            queue=asyncio.Queue(maxsize=QUEUE_MAXSIZE),
        )
        self._channels[channel_name] = sub
        self._routes[sub.pusher_channel] = channel_name
        sub.consumer_task = asyncio.create_task(self._consume(sub))

        self._ensure_runner()
        if self._connected:
            sub.ack_task = asyncio.create_task(self._subscribe_with_ack(sub))
        # not connected: the (re)connect path picks the sub up in
        # _resubscribe_all, it stays pending until then

        return True

    async def unsubscribe(self, channel_name: str) -> bool:
        """
        Unsubscribe a channel, draining its queue before stopping the consumer.

        Args:
            channel_name (str): The name of the channel to unsubscribe

        Returns:
            bool: True if the channel was subscribed, False otherwise
        """
        sub = self._channels.pop(channel_name, None)
        if sub is None:
            logger.warning("Channel %s is not subscribed", channel_name)
            return False

        # drop the route first so the reader stops enqueueing for this channel
        self._routes.pop(sub.pusher_channel, None)

        if sub.ack_task is not None:
            sub.ack_task.cancel()

        # best effort, the socket may be down which is fine too
        ws = self._ws
        if ws is not None:
            frame = {
                "event": "pusher:unsubscribe",
                "data": {"channel": sub.pusher_channel},
            }
            with contextlib.suppress(websockets.exceptions.ConnectionClosed):
                await ws.send(json.dumps(frame))

        await self._stop_consumer(sub)
        logger.info("Unsubscribed from channel %s", channel_name)
        return True

    def channel_state(self, channel_name: str) -> Optional[str]:
        """
        Get the subscription state of a channel.

        Args:
            channel_name (str): The name of the channel

        Returns:
            Optional[str]: PENDING, SUBSCRIBED or ERRORED, None if not subscribed
        """
        sub = self._channels.get(channel_name)
        return sub.state if sub is not None else None

    def active_count(self) -> int:
        """
        Returns:
            int: Number of channels registered on the connection
        """
        return len(self._channels)

    async def close(self) -> None:
        """
        Shut down the connection and all consumers, draining queues first.

        Returns:
            None
        """
        self._closing = True
        if self._runner is not None:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None
        for sub in list(self._channels.values()):
            if sub.ack_task is not None:
                sub.ack_task.cancel()
            await self._stop_consumer(sub)
        self._channels.clear()
        self._routes.clear()

    def _ensure_runner(self) -> None:
        # start the connection loop if it isn't running yet
        if self._runner is None or self._runner.done():
            self._closing = False
            self._runner = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """
        Connection loop: connect, resubscribe everything, read frames until
        the connection drops, back off, repeat. Never gives up, only close()
        stops it.

        Returns:
            None
        """
        connection_retries = 0
        other_retries = 0

        while not self._closing:
            error: Optional[BaseException] = None
            try:
                async with websockets.connect(
                    WEBSOCKET_URL,
                    ping_interval=PING_INTERVAL_SECONDS,
                    ping_timeout=PING_TIMEOUT_SECONDS,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    # backoff starts from scratch after a good connection
                    connection_retries = 0
                    other_retries = 0
                    logger.info(
                        "Connected to Kick WebSocket (%d channels)",
                        len(self._channels),
                    )
                    self._resubscribe_all()
                    async for raw in ws:
                        await self._handle_frame(raw)
                # a clean server close ends the iterator without raising,
                # reconnect in that case too
            except websockets.exceptions.ConnectionClosed as e:
                error = e
            except Exception as e:
                error = e
            finally:
                self._ws = None
                self._connected = False

            if self._closing:
                break

            error_code = getattr(error, "code", None)
            if error_code == 1011 or (
                error_code is not None and 4200 <= error_code <= 4299
            ):
                # 4200-4299 is pusher's "reconnect immediately" class
                connection_retries += 1
                delay = min(
                    CONNECTION_BASE_DELAY * (1.5 ** min(connection_retries, 8)),
                    MAX_BACKOFF_DELAY,
                )
                attempt = connection_retries
            else:
                other_retries += 1
                delay = min(
                    BASE_DELAY * (2 ** min(other_retries - 1, 8)), MAX_BACKOFF_DELAY
                )
                attempt = other_retries
            # jitter so retries don't land in lockstep
            delay += random.uniform(0, delay / 4)

            logger.warning(
                "WebSocket connection lost (%s, attempt %d). Reconnecting in %.1f seconds...",
                error if error is not None else "closed cleanly",
                attempt,
                delay,
            )
            await asyncio.sleep(delay)

    def _resubscribe_all(self) -> None:
        # fire-and-forget on purpose: acks arrive on this same socket, so the
        # runner has to get back to reading frames before any ack can land,
        # awaiting them here would deadlock. uses the cached chatroom ids, so
        # a reconnect costs zero http requests
        for sub in self._channels.values():
            sub.state = PENDING
            sub.ack.clear()
            if sub.ack_task is not None:
                sub.ack_task.cancel()
            sub.ack_task = asyncio.create_task(self._subscribe_with_ack(sub))

    async def _subscribe_with_ack(self, sub: ChannelSub) -> None:
        """
        Send the subscribe frame and wait for pusher's ack, re-fetching the
        chatroom ID on timeout in case it changed. Only sends frames, never
        touches the connection itself: reconnects are the runner's job.

        Args:
            sub (ChannelSub): The channel subscription to subscribe

        Returns:
            None
        """
        for _ in range(1 + SUBSCRIBE_ACK_RETRIES):
            ws = self._ws
            if ws is None:
                # connection dropped, the reconnect path respawns this task
                return
            sub.ack.clear()
            frame = {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": sub.pusher_channel},
            }
            try:
                await ws.send(json.dumps(frame))
            except websockets.exceptions.ConnectionClosed:
                return

            try:
                await asyncio.wait_for(sub.ack.wait(), SUBSCRIBE_ACK_TIMEOUT)
                return  # _handle_frame marked it subscribed
            except asyncio.TimeoutError:
                pass

            # no ack, the chatroom id may be stale (channel recreated),
            # look it up again before the next attempt
            try:
                async with self._lookup_sem:
                    chatroom_id = await get_chatroom_id(sub.channel_name)
            except ChannelNotFoundError as e:
                logger.error(
                    "Channel %s no longer exists on Kick, stopping: %s",
                    sub.channel_name,
                    e,
                )
                sub.state = ERRORED
                return
            except ChatroomIdError as e:
                logger.warning(
                    "Could not refresh chatroom ID for %s: %s", sub.channel_name, e
                )
                continue

            if chatroom_id != sub.chatroom_id:
                logger.info(
                    "Chatroom ID for %s changed %s -> %s",
                    sub.channel_name,
                    sub.chatroom_id,
                    chatroom_id,
                )
                self._routes.pop(sub.pusher_channel, None)
                sub.chatroom_id = chatroom_id
                sub.pusher_channel = f"chatrooms.{chatroom_id}.v2"
                self._routes[sub.pusher_channel] = sub.channel_name
                await self.storage.set_chatroom_id(sub.channel_name, chatroom_id)

        logger.error(
            "No subscription ack for %s after %d attempts, marking errored",
            sub.channel_name,
            1 + SUBSCRIBE_ACK_RETRIES,
        )
        sub.state = ERRORED

    async def _handle_frame(self, raw: str) -> None:
        """
        Parse one frame from the shared socket and route it to the right
        channel's queue. Connection-level frames and post-unsubscribe
        stragglers keep the old unhandled-message logging behavior.

        Args:
            raw (str): The raw frame from the websocket

        Returns:
            None
        """
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Dropping non-JSON frame: %.200s", raw)
            return
        if not isinstance(message, dict):
            logger.warning("Dropping non-object frame: %.200s", raw)
            return

        # pusher pings quiet connections at the application level and
        # closes with 4201 unless it gets a pong back
        if message.get("event") == PUSHER_PING_EVENT:
            ws = self._ws
            if ws is not None:
                pong = {"event": PUSHER_PONG_EVENT, "data": {}}
                with contextlib.suppress(websockets.exceptions.ConnectionClosed):
                    await ws.send(json.dumps(pong))
            return

        channel_name = self._routes.get(message.get("channel"))

        # acks are connection bookkeeping, not chat. intercept them here,
        # handle_websocket_message would just ignore them (IGNORED_EVENTS)
        if message.get("event") == PUSHER_INTERNAL_SUBSCRIPTION_SUCCEEDED_EVENT:
            sub = self._channels.get(channel_name) if channel_name else None
            if sub is not None:
                sub.state = SUBSCRIBED
                sub.ack.set()
                logger.info(
                    "Subscribed to chatroom %s (%s)", sub.chatroom_id, channel_name
                )
            return

        if channel_name is None:
            await handle_websocket_message(message)
            return

        sub = self._channels.get(channel_name)
        if sub is None:
            return
        try:
            sub.queue.put_nowait(message)
        except asyncio.QueueFull:
            # an awaited put would let one slow channel stall every other
            # channel on the shared socket, so drop instead
            logger.warning("Queue full for %s, dropping message", channel_name)

    async def _consume(self, sub: ChannelSub) -> None:
        """
        Drain one channel's queue: parse each message and store it.

        Args:
            sub (ChannelSub): The channel subscription to consume

        Returns:
            None
        """
        while True:
            message = await sub.queue.get()
            try:
                kick_event = await handle_websocket_message(message)
                if kick_event:
                    success = await self.storage.store_event(
                        sub.channel_name, kick_event
                    )
                    if not success:
                        logger.error("Failed to store event for %s", sub.channel_name)
            except Exception as e:
                # one bad message must not kill the consumer or wedge drains
                logger.error("Error handling message for %s: %s", sub.channel_name, e)
            finally:
                sub.queue.task_done()

    async def _stop_consumer(self, sub: ChannelSub) -> None:
        # let the consumer work through whatever is queued, then cancel it
        try:
            await asyncio.wait_for(sub.queue.join(), DRAIN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout draining queue for %s, dropping %d messages",
                sub.channel_name,
                sub.queue.qsize(),
            )
        if sub.consumer_task is not None:
            sub.consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sub.consumer_task


async def get_chatroom_id(channel_name: str) -> int:
    """
    Fetches and returns the chatroom ID for a given channel name.

    Args:
        channel_name (str): The name of the channel to get the chatroom ID for

    Returns:
        int: The chatroom ID for the given channel name

    Raises:
        ChannelNotFoundError: If the channel does not exist on Kick (404).
        ChatroomIdError: If the lookup failed for any other reason (retryable).
    """
    channel_info_result = await get_channel_info(channel_name)
    if not channel_info_result.success or not channel_info_result.data:
        logger.error(
            "Failed to get channel info for %s: %s",
            channel_name,
            channel_info_result.error,
        )
        if channel_info_result.status_code == 404:
            raise ChannelNotFoundError(f"Channel '{channel_name}' not found on Kick")
        raise ChatroomIdError(
            f"Failed to get channel info for '{channel_name}': {channel_info_result.error}"
        )

    chatroom_id = None
    if isinstance(channel_info_result.data, dict):
        chatroom_id = channel_info_result.data.get("chatroom", {}).get("id")

    if not chatroom_id:
        raise ChatroomIdError(f"No chatroom ID in channel info for '{channel_name}'")
    return chatroom_id


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
