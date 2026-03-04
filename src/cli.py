#!/usr/bin/env python3
"""
Kick Chat Scraper - Long-running server application with interactive CLI
"""
import asyncio
import logging
import sys
from typing import Dict
import websockets

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import NestedCompleter

from kick_api import get_channel_info
from kick_chat_listener import listen_to_chat
from storage.storage_factory import create_storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="kick_scraper.log",
    filemode="a",
)

logger = logging.getLogger(__name__)
console = Console()


class KickChatLogger:
    """
    Main scraper application managing multiple channel tasks.
    This is the entry point for the CLI.
    """

    def __init__(self):
        self.storage = create_storage()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.stop_events: Dict[str, asyncio.Event] = {}
        self.running = True

    async def initialize(self) -> bool:
        """
        Initialize the scraper.

        Returns:
            bool: True if initialization was successful, False otherwise
        """
        if not await self.storage.initialize():
            logger.error("Failed to initialize storage")
            return False
        return True

    async def start_channel_scraping(self, channel_name: str) -> bool:
        """
        Start scraping a specific channel.

        Returns:
            bool: True if scraping was started successfully, False otherwise
        """
        if channel_name in self.active_tasks:
            logger.warning("Channel %s is already being scraped", channel_name)
            return True

        stop_event = asyncio.Event()
        self.stop_events[channel_name] = stop_event

        task = asyncio.create_task(
            self._scrape_channel_with_retry(channel_name, stop_event)
        )
        self.active_tasks[channel_name] = task

        logger.info("Started scraping channel: %s", channel_name)
        return True

    async def _scrape_channel_with_retry(
        self, channel_name: str, stop_event: asyncio.Event
    ) -> None:
        """
        Scrape a channel with automatic retry on failures.

        Returns:
            None
        """
        retry_count = 0
        connection_retry_count = 0
        max_retries = 10
        max_connection_retries = 50
        base_delay = 2
        connection_base_delay = 1

        while not stop_event.is_set() and self.running:
            try:
                await listen_to_chat(channel_name, self.storage, stop_event)
                # Reset counters on successful connection
                retry_count = 0
                connection_retry_count = 0
                break
            except websockets.exceptions.ConnectionClosed as e:
                connection_retry_count += 1

                # Check if this is a common error that needs longer backoff
                error_code = getattr(e, "code", None)
                if error_code in [4200, 1011]:  # Server restart or ping timeout
                    if connection_retry_count > max_connection_retries:
                        logger.error(
                            "Max connection retries exceeded for channel %s (error %s), stopping",
                            channel_name,
                            error_code,
                        )
                        break

                    # Start at 1s, cap at 30s, with 30s max delay for persistent connection issues
                    if connection_retry_count <= 10:
                        delay = min(
                            connection_base_delay
                            * (1.5 ** min(connection_retry_count, 8)),
                            30,
                        )
                    else:
                        delay = 30  # Max delay for persistent connection issues
                    logger.warning(
                        "WebSocket connection closed for %s (error %s, attempt %d/%d). Retrying in %.1f seconds...",
                        channel_name,
                        error_code,
                        connection_retry_count,
                        max_connection_retries,
                        delay,
                    )
                else:
                    # Unknown connection error, treat as regular retry
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error(
                            "Max retries exceeded for channel %s, stopping",
                            channel_name,
                        )
                        break
                    delay = base_delay * (2 ** (retry_count - 1))
                    logger.warning(
                        "Connection error for %s (attempt %d/%d): %s. Retrying in %d seconds...",
                        channel_name,
                        retry_count,
                        max_retries,
                        e,
                        delay,
                    )

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(
                        "Max retries exceeded for channel %s, stopping", channel_name
                    )
                    break

                delay = min(base_delay * (2 ** (retry_count - 1)), 30)
                logger.warning(
                    "Error in channel %s (attempt %d/%d): %s. Retrying in %d seconds...",
                    channel_name,
                    retry_count,
                    max_retries,
                    e,
                    delay,
                )

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    pass

        if channel_name in self.active_tasks:
            del self.active_tasks[channel_name]
        if channel_name in self.stop_events:
            del self.stop_events[channel_name]

        logger.info("Stopped scraping channel: %s", channel_name)

    async def stop_channel_scraping(self, channel_name: str) -> bool:
        """
        Stop scraping a specific channel.

        Returns:
            bool: True if scraping was stopped successfully, False otherwise
        """
        if channel_name not in self.active_tasks:
            logger.warning("Channel %s is not being scraped", channel_name)
            return False

        if channel_name in self.stop_events:
            self.stop_events[channel_name].set()

        task = self.active_tasks.get(channel_name)
        if task:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout waiting for channel %s to stop, cancelling task",
                    channel_name,
                )
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except asyncio.TimeoutError:
                    logger.error("Failed to cancel task for channel %s", channel_name)

        return True

    async def load_and_start_active_channels(self) -> None:
        """
        Load all active channels from database and start scraping them.

        Returns:
            None
        """
        active_channels = await self.storage.get_active_channels()

        if not active_channels:
            logger.info("No active channels to resume")
            return

        console.print(f"[dim]resuming {len(active_channels)} active channels...[/dim]")
        logger.info(
            "Resuming %d active channels: %s",
            len(active_channels),
            ", ".join(active_channels),
        )

        for channel_name in active_channels:
            await self.start_channel_scraping(channel_name)

    async def add_channel(self, channel_name: str) -> bool:
        """
        Add a new channel and start scraping it.

        Returns:
            bool: True if channel was added and scraping started successfully, False otherwise
        """
        if await self.storage.channel_exists(channel_name):
            logger.warning("Channel '%s' already exists in database", channel_name)
            console.print(f"[dim]'{channel_name}' already exists[/dim]")
            return True

        console.print(f"[dim]checking '{channel_name}'...[/dim]")
        logger.info("Checking if channel '%s' exists on Kick...", channel_name)
        channel_info = get_channel_info(channel_name)

        if not channel_info.success:
            logger.error(
                "Channel '%s' not found or error: %s", channel_name, channel_info.error
            )
            console.print(f"[red]'{channel_name}': {channel_info.error}[/red]")
            return False

        if not await self.storage.add_channel(channel_name):
            logger.error("Failed to add channel '%s' to database", channel_name)
            console.print(f"[red]failed to add '{channel_name}' to database[/red]")
            return False

        if await self.start_channel_scraping(channel_name):
            logger.info(
                "Successfully added and started scraping channel '%s'", channel_name
            )
            console.print(f"[green]'{channel_name}' added[/green]")
            return True
        else:
            logger.error("Failed to start scraping channel '%s'", channel_name)
            console.print(f"[red]failed to start scraping '{channel_name}'[/red]")
            return False

    async def pause_channel(self, channel_name: str) -> bool:
        """
        Pause a channel (stop scraping and mark as paused).

        Returns:
            bool: True if channel was paused successfully, False otherwise
        """
        await self.stop_channel_scraping(channel_name)

        if await self.storage.pause_channel(channel_name):
            logger.info("Channel '%s' paused successfully", channel_name)
            console.print(f"[yellow]'{channel_name}' paused[/yellow]")
            return True
        else:
            logger.error("Failed to pause channel '%s'", channel_name)
            console.print(f"[red]failed to pause '{channel_name}'[/red]")
            return False

    async def resume_channel(self, channel_name: str) -> bool:
        """
        Resume a paused channel.

        Returns:
            bool: True if channel was resumed successfully, False otherwise
        """
        if not await self.storage.resume_channel(channel_name):
            logger.error("Failed to resume channel '%s'", channel_name)
            console.print(f"[red]failed to resume '{channel_name}'[/red]")
            return False

        if await self.start_channel_scraping(channel_name):
            logger.info("Channel '%s' resumed successfully", channel_name)
            console.print(f"[green]'{channel_name}' resumed[/green]")
            return True
        else:
            logger.error(
                "Failed to start scraping for resumed channel '%s'", channel_name
            )
            console.print(f"[red]failed to start scraping '{channel_name}'[/red]")
            return False

    async def resume_all_channels(self) -> bool:
        """
        Resume all channels.

        Returns:
            bool: True if resumed successfully, False otherwise
        """
        channels = await self.storage.get_all_channels()

        if not channels:
            logger.info("No channels to resume")
            return True
        try:
            for channel_name in channels:
                await self.resume_channel(channel_name)

            return True
        except Exception as e:
            logger.error("Failed to resume all channels: %s", e)
            return False

    async def list_channels(self) -> None:
        """
        List all channels with their status.

        Returns:
            None
        """
        channels = await self.storage.list_all_channels()

        if not channels:
            console.print("[dim]no channels found[/dim]")
            return

        table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim", padding=(0, 1))
        table.add_column("channel", style="bold")
        table.add_column("status")
        table.add_column("scraping")
        table.add_column("added", style="dim")
        table.add_column("paused at", style="dim")

        for channel in channels:
            status = "[green]active[/green]" if not channel["paused"] else "[yellow]paused[/yellow]"
            scraping = "[green]running[/green]" if channel["name"] in self.active_tasks else "[dim]stopped[/dim]"
            paused_at = str(channel["paused_at"]) if channel["paused_at"] else ""
            table.add_row(
                channel["name"],
                status,
                scraping,
                str(channel["added_at"]),
                paused_at,
            )

        console.print(table)

    async def show_stats(self, channel_name: str) -> None:
        """
        Show stats for a channel.

        Returns:
            None
        """
        if not await self.storage.channel_exists(channel_name):
            console.print(f"[red]'{channel_name}' not found[/red]")
            return

        stats = await self.storage.get_channel_stats(channel_name)

        if not stats:
            console.print(f"[dim]no stats available for '{channel_name}'[/dim]")
            return

        console.print(f"\n[bold]{channel_name}[/bold]")

        overview = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        overview.add_column(style="dim")
        overview.add_column()
        overview.add_row("total messages", str(stats["total_messages"]))
        overview.add_row("unique users", str(stats["unique_users"]))
        if stats["date_range"][0] and stats["date_range"][1]:
            overview.add_row("date range", f"{stats['date_range'][0]}  —  {stats['date_range'][1]}")
        console.print(overview)

        if stats["message_counts"]:
            console.print("[dim]by event type[/dim]")
            counts = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
            counts.add_column(style="dim")
            counts.add_column()
            for event_type, count in stats["message_counts"].items():
                counts.add_row(event_type, str(count))
            console.print(counts)

    async def shutdown(self) -> None:
        """
        Gracefully shutdown all scraping tasks.

        Returns:
            None
        """
        logger.info("Shutting down scraper...")
        self.running = False

        for channel_name in list(self.active_tasks.keys()):
            await self.stop_channel_scraping(channel_name)

        logger.info("All scraping tasks stopped")

    async def _build_completer(self) -> NestedCompleter:
        channels = await self.storage.get_all_channels()
        channel_dict = {ch: None for ch in channels}
        return NestedCompleter.from_nested_dict({
            "add": None,
            "list": None,
            "pause": channel_dict,
            "resume": channel_dict,
            "stats": channel_dict,
            "exit": None,
            "help": None,
        })

    async def run_cli(self) -> None:
        """
        Run the interactive CLI.

        Returns:
            None
        """
        console.print(Panel(
            "[dim]add  list  pause <channel>  resume <channel>  stats <channel>  exit  help[/dim]",
            title="[bold]kick chat scraper[/bold]",
            border_style="dim",
        ))

        session = PromptSession()

        while self.running:
            try:
                active_count = len(self.active_tasks)
                prompt = f"kick-scraper ({active_count} active)> "
                completer = await self._build_completer()

                user_input = await session.prompt_async(prompt, completer=completer)
                await self.handle_command(user_input.strip())

            except (EOFError, KeyboardInterrupt):
                await self.cleanup_and_exit()
                break
            except asyncio.CancelledError:
                await self.cleanup_and_exit()
                break
            except Exception as e:
                logger.error("Error in CLI: %s", e)

    async def handle_command(self, command: str) -> None:
        """
        Handle CLI commands.

        Returns:
            None
        """
        if not command:
            return

        parts = command.split()
        cmd = parts[0].lower()

        if cmd == "exit":
            await self.cleanup_and_exit()

        elif cmd == "add" and len(parts) == 2:
            channel_name = parts[1]
            await self.add_channel(channel_name)

        elif cmd == "list":
            await self.list_channels()

        elif cmd == "pause" and len(parts) == 2:
            channel_name = parts[1]
            await self.pause_channel(channel_name)

        # if an argument is given, resume the channel
        elif cmd == "resume" and len(parts) == 2:
            channel_name = parts[1]
            await self.resume_channel(channel_name)

        # if no argument is given, resume all channels
        elif cmd == "resume" and len(parts) == 1:
            # temporary solution to resume all channels in case of errors
            await self.load_and_start_active_channels()

        elif cmd == "stats" and len(parts) == 2:
            channel_name = parts[1]
            await self.show_stats(channel_name)

        elif cmd == "help":
            table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
            table.add_column(style="bold")
            table.add_column(style="dim")
            table.add_row("add <channel>", "start scraping a channel")
            table.add_row("list", "list all channels and their status")
            table.add_row("pause <channel>", "pause scraping for a channel")
            table.add_row("resume", "restart active channels")
            table.add_row("resume <channel>", "resume a paused channel")
            table.add_row("stats <channel>", "show statistics for a channel")
            table.add_row("exit", "shutdown")
            console.print(table)

        else:
            console.print("[dim]unknown command — type 'help' for available commands[/dim]")

    async def cleanup_and_exit(self) -> None:
        """
        Clean exit message.

        Returns:
            None
        """
        console.print("\n[dim]shutting down...[/dim]")
        self.running = False


async def main():
    """
    Main entry point for the CLI
    """
    scraper = KickChatLogger()

    try:
        if not await scraper.initialize():
            logger.error("Failed to initialize scraper")
            return 1

        await scraper.load_and_start_active_channels()

        await scraper.run_cli()

    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return 1

    finally:
        await scraper.shutdown()

    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Application interrupted")
        sys.exit(0)
