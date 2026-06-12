#!/usr/bin/env python3
"""Kick Chat Scraper - Long-running server application with interactive CLI."""

import asyncio
import logging
import sys

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import NestedCompleter
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from kick_api import close_session, get_channel_info
from kick_chat_listener import (
    ChannelNotFoundError,
    ChatConnectionManager,
    ChatroomIdError,
)
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
    """Main scraper application managing multiple channel tasks.

    This is the entry point for the CLI.
    """

    def __init__(self):
        """Set up storage, the connection manager, and CLI state."""
        self.storage = create_storage()
        self.manager = ChatConnectionManager(self.storage)
        self.running = True

    async def initialize(self) -> bool:
        """Initialize the scraper.

        Returns:
            bool: True if initialization was successful, False otherwise

        """
        if not await self.storage.initialize():
            logger.error("Failed to initialize storage")
            return False
        return True

    async def load_and_start_active_channels(self) -> None:
        """Load all active channels from database and start scraping them.

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

        async def start_one(channel_name: str) -> None:
            try:
                await self.manager.subscribe(channel_name)
            except (ChannelNotFoundError, ChatroomIdError) as e:
                logger.error("Failed to start scraping %s: %s", channel_name, e)

        await asyncio.gather(*(start_one(name) for name in active_channels))

    async def add_channel(self, channel_name: str) -> bool:
        """Add a new channel and start scraping it.

        Returns:
            bool: True if channel was added and scraping started successfully, False otherwise

        """
        if await self.storage.channel_exists(channel_name):
            logger.warning("Channel '%s' already exists in database", channel_name)
            console.print(f"[dim]'{channel_name}' already exists[/dim]")
            return True

        console.print(f"[dim]checking '{channel_name}'...[/dim]")
        logger.info("Checking if channel '%s' exists on Kick...", channel_name)
        channel_info = await get_channel_info(channel_name)

        if not channel_info.success:
            logger.error(
                "Channel '%s' not found or error: %s",
                channel_name,
                channel_info.error,
            )
            console.print(f"[red]'{channel_name}': {channel_info.error}[/red]")
            return False

        if not await self.storage.add_channel(channel_name):
            logger.error("Failed to add channel '%s' to database", channel_name)
            console.print(f"[red]failed to add '{channel_name}' to database[/red]")
            return False

        try:
            await self.manager.subscribe(channel_name)
        except (ChannelNotFoundError, ChatroomIdError) as e:
            logger.error("Failed to start scraping channel '%s': %s", channel_name, e)
            console.print(f"[red]failed to start scraping '{channel_name}'[/red]")
            return False

        logger.info(
            "Successfully added and started scraping channel '%s'",
            channel_name,
        )
        console.print(f"[green]'{channel_name}' added[/green]")
        return True

    async def pause_channel(self, channel_name: str) -> bool:
        """Pause a channel (stop scraping and mark as paused).

        Returns:
            bool: True if channel was paused successfully, False otherwise

        """
        await self.manager.unsubscribe(channel_name)

        if await self.storage.pause_channel(channel_name):
            logger.info("Channel '%s' paused successfully", channel_name)
            console.print(f"[yellow]'{channel_name}' paused[/yellow]")
            return True
        logger.error("Failed to pause channel '%s'", channel_name)
        console.print(f"[red]failed to pause '{channel_name}'[/red]")
        return False

    async def resume_channel(self, channel_name: str) -> bool:
        """Resume a paused channel.

        Returns:
            bool: True if channel was resumed successfully, False otherwise

        """
        if not await self.storage.resume_channel(channel_name):
            logger.error("Failed to resume channel '%s'", channel_name)
            console.print(f"[red]failed to resume '{channel_name}'[/red]")
            return False

        try:
            # fresh lookup, resume is the manual fix for a stale chatroom id
            await self.manager.subscribe(channel_name, refresh=True)
        except (ChannelNotFoundError, ChatroomIdError) as e:
            logger.error(
                "Failed to start scraping for resumed channel '%s': %s",
                channel_name,
                e,
            )
            console.print(f"[red]failed to start scraping '{channel_name}'[/red]")
            return False

        logger.info("Channel '%s' resumed successfully", channel_name)
        console.print(f"[green]'{channel_name}' resumed[/green]")
        return True

    async def list_channels(self) -> None:
        """List all channels with their status.

        Returns:
            None

        """
        channels = await self.storage.list_all_channels()

        if not channels:
            console.print("[dim]no channels found[/dim]")
            return

        table = Table(
            box=box.SIMPLE,
            show_header=True,
            header_style="bold dim",
            padding=(0, 1),
        )
        table.add_column("channel", style="bold")
        table.add_column("status")
        table.add_column("scraping")
        table.add_column("added", style="dim")
        table.add_column("paused at", style="dim")

        state_display = {
            "subscribed": "[green]running[/green]",
            "pending": "[yellow]pending[/yellow]",
            "errored": "[red]error[/red]",
        }

        for channel in channels:
            status = (
                "[green]active[/green]"
                if not channel["paused"]
                else "[yellow]paused[/yellow]"
            )
            scraping = state_display.get(
                self.manager.channel_state(channel["name"]),
                "[dim]stopped[/dim]",
            )
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
        """Show stats for a channel.

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
            overview.add_row(
                "date range",
                f"{stats['date_range'][0]}  —  {stats['date_range'][1]}",
            )
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
        """Gracefully shutdown all scraping tasks.

        Returns:
            None

        """
        logger.info("Shutting down scraper...")
        self.running = False

        await self.manager.close()
        await close_session()

        logger.info("All scraping tasks stopped")

    async def _build_completer(self) -> NestedCompleter:
        channels = await self.storage.get_all_channels()
        channel_dict = dict.fromkeys(channels)
        return NestedCompleter.from_nested_dict(
            {
                "add": None,
                "list": None,
                "pause": channel_dict,
                "resume": channel_dict,
                "stats": channel_dict,
                "exit": None,
                "help": None,
            },
        )

    async def run_cli(self) -> None:
        """Run the interactive CLI.

        Returns:
            None

        """
        console.print(
            Panel(
                "[dim]add  list  pause <channel>  resume <channel>  stats <channel>  exit  help[/dim]",
                title="[bold]kick chat scraper[/bold]",
                border_style="dim",
            ),
        )

        session = PromptSession()

        while self.running:
            try:
                active_count = self.manager.active_count()
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
        """Handle CLI commands.

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
            console.print(
                "[dim]unknown command — type 'help' for available commands[/dim]",
            )

    async def cleanup_and_exit(self) -> None:
        """Clean exit message.

        Returns:
            None

        """
        console.print("\n[dim]shutting down...[/dim]")
        self.running = False


async def main():
    """Run the CLI application."""
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
