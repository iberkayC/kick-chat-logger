# Kick Chat Scraper

A Python-based Kick.com chat logger. Collects chat messages, user subscriptions, timeouts, bans, unbans, message deletions, pinned message events, etc. asynchronously from multiple channels and stores them in a database backend (SQLite or PostgreSQL).

## Features

The tool connects to Kick.com's WebSocket to log live chat events in real time. The system records almost all events, including chat messages, user subscriptions, bans and unbans, message deletions, pinned messages, stream hosting, and chatroom setting changes. All data is stored in a database backend (SQLite or PostgreSQL), with each channel having its own table. There is a command-line interface for adding, pausing, resuming, and managing channels, as well as viewing statistics. The program is designed to handle errors automatically, with built-in reconnection and retry logic to keep it running smoothly. It also uses curl-cffi to get around Cloudflare protection and avoid detection.

## Technical Notes

### How it Works
- Everything runs asynchronously with `asyncio`, so you can scrape a lot of channels at once without blocking (soak tested with 100 channels for 30+ hours without issues).
- All channels are multiplexed over a single Pusher WebSocket connection. Each channel gets its own queue and consumer, so one busy channel can't stall the others.
- The code is split up into logical pieces: API handling, database operations, websocket and event handling, and the CLI.
- Each type of Kick event (chat, subs, bans, etc.) gets its own parsing logic, so one can change the way events are handled.

### What's Under the Hood
- **WebSockets**: Uses the `websockets` library to listen to Kick's live chat stream.
- **API Calls**: `curl-cffi` is used for HTTP requests, mostly because Kick uses Cloudflare for protection and this gets around it.
- **Database**: Storage is pluggable. Use SQLite (`aiosqlite`) or PostgreSQL (`asyncpg`). Switch in `config.py`.
- **CLI**: The command-line interface is built with `prompt_toolkit` and `rich`.

### Some Design Choices
- Channel names get cleaned up before being used as table names, so unique names are guaranteed, this handling might not cover all edge cases, but is sufficient.
- If the connection drops, it'll keep retrying forever with capped exponential backoff and jitter instead of spamming the server. Channel-level failures (banned or renamed channels) are marked as errored instead and can be retried with `resume <channel>`.
- The WebSocket connection self-pings (see `PING_INTERVAL_SECONDS` in `config.py`) to stay alive and notice dead connections quickly, and answers Pusher's application-level pings.
- Chatroom IDs are cached in the database, so restarts don't hit the Kick API for channels it has already seen. Fresh lookups are capped by `CHANNEL_LOOKUP_CONCURRENCY` in `config.py`, and `resume <channel>` always looks the ID up fresh in case the cached one went stale.
- Configs are stored in `config.py`, so one can change the websocket URL if Kick changes it, ping interval, messages, etc.
- Raw payloads are stored for debugging, though this increases storage requirements.
- Each channel gets its own database table for better query performance and easier data management.

## Installation

1. **Clone the repository**:
   ```
   git clone https://github.com/iberkayC/kick-chat-logger
   cd kick-chat-logger
   ```

2. **Use a virtual environment**:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Start the scraper
```
cd src
python cli.py
```

### Available Commands
- `add <channel_name>` - Add and start monitoring a channel
- `bulkadd <file>` - Add and start monitoring every channel listed in a file (one name per line, blank lines and `#` comments ignored)
- `pause <channel_name>` - Temporarily stop monitoring a channel
- `resume <channel_name>` - Resume monitoring a paused channel
- `list` - Show all channels and their status
- `stats <channel_name>` - Display statistics for a specific channel
- `resume` - Restart all active channels
- `help` - Show available commands
- `exit` - Shutdown the scraper

## Database Schema

The system uses the following two main table types (schema identical for SQLite and PostgreSQL):

### Channels Table
Tracks all monitored channels and their status:
- `id` - Primary key
- `name` - Normalized channel name
- `added_at` - UTC timestamp when channel was added
- `paused` - Whether monitoring is paused
- `paused_at` - UTC timestamp when channel was paused

### Channel Event Tables
Each channel gets its own table (prefixed with `kickchat_` on default) containing:
- `id` - Primary key
- `event_type` - Type of event (chat message, subscription, ban, etc.)
- `event_id` - Kick's event identifier
- `chatroom_id` - Kick's chatroom identifier
- `timestamp` - Event timestamp from Kick
- `user_id` - User's Kick ID
- `username` - User's display name
- `content` - Processed message content or event description
- `sender_data` - User metadata
- `metadata` - Event-specific metadata
- `raw_payload` - Original JSON payload (for debugging, can be removed if wanted)
- `created_at` - UTC timestamp when event was stored locally

## Known Issues

- If a channel gets banned or renamed while being logged, Pusher may still accept the subscription, so `list` can show it as running while it logs nothing. There is no automatic staleness detection, check `stats` once in a while and `resume <channel>` anything suspicious to refresh it.
- Channels that fail to subscribe (bans, renames, lookup errors) show as errored in `list` and stay that way until manually retried with `resume <channel>`.

## Contact

You may contact me at `ceylaniberkay@gmail.com`

## Storage Configuration
The default storage type is SQLite which gets database locked errors on high traffic.

1. Open `src/config.py`.
2. Set
   ```
   STORAGE_TYPE = "sqlite"       # or "postgresql"
   ```
3. For PostgreSQL, copy `.env.example` to `.env` and fill in your connection details:
   ```
   KICK_PG_HOST=localhost
   KICK_PG_PORT=5432
   KICK_PG_DB=kick_chat_logger
   KICK_PG_USER=kickscraper
   KICK_PG_PASSWORD=changeme
   ```
