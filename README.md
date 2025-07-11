# Kick Chat Scraper

A Python-based Kick.com chat logger. Collects chat messages, user subscriptions, timeouts, bans, unbans, message deletions, pinned message events, etc. asynchronously from multiple channels and stores them in a database backend (SQLite or PostgreSQL).

I mostly created this for fun to run via SSH on my small Linux server in a tmux session. Educational purposes only. 

## Features

The tool connects to Kick.com's WebSocket to log live chat events in real time. The system records almost all events, including chat messages, user subscriptions, bans and unbans, message deletions, pinned messages, stream hosting, and chatroom setting changes. All data is stored in a database backend (SQLite or PostgreSQL), with each channel having its own table. There is a command-line interface for adding, pausing, resuming, and managing channels, as well as viewing statistics. The program is designed to handle errors automatically, with built-in reconnection and retry logic to keep it running smoothly. It also uses curl-cffi to get around Cloudflare protection and avoid detection.

## Technical Notes

### How it Works
- Everything runs asynchronously with `asyncio`, so you can scrape a lot of channels at once without blocking (tested for 100~ channels simultaneously without issues).
- The code is split up into logical pieces: API handling, database operations, websocket and event handling, and the CLI.
- Each type of Kick event (chat, subs, bans, etc.) gets its own parsing logic, so one can change the way events are handled.

### What's Under the Hood
- **WebSockets**: Uses the `websockets` library to listen to Kick's live chat stream.
- **API Calls**: `curl-cffi` is used for HTTP requests, mostly because Kick uses Cloudflare for protection and this gets around it.
- **Database**: Storage is pluggable. Use SQLite (`aiosqlite`) or PostgreSQL (`asyncpg`). Switch in `config.py`.
- **CLI**: The command-line interface is built with `aioconsole`.

### Some Design Choices
- Channel names get cleaned up before being used as table names, so unique names are guaranteed, this handling might not cover all edge cases, but is sufficient.
- If the connection drops, it'll keep retrying with exponential backoff instead of spamming the server to not get blocked.
- The WebSocket connection self-pings every some minutes (see `PING_INTERVAL_MINUTES` in `config.py`) to stay alive.
- Configs are stored in `config.py`, so one can change the websocket URL if Kick changes it, ping interval, messages, etc.
- Raw payloads are stored for debugging, though this increases storage requirements.
- Each channel gets its own database table for better query performance and easier data management.

## Installation

1. **Clone the repository**:
   ```
   git clone https://github.com/iberkayC/kick-chat-logger
   cd kick-chat-logger/src
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
python cli.py
```

### Available Commands
- `add <channel_name>` - Add and start monitoring a channel
- `pause <channel_name>` - Temporarily stop monitoring a channel
- `resume <channel_name>` - Resume monitoring a paused channel
- `remove <channel_name>` - Completely remove a channel from monitoring
- `list` - Show all channels and their status
- `stats <channel_name>` - Display statistics for a specific channel
- `resume` - Resume all channels (needs improvement, temporary solution)
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

- The logging is not the best, gets the job done.
- Before all websocket connection closeds were handled, sometimes the `PAUSED` and `SCRAPING` states were not synchronized. Now, it's probably fixed, but they are not explicitly synchronized. So, one might need to resume manually even though the channel is already resumed. Retrying till connected is not a good idea, because of bans, name changes, etc. The synchronization fix is not hard, but I didn't do it as a choice.
- If you are scraping a lot of channels, the startup is slow, this is not really fixable, but one might want to make it run on the background and allow CLI input.

## Contact

You may contact me at `ceylaniberkay@gmail.com`

## Storage Configuration
The default storage type is SQLite which gets database locked errors on high traffic.

1. Open `src/config.py`.
2. Set
   ```
   STORAGE_TYPE = "sqlite"       # or "postgresql"
   ```
3. For PostgreSQL, adjust the connection details just below:
   ```
   DEFAULT_PG_HOST = "localhost"
   DEFAULT_PG_PORT = 5432
   DEFAULT_PG_DB   = "kick_chat_logger"
   DEFAULT_PG_USER = "kickscraper"
   DEFAULT_PG_PASSWORD = "password"
   ```
   
Consider using environment variables instead of hardcoded credentials, I haven't bothered.
