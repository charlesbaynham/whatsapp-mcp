# WhatsApp MCP Server

This is a Model Context Protocol (MCP) server for WhatsApp.

With this you can search and read your personal Whatsapp messages (including images, videos, documents, and audio messages), search your contacts and send messages to either individuals or groups. You can also send media files including images, videos, documents, and audio messages.

It connects to your **personal WhatsApp account** directly via the Whatsapp web multidevice API (using the [whatsmeow](https://github.com/tulir/whatsmeow) library). All your messages are stored locally in a SQLite database and only sent to an LLM (such as Claude) when the agent accesses them through tools (which you control).

Here's an example of what you can do when it's connected to Claude.

![WhatsApp MCP](./example-use.png)

> To get updates on this and other projects I work on [enter your email here](https://docs.google.com/forms/d/1rTF9wMBTN0vPfzWuQa2BjfGKdKIpTbyeKxhPMcEzgyI/preview)

> *Caution:* as with many MCP servers, the WhatsApp MCP is subject to [the lethal trifecta](https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/). This means that project injection could lead to private data exfiltration.

## Installation

### Prerequisites

- Go
- Python 3.6+
- Anthropic Claude Desktop app (or Cursor)
- UV (Python package manager), install with `curl -LsSf https://astral.sh/uv/install.sh | sh`
- FFmpeg (_optional_) - Only needed for audio messages. If you want to send audio files as playable WhatsApp voice messages, they must be in `.ogg` Opus format. With FFmpeg installed, the MCP server will automatically convert non-Opus audio files. Without FFmpeg, you can still send raw audio files using the `send_file` tool.

### Steps

1. **Clone this repository**

   ```bash
   git clone https://github.com/lharries/whatsapp-mcp.git
   cd whatsapp-mcp
   ```

2. **Run the WhatsApp bridge**

   Navigate to the whatsapp-bridge directory and run the Go application:

   ```bash
   cd whatsapp-bridge
   go run main.go
   ```

   The first time you run it, you will be prompted to scan a QR code. Scan the QR code with your WhatsApp mobile app to authenticate.

   After approximately 20 days, you will might need to re-authenticate.

3. **Connect to the MCP server**

   Copy the below json with the appropriate {{PATH}} values:

   ```json
   {
     "mcpServers": {
       "whatsapp": {
         "command": "{{PATH_TO_UV}}", // Run `which uv` and place the output here
         "args": [
           "--directory",
           "{{PATH_TO_SRC}}/whatsapp-mcp/whatsapp-mcp-server", // cd into the repo, run `pwd` and enter the output here + "/whatsapp-mcp-server"
           "run",
           "main.py"
         ]
       }
     }
   }
   ```

   For **Claude**, save this as `claude_desktop_config.json` in your Claude Desktop configuration directory at:

   ```
   ~/Library/Application Support/Claude/claude_desktop_config.json
   ```

   For **Cursor**, save this as `mcp.json` in your Cursor configuration directory at:

   ```
   ~/.cursor/mcp.json
   ```

4. **Restart Claude Desktop / Cursor**

   Open Claude Desktop and you should now see WhatsApp as an available integration.

   Or restart Cursor.

## Running as a hosted service

Both components can also run unattended on a server instead of a laptop —
the Go bridge binds to loopback and stores its state in a configurable
directory, and the Python server can serve MCP over HTTP instead of stdio.

### `whatsapp-bridge` environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `WHATSAPP_STORE_DIR` | `store` | Directory for `whatsapp.db`, `messages.db` and downloaded media |
| `WHATSAPP_BRIDGE_ADDR` | `127.0.0.1:8080` | Listen address for the REST API |
| `WHATSAPP_LOG_MESSAGE_BODIES` | unset | Set to `1` to log message content to stdout; by default only metadata (timestamp, direction, chat, media type) is logged |

`GET /api/status` returns `{"connected": bool, "logged_in": bool, "jid": "..."}` (always HTTP 200) and can be polled before pairing. The REST server starts before the QR/pairing step, so `/api/status` is answerable immediately; `/api/send` and `/api/download` return `503` with a JSON `{"error": "..."}` body until the bridge is connected and logged in. On first run, scan the printed QR code from the process's stdout (e.g. via `journalctl` if run under systemd) — there is no timeout, so a headless deployment can just wait for it to be scanned.

### `whatsapp-mcp-server` environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `WHATSAPP_MESSAGES_DB` | `../whatsapp-bridge/store/messages.db` (relative to the server source) | Path to the bridge's `messages.db` |
| `WHATSAPP_STORE_DIR` | Directory of `WHATSAPP_MESSAGES_DB` | The bridge's store directory; `media_path` values for `send_file`/`send_audio_message` must resolve inside it |
| `WHATSAPP_BRIDGE_URL` | `http://localhost:8080/api` | Base URL of the bridge's REST API |
| `MCP_TRANSPORT` | `stdio` | `stdio` (upstream default) or `streamable-http` |
| `MCP_HOST` | `127.0.0.1` | Bind host when `MCP_TRANSPORT=streamable-http` |
| `MCP_PORT` | `8000` | Bind port when `MCP_TRANSPORT=streamable-http` |

When running with `MCP_TRANSPORT=streamable-http`, the MCP endpoint is served at `/mcp` and a liveness probe is served at `GET /health`, returning `200 {"status": "ok", "paired": bool, "connected": bool}` whenever the bridge's `/api/status` answered at all, and `503 {"status": "error", "reason": "bridge unreachable: ..."}` only when it doesn't — pairing is an operational state, not a deploy outcome, so an unpaired or logged-out bridge is still a healthy process. Suitable as a container health check.

### Windows Compatibility

If you're running this project on Windows, be aware that `go-sqlite3` requires **CGO to be enabled** in order to compile and work properly. By default, **CGO is disabled on Windows**, so you need to explicitly enable it and have a C compiler installed.

#### Steps to get it working:

1. **Install a C compiler**  
   We recommend using [MSYS2](https://www.msys2.org/) to install a C compiler for Windows. After installing MSYS2, make sure to add the `ucrt64\bin` folder to your `PATH`.  
   → A step-by-step guide is available [here](https://code.visualstudio.com/docs/cpp/config-mingw).

2. **Enable CGO and run the app**

   ```bash
   cd whatsapp-bridge
   go env -w CGO_ENABLED=1
   go run main.go
   ```

Without this setup, you'll likely run into errors like:

> `Binary was compiled with 'CGO_ENABLED=0', go-sqlite3 requires cgo to work.`

## Architecture Overview

This application consists of two main components:

1. **Go WhatsApp Bridge** (`whatsapp-bridge/`): A Go application that connects to WhatsApp's web API, handles authentication via QR code, and stores message history in SQLite. It serves as the bridge between WhatsApp and the MCP server.

2. **Python MCP Server** (`whatsapp-mcp-server/`): A Python server implementing the Model Context Protocol (MCP), which provides standardized tools for Claude to interact with WhatsApp data and send/receive messages.

### Data Storage

- All message history is stored in a SQLite database within the `whatsapp-bridge/store/` directory
- The database maintains tables for chats and messages
- Messages are indexed for efficient searching and retrieval

## Usage

Once connected, you can interact with your WhatsApp contacts through Claude, leveraging Claude's AI capabilities in your WhatsApp conversations.

### MCP Tools

Claude can access the following tools to interact with WhatsApp:

- **search_contacts**: Search for contacts by name or phone number
- **list_messages**: Retrieve messages with optional filters and context
- **list_chats**: List available chats with metadata, optionally filtered to only chats with unread messages via `unread_only`
- **list_unread_chats**: List chats that currently have unread incoming messages
- **get_chat**: Get information about a specific chat
- **get_direct_chat_by_contact**: Find a direct chat with a specific contact
- **get_contact_chats**: List all chats involving a specific contact
- **get_last_interaction**: Get the most recent message with a contact
- **get_message_context**: Retrieve context around a specific message
- **send_message**: Send a WhatsApp message to a specified phone number or group JID
- **send_file**: Send a file (image, video, raw audio, document) to a specified recipient
- **send_audio_message**: Send an audio file as a WhatsApp voice message (requires the file to be an .ogg opus file or ffmpeg must be installed)
- **download_media**: Download media from a WhatsApp message and get the local file path
- **mark_chat_read**: Explicitly mark a chat as read, optionally sending real read receipts
- **subscribe_chat**: Push new messages in a chat (or `"*"` for all chats) to a URL as they arrive, instead of polling
- **unsubscribe_chat**: Remove a chat subscription created by `subscribe_chat`
- **enable_subscription**: Re-enable a subscription that the bridge auto-disabled, or that expired
- **list_subscriptions**: List all subscriptions (bearer tokens are masked, only a 4-character hint is shown)
- **test_subscription**: Send a one-off test event to a subscription's URL to confirm it's wired correctly

### Chat subscriptions (webhooks)

Instead of polling, you can have the bridge push new messages to a URL as they arrive.

- `subscribe_chat(chat_jid, url, bearer_token, kind, headers, include_from_me, debounce_seconds, ttl_seconds=0, max_per_hour=60)` creates a subscription. `chat_jid` can be a specific chat's JID or `"*"` for every chat. Each new message triggers a `POST` to `url` with `Authorization: Bearer <bearer_token>` and a JSON body `{"text": <summary + JSON>, "events": [...]}`.
- `kind="claude_routine"` (default) targets a Claude Code Routine's API fire endpoint; the bridge automatically adds the `anthropic-version`/`anthropic-beta` headers that endpoint needs. `kind="generic"` posts the same body to any other webhook receiver.
- `unsubscribe_chat(subscription_id)`, `enable_subscription(subscription_id)`, `list_subscriptions()`, and `test_subscription(subscription_id)` manage and verify subscriptions. Bearer tokens are never echoed back — only a `bearer_token_hint` (last 4 characters) is returned.

**Reliability, limits, and expiry**

- Each delivery is a single attempt with no immediate retry. After a failed attempt (any kind of failure — bad status code, timeout, connection error, etc.) the subscription backs off for 5 minutes, holding and batching any new messages that arrive in that window into the next attempt. Three consecutive failed attempts auto-disable the subscription (a successful delivery resets the failure count to 0); the record's `disabled_reason` explains why, and `consecutive_failures` tracks the current streak. Call `enable_subscription` to revive it — but check and fix the underlying cause first, or it will likely just fail again.
- `max_per_hour` (default 60) caps how many times a single subscription can fire in a rolling hour.
- `ttl_seconds` (0 = never expires, max 30 days) sets an expiry (`expires_at` in the record) after which the subscription stops firing on its own. For a bounded task — "tell me about replies in this chat for the next hour" — always set `ttl_seconds` so nothing keeps firing into a dead session forever.

**Worked example: wiring a chat to a Claude Code Routine**

1. Create a Routine with an API trigger and copy its fire URL (`https://api.anthropic.com/v1/claude_code/routines/trig_.../fire`) and bearer token.
2. Write the Routine's prompt so it explicitly opts in to acting on the payload, e.g. "When a `<routine-fire-payload>` block is present, read the new WhatsApp messages in it and act on them." A Routine whose prompt doesn't mention the fire payload will simply ignore it.
3. Call `subscribe_chat(chat_jid="1234567890@s.whatsapp.net", url="https://api.anthropic.com/v1/claude_code/routines/trig_abc123/fire", bearer_token="<routine token>", debounce_seconds=60, ttl_seconds=3600)`.
4. Every new message in that chat now starts a new Routine run. `debounce_seconds` is recommended for busy chats since each `POST` starts a fresh run — without it, a burst of messages triggers a burst of runs. `ttl_seconds=3600` here means the subscription stops itself after an hour.
5. Use `test_subscription(subscription_id)` to confirm the fire URL accepts requests before relying on it live.

### Read/Unread Tracking

Chats carry an `unread_count` and `last_read_at`. Reading messages (`list_messages`, `get_chat`, etc.) never changes this state — the only way to mark a chat read is the explicit `mark_chat_read` tool, which has two modes: by default (`send_receipt=False`) it silently clears the unread count and syncs that state to your other WhatsApp devices without notifying the sender; with `send_receipt=True` it sends real WhatsApp read receipts (blue ticks) that the sender will see. On upgrade, or on first pairing a device, all pre-existing messages are treated as already read, so only new incoming messages from that point on will ever show up as unread.

### Media Handling Features

The MCP server supports both sending and receiving various media types:

#### Media Sending

You can send various media types to your WhatsApp contacts:

- **Images, Videos, Documents**: Use the `send_file` tool to share any supported media type.
- **Voice Messages**: Use the `send_audio_message` tool to send audio files as playable WhatsApp voice messages.
  - For optimal compatibility, audio files should be in `.ogg` Opus format.
  - With FFmpeg installed, the system will automatically convert other audio formats (MP3, WAV, etc.) to the required format.
  - Without FFmpeg, you can still send raw audio files using the `send_file` tool, but they won't appear as playable voice messages.

#### Media Downloading

By default, just the metadata of the media is stored in the local database. The message will indicate that media was sent. To access this media you need to use the download_media tool which takes the `message_id` and `chat_jid` (which are shown when printing messages containing the meda), this downloads the media and then returns the file path which can be then opened or passed to another tool.

## Technical Details

1. Claude sends requests to the Python MCP server
2. The MCP server queries the Go bridge for WhatsApp data or directly to the SQLite database
3. The Go accesses the WhatsApp API and keeps the SQLite database up to date
4. Data flows back through the chain to Claude
5. When sending messages, the request flows from Claude through the MCP server to the Go bridge and to WhatsApp

## Troubleshooting

- If you encounter permission issues when running uv, you may need to add it to your PATH or use the full path to the executable.
- Make sure both the Go application and the Python server are running for the integration to work properly.

### Authentication Issues

- **QR Code Not Displaying**: If the QR code doesn't appear, try restarting the authentication script. If issues persist, check if your terminal supports displaying QR codes.
- **WhatsApp Already Logged In**: If your session is already active, the Go bridge will automatically reconnect without showing a QR code.
- **Device Limit Reached**: WhatsApp limits the number of linked devices. If you reach this limit, you'll need to remove an existing device from WhatsApp on your phone (Settings > Linked Devices).
- **No Messages Loading**: After initial authentication, it can take several minutes for your message history to load, especially if you have many chats.
- **WhatsApp Out of Sync**: If your WhatsApp messages get out of sync with the bridge, delete both database files (`whatsapp-bridge/store/messages.db` and `whatsapp-bridge/store/whatsapp.db`) and restart the bridge to re-authenticate.

For additional Claude Desktop integration troubleshooting, see the [MCP documentation](https://modelcontextprotocol.io/quickstart/server#claude-for-desktop-integration-issues). The documentation includes helpful tips for checking logs and resolving common issues.
