# Splitting the WhatsApp interface from the MCP server

Goal: one local WhatsApp service that any number of non-LLM clients (a
Hindsight forwarder first) and the MCP server can share, with push delivery,
without duplicating logic.

## Where we already are

The split is half done. The Go bridge is already the WhatsApp service:

- Loopback-only REST on `WHATSAPP_BRIDGE_ADDR` (default `127.0.0.1:8080`).
- Write side is complete: `/api/send`, `/api/download`, `/api/mark-read`,
  `/api/resync`, `/api/status`.
- Push exists: `/api/webhooks` subscriptions POST new messages to a URL.

What still lives in the Python MCP server (`whatsapp.py`) and blocks a second
client:

| Concern | Today | Problem |
| --- | --- | --- |
| All reads: `list_messages`, `list_chats`, `search_contacts`, `get_chat`, `get_message_context`, unread counts | Direct SQLite on `messages.db` | Every client would re-implement the SQL (unread `julianday` logic, LID resolution, context windows). |
| Voice notes | Python runs ffmpeg, writes temp `.ogg` into the store | Any client wanting voice notes needs ffmpeg + store access. |
| Path containment for `media_path` | Duplicated in Python and Go | Fine, but the Python copy goes away once reads move. |
| Push delivery semantics | Single attempt, 5 min cooldown, 3 fails then auto-disable, no persistence of missed events | Tuned for firing Claude routines (cost cap). Wrong for a forwarder that must see every message. |

So the work is: finish the bridge's REST surface, make the MCP server a pure
HTTP client, and add a durable event stream for local consumers.

## Target architecture

```
                 ┌──────────────────────────────┐
  WhatsApp ◄────►│  whatsapp-bridge (Go)         │  unix:/run/whatsapp/bridge.sock
                 │  - whatsmeow session          │
                 │  - messages.db (sole owner)   │
                 │  - REST read + write API      │
                 │  - GET /api/events  (SSE)     │
                 │  - outbound webhooks (remote) │
                 └───┬──────────┬───────────┬────┘
                     │          │           │
        ┌────────────┘          │           └───────────────┐
        ▼                       ▼                           ▼
 whatsapp-mcp-server     hindsight-forwarder          future clients
 (Python, thin HTTP      (Python, SSE consumer,       (any language,
  client, MCP tools)      calls Hindsight retain)      curl works)
```

Rules:

1. The bridge is the only process that opens `messages.db`.
2. Clients talk HTTP over a Unix socket only. Nothing listens on TCP.
   Anyone who can open the socket has full, unauthenticated access to the
   WhatsApp account, so access is confined by filesystem permissions and
   systemd sandboxing (see Binding and Blast radius below).
3. A shared filesystem is *not* required for
   text; media paths are optional convenience for co-located clients.
4. Two push mechanisms with different guarantees and trust models:
   - **Events stream** (new): local, durable, cursor-based, at-least-once.
     Unauthenticated, because it is only reachable through the socket.
     For anything that must not miss messages.
   - **Webhooks** (existing): outbound POST, lossy by design, rate-capped.
     Authenticated (bearer token per subscription), because the consumer
     is remote. For Claude routines and other remote targets.

## Bridge API additions

### Read endpoints (port SQL from `whatsapp.py` verbatim, then delete it there)

| Endpoint | Replaces |
| --- | --- |
| `GET /api/chats?limit&page&query&sort_by&include_last_message` | `list_chats` |
| `GET /api/chats/unread?limit&page` | `list_unread_chats` |
| `GET /api/chats/{jid}?include_last_message` | `get_chat` |
| `GET /api/chats/by-phone/{number}` | `get_direct_chat_by_contact` |
| `GET /api/contacts?query=` | `search_contacts` |
| `GET /api/contacts/{jid}/chats?limit&page` | `get_contact_chats` |
| `GET /api/contacts/{jid}/last-interaction` | `get_last_interaction` |
| `GET /api/messages?chat_jid&sender&query&after&before&limit&page&context_before&context_after` | `list_messages` |
| `GET /api/messages/{chat_jid}/{id}/context?before&after` | `get_message_context` |

JSON shapes mirror the existing `*_to_dict` outputs so the MCP tool return
types do not change. Tests: `test_unread.py` and `test_serialization.py`
become Go table tests against a temp SQLite file (the fixture logic already
exists in `readstate_test.go`).

### Event stream

`GET /api/events?since=<cursor>` returning `text/event-stream`.

- Backed by a new append-only `events` table (`id INTEGER PRIMARY KEY`,
  `type`, `chat_jid`, `message_id`, `payload` JSON, `created_at`). A row is
  appended at the moment a message becomes *publishable* (see Transcription
  below), not when it is stored. The cursor is the event id: monotonic,
  survives edits and history-sync backfill, and lets the event log be the
  single source for both SSE and webhooks.
- On connect the bridge replays events `> since`, then streams live. Each
  SSE frame carries `id: <event id>` so `Last-Event-ID` reconnect works
  with a plain SSE library. Client persists its cursor; the bridge keeps no
  per-client state.
- Event types: `message.new`, `message.updated` (edits, and on-demand
  transcript backfill), `chat.read` (from `/api/mark-read`),
  `bridge.status` (connected / logged_in transitions). Message payload is
  the existing `WebhookEvent` struct plus `media_type`, `filename`,
  `has_media`, `transcript`, and `transcription_status`.
- Filters as query params: `chat_jid`, `include_from_me`, `types`.
- Implementation: `publish(event)` appends the row, then fans out to SSE
  subscribers and to the webhook dispatcher (which replaces the direct
  `dispatcher.Notify` at main.go:617). Replay-then-subscribe must be
  ordered (subscribe first, then replay, dedupe by id) to avoid a gap.

Why SSE over long-poll or a Unix socket protocol: resumable with one header,
works from `curl`, `httpx`, and Go with no extra dependencies, and the
bridge already speaks HTTP.

### Media

- `GET /api/media/{chat_jid}/{message_id}`: download if needed, then stream
  bytes with the right `Content-Type`. Lets non-co-located clients (or a
  hardened unit without store access) fetch attachments. `/api/download`
  stays for the path-based flow.
- `POST /api/send` gains `voice_note: true`. The bridge runs ffmpeg
  (`libopus`, 32k, 24 kHz, same as `audio.py`) into `store/tmp` and cleans up.
  Optionally accept `multipart/form-data` so a client can upload a file
  rather than write into the store. `audio.py` is then deleted.
### Transcription of incoming voice notes

Local, no network, whisper.cpp (`whisper-cli` from nixpkgs, CPU-only) run
as a subprocess, same pattern as ffmpeg. Model defaults to `base` (the host
is compute-constrained); path and model via env, upgradeable later without
code changes.

Publication is gated on transcription, so consumers see one event per
voice note, never a raw-media event followed by a transcript event. That
matters for webhooks (one Claude routine run, not two) and for the
Hindsight forwarder (one retain, not two).

- Receive path for a PTT/audio message: store row, download media, enqueue
  transcription. Text messages are published immediately as now; only the
  voice note itself waits. Ordering within a chat is therefore by
  publication time, and a text reply can be published before the voice
  note it answers. Consumers already handle this since `timestamp` is the
  WhatsApp send time.
- One worker goroutine, serialised: at most one whisper process at a time.
  Bounded queue; a bridge restart re-enqueues rows with
  `transcription_status = 'pending'` on startup so nothing is lost.
- Per-message timeout, default 120 s. On failure or timeout the message is
  published anyway with `transcript = null` and `transcription_status =
  'failed'` (or `'timeout'`), so a bad audio file cannot block the stream.
- Payload for a voice note: `media_type = "audio"`, `has_media = true`,
  `transcript = "<text>"`, `transcription_status = "ok"`. `content` is
  left as WhatsApp delivered it (usually empty), so consumers can tell
  speech from typed text, and the raw file is fetchable via
  `GET /api/media/{chat_jid}/{message_id}`. The webhook text summary
  renders it as `[voice note, 0:42] <transcript>`.
- Schema: `messages.transcript TEXT`, `messages.transcription_status TEXT`
  (`pending`, `ok`, `failed`, `timeout`, `disabled`). Read endpoints return
  both.
- Backfill on demand via `POST /api/messages/{chat_jid}/{id}/transcribe`,
  which emits `message.updated` when done. This is the only case where a
  transcript arrives as a second event, and it is caller-initiated.
- Transcription off (`WHATSAPP_TRANSCRIBE=0` or no model file) publishes
  voice notes immediately with `transcription_status = 'disabled'`.

### Binding: Unix socket, not TCP

`WHATSAPP_BRIDGE_ADDR=unix:/run/whatsapp/bridge.sock` becomes the default
for the hosted deployment. `http.Server.Serve` on a `net.Listen("unix", ...)`
listener is the whole server-side change; the bridge removes a stale socket
before listening and chmods the new one to `0660`. SSE is just a long-lived
HTTP response, so it works unchanged. The TCP form stays supported for
laptop use with Claude Desktop over stdio.

Clients: `httpx.HTTPTransport(uds=...)` in Python (drop `requests`, which
needs an add-on for this), `--unix-socket` in curl, a custom `DialContext`
in Go. The URL host is a placeholder; `http://whatsapp/api/...` by
convention.

### Blast radius

Opening the socket is the credential. Confinement:

- Socket owned `whatsapp:whatsapp-clients`, mode `0660`, in a
  `RuntimeDirectory` only the bridge can write.
- Each client runs as its own system user with `SupplementaryGroups =
  [ "whatsapp-clients" ]`. Nothing else on the host is in that group.
- Clients get the existing hardening set plus `RestrictAddressFamilies`
  narrowed to what they need: the forwarder needs `AF_UNIX` and whatever
  Hindsight is reached over; a client that only talks to the bridge gets
  `AF_UNIX` alone. The bridge itself keeps `AF_INET`/`AF_INET6` for
  whatsmeow's outbound connection but no longer listens on either.
- Client units get no `ReadWritePaths` into the store. Media reaches them
  through `GET /api/media/...` over the socket, not through the filesystem.
- `store/` stays `0750 whatsapp:whatsapp`, so a compromised client cannot
  read `messages.db` or the session keys directly.

## Client side

### `whatsapp-client` (Python package, in-repo)

Thin `httpx` wrapper (Unix-socket transport by default): one method per endpoint, dataclasses matching the JSON,
an `events(since, on_event)` generator with automatic reconnect and cursor
callback. No SQLite, no ffmpeg. This is the only piece the MCP server and
the forwarder share.

### `whatsapp-mcp-server`

Becomes tool docstrings plus calls into `whatsapp-client`. Drops the
`WHATSAPP_MESSAGES_DB` and `WHATSAPP_STORE_DIR` env vars and the `sqlite3`
usage. `send_file` uploads via multipart instead of naming a store path, so the
MCP unit no longer needs store access either.

### `hindsight-forwarder`

First non-LLM client. Consumes `/api/events`, filters by chat, calls the
Hindsight retain endpoint, persists the cursor to a file in its state dir.
Failure mode is "stop advancing the cursor", so a Hindsight outage means
replay on recovery rather than loss. Configuration: bridge URL, Hindsight
URL/token/bank, chat allowlist.

## Deployment (nix)

Same single container; `nix/whatsapp.nix` gains one systemd unit per client
and a `services.whatsapp.clients.<name>` option set (user, socket group
membership, address families, env). Client units get the hardening above,
`After=whatsapp-bridge.service`, and the socket path. Only the MCP unit is
reachable from outside the container; forwarder units have no listening
port at all. ffmpeg and whisper.cpp are on the bridge unit's `path` only.

## Phasing

1. **Read API in Go** with table tests, plus Unix-socket listening and the
   socket/group plumbing in nix. Additive, no client change.
2. **`whatsapp-client` package + MCP server switch**. Delete SQLite and
   ffmpeg from Python. Existing MCP tool contracts unchanged, so this is
   verifiable against the live server with the same tool calls.
3. **Events stream** (`events` table, `publish()`, SSE handler, webhook
   dispatcher fed from it, client generator).
4. **Voice notes, media streaming, multipart upload in Go**.
5. **Whisper transcription** (columns, serialised worker, publication gate,
   restart recovery, backfill endpoint).
6. **hindsight-forwarder** plus nix unit.

Phases 1 and 2 are the bulk of the work (about 500 lines of SQL to port,
most of it mechanical). Phase 3 is the one new design element and is small
in code. Everything is additive until phase 2 removes Python's SQLite path,
so the MCP server keeps working throughout.

## Open decisions

- Should webhooks eventually be reimplemented as an events-stream consumer
  inside the bridge? Cleaner (one source of truth for "new message"), but
  not needed now. Leave as is.
- Cursor per client stored client-side (proposed) vs server-side named
  consumers. Client-side is simpler and stateless for the bridge; revisit
  only if a client cannot persist a file.
