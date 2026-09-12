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

- Cursor is the SQLite `rowid` of `messages` (monotonic insertion order,
  which is exactly "what arrived since I last looked", including history
  sync backfill). Note `INSERT OR REPLACE` in `StoreMessage` reassigns
  rowid on edits; switch it to an upsert (`ON CONFLICT DO UPDATE`) so a
  re-stored message keeps its position, and emit `message.updated` instead.
- On connect the bridge replays rows `> since` from the DB, then streams
  live. Each SSE frame carries `id: <rowid>` so `Last-Event-ID` reconnect
  works with a plain SSE library. Client persists its cursor; the bridge
  keeps no per-client state.
- Event types: `message.new`, `message.updated`, `chat.read` (from
  `/api/mark-read`), `bridge.status` (connected / logged_in transitions).
  Payload for messages is the existing `WebhookEvent` struct plus `rowid`,
  `media_type`, `filename`, and `has_media`.
- Filters as query params: `chat_jid`, `include_from_me`, `types`.
- Implementation: one fan-out hub in Go fed from the same call site as
  `dispatcher.Notify` (main.go:617). Replay-then-subscribe must be ordered
  (subscribe first, then replay, dedupe by rowid) to avoid a gap.

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
- **Voice-note transcription**, local, no network. When an incoming
  message is a PTT/audio, the bridge downloads it and runs Whisper on it,
  storing the result in a new `messages.transcript` column. Runner:
  `whisper.cpp` (`whisper-cli`, packaged in nixpkgs, CPU-only, ~1 s per
  10 s of audio with the `base` model) invoked as a subprocess, same
  pattern as ffmpeg. Model file path and enable flag via env. Transcripts
  surface everywhere content does: `content` in the read API and events is
  left as-is, and a separate `transcript` field is added, so consumers can
  tell speech from typed text. A `message.updated` event fires once the
  transcript lands (it is async, seconds after `message.new`). Failed or
  disabled transcription leaves the field null. Backfill is on demand via
  `POST /api/messages/{chat_jid}/{id}/transcribe`.

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
3. **Events stream** (upsert fix, hub, SSE handler, client generator).
4. **Voice notes, media streaming, multipart upload in Go**.
5. **Whisper transcription** (column, subprocess runner, `message.updated`).
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
