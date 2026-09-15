# whatsapp-bridge REST API

All endpoints are under `/api`, JSON in and out, served on the address in
`WHATSAPP_BRIDGE_ADDR` (a Unix socket in the hosted deployment: whoever can
open it has full access, so there is no authentication). Errors are
`{"error": "..."}` with a 4xx/5xx status. Endpoints marked *ready* return
`503` until the bridge is connected and logged in.

## Status

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/status` | `{connected, logged_in, jid, nct_salt, reachout_timelock, send_queue}`; always 200 |
| GET | `/reachout-timelock` | *ready*. Asks WhatsApp directly for the account's reach-out time-lock state (see below) instead of waiting for a passive report, and updates it as a side effect. `502` if the query itself fails. |

`reachout_timelock`: `{active, enforcement_type, ends, checked_at}` —
WhatsApp's server-side rate limit on first-contact sends (error 463 on the
send that trips it). `ends` and `checked_at` are RFC 3339 or `null`. Tracked
from three sources: an unsolicited WhatsApp notification, a `/reachout-timelock`
query, and a 463 seen on a send — whichever last reported. A first-contact
`/send` is refused outright while it is active; an established chat is never
affected.

`send_queue`: `{pending, new_contacts_held}` — the main send queue's backlog
(the in-flight send included) and how many first-contact sends are still held
in the new-contact queue in front of it.

## Reading

| Method | Path | Query | Returns |
| --- | --- | --- | --- |
| GET | `/chats` | `query, limit, page, include_last_message, sort_by=last_active\|name, unread_only` | `[Chat]` |
| GET | `/chats/unread` | `limit, page` | `[Chat]` with unread incoming messages |
| GET | `/chats/{jid}` | `include_last_message` | `Chat` or 404 |
| GET | `/chats/by-phone/{number}` | | the 1:1 `Chat` whose JID contains the number, or 404 |
| GET | `/contacts` | `query` | `[Contact]` (non-group chats matching name or JID) |
| GET | `/contacts/{jid}/chats` | `limit, page` | `[Chat]` the contact has sent to, plus their 1:1 chat |
| GET | `/contacts/{jid}/last-interaction` | | newest `Message` involving the contact, or 404 |
| GET | `/messages` | `chat_jid, sender, query, after, before, limit, page, include_context, context_before, context_after` | `[Message]`, newest first; with `include_context` each hit is expanded to before/hit/after |
| GET | `/messages/{id}/context` | `before, after, chat_jid` | `{message, before, after}` (before/after in chronological order) |
| GET | `/polls` | `chat_jid, limit, page` | `[Message]` with `media_type: poll`, newest first, each with its `poll` outcome |
| GET | `/polls/{chat_jid}/{message_id}` | | `PollResults` or 404 |

`Message`: `id, chat_jid, chat_name, sender, sender_name, content, timestamp
(RFC 3339), is_from_me, media_type?, filename?, transcript?,
transcription_status?, poll?`. `content` is what WhatsApp delivered (empty for a
voice note); `transcript` is the spoken text; `transcription_status` is
`ok`, `pending`, `failed`, `timeout` or `disabled`.

`Poll` (the `poll` field of a `media_type: poll` message, whose `content` is
the question): `question, options, selectable_count (1 = single choice, 0 =
any number), results, total_voters`. `results` is one `{option, votes,
voters}` per option in poll order, `voters` being names; it is the tally
*now* — a voter can change or withdraw their vote.

`PollResults`: the `Poll` fields plus `message_id, chat_jid, chat_name,
sender, sender_name, is_from_me, timestamp` and `votes`, one `{voter,
voter_name, selected, timestamp}` per person who has voted (a withdrawn
vote is an empty `selected`).

`Chat`: `jid, name, last_message_time, last_message, last_sender,
last_is_from_me, unread_count, last_read_at, is_group`.

`Contact`: `phone_number, name, jid`.

## Sending and media (*ready*)

| Method | Path | Body | Notes |
| --- | --- | --- | --- |
| POST | `/send` | JSON `{recipient, message, media_path?, voice_note?, poll?, block?}` or `multipart/form-data` with the same fields (minus `poll`) plus a `file` part | `media_path` must lie inside the store; an upload needs no store access. `voice_note: true` transcodes to Ogg Opus with ffmpeg and sends a playable voice message. `poll: {question, options, selectable_count?}` sends a poll instead of text or media (2–12 distinct options; `selectable_count` 1 = single choice, the default, 0 = any number); it is stored as a `media_type: poll` message and its votes are collected as they arrive. **Asynchronous by default**: answers `202` with `{success, queued: true, id, ahead, estimated_wait_seconds}` once the message is on the rate-limited send queue. A **first contact** (a person the bridge has no chat with) is held first in a separate new-contact queue spaced ~30 min apart (see the README): the `202` then has `new_contact: true`, `ahead` counts the new contacts in front of it and `estimated_wait_seconds` is hours rather than seconds. `block: true` waits for it to leave and answers `200`/`500` with the real outcome — minutes, potentially; for a poll the outcome's message names the poll's message id. A blocking send to a **first contact** is refused with `422` (`{success: false, new_contact: true, estimated_wait_seconds, message}`) and nothing is queued: it would block for hours, so resubmit without `block`. `503` when the queue is full. |
| GET | `/send/{id}` | | ⚠️ Queue and results are in memory: a restart fails everything still waiting and forgets every id. How a submission went: `{id, state, success, message, new_contact, queued_at, sent_at}`, `state` one of `queued`, `sent`, `failed`; `new_contact: true` marks a first contact, which stays `queued` for as long as the new-contact queue holds it. Only recent sends are kept; `404` once one ages out. |
| POST | `/download` | `{message_id, chat_jid}` | Downloads into the store; returns `{success, message, filename, path}` |
| GET | `/media/{chat_jid}/{message_id}` | | Streams the attachment's bytes (downloading first if needed) |
| POST | `/mark-read` | `{chat_jid, send_receipt}` | The only way read state changes. `send_receipt` sends real blue ticks. Emits `chat.read`. |
| POST | `/resync` | `{chat_jid, oldest_message_id, oldest_message_timestamp, oldest_message_from_me?, count?}` | Asks WhatsApp for older history before a known message |
| POST | `/messages/{chat_jid}/{id}/transcribe` | | Queues an on-demand transcription of a voice note; a `message.updated` event follows. 503 if transcription is disabled. |

## Events (push)

`GET /api/events` is a `text/event-stream`. It replays every event with id
greater than `?since=` (or the `Last-Event-ID` header), then streams live,
with a `: ping` comment every 15 s. Each frame is

```
id: 123
event: message.new
data: {"id":123,"type":"message.new","chat_jid":"...","message_id":"...","is_from_me":false,"created_at":"...","data":{...}}
```

Persist the last `id` you have fully handled and pass it back as `since` on
reconnect; delivery is at-least-once and the bridge keeps no per-client
state. A consumer that falls too far behind the live feed is disconnected
so that it reconnects and replays, rather than silently skipping events.

Filters: `chat_jid` (events with no chat, like `bridge.status`, always
pass), `types` (comma-separated), `include_from_me` (default false).

| Event | Data | When |
| --- | --- | --- |
| `message.new` | message (`message_id, chat_jid, chat_name, sender, sender_name, content, timestamp, is_from_me, media_type?, filename?, has_media, transcript?, transcription_status?, duration_seconds?, poll?`) | A message became publishable. A voice note is published once, after transcription (or after the transcription timeout), never as media first and text later. A poll carries its `poll` (question, options, empty tally). |
| `message.updated` | same | An on-demand transcription finished |
| `poll.vote` | same envelope, `sender` being the voter, plus `poll_vote: {poll_id, question, selected, results, total_voters}` | Someone voted on (or withdrew their vote from) a poll the bridge knows. `results` is the whole poll's tally after this vote. Fires webhooks like a message; not stored as a message. |
| `chat.read` | `{chat_jid, up_to, source: api\|receipt\|app_state}` | Read marker advanced |
| `bridge.status` | `{connected, logged_in, jid?}` | Connection state changed |

## Webhooks (outbound push to remote consumers)

Unchanged from before and fed from the same event log: `GET/POST
/webhooks`, `DELETE /webhooks/{id}`, `POST /webhooks/{id}/test`,
`POST /webhooks/{id}/enable`. Deliveries carry a bearer token per
subscription because the consumer is remote; they are single-attempt,
rate-capped and auto-disabling, which suits firing a Claude routine but not
a consumer that must see every message. Use the event stream for that.
