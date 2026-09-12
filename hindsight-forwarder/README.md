# hindsight-forwarder

Consumes `whatsapp-bridge`'s event stream and retains WhatsApp messages into a
Hindsight memory bank. Purely mechanical: no LLM, no polling. Configuration is
by environment variable; see the module docstring in
`src/hindsight_forwarder/main.py`. In the nix deployment the secret-bearing
ones come from `<state>/secrets/hindsight.env`.

## A chat is a document, not a pile of messages

Hindsight extracts facts through an LLM, so what it is given matters more than
that it is given everything.

- **Each message is appended to the chat's current document** (`update_mode:
  "append"`, which reprocesses only the new chunk) rather than retained as a
  standalone memory. "Yes, book it" means nothing on its own and everything
  after the message before it.
- **Every memory is tagged with where it came from**, at three widths:
  `source:whatsapp`, `account:<slug>` (`FORWARDER_ACCOUNT`, so two bridges
  sharing a bank stay distinguishable) and `chat:<jid>`, plus anything in
  `FORWARDER_TAGS`. Tags are what recall filters on; metadata is carried
  alongside but does not filter. Recall's default `tags_match` is `any`,
  which also returns *untagged* memories — pass `any_strict` to get WhatsApp
  and nothing else.
- **`context` names the participants**, because Hindsight injects it straight
  into the extraction prompt: *"WhatsApp conversation on Charles's personal
  WhatsApp account between Charles and Gaby. Each line is `[date time]
  Speaker: message`…"*. `FORWARDER_OWNER_NAME` and `FORWARDER_ACCOUNT_LABEL`
  fill in the names; `HINDSIGHT_CONTEXT_EXTRA` adds a sentence of your own.
- **A document is never closed.** Append costs the new chunk, not the
  document, so a chat is one document for as long as it lasts. The id carries
  the time it was opened plus a random suffix:
  `whatsapp:<chat_jid>:20260912T192935Z-4f1a9c`.
- Voice notes arrive already transcribed (the bridge publishes them only once
  the transcript is in), so each note is one line of the transcript.

## State, and why it must be durable

`FORWARDER_STATE_DIR` holds one `state.json` carrying the cursor (the last
event id handed to Hindsight) and, per chat, the open document and its message
count. They are written together and atomically: the cursor says what was
sent, the sessions say what it was appended to.

Losing that file is not neutral — with `append`, replaying an event **adds the
line a second time** instead of replacing it, so the directory belongs on the
container's state volume, never on a cattle rootfs.

Losing it anyway is survivable by construction: every chat simply opens a new
document and carries on, and the old one keeps the history it already holds.
That is what the random suffix in the id buys. Derive the id from the chat and
a message instead and a wiped state, replaying the same events, rebuilds the
*previous* id — and a first write replaces, so it would delete a document the
event log can no longer refill. The cost of a wipe is therefore a seam in the
bank (and whatever the event log replays appearing in both documents), never a
deletion.

A Hindsight outage is safe: the state advances only after retain returns, so
recovery replays at most the message in flight.
