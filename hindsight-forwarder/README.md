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
- **`context` names the participants**, because Hindsight injects it straight
  into the extraction prompt: *"WhatsApp conversation on Charles's personal
  WhatsApp account between Charles and Gaby. Each line is `[date time]
  Speaker: message`…"*. `FORWARDER_OWNER_NAME` and `FORWARDER_ACCOUNT_LABEL`
  fill in the names; `HINDSIGHT_CONTEXT_EXTRA` adds a sentence of your own.
- **Documents roll over** so no document grows without bound: a chat idle for
  `FORWARDER_SESSION_GAP_DAYS` (7) starts a new one, and so does one that has
  reached `FORWARDER_MAX_MESSAGES` (200). The document id carries the time the
  document opened: `whatsapp:<chat_jid>:20260912T192935Z`.
- Voice notes arrive already transcribed (the bridge publishes them only once
  the transcript is in), so each note is one line of the transcript.

## State, and why it must be durable

`FORWARDER_STATE_DIR` holds one `state.json` carrying the cursor (the last
event id handed to Hindsight) and, per chat, the open document and its message
count. They are written together and atomically: the cursor says what was
sent, the sessions say what it was appended to.

Losing that file is not neutral — with `append`, replaying an event **adds the
line a second time** instead of replacing it, so the directory belongs on the
container's state volume, never on a cattle rootfs. A Hindsight outage is
safe: the state advances only after retain returns, so recovery replays at
most the message in flight.
