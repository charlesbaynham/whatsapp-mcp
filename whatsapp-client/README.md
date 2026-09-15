# whatsapp-client

Thin `httpx` wrapper over the `whatsapp-bridge` REST API. Shared by the MCP
server and every other client in this repo; nothing here opens the message
database or shells out to ffmpeg, the bridge does all of that.

```python
from whatsapp_client import WhatsAppClient

wa = WhatsAppClient()  # WHATSAPP_BRIDGE_URL, default unix:/run/whatsapp/bridge.sock
wa.status()
wa.list_chats(limit=5)
wa.send_message("447700900000", "hello")
for ev in wa.events(since=0):
    print(ev.id, ev.type, ev.data)
```

`WHATSAPP_BRIDGE_URL` is either `unix:/path/to/bridge.sock` or an
`http://host:port` URL (a trailing `/api` is tolerated).

`send_message` / `send_file` / `send_poll` return as soon as the bridge has **queued** the
message — sends are rate limited and go out a randomised delay later. The
returned `id` reads back through `send_status(id)` (`queued`, `sent`, `failed`).
Pass `block=True` to wait for the send itself instead; that can take minutes,
and giving up on the wait loses only the outcome, not the message.

A **first contact** — someone the bridge has never had a chat with — is held
much longer, in a separate queue spaced about 30 minutes apart on average; the
reply then has `new_contact: True` and an `estimated_wait_seconds`. A
`block=True` send to such a recipient is refused (`BridgeError`, status 422)
rather than held for hours; resubmit it without `block`.

`send_poll(recipient, question, options, selectable_count=1)` sends a poll;
`get_poll(chat_jid, message_id)` reads its outcome back (tally per option and
each voter's current selection), and `list_polls()` finds the polls. Votes
arrive on the event stream as `poll.vote` events.
