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

`send_message` / `send_file` can block for several minutes: the bridge
rate-limits outbound sends and queues anything that arrives early, so a call
returns when the message actually goes out. Their timeout is set above the
bridge's queue cap deliberately — hanging up early drops the queued message
rather than sending it.
