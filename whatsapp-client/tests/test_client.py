import io
import json
import unittest

import httpx

from whatsapp_client import BridgeError, BridgeUnavailable, WhatsAppClient, parse_bridge_url
from whatsapp_client.client import _parse_sse


class ParseBridgeURLTests(unittest.TestCase):
    def test_unix(self):
        self.assertEqual(parse_bridge_url("unix:/run/whatsapp/bridge.sock"), ("http://whatsapp", "/run/whatsapp/bridge.sock"))

    def test_http_with_api_suffix(self):
        self.assertEqual(parse_bridge_url("http://127.0.0.1:8080/api"), ("http://127.0.0.1:8080", None))
        self.assertEqual(parse_bridge_url("http://localhost:8080/"), ("http://localhost:8080", None))

    def test_rejects_garbage(self):
        with self.assertRaises(ValueError):
            parse_bridge_url("unix:")
        with self.assertRaises(ValueError):
            parse_bridge_url("localhost:8080")


class Recorder:
    def __init__(self, responder):
        self.requests = []
        self.responder = responder

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.responder(request)


def make_client(responder):
    rec = Recorder(responder)
    return WhatsAppClient("http://bridge", transport=httpx.MockTransport(rec)), rec


class RequestShapeTests(unittest.TestCase):
    def test_list_chats_params(self):
        client, rec = make_client(lambda r: httpx.Response(200, json=[]))
        client.list_chats(query="bob", limit=5, unread_only=True, include_last_message=False)
        req = rec.requests[0]
        self.assertEqual(req.url.path, "/api/chats")
        self.assertEqual(dict(req.url.params), {
            "query": "bob", "limit": "5", "page": "0", "include_last_message": "false",
            "sort_by": "last_active", "unread_only": "true"})

    def test_get_chat_404_is_none(self):
        client, _ = make_client(lambda r: httpx.Response(404, json={"error": "nope"}))
        self.assertIsNone(client.get_chat("x@s.whatsapp.net"))

    def test_other_errors_raise_with_message(self):
        client, _ = make_client(lambda r: httpx.Response(503, json={"error": "not connected"}))
        with self.assertRaises(BridgeError) as cm:
            client.send_message("1", "hi")
        self.assertEqual(cm.exception.status, 503)
        self.assertIn("not connected", str(cm.exception))

    def test_transport_error_is_unavailable(self):
        def boom(r):
            raise httpx.ConnectError("refused")
        client, _ = make_client(boom)
        with self.assertRaises(BridgeUnavailable):
            client.status()

    def test_send_message_body(self):
        client, rec = make_client(lambda r: httpx.Response(200, json={"success": True, "message": "ok"}))
        out = client.send_message("447700900000", "hello")
        self.assertEqual(out["success"], True)
        self.assertEqual(json.loads(rec.requests[0].content), {"recipient": "447700900000", "message": "hello"})

    def test_send_file_uploads_data_as_multipart(self):
        client, rec = make_client(lambda r: httpx.Response(200, json={"success": True, "message": "ok"}))
        client.send_file("1", data=io.BytesIO(b"abc"), filename="note.txt", caption="cap", voice_note=True)
        req = rec.requests[0]
        self.assertTrue(req.headers["content-type"].startswith("multipart/form-data"))
        body = req.content
        self.assertIn(b'name="file"; filename="note.txt"', body)
        self.assertIn(b"abc", body)
        self.assertIn(b'name="voice_note"\r\n\r\ntrue', body)
        self.assertIn(b'name="message"\r\n\r\ncap', body)

    def test_send_file_unreadable_path_passes_through(self):
        client, rec = make_client(lambda r: httpx.Response(200, json={"success": True, "message": "ok"}))
        client.send_file("1", path="/data/store/x/does-not-exist.jpg")
        self.assertEqual(json.loads(rec.requests[0].content), {
            "recipient": "1", "message": "", "media_path": "/data/store/x/does-not-exist.jpg", "voice_note": False})

    def test_webhook_crud_status_codes(self):
        client, rec = make_client(lambda r: httpx.Response(201 if r.method == "POST" else 204, json={"id": 1}))
        self.assertEqual(client.create_webhook(chat_jid="*", url="https://x")["id"], 1)
        self.assertIsNone(client.delete_webhook(1))
        self.assertEqual([r.method for r in rec.requests], ["POST", "DELETE"])

    def test_get_media_returns_bytes_and_type(self):
        client, _ = make_client(lambda r: httpx.Response(200, content=b"OggS", headers={"content-type": "audio/ogg"}))
        data, ctype = client.get_media("c@s.whatsapp.net", "m1")
        self.assertEqual((data, ctype), (b"OggS", "audio/ogg"))


class SSEParserTests(unittest.TestCase):
    def test_parses_frames(self):
        lines = iter([
            ": comment", "id: 7", "event: message.new", 'data: {"a": 1}', "",
            "id: 8", "event: chat.read", 'data: {"b":', 'data: 2}', "",
            "id: x", 'data: {}', "",
        ])
        evs = list(_parse_sse(lines))
        self.assertEqual([(e.id, e.type, e.data) for e in evs], [
            (7, "message.new", {"a": 1}),
            (8, "chat.read", {"b": 2}),
        ])


class EventsStreamTests(unittest.TestCase):
    def test_events_yields_and_resumes_cursor(self):
        calls = []

        def responder(r):
            calls.append(dict(r.url.params))
            if len(calls) == 1:
                body = 'id: 5\nevent: message.new\ndata: {"x":1}\n\nid: 6\nevent: message.new\ndata: {"x":2}\n\n'
                return httpx.Response(200, content=body.encode(), headers={"content-type": "text/event-stream"})
            raise httpx.ConnectError("gone")

        client, _ = make_client(responder)
        seen = []
        gen = client.events(since=4, reconnect_delay=0, on_cursor=seen.append)
        first = next(gen)
        second = next(gen)
        self.assertEqual((first.id, second.id), (5, 6))
        self.assertEqual(seen, [5])  # on_cursor fires after the consumer has taken the event
        self.assertEqual(calls[0]["since"], "4")
        gen.close()


if __name__ == "__main__":
    unittest.main()
