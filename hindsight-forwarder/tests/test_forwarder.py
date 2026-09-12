import json
import tempfile
import unittest
from pathlib import Path

import httpx

from hindsight_forwarder.main import Config, Cursor, Hindsight, render, retain_item, retain_with_retry, run, wants
from whatsapp_client import Event


def ev(id, chat="c@s.whatsapp.net", type="message.new", **payload):
    payload.setdefault("chat_jid", chat)
    payload.setdefault("message_id", f"m{id}")
    return Event(id=id, type=type, data={"chat_jid": chat, "data": payload})


class ConfigTests(unittest.TestCase):
    def test_from_env(self):
        cfg = Config.from_env({"HINDSIGHT_URL": "https://h/", "HINDSIGHT_BANK": "b", "FORWARDER_CHATS": "a@s.whatsapp.net, g@g.us",
                               "FORWARDER_INCLUDE_FROM_ME": "false", "STATE_DIRECTORY": "/var/lib/x"})
        self.assertEqual(cfg.retain_url, "https://h/v1/default/banks/b/memories")
        self.assertEqual(cfg.chats, {"a@s.whatsapp.net", "g@g.us"})
        self.assertFalse(cfg.include_from_me)
        self.assertEqual(cfg.state_dir, Path("/var/lib/x"))


class FilterAndRenderTests(unittest.TestCase):
    def test_wants_respects_allowlist_type_and_from_me(self):
        cfg = Config(hindsight_url="h", chats={"c@s.whatsapp.net"}, include_from_me=False)
        self.assertTrue(wants(cfg, ev(1)))
        self.assertFalse(wants(cfg, ev(2, chat="x@s.whatsapp.net")))
        self.assertFalse(wants(cfg, ev(3, type="chat.read")))
        self.assertFalse(wants(cfg, ev(4, is_from_me=True)))
        self.assertTrue(wants(Config(hindsight_url="h"), ev(5, chat="anything@g.us")))

    def test_render_text_voice_and_media(self):
        self.assertEqual(render({"sender_name": "Carol", "chat_name": "Carol", "content": "hi"}),
                         "WhatsApp message from Carol in Carol: hi")
        self.assertEqual(render({"is_from_me": True, "chat_name": "Lab", "media_type": "audio", "transcript": "on my way"}),
                         "WhatsApp message from Me in Lab: [voice note] on my way")
        self.assertEqual(render({"sender": "1", "chat_jid": "g@g.us", "media_type": "image", "filename": "a.jpg", "content": "look"}),
                         "WhatsApp message from 1 in g@g.us: [image: a.jpg] look")
        self.assertIn("transcription failed", render({"sender": "1", "media_type": "audio", "transcription_status": "failed"}))

    def test_retain_item_shape(self):
        cfg = Config(hindsight_url="h", context="wa")
        item = retain_item(cfg, ev(9, sender_name="Bob", chat_name="Bob", content="x", timestamp="2026-09-12T10:00:00Z"))
        self.assertEqual(item["document_id"], "whatsapp:c@s.whatsapp.net:m9")
        self.assertEqual(item["timestamp"], "2026-09-12T10:00:00Z")
        self.assertEqual(item["context"], "wa")
        self.assertEqual(item["metadata"]["event_id"], "9")
        self.assertEqual(item["tags"], ["chat:c@s.whatsapp.net"])


class CursorTests(unittest.TestCase):
    def test_roundtrip_and_missing(self):
        with tempfile.TemporaryDirectory() as d:
            c = Cursor(Path(d) / "sub" / "cursor")
            self.assertEqual(c.load(), 0)
            c.save(42)
            self.assertEqual(c.load(), 42)
            (Path(d) / "sub" / "cursor").write_text("junk")
            self.assertEqual(c.load(), 0)


class RetainTests(unittest.TestCase):
    def test_posts_items_with_bearer(self):
        seen = []

        def handler(r):
            seen.append(r)
            return httpx.Response(200, json={"ok": True})

        cfg = Config(hindsight_url="https://h", api_key="k", bank="b")
        hs = Hindsight(cfg, transport=httpx.MockTransport(handler))
        hs.retain([{"content": "x"}])
        self.assertEqual(seen[0].url, "https://h/v1/default/banks/b/memories")
        self.assertEqual(seen[0].headers["authorization"], "Bearer k")
        self.assertEqual(json.loads(seen[0].content), {"items": [{"content": "x"}]})

    def test_retry_until_success_with_backoff(self):
        attempts = []

        def handler(r):
            attempts.append(1)
            return httpx.Response(500 if len(attempts) < 3 else 200)

        hs = Hindsight(Config(hindsight_url="https://h"), transport=httpx.MockTransport(handler))
        slept = []
        retain_with_retry(hs, [{"content": "x"}], sleep=slept.append)
        self.assertEqual(len(attempts), 3)
        self.assertEqual(slept, [2.0, 4.0])


class RunTests(unittest.TestCase):
    def test_cursor_advances_only_after_retain(self):
        posted = []

        def handler(r):
            posted.append(json.loads(r.content))
            return httpx.Response(200)

        cfg = Config(hindsight_url="https://h", chats={"c@s.whatsapp.net"})
        hs = Hindsight(cfg, transport=httpx.MockTransport(handler))
        with tempfile.TemporaryDirectory() as d:
            cursor = Cursor(Path(d) / "cursor")
            events = [ev(1, content="a"), ev(2, chat="other@s.whatsapp.net", content="skip"), ev(3, content="b")]
            run(cfg, wa=None, hs=hs, cursor=cursor, events=iter(events), sleep=lambda s: None)
            self.assertEqual(cursor.load(), 3)
        self.assertEqual([p["items"][0]["content"] for p in posted],
                         ["WhatsApp message from unknown in c@s.whatsapp.net: a",
                          "WhatsApp message from unknown in c@s.whatsapp.net: b"])


if __name__ == "__main__":
    unittest.main()
