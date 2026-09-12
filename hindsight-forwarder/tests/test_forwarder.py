import json
import tempfile
import unittest
from pathlib import Path

import httpx

from hindsight_forwarder.main import (Config, Hindsight, Session, State, describe, retain_item,
                                      retain_with_retry, run, session_for, transcript_line, wants)
from whatsapp_client import Event


def ev(id, chat="c@s.whatsapp.net", type="message.new", **payload):
    payload.setdefault("chat_jid", chat)
    payload.setdefault("message_id", f"m{id}")
    payload.setdefault("timestamp", "2026-09-12T10:00:00Z")
    return Event(id=id, type=type, data={"chat_jid": chat, "data": payload})


def forwarding_config(**kw):
    kw.setdefault("hindsight_url", "https://h")
    return Config(**kw)


class ConfigTests(unittest.TestCase):
    def test_from_env(self):
        cfg = Config.from_env({"HINDSIGHT_URL": "https://h/", "HINDSIGHT_BANK": "b",
                               "FORWARDER_CHATS": "a@s.whatsapp.net, g@g.us", "FORWARDER_INCLUDE_FROM_ME": "false",
                               "FORWARDER_SESSION_GAP_DAYS": "3", "FORWARDER_MAX_MESSAGES": "50",
                               "FORWARDER_OWNER_NAME": "Charles", "STATE_DIRECTORY": "/var/lib/x"})
        self.assertEqual(cfg.retain_url, "https://h/v1/default/banks/b/memories")
        self.assertEqual(cfg.chats, {"a@s.whatsapp.net", "g@g.us"})
        self.assertFalse(cfg.include_from_me)
        self.assertEqual(cfg.session_gap.days, 3)
        self.assertEqual(cfg.max_messages, 50)
        self.assertEqual(cfg.owner_name, "Charles")
        self.assertEqual(cfg.state_dir, Path("/var/lib/x"))


class FilterTests(unittest.TestCase):
    def test_wants_respects_allowlist_type_and_from_me(self):
        cfg = forwarding_config(chats={"c@s.whatsapp.net"}, include_from_me=False)
        self.assertTrue(wants(cfg, ev(1)))
        self.assertFalse(wants(cfg, ev(2, chat="x@s.whatsapp.net")))
        self.assertFalse(wants(cfg, ev(3, type="chat.read")))
        self.assertFalse(wants(cfg, ev(4, is_from_me=True)))
        self.assertTrue(wants(forwarding_config(), ev(5, chat="anything@g.us")))


class TranscriptTests(unittest.TestCase):
    def test_line_is_dated_and_attributed(self):
        cfg = forwarding_config(owner_name="Charles")
        self.assertEqual(transcript_line(cfg, {"sender_name": "Gaby", "content": "hi", "timestamp": "2026-09-12T20:29:35+01:00"}),
                         "[2026-09-12 20:29] Gaby: hi")
        self.assertEqual(transcript_line(cfg, {"is_from_me": True, "content": "on my way", "timestamp": "2026-09-12T20:30:00Z"}),
                         "[2026-09-12 20:30] Charles: on my way")

    def test_line_renders_voice_and_media(self):
        cfg = forwarding_config()
        self.assertIn("(voice note) later", transcript_line(cfg, {"media_type": "audio", "transcript": "later"}))
        self.assertIn("[image: a.jpg] look", transcript_line(cfg, {"media_type": "image", "filename": "a.jpg", "content": "look"}))
        self.assertIn("transcription failed", transcript_line(cfg, {"media_type": "audio", "transcription_status": "failed"}))

    def test_sender_falls_back_to_number(self):
        self.assertIn("447700900123:", transcript_line(forwarding_config(), {"sender": "447700900123", "content": "x"}))


class ContextTests(unittest.TestCase):
    def test_direct_chat_names_both_parties(self):
        cfg = forwarding_config(owner_name="Charles", account_label="Charles's personal WhatsApp account")
        ctx = describe(cfg, {"chat_jid": "447984441981@s.whatsapp.net", "chat_name": "Gaby"})
        self.assertIn("between Charles and Gaby", ctx)
        self.assertIn("personal WhatsApp account", ctx)
        self.assertIn('"Charles" is the owner', ctx)

    def test_group_chat_is_described_as_a_group(self):
        ctx = describe(forwarding_config(), {"chat_jid": "123@g.us", "chat_name": "Climbing trip"})
        self.assertIn('group chat "Climbing trip"', ctx)

    def test_extra_is_appended(self):
        ctx = describe(forwarding_config(context_extra="Household logistics."), {"chat_jid": "a@s.whatsapp.net"})
        self.assertTrue(ctx.endswith("Household logistics."))


class SessionTests(unittest.TestCase):
    def test_first_message_starts_a_document_named_for_its_time(self):
        state = State(Path(tempfile.mkdtemp()) / "state.json")
        session, started = session_for(forwarding_config(), state, ev(1, timestamp="2026-09-12T19:29:35Z"))
        self.assertTrue(started)
        self.assertEqual(session.document_id, "whatsapp:c@s.whatsapp.net:20260912T192935Z")

    def test_recent_message_appends_to_the_same_document(self):
        cfg, state = forwarding_config(), State(Path(tempfile.mkdtemp()) / "state.json")
        first, _ = session_for(cfg, state, ev(1, timestamp="2026-09-12T10:00:00Z"))
        first.last_timestamp, first.messages = "2026-09-12T10:00:00Z", 1
        state.sessions["c@s.whatsapp.net"] = first
        session, started = session_for(cfg, state, ev(2, timestamp="2026-09-13T10:00:00Z"))
        self.assertFalse(started)
        self.assertEqual(session.document_id, first.document_id)

    def test_a_weeks_silence_starts_a_new_document(self):
        cfg, state = forwarding_config(), State(Path(tempfile.mkdtemp()) / "state.json")
        first, _ = session_for(cfg, state, ev(1, timestamp="2026-09-01T10:00:00Z"))
        first.last_timestamp, first.messages = "2026-09-01T10:00:00Z", 3
        state.sessions["c@s.whatsapp.net"] = first
        session, started = session_for(cfg, state, ev(2, timestamp="2026-09-08T10:00:01Z"))
        self.assertTrue(started)
        self.assertNotEqual(session.document_id, first.document_id)

    def test_a_full_document_rolls_over(self):
        cfg = forwarding_config(max_messages=2)
        state = State(Path(tempfile.mkdtemp()) / "state.json")
        first, _ = session_for(cfg, state, ev(1))
        first.last_timestamp, first.messages = "2026-09-12T10:00:00Z", 2
        state.sessions["c@s.whatsapp.net"] = first
        session, started = session_for(cfg, state, ev(2, timestamp="2026-09-12T10:01:00Z"))
        self.assertTrue(started)
        self.assertNotEqual(session.document_id, first.document_id)


class RetainItemTests(unittest.TestCase):
    def test_first_item_replaces_and_later_ones_append(self):
        cfg = forwarding_config()
        state = State(Path(tempfile.mkdtemp()) / "state.json")
        session, started = session_for(cfg, state, ev(9, sender_name="Bob", chat_name="Bob", content="x"))
        item = retain_item(cfg, ev(9, sender_name="Bob", chat_name="Bob", content="x"), session, started)
        self.assertNotIn("update_mode", item)
        self.assertEqual(item["document_id"], session.document_id)
        self.assertEqual(item["timestamp"], "2026-09-12T10:00:00Z")
        self.assertEqual(item["metadata"]["event_id"], "9")
        self.assertEqual(item["tags"], ["source:whatsapp", "chat:c@s.whatsapp.net"])
        self.assertEqual(retain_item(cfg, ev(10), session, False)["update_mode"], "append")


class StateTests(unittest.TestCase):
    def test_roundtrip_and_corruption(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "sub" / "state.json"
            state = State(path)
            self.assertEqual(state.cursor, 0)
            state.cursor = 42
            state.sessions["c@s.whatsapp.net"] = session = Session("doc", "2026-09-12T10:00:00Z", 3)
            state.save()
            reloaded = State(path)
            self.assertEqual(reloaded.cursor, 42)
            self.assertEqual(reloaded.sessions["c@s.whatsapp.net"].document_id, session.document_id)
            self.assertEqual(reloaded.sessions["c@s.whatsapp.net"].messages, 3)
            path.write_text("junk")
            self.assertEqual(State(path).cursor, 0)


class RetainTests(unittest.TestCase):
    def test_posts_items_with_bearer(self):
        seen = []

        def handler(r):
            seen.append(r)
            return httpx.Response(200, json={"ok": True})

        cfg = forwarding_config(api_key="k", bank="b")
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

        hs = Hindsight(forwarding_config(), transport=httpx.MockTransport(handler))
        slept = []
        retain_with_retry(hs, [{"content": "x"}], sleep=slept.append)
        self.assertEqual(len(attempts), 3)
        self.assertEqual(slept, [2.0, 4.0])


class RunTests(unittest.TestCase):
    def test_chat_becomes_one_appended_document_and_state_follows(self):
        posted = []

        def handler(r):
            posted.append(json.loads(r.content)["items"][0])
            return httpx.Response(200)

        cfg = forwarding_config(chats={"c@s.whatsapp.net"}, owner_name="Charles")
        hs = Hindsight(cfg, transport=httpx.MockTransport(handler))
        with tempfile.TemporaryDirectory() as d:
            state = State(Path(d) / "state.json")
            events = [ev(1, sender_name="Gaby", chat_name="Gaby", content="a"),
                      ev(2, chat="other@s.whatsapp.net", content="skip"),
                      ev(3, sender_name="Gaby", chat_name="Gaby", content="b", timestamp="2026-09-12T10:05:00Z")]
            run(cfg, wa=None, hs=hs, state=state, events=iter(events))
            self.assertEqual(state.cursor, 3)
            self.assertEqual(State(Path(d) / "state.json").sessions["c@s.whatsapp.net"].messages, 2)
        self.assertEqual([p["content"] for p in posted], ["[2026-09-12 10:00] Gaby: a", "[2026-09-12 10:05] Gaby: b"])
        self.assertEqual([p.get("update_mode") for p in posted], [None, "append"])
        self.assertEqual(posted[0]["document_id"], posted[1]["document_id"])


if __name__ == "__main__":
    unittest.main()
