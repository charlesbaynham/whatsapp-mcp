import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import whatsapp


def _make_db(path):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE chats (
            jid TEXT PRIMARY KEY,
            name TEXT,
            last_message_time TIMESTAMP,
            last_read_timestamp TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE messages (
            id TEXT,
            chat_jid TEXT,
            sender TEXT,
            content TEXT,
            timestamp TIMESTAMP,
            is_from_me INTEGER,
            media_type TEXT
        )
    """)
    conn.commit()
    conn.close()


def _insert_chat(path, jid, name, last_message_time, last_read_timestamp):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO chats (jid, name, last_message_time, last_read_timestamp) VALUES (?, ?, ?, ?)",
        (jid, name, last_message_time, last_read_timestamp),
    )
    conn.commit()
    conn.close()


def _insert_message(path, msg_id, chat_jid, sender, content, timestamp, is_from_me):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type) "
        "VALUES (?, ?, ?, ?, ?, ?, NULL)",
        (msg_id, chat_jid, sender, content, timestamp, 1 if is_from_me else 0),
    )
    conn.commit()
    conn.close()


class UnreadCountTests(unittest.TestCase):
    """chat A: marker NULL, 2 incoming + 1 outgoing -> unread_count 2
    chat B: marker after all messages -> unread_count 0
    chat C: marker between two incoming messages, one stored with a non-UTC
    offset that a naive raw-string comparison would get wrong -> unread_count 1
    """

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "messages.db")
        _make_db(self.db_path)

        self._orig_db_path = whatsapp.MESSAGES_DB_PATH
        whatsapp.MESSAGES_DB_PATH = self.db_path

        sender = "447700900000@s.whatsapp.net"

        # Chat A: never read (NULL marker). 2 incoming, 1 outgoing.
        _insert_chat(self.db_path, "a@s.whatsapp.net", "Alice", "2026-09-09T09:00:00Z", None)
        _insert_message(self.db_path, "a1", "a@s.whatsapp.net", sender, "hi", "2026-09-09T07:00:00Z", False)
        _insert_message(self.db_path, "a2", "a@s.whatsapp.net", sender, "there", "2026-09-09T08:00:00Z", False)
        _insert_message(self.db_path, "a3", "a@s.whatsapp.net", "me", "reply", "2026-09-09T09:00:00Z", True)

        # Chat B: fully read (marker after all messages).
        _insert_chat(self.db_path, "b@s.whatsapp.net", "Bob", "2026-09-08T10:00:00Z", "2026-09-08T12:00:00Z")
        _insert_message(self.db_path, "b1", "b@s.whatsapp.net", sender, "yo", "2026-09-08T09:00:00Z", False)
        _insert_message(self.db_path, "b2", "b@s.whatsapp.net", sender, "sup", "2026-09-08T10:00:00Z", False)

        # Chat C: marker between two incoming messages. msg c2 is stored with a
        # -05:00 offset that is lexically "earlier" than the marker's hour but
        # is actually later once converted to UTC (08:00-05:00 == 13:00 UTC,
        # after the 12:00 UTC marker) -- naive string comparison would wrongly
        # call it read; julianday() must get it right.
        _insert_chat(self.db_path, "c@s.whatsapp.net", "Carol", "2026-09-09T18:00:00-05:00", "2026-09-09T12:00:00Z")
        _insert_message(self.db_path, "c1", "c@s.whatsapp.net", sender, "before", "2026-09-09T10:00:00Z", False)
        _insert_message(self.db_path, "c2", "c@s.whatsapp.net", sender, "after", "2026-09-09T08:00:00-05:00", False)

    def tearDown(self):
        whatsapp.MESSAGES_DB_PATH = self._orig_db_path
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_get_chat_unread_counts(self):
        self.assertEqual(whatsapp.get_chat("a@s.whatsapp.net").unread_count, 2)
        self.assertEqual(whatsapp.get_chat("b@s.whatsapp.net").unread_count, 0)
        self.assertEqual(whatsapp.get_chat("c@s.whatsapp.net").unread_count, 1)

    def test_get_chat_last_read_at(self):
        self.assertIsNone(whatsapp.get_chat("a@s.whatsapp.net").last_read_at)
        self.assertIsNotNone(whatsapp.get_chat("b@s.whatsapp.net").last_read_at)

    def test_list_chats_unread_only_filters(self):
        chats = whatsapp.list_chats(unread_only=True, limit=50)
        jids = {c.jid for c in chats}
        self.assertEqual(jids, {"a@s.whatsapp.net", "c@s.whatsapp.net"})

    def test_list_chats_default_returns_all(self):
        chats = whatsapp.list_chats(limit=50)
        jids = {c.jid for c in chats}
        self.assertEqual(jids, {"a@s.whatsapp.net", "b@s.whatsapp.net", "c@s.whatsapp.net"})

    def test_list_unread_chats_matches_filter(self):
        chats = whatsapp.list_unread_chats(limit=50)
        jids = {c.jid for c in chats}
        self.assertEqual(jids, {"a@s.whatsapp.net", "c@s.whatsapp.net"})
        counts = {c.jid: c.unread_count for c in chats}
        self.assertEqual(counts["a@s.whatsapp.net"], 2)
        self.assertEqual(counts["c@s.whatsapp.net"], 1)


class MarkChatReadTests(unittest.TestCase):
    def test_success_response_passes_through_fields(self):
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {
            "success": True,
            "message": "marked read",
            "marked_count": 2,
            "receipt_sent": False,
        }
        with mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            result = whatsapp.mark_chat_read("a@s.whatsapp.net", send_receipt=False)

        post.assert_called_once()
        args, kwargs = post.call_args
        self.assertEqual(kwargs["json"], {"chat_jid": "a@s.whatsapp.net", "send_receipt": False})
        self.assertEqual(result, {
            "success": True,
            "message": "marked read",
            "marked_count": 2,
            "receipt_sent": False,
        })

    def test_non_200_response(self):
        fake_response = mock.Mock(status_code=503, text="bridge not connected")
        with mock.patch("whatsapp.requests.post", return_value=fake_response):
            result = whatsapp.mark_chat_read("a@s.whatsapp.net")

        self.assertFalse(result["success"])
        self.assertEqual(result["marked_count"], 0)
        self.assertFalse(result["receipt_sent"])
        self.assertIn("503", result["message"])

    def test_request_exception(self):
        import requests
        with mock.patch("whatsapp.requests.post", side_effect=requests.RequestException("boom")):
            result = whatsapp.mark_chat_read("a@s.whatsapp.net")

        self.assertFalse(result["success"])
        self.assertEqual(result["marked_count"], 0)
        self.assertFalse(result["receipt_sent"])
        self.assertIn("Request error", result["message"])

    def test_missing_chat_jid(self):
        result = whatsapp.mark_chat_read("")
        self.assertFalse(result["success"])
        self.assertEqual(result["marked_count"], 0)


if __name__ == "__main__":
    unittest.main()
