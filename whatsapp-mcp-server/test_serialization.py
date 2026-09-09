import unittest
from datetime import datetime
from unittest import mock

import whatsapp
from whatsapp import Chat, Contact, Message, MessageContext


def _message(is_from_me=False, sender="447700900000@s.whatsapp.net"):
    return Message(
        timestamp=datetime(2026, 9, 9, 12, 30, 5),
        sender=sender,
        content="hello",
        is_from_me=is_from_me,
        chat_jid="447700900000@s.whatsapp.net",
        id="ABC123",
        chat_name="Alice",
        media_type=None,
    )


class MessageSerializationTests(unittest.TestCase):
    def test_timestamp_becomes_iso_string(self):
        with mock.patch("whatsapp.get_sender_name", return_value="Alice"):
            data = whatsapp.message_to_dict(_message())
        self.assertEqual(data["timestamp"], "2026-09-09T12:30:05")

    def test_sender_name_resolved_for_others(self):
        with mock.patch("whatsapp.get_sender_name", return_value="Alice"):
            data = whatsapp.message_to_dict(_message(is_from_me=False))
        self.assertEqual(data["sender_name"], "Alice")

    def test_sender_name_is_me_when_from_me(self):
        with mock.patch(
            "whatsapp.get_sender_name",
            side_effect=AssertionError("must not look up our own sender"),
        ):
            data = whatsapp.message_to_dict(_message(is_from_me=True))
        self.assertEqual(data["sender_name"], "Me")

    def test_result_is_plain_dict(self):
        with mock.patch("whatsapp.get_sender_name", return_value="Alice"):
            data = whatsapp.message_to_dict(_message())
        self.assertIsInstance(data, dict)
        self.assertNotIsInstance(data["timestamp"], datetime)


class ChatSerializationTests(unittest.TestCase):
    def test_is_group_included_for_group_jid(self):
        chat = Chat(jid="123@g.us", name="Group", last_message_time=None)
        self.assertTrue(whatsapp.chat_to_dict(chat)["is_group"])

    def test_is_group_false_for_direct_jid(self):
        chat = Chat(jid="123@s.whatsapp.net", name="Bob", last_message_time=None)
        self.assertFalse(whatsapp.chat_to_dict(chat)["is_group"])

    def test_last_message_time_none_stays_none(self):
        chat = Chat(jid="123@s.whatsapp.net", name="Bob", last_message_time=None)
        self.assertIsNone(whatsapp.chat_to_dict(chat)["last_message_time"])

    def test_last_message_time_becomes_iso_string(self):
        chat = Chat(
            jid="123@s.whatsapp.net",
            name="Bob",
            last_message_time=datetime(2026, 9, 9, 8, 0, 0),
        )
        self.assertEqual(
            whatsapp.chat_to_dict(chat)["last_message_time"], "2026-09-09T08:00:00"
        )


class ContactSerializationTests(unittest.TestCase):
    def test_contact_to_dict_is_plain_dict(self):
        contact = Contact(phone_number="447700900000", name="Alice", jid="447700900000@s.whatsapp.net")
        self.assertEqual(
            whatsapp.contact_to_dict(contact),
            {"phone_number": "447700900000", "name": "Alice", "jid": "447700900000@s.whatsapp.net"},
        )


class MessageContextSerializationTests(unittest.TestCase):
    def test_shape_and_ordering_preserved(self):
        context = MessageContext(
            message=_message(),
            before=[_message(sender="before@s.whatsapp.net")],
            after=[_message(sender="after@s.whatsapp.net")],
        )
        with mock.patch("whatsapp.get_sender_name", side_effect=lambda jid: jid):
            data = whatsapp.message_context_to_dict(context)

        self.assertEqual(set(data), {"message", "before", "after"})
        self.assertEqual(data["before"][0]["sender_name"], "before@s.whatsapp.net")
        self.assertEqual(data["after"][0]["sender_name"], "after@s.whatsapp.net")
        self.assertIsInstance(data["message"], dict)


if __name__ == "__main__":
    unittest.main()
