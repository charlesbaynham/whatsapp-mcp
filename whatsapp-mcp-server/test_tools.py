"""Tool-level tests: the MCP tools are thin wrappers, so these check error
folding and the few places the server reshapes bridge output."""

import unittest
from unittest import mock

import main
from whatsapp_client import BridgeError


class ErrorFoldingTests(unittest.TestCase):
    def test_send_message_bridge_error_becomes_failure_dict(self):
        with mock.patch.object(main.wa, "send_message", side_effect=BridgeError("not connected", status=503)):
            out = main.send_message("1", "hi")
        self.assertEqual(out, {"success": False, "message": "not connected"})

    def test_send_message_passes_bridge_result_through(self):
        with mock.patch.object(main.wa, "send_message", return_value={"success": True, "message": "sent"}):
            self.assertEqual(main.send_message("1", "hi"), {"success": True, "message": "sent"})

    def test_mark_chat_read_error_keeps_shape(self):
        with mock.patch.object(main.wa, "mark_chat_read", side_effect=BridgeError("boom")):
            out = main.mark_chat_read("c@s.whatsapp.net")
        self.assertEqual(out, {"success": False, "message": "boom", "marked_count": 0, "receipt_sent": False})

    def test_unsubscribe_404(self):
        with mock.patch.object(main.wa, "delete_webhook", side_effect=BridgeError("nf", status=404)):
            self.assertIn("not found", main.unsubscribe_chat(7)["message"])

    def test_send_audio_sets_voice_note(self):
        with mock.patch.object(main.wa, "send_file", return_value={"success": True, "message": "ok"}) as sf:
            main.send_audio_message("1", "/tmp/x.mp3")
        sf.assert_called_once_with("1", path="/tmp/x.mp3", voice_note=True)


class ReshapeTests(unittest.TestCase):
    def test_last_interaction_formats_transcript(self):
        msg = {"id": "m1", "chat_jid": "c@s.whatsapp.net", "chat_name": "Carol", "sender_name": "Carol",
               "timestamp": "2026-09-09T10:00:00Z", "content": "", "media_type": "audio",
               "transcript": "hello there"}
        with mock.patch.object(main.wa, "get_last_interaction", return_value=msg):
            out = main.get_last_interaction("c@s.whatsapp.net")
        self.assertIn("[voice note] hello there", out)
        self.assertIn("From: Carol", out)

    def test_last_interaction_none(self):
        with mock.patch.object(main.wa, "get_last_interaction", return_value=None):
            self.assertIsNone(main.get_last_interaction("x"))

    def test_subscription_never_leaks_token(self):
        with mock.patch.object(main.wa, "create_webhook", return_value={"id": 1, "bearer_token": "SECRET", "bearer_token_hint": "CRET"}):
            out = main.subscribe_chat("*", "https://x")
        self.assertNotIn("bearer_token", out["subscription"])
        self.assertEqual(out["subscription"]["bearer_token_hint"], "CRET")

    def test_download_media_shapes(self):
        with mock.patch.object(main.wa, "download_media", return_value={"success": True, "path": "/data/store/x"}):
            self.assertEqual(main.download_media("m", "c")["file_path"], "/data/store/x")
        with mock.patch.object(main.wa, "download_media", side_effect=BridgeError("nope")):
            self.assertFalse(main.download_media("m", "c")["success"])


if __name__ == "__main__":
    unittest.main()
