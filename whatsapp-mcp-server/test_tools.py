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
        sf.assert_called_once_with("1", path="/tmp/x.mp3", voice_note=True, block=False)

    def test_sends_queue_by_default(self):
        with mock.patch.object(main.wa, "send_message", return_value={"success": True, "queued": True}) as sm:
            main.send_message("1", "hi")
        sm.assert_called_once_with("1", "hi", block=False)
        with mock.patch.object(main.wa, "send_file", return_value={"success": True, "message": "ok"}) as sf:
            main.send_file("1", "/tmp/x.jpg")
        sf.assert_called_once_with("1", path="/tmp/x.jpg", block=False)

    def test_block_reaches_the_bridge(self):
        with mock.patch.object(main.wa, "send_message", return_value={"success": True, "message": "sent"}) as sm:
            main.send_message("1", "hi", block=True)
        sm.assert_called_once_with("1", "hi", block=True)

    def test_get_send_status_folds_errors(self):
        with mock.patch.object(main.wa, "send_status", return_value={"id": "snd-1", "state": "sent"}):
            self.assertEqual(main.get_send_status("snd-1")["state"], "sent")
        with mock.patch.object(main.wa, "send_status", side_effect=BridgeError("nf", status=404)):
            self.assertFalse(main.get_send_status("snd-1")["success"])
        self.assertFalse(main.get_send_status("")["success"])

    def test_get_reachout_timelock_folds_errors(self):
        with mock.patch.object(main.wa, "reachout_timelock", return_value={"active": True}):
            self.assertTrue(main.get_reachout_timelock()["active"])
        with mock.patch.object(main.wa, "reachout_timelock", side_effect=BridgeError("unreachable")):
            self.assertFalse(main.get_reachout_timelock()["success"])


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


class BatchSendTests(unittest.TestCase):
    """send_messages queues each message through the ordinary send path."""

    def test_queues_each_message_in_order(self):
        replies = [{"success": True, "queued": True, "id": "snd-1"},
                   {"success": True, "queued": True, "id": "snd-2"}]
        with mock.patch.object(main.wa, "send_message", side_effect=replies) as sm:
            out = main.send_messages([
                {"recipient": "1", "message": "first"},
                {"recipient": "2@g.us", "message": "second"},
            ])
        self.assertEqual([c.args for c in sm.call_args_list], [("1", "first"), ("2@g.us", "second")])
        self.assertTrue(out["success"])
        self.assertEqual(out["queued"], 2)
        self.assertEqual([r["id"] for r in out["results"]], ["snd-1", "snd-2"])

    def test_a_bad_entry_queues_nothing(self):
        with mock.patch.object(main.wa, "send_message") as sm:
            empty_text = main.send_messages([{"recipient": "1", "message": "hi"},
                                             {"recipient": "2", "message": ""}])
            no_recipient = main.send_messages([{"recipient": "", "message": "hi"}])
            malformed = main.send_messages([{"recipient": "1"}])
            empty_batch = main.send_messages([])
        sm.assert_not_called()
        for out in (empty_text, no_recipient, malformed, empty_batch):
            self.assertFalse(out["success"])
            self.assertEqual(out["queued"], 0)

    def test_submission_stops_at_the_first_failure(self):
        replies = [{"success": True, "queued": True, "id": "snd-1"},
                   BridgeError("Send queue is full", status=503)]
        with mock.patch.object(main.wa, "send_message", side_effect=replies) as sm:
            out = main.send_messages([{"recipient": "1", "message": "a"},
                                      {"recipient": "1", "message": "b"},
                                      {"recipient": "1", "message": "c"}])
        self.assertEqual(sm.call_count, 2)  # the third is never submitted
        self.assertFalse(out["success"])
        self.assertEqual(out["queued"], 1)
        self.assertEqual([r["success"] for r in out["results"]], [True, False, False])
        self.assertIn("queue is full", out["results"][2]["message"])

    def test_accepts_parsed_models(self):
        with mock.patch.object(main.wa, "send_message", return_value={"success": True, "id": "snd-1"}) as sm:
            out = main.send_messages([main.OutgoingMessage(recipient="1", message="hi")])
        sm.assert_called_once_with("1", "hi")
        self.assertTrue(out["success"])

if __name__ == "__main__":
    unittest.main()


class NewContactWarningTests(unittest.TestCase):
    """A first contact is held far longer than usual; the agent is told so."""

    def test_single_send_to_a_first_contact_warns(self):
        reply = {"success": True, "queued": True, "id": "snd-1", "new_contact": True,
                 "ahead": 2, "estimated_wait_seconds": 3600}
        with mock.patch.object(main.wa, "send_message", return_value=reply):
            out = main.send_message("447700900000", "hi")
        self.assertTrue(out["new_contact"])
        self.assertIn("never been messaged", out["warning"])
        self.assertIn("about 1.0 h", out["warning"])
        self.assertIn("Tell the user", out["warning"])

    def test_ordinary_send_has_no_warning(self):
        with mock.patch.object(main.wa, "send_message", return_value={"success": True, "queued": True, "id": "snd-1"}):
            out = main.send_message("447700900000", "hi")
        self.assertNotIn("warning", out)

    def test_media_sends_warn_too(self):
        reply = {"success": True, "queued": True, "id": "snd-1", "new_contact": True, "estimated_wait_seconds": 90}
        with mock.patch.object(main.wa, "send_file", return_value=reply):
            self.assertIn("about 2 min", main.send_file("1", "/tmp/x.jpg")["warning"])
            self.assertIn("warning", main.send_audio_message("1", "/tmp/x.ogg"))

    def test_batch_counts_first_contacts_and_estimates_completion(self):
        replies = [{"success": True, "queued": True, "id": "snd-1", "estimated_wait_seconds": 30},
                   {"success": True, "queued": True, "id": "snd-2", "new_contact": True,
                    "ahead": 0, "estimated_wait_seconds": 60},
                   {"success": True, "queued": True, "id": "snd-3", "new_contact": True,
                    "ahead": 1, "estimated_wait_seconds": 1860}]
        with mock.patch.object(main.wa, "send_message", side_effect=replies):
            out = main.send_messages([{"recipient": "1", "message": "a"},
                                      {"recipient": "2", "message": "b"},
                                      {"recipient": "3", "message": "c"}])
        self.assertTrue(out["success"])
        self.assertEqual(out["new_contacts"], 2)
        self.assertEqual(out["estimated_completion_seconds"], 1860)
        self.assertIn("2 of the 3 queued", out["warning"])
        self.assertIn("about 31 min", out["warning"])
        self.assertNotIn("new_contact", out["results"][0])
        self.assertTrue(out["results"][2]["new_contact"])
        self.assertEqual(out["results"][2]["estimated_wait_seconds"], 1860)

    def test_batch_of_established_chats_has_no_warning(self):
        replies = [{"success": True, "queued": True, "id": "snd-1", "estimated_wait_seconds": 0},
                   {"success": True, "queued": True, "id": "snd-2", "ahead": 1, "estimated_wait_seconds": 30}]
        with mock.patch.object(main.wa, "send_message", side_effect=replies):
            out = main.send_messages([{"recipient": "1", "message": "a"}, {"recipient": "1", "message": "b"}])
        self.assertEqual(out["new_contacts"], 0)
        self.assertEqual(out["estimated_completion_seconds"], 30)
        self.assertNotIn("warning", out)
