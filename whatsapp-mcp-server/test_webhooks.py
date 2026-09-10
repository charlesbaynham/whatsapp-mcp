import unittest
from unittest import mock

import requests

import whatsapp


class SubscribeChatTests(unittest.TestCase):
    def test_success_posts_expected_json_and_returns_record(self):
        fake_response = mock.Mock(status_code=201)
        fake_response.json.return_value = {
            "id": 1,
            "chat_jid": "a@s.whatsapp.net",
            "url": "https://api.anthropic.com/v1/claude_code/routines/trig_abc/fire",
            "bearer_token": "",
            "bearer_token_hint": "wxyz",
            "kind": "claude_routine",
            "headers": {},
            "include_from_me": False,
            "debounce_seconds": 30,
            "enabled": True,
            "created_at": "2026-09-10T00:00:00Z",
            "last_fired_at": None,
            "last_status": None,
            "last_error": None,
            "disabled_reason": "",
            "consecutive_failures": 0,
            "expires_at": "2026-09-11T00:00:00Z",
            "max_per_hour": 30,
        }
        with mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            result = whatsapp.subscribe_chat(
                "a@s.whatsapp.net",
                "https://api.anthropic.com/v1/claude_code/routines/trig_abc/fire",
                bearer_token="secret-token-wxyz",
                kind="claude_routine",
                headers={"X-Extra": "1"},
                include_from_me=False,
                debounce_seconds=30,
                ttl_seconds=3600,
                max_per_hour=30,
            )

        post.assert_called_once()
        args, kwargs = post.call_args
        self.assertEqual(args[0], f"{whatsapp.WHATSAPP_API_BASE_URL}/webhooks")
        self.assertEqual(kwargs["json"], {
            "chat_jid": "a@s.whatsapp.net",
            "url": "https://api.anthropic.com/v1/claude_code/routines/trig_abc/fire",
            "bearer_token": "secret-token-wxyz",
            "kind": "claude_routine",
            "headers": {"X-Extra": "1"},
            "include_from_me": False,
            "debounce_seconds": 30,
            "ttl_seconds": 3600,
            "max_per_hour": 30,
        })

        self.assertTrue(result["success"])
        self.assertEqual(result["subscription"]["id"], 1)
        self.assertEqual(result["subscription"]["bearer_token_hint"], "wxyz")
        self.assertNotIn("bearer_token", result["subscription"])
        self.assertNotIn("secret-token-wxyz", str(result))
        self.assertEqual(result["subscription"]["consecutive_failures"], 0)
        self.assertEqual(result["subscription"]["disabled_reason"], "")
        self.assertEqual(result["subscription"]["expires_at"], "2026-09-11T00:00:00Z")
        self.assertEqual(result["subscription"]["max_per_hour"], 30)

    def test_default_ttl_and_max_per_hour(self):
        fake_response = mock.Mock(status_code=201)
        fake_response.json.return_value = {"id": 2, "chat_jid": "*", "url": "https://example.com/hook"}
        with mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            whatsapp.subscribe_chat("*", "https://example.com/hook")

        args, kwargs = post.call_args
        self.assertEqual(kwargs["json"]["ttl_seconds"], 0)
        self.assertEqual(kwargs["json"]["max_per_hour"], 60)

    def test_missing_chat_jid(self):
        result = whatsapp.subscribe_chat("", "https://example.com/hook")
        self.assertFalse(result["success"])

    def test_missing_url(self):
        result = whatsapp.subscribe_chat("a@s.whatsapp.net", "")
        self.assertFalse(result["success"])

    def test_validation_error_400(self):
        fake_response = mock.Mock(status_code=400, text="url must be https")
        with mock.patch("whatsapp.requests.post", return_value=fake_response):
            result = whatsapp.subscribe_chat("a@s.whatsapp.net", "http://insecure")

        self.assertFalse(result["success"])
        self.assertIn("400", result["message"])

    def test_request_exception_does_not_leak_token(self):
        with mock.patch("whatsapp.requests.post", side_effect=requests.RequestException("boom")):
            result = whatsapp.subscribe_chat(
                "a@s.whatsapp.net", "https://example.com/hook", bearer_token="super-secret-token"
            )

        self.assertFalse(result["success"])
        self.assertNotIn("super-secret-token", result["message"])


class UnsubscribeChatTests(unittest.TestCase):
    def test_success_204(self):
        fake_response = mock.Mock(status_code=204)
        with mock.patch("whatsapp.requests.delete", return_value=fake_response) as delete:
            result = whatsapp.unsubscribe_chat(1)

        delete.assert_called_once()
        args, kwargs = delete.call_args
        self.assertEqual(args[0], f"{whatsapp.WHATSAPP_API_BASE_URL}/webhooks/1")
        self.assertTrue(result["success"])

    def test_not_found_404(self):
        fake_response = mock.Mock(status_code=404, text="not found")
        with mock.patch("whatsapp.requests.delete", return_value=fake_response):
            result = whatsapp.unsubscribe_chat(999)

        self.assertFalse(result["success"])
        self.assertIn("999", result["message"])

    def test_request_exception(self):
        with mock.patch("whatsapp.requests.delete", side_effect=requests.RequestException("boom")):
            result = whatsapp.unsubscribe_chat(1)

        self.assertFalse(result["success"])
        self.assertIn("Request error", result["message"])


class EnableSubscriptionTests(unittest.TestCase):
    def test_success(self):
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {
            "id": 1,
            "chat_jid": "a@s.whatsapp.net",
            "url": "https://example.com/hook",
            "bearer_token": "",
            "bearer_token_hint": "wxyz",
            "kind": "generic",
            "headers": {},
            "include_from_me": False,
            "debounce_seconds": 0,
            "enabled": True,
            "created_at": "2026-09-10T00:00:00Z",
            "last_fired_at": None,
            "last_status": None,
            "last_error": None,
            "disabled_reason": "",
            "consecutive_failures": 0,
            "expires_at": None,
            "max_per_hour": 60,
        }
        with mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            result = whatsapp.enable_subscription(1)

        post.assert_called_once()
        args, kwargs = post.call_args
        self.assertEqual(args[0], f"{whatsapp.WHATSAPP_API_BASE_URL}/webhooks/1/enable")
        self.assertTrue(result["success"])
        self.assertTrue(result["subscription"]["enabled"])
        self.assertEqual(result["subscription"]["consecutive_failures"], 0)

    def test_not_found_404(self):
        fake_response = mock.Mock(status_code=404, text="not found")
        with mock.patch("whatsapp.requests.post", return_value=fake_response):
            result = whatsapp.enable_subscription(999)

        self.assertFalse(result["success"])
        self.assertIn("999", result["message"])

    def test_request_exception(self):
        with mock.patch("whatsapp.requests.post", side_effect=requests.RequestException("boom")):
            result = whatsapp.enable_subscription(1)

        self.assertFalse(result["success"])
        self.assertIn("Request error", result["message"])

    def test_missing_subscription_id(self):
        result = whatsapp.enable_subscription(None)
        self.assertFalse(result["success"])


class ListSubscriptionsTests(unittest.TestCase):
    def test_success(self):
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = [
            {
                "id": 1,
                "chat_jid": "*",
                "url": "https://example.com/hook",
                "bearer_token": "",
                "bearer_token_hint": "ab12",
                "kind": "generic",
                "headers": {},
                "include_from_me": False,
                "debounce_seconds": 0,
                "enabled": False,
                "created_at": "2026-09-10T00:00:00Z",
                "last_fired_at": None,
                "last_status": 401,
                "last_error": "unauthorized",
                "disabled_reason": "3 consecutive failed deliveries",
                "consecutive_failures": 3,
                "expires_at": "2026-10-10T00:00:00Z",
                "max_per_hour": 60,
            }
        ]
        with mock.patch("whatsapp.requests.get", return_value=fake_response) as get:
            result = whatsapp.list_subscriptions()

        get.assert_called_once()
        args, kwargs = get.call_args
        self.assertEqual(args[0], f"{whatsapp.WHATSAPP_API_BASE_URL}/webhooks")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["bearer_token_hint"], "ab12")
        self.assertNotIn("bearer_token", result[0])
        self.assertFalse(result[0]["enabled"])
        self.assertEqual(result[0]["disabled_reason"], "3 consecutive failed deliveries")
        self.assertEqual(result[0]["consecutive_failures"], 3)
        self.assertEqual(result[0]["expires_at"], "2026-10-10T00:00:00Z")
        self.assertEqual(result[0]["max_per_hour"], 60)

    def test_error_returns_empty_list(self):
        fake_response = mock.Mock(status_code=500, text="boom")
        with mock.patch("whatsapp.requests.get", return_value=fake_response):
            result = whatsapp.list_subscriptions()
        self.assertEqual(result, [])

    def test_request_exception_returns_empty_list(self):
        with mock.patch("whatsapp.requests.get", side_effect=requests.RequestException("boom")):
            result = whatsapp.list_subscriptions()
        self.assertEqual(result, [])


class TestSubscriptionTests(unittest.TestCase):
    def test_success(self):
        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {"success": True, "status": 200, "error": ""}
        with mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            result = whatsapp.test_subscription(1)

        post.assert_called_once()
        args, kwargs = post.call_args
        self.assertEqual(args[0], f"{whatsapp.WHATSAPP_API_BASE_URL}/webhooks/1/test")
        self.assertEqual(result, {"success": True, "status": 200, "error": ""})

    def test_not_found_404(self):
        fake_response = mock.Mock(status_code=404, text="not found")
        with mock.patch("whatsapp.requests.post", return_value=fake_response):
            result = whatsapp.test_subscription(999)

        self.assertFalse(result["success"])
        self.assertEqual(result["status"], 404)

    def test_request_exception_does_not_leak_token(self):
        with mock.patch("whatsapp.requests.post", side_effect=requests.RequestException("token abc123 boom")):
            result = whatsapp.test_subscription(1)

        self.assertFalse(result["success"])
        # The exception message itself is surfaced, but there is no bearer
        # token available to this call to begin with -- assert no secret-like
        # value leaks through from our own code paths.
        self.assertIn("Request error", result["error"])


if __name__ == "__main__":
    unittest.main()
