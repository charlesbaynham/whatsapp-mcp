import os
import shutil
import tempfile
import unittest
from unittest import mock

import whatsapp


class ContainmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp_root = tempfile.mkdtemp()
        self.store_dir = os.path.join(self.tmp_root, "store")
        os.makedirs(self.store_dir)
        self.outside_dir = os.path.join(self.tmp_root, "outside")
        os.makedirs(self.outside_dir)

        self._orig_store_dir = whatsapp.STORE_DIR
        whatsapp.STORE_DIR = self.store_dir

    def tearDown(self):
        whatsapp.STORE_DIR = self._orig_store_dir
        shutil.rmtree(self.tmp_root, ignore_errors=True)

    @staticmethod
    def _write(path, content=b"data"):
        with open(path, "wb") as f:
            f.write(content)

    def test_resolve_in_store_accepts_path_inside(self):
        inside = os.path.join(self.store_dir, "clip.ogg")
        self._write(inside)
        resolved = whatsapp._resolve_in_store(inside)
        self.assertEqual(os.path.realpath(inside), resolved)

    def test_resolve_in_store_rejects_path_outside(self):
        outside = os.path.join(self.outside_dir, "secret.wav")
        self._write(outside)
        with self.assertRaises(ValueError):
            whatsapp._resolve_in_store(outside)

    def test_resolve_in_store_rejects_symlink_escape(self):
        outside = os.path.join(self.outside_dir, "secret.wav")
        self._write(outside)
        symlink = os.path.join(self.store_dir, "escape.wav")
        try:
            os.symlink(outside, symlink)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks not supported in this environment")
        with self.assertRaises(ValueError):
            whatsapp._resolve_in_store(symlink)

    def test_send_audio_message_rejects_outside_path_before_conversion(self):
        # Deliberately readable and a real audio-shaped file: the point is that
        # containment, not file validity, is what must block it.
        outside = os.path.join(self.outside_dir, "voice.wav")
        self._write(outside)

        with mock.patch(
            "whatsapp.audio.convert_to_opus_ogg_temp",
            side_effect=AssertionError("must not convert a path outside the store"),
        ), mock.patch(
            "whatsapp.requests.post",
            side_effect=AssertionError("must not send a path outside the store"),
        ):
            success, message = whatsapp.send_audio_message("447700900000", outside)

        self.assertFalse(success)
        self.assertIn("store directory", message)

    def test_send_audio_message_accepts_path_inside_store(self):
        inside = os.path.join(self.store_dir, "voice.ogg")
        self._write(inside)

        fake_response = mock.Mock(status_code=200)
        fake_response.json.return_value = {"success": True, "message": "sent"}

        with mock.patch(
            "whatsapp.audio.convert_to_opus_ogg_temp",
            side_effect=AssertionError("already .ogg, must not convert"),
        ), mock.patch("whatsapp.requests.post", return_value=fake_response) as post:
            success, message = whatsapp.send_audio_message("447700900000", inside)

        self.assertTrue(success)
        post.assert_called_once()

    def test_send_audio_message_rejects_symlink_escape_before_conversion(self):
        outside = os.path.join(self.outside_dir, "secret.wav")
        self._write(outside)
        symlink = os.path.join(self.store_dir, "escape.wav")
        try:
            os.symlink(outside, symlink)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks not supported in this environment")

        with mock.patch(
            "whatsapp.audio.convert_to_opus_ogg_temp",
            side_effect=AssertionError("must not convert a symlink escaping the store"),
        ), mock.patch(
            "whatsapp.requests.post",
            side_effect=AssertionError("must not send a symlink escaping the store"),
        ):
            success, message = whatsapp.send_audio_message("447700900000", symlink)

        self.assertFalse(success)
        self.assertIn("store directory", message)

    def test_send_file_rejects_outside_path_before_isfile(self):
        # Deliberately does not create this file: containment must be checked
        # before existence, so the error must not be "not found" either.
        outside = os.path.join(self.outside_dir, "does_not_exist.bin")

        with mock.patch(
            "whatsapp.requests.post",
            side_effect=AssertionError("must not send a path outside the store"),
        ):
            success, message = whatsapp.send_file("447700900000", outside)

        self.assertFalse(success)
        self.assertIn("store directory", message)
        self.assertNotIn("not found", message)


if __name__ == "__main__":
    unittest.main()
