"""Client for the whatsapp-bridge REST API.

Every method maps to one bridge endpoint and returns the decoded JSON
(plain dicts/lists) or raises BridgeError. Connection failures raise
BridgeUnavailable, a BridgeError subclass, so callers can treat "bridge is
down" separately from "bridge said no".
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple

import httpx

DEFAULT_BRIDGE_URL = "unix:/run/whatsapp/bridge.sock"
UNIX_PLACEHOLDER_HOST = "http://whatsapp"

REQUEST_TIMEOUT = 30.0
MEDIA_TIMEOUT = 120.0


class BridgeError(Exception):
    """The bridge returned an error response."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class BridgeUnavailable(BridgeError):
    """The bridge could not be reached at all."""


@dataclass
class Event:
    """One frame from GET /api/events.

    ``data`` is the whole event record (id, type, chat_jid, message_id,
    is_from_me, created_at, data); ``payload`` is its inner ``data`` field,
    e.g. the message for message.new.
    """

    id: int
    type: str
    data: Dict[str, Any]

    @property
    def payload(self) -> Dict[str, Any]:
        inner = self.data.get("data")
        return inner if isinstance(inner, dict) else {}

    @property
    def chat_jid(self) -> str:
        return str(self.data.get("chat_jid") or "")


def parse_bridge_url(url: str) -> Tuple[str, Optional[str]]:
    """Split a bridge URL into (http base, unix socket path or None).

    Accepts ``unix:/run/whatsapp/bridge.sock`` or ``http://host:port`` with
    an optional trailing ``/api`` (the older env var convention).
    """
    url = url.strip()
    if url.startswith("unix:"):
        path = url[len("unix:"):]
        if not path:
            raise ValueError("unix: bridge URL has no socket path")
        return UNIX_PLACEHOLDER_HOST, path
    base = url.rstrip("/")
    if base.endswith("/api"):
        base = base[: -len("/api")]
    if not base.startswith(("http://", "https://")):
        raise ValueError(f"bridge URL must be unix:/path or http(s)://host:port, got {url!r}")
    return base, None


def _error_text(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict):
            for key in ("error", "message"):
                if body.get(key):
                    return str(body[key])
    except ValueError:
        pass
    return resp.text.strip() or f"HTTP {resp.status_code}"


class WhatsAppClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = REQUEST_TIMEOUT,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        base_url = base_url or os.environ.get("WHATSAPP_BRIDGE_URL", DEFAULT_BRIDGE_URL)
        self.base, self.socket_path = parse_bridge_url(base_url)
        if transport is None and self.socket_path is not None:
            transport = httpx.HTTPTransport(uds=self.socket_path)
        self._http = httpx.Client(base_url=self.base, timeout=timeout, transport=transport)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "WhatsAppClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # --- plumbing ---

    def _request(self, method: str, path: str, *, ok: Tuple[int, ...] = (200,), **kw: Any) -> httpx.Response:
        try:
            resp = self._http.request(method, "/api" + path, **kw)
        except httpx.TransportError as e:
            raise BridgeUnavailable(f"bridge unreachable at {self.socket_path or self.base}: {e}") from e
        if resp.status_code not in ok:
            raise BridgeError(_error_text(resp), status=resp.status_code)
        return resp

    def _json(self, method: str, path: str, **kw: Any) -> Any:
        resp = self._request(method, path, **kw)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    @staticmethod
    def _params(**kw: Any) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in kw.items():
            if v is None or v == "":
                continue
            out[k] = "true" if v is True else "false" if v is False else v
        return out

    # --- status ---

    def status(self) -> Dict[str, Any]:
        return self._json("GET", "/status")

    # --- chats and contacts ---

    def list_chats(
        self,
        query: Optional[str] = None,
        limit: int = 20,
        page: int = 0,
        include_last_message: bool = True,
        sort_by: str = "last_active",
        unread_only: bool = False,
    ) -> List[Dict[str, Any]]:
        return self._json("GET", "/chats", params=self._params(
            query=query, limit=limit, page=page, include_last_message=include_last_message,
            sort_by=sort_by, unread_only=unread_only))

    def list_unread_chats(self, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
        return self._json("GET", "/chats/unread", params=self._params(limit=limit, page=page))

    def get_chat(self, chat_jid: str, include_last_message: bool = True) -> Optional[Dict[str, Any]]:
        try:
            return self._json("GET", f"/chats/{chat_jid}", params=self._params(include_last_message=include_last_message))
        except BridgeError as e:
            if e.status == 404:
                return None
            raise

    def get_direct_chat_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        try:
            return self._json("GET", f"/chats/by-phone/{phone}")
        except BridgeError as e:
            if e.status == 404:
                return None
            raise

    def search_contacts(self, query: str) -> List[Dict[str, Any]]:
        return self._json("GET", "/contacts", params=self._params(query=query))

    def get_contact_chats(self, jid: str, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
        return self._json("GET", f"/contacts/{jid}/chats", params=self._params(limit=limit, page=page))

    def get_last_interaction(self, jid: str) -> Optional[Dict[str, Any]]:
        try:
            return self._json("GET", f"/contacts/{jid}/last-interaction")
        except BridgeError as e:
            if e.status == 404:
                return None
            raise

    # --- messages ---

    def list_messages(
        self,
        after: Optional[str] = None,
        before: Optional[str] = None,
        sender: Optional[str] = None,
        chat_jid: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 20,
        page: int = 0,
        include_context: bool = False,
        context_before: int = 1,
        context_after: int = 1,
    ) -> List[Dict[str, Any]]:
        return self._json("GET", "/messages", params=self._params(
            after=after, before=before, sender=sender, chat_jid=chat_jid, query=query,
            limit=limit, page=page, include_context=include_context,
            context_before=context_before, context_after=context_after))

    def get_message_context(self, message_id: str, before: int = 5, after: int = 5,
                            chat_jid: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            return self._json("GET", f"/messages/{message_id}/context",
                              params=self._params(before=before, after=after, chat_jid=chat_jid))
        except BridgeError as e:
            if e.status == 404:
                return None
            raise

    # --- sending ---

    def send_message(self, recipient: str, message: str) -> Dict[str, Any]:
        return self._json("POST", "/send", json={"recipient": recipient, "message": message})

    def send_file(
        self,
        recipient: str,
        *,
        path: Optional[str] = None,
        data: Optional[BinaryIO] = None,
        filename: Optional[str] = None,
        caption: str = "",
        voice_note: bool = False,
    ) -> Dict[str, Any]:
        """Send an attachment.

        ``data`` (a file-like object) or a locally readable ``path`` is
        uploaded to the bridge. A ``path`` this process cannot read is passed
        through as a store-relative path for the bridge to resolve itself
        (it must lie inside the bridge's store directory).
        """
        if data is None and path is None:
            raise ValueError("send_file needs path or data")
        fields = {"recipient": recipient, "message": caption, "voice_note": "true" if voice_note else "false"}
        if data is not None:
            files = {"file": (filename or "upload", data)}
            return self._json("POST", "/send", data=fields, files=files, timeout=MEDIA_TIMEOUT)
        assert path is not None
        if os.path.isfile(path) and os.access(path, os.R_OK):
            with open(path, "rb") as fh:
                files = {"file": (filename or os.path.basename(path), fh)}
                return self._json("POST", "/send", data=fields, files=files, timeout=MEDIA_TIMEOUT)
        return self._json("POST", "/send", timeout=MEDIA_TIMEOUT, json={
            "recipient": recipient, "message": caption, "media_path": path, "voice_note": voice_note})

    # --- media ---

    def download_media(self, message_id: str, chat_jid: str) -> Dict[str, Any]:
        """Ask the bridge to download media into its store; returns {success, message, filename, path}."""
        return self._json("POST", "/download", timeout=MEDIA_TIMEOUT,
                          json={"message_id": message_id, "chat_jid": chat_jid})

    def get_media(self, chat_jid: str, message_id: str) -> Tuple[bytes, str]:
        """Fetch media bytes over the socket. Returns (bytes, content_type)."""
        resp = self._request("GET", f"/media/{chat_jid}/{message_id}", timeout=MEDIA_TIMEOUT)
        return resp.content, resp.headers.get("content-type", "application/octet-stream")

    # --- read state and history ---

    def mark_chat_read(self, chat_jid: str, send_receipt: bool = False) -> Dict[str, Any]:
        return self._json("POST", "/mark-read", json={"chat_jid": chat_jid, "send_receipt": send_receipt})

    def resync(self, chat_jid: str, oldest_message_id: str, oldest_message_timestamp: int,
               oldest_message_from_me: bool = False, count: int = 50) -> Dict[str, Any]:
        return self._json("POST", "/resync", json={
            "chat_jid": chat_jid, "oldest_message_id": oldest_message_id,
            "oldest_message_timestamp": oldest_message_timestamp,
            "oldest_message_from_me": oldest_message_from_me, "count": count})

    # --- webhooks ---

    def list_webhooks(self) -> List[Dict[str, Any]]:
        return self._json("GET", "/webhooks")

    def create_webhook(self, **fields: Any) -> Dict[str, Any]:
        return self._json("POST", "/webhooks", ok=(201,), json=fields)

    def delete_webhook(self, webhook_id: int) -> None:
        self._request("DELETE", f"/webhooks/{webhook_id}", ok=(204,))

    def enable_webhook(self, webhook_id: int) -> Dict[str, Any]:
        return self._json("POST", f"/webhooks/{webhook_id}/enable")

    def test_webhook(self, webhook_id: int) -> Dict[str, Any]:
        return self._json("POST", f"/webhooks/{webhook_id}/test")

    # --- events ---

    def events(
        self,
        since: int = 0,
        *,
        chat_jid: Optional[str] = None,
        types: Optional[List[str]] = None,
        include_from_me: bool = False,
        reconnect_delay: float = 2.0,
        on_cursor: Optional[Callable[[int], None]] = None,
    ) -> Iterator[Event]:
        """Yield events from GET /api/events, resuming after the last seen id.

        Reconnects forever on transport errors, resuming from the last event
        id it yielded. ``on_cursor`` is called with each id after the caller
        has consumed the event, which is the right moment to persist it.
        """
        cursor = since
        params = self._params(chat_jid=chat_jid, include_from_me=include_from_me,
                              types=",".join(types) if types else None)
        while True:
            params["since"] = cursor
            try:
                with self._http.stream("GET", "/api/events", params=params, timeout=httpx.Timeout(30.0, read=None)) as resp:
                    if resp.status_code != 200:
                        resp.read()
                        raise BridgeError(_error_text(resp), status=resp.status_code)
                    for ev in _parse_sse(resp.iter_lines()):
                        yield ev
                        cursor = ev.id
                        if on_cursor is not None:
                            on_cursor(cursor)
            except httpx.TransportError:
                time.sleep(reconnect_delay)
                continue
            except BridgeError as e:
                if e.status is not None and 400 <= e.status < 500:
                    raise
                time.sleep(reconnect_delay)
                continue
            # A clean end of stream (bridge shutting down): reconnect too.
            time.sleep(reconnect_delay)


def _parse_sse(lines: Iterator[str]) -> Iterator[Event]:
    """Minimal text/event-stream parser: id/event/data fields, blank-line delimited."""
    ev_id: Optional[int] = None
    ev_type = "message"
    data_lines: List[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data_lines and ev_id is not None:
                yield Event(id=ev_id, type=ev_type, data=json.loads("\n".join(data_lines)))
            ev_id, ev_type, data_lines = None, "message", []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "id":
            try:
                ev_id = int(value)
            except ValueError:
                ev_id = None
        elif field == "event":
            ev_type = value
        elif field == "data":
            data_lines.append(value)
