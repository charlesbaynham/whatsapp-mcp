"""Forward WhatsApp messages into Hindsight memory. No LLM involved.

Consumes the bridge's event stream from a persisted cursor, retains each
message via Hindsight's REST API, and only then advances the cursor. A
Hindsight outage therefore means replay on recovery, never loss.

Configuration (environment):
  WHATSAPP_BRIDGE_URL      unix:/run/whatsapp/bridge.sock or http://host:port
  HINDSIGHT_URL            Hindsight API base URL, e.g. https://hindsight.example
  HINDSIGHT_API_KEY        Bearer token (optional for an unauthenticated local instance)
  HINDSIGHT_BANK           Memory bank id (default: whatsapp)
  HINDSIGHT_RETAIN_PATH    Path template (default: /v1/default/banks/{bank}/memories)
  HINDSIGHT_CONTEXT        Memory context/category (default: whatsapp)
  FORWARDER_CHATS          Comma-separated chat JIDs to forward; empty = every chat
  FORWARDER_INCLUDE_FROM_ME  Forward your own messages too (default: true)
  FORWARDER_STATE_DIR      Where the cursor file lives (default: $STATE_DIRECTORY or .)
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import httpx

from whatsapp_client import BridgeError, Event, WhatsAppClient

log = logging.getLogger("hindsight-forwarder")


@dataclass
class Config:
    hindsight_url: str
    api_key: str = ""
    bank: str = "whatsapp"
    retain_path: str = "/v1/default/banks/{bank}/memories"
    context: str = "whatsapp"
    chats: Set[str] = field(default_factory=set)
    include_from_me: bool = True
    state_dir: Path = Path(".")

    @classmethod
    def from_env(cls, env: Dict[str, str]) -> "Config":
        chats = {c.strip() for c in env.get("FORWARDER_CHATS", "").split(",") if c.strip()}
        return cls(
            hindsight_url=env.get("HINDSIGHT_URL", "").rstrip("/"),
            api_key=env.get("HINDSIGHT_API_KEY", ""),
            bank=env.get("HINDSIGHT_BANK", "whatsapp"),
            retain_path=env.get("HINDSIGHT_RETAIN_PATH", "/v1/default/banks/{bank}/memories"),
            context=env.get("HINDSIGHT_CONTEXT", "whatsapp"),
            chats=chats,
            include_from_me=env.get("FORWARDER_INCLUDE_FROM_ME", "true").lower() not in ("0", "false", "no"),
            state_dir=Path(env.get("FORWARDER_STATE_DIR") or env.get("STATE_DIRECTORY") or "."),
        )

    @property
    def retain_url(self) -> str:
        return self.hindsight_url + self.retain_path.format(bank=self.bank)


class Cursor:
    """The last event id fully handed to Hindsight, persisted atomically."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> int:
        try:
            return int(self.path.read_text().strip() or 0)
        except FileNotFoundError:
            return 0
        except ValueError:
            log.warning("cursor file %s is corrupt; starting from 0", self.path)
            return 0

    def save(self, value: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(str(value))
        os.replace(tmp, self.path)


def wants(cfg: Config, ev: Event) -> bool:
    if ev.type != "message.new":
        return False
    if cfg.chats and ev.chat_jid not in cfg.chats:
        return False
    if not cfg.include_from_me and ev.payload.get("is_from_me"):
        return False
    return True


def render(msg: Dict[str, Any]) -> str:
    """One memory per message, readable on its own."""
    who = "Me" if msg.get("is_from_me") else (msg.get("sender_name") or msg.get("sender") or "unknown")
    where = msg.get("chat_name") or msg.get("chat_jid") or "unknown chat"
    body = (msg.get("content") or "").strip()
    if msg.get("transcript"):
        body = f"[voice note] {msg['transcript'].strip()}"
    elif msg.get("media_type"):
        note = f"[{msg['media_type']}"
        if msg.get("filename"):
            note += f": {msg['filename']}"
        if msg.get("media_type") == "audio" and msg.get("transcription_status"):
            note += f", transcription {msg['transcription_status']}"
        note += "]"
        body = f"{note} {body}".strip()
    return f"WhatsApp message from {who} in {where}: {body}"


def retain_item(cfg: Config, ev: Event) -> Dict[str, Any]:
    msg = ev.payload
    item: Dict[str, Any] = {
        "content": render(msg),
        "context": cfg.context,
        "document_id": f"whatsapp:{msg.get('chat_jid')}:{msg.get('message_id')}",
        "metadata": {
            "source": "whatsapp",
            "chat_jid": str(msg.get("chat_jid") or ""),
            "chat_name": str(msg.get("chat_name") or ""),
            "sender": str(msg.get("sender") or ""),
            "message_id": str(msg.get("message_id") or ""),
            "media_type": str(msg.get("media_type") or ""),
            "event_id": str(ev.id),
        },
        "tags": [f"chat:{msg.get('chat_jid')}"],
    }
    if msg.get("timestamp"):
        item["timestamp"] = msg["timestamp"]
    return item


class Hindsight:
    def __init__(self, cfg: Config, transport: Optional[httpx.BaseTransport] = None):
        headers = {"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {}
        self.url = cfg.retain_url
        self._http = httpx.Client(headers=headers, timeout=60.0, transport=transport)

    def retain(self, items: List[Dict[str, Any]]) -> None:
        resp = self._http.post(self.url, json={"items": items})
        if resp.status_code >= 300:
            raise RuntimeError(f"Hindsight retain failed: HTTP {resp.status_code} {resp.text[:300]}")


def retain_with_retry(hs: Hindsight, items: List[Dict[str, Any]], *, sleep=time.sleep, max_delay: float = 300.0) -> None:
    """Retry forever with exponential backoff. The cursor must not advance until this returns."""
    delay = 2.0
    while True:
        try:
            hs.retain(items)
            return
        except (httpx.HTTPError, RuntimeError) as e:
            log.warning("retain failed (%s); retrying in %.0fs", e, delay)
            sleep(delay)
            delay = min(delay * 2, max_delay)


def run(cfg: Config, wa: WhatsAppClient, hs: Hindsight, cursor: Cursor, *, events: Optional[Iterable[Event]] = None, sleep=time.sleep) -> None:
    since = cursor.load()
    log.info("forwarding from event %d to %s (chats: %s)", since, hs.url, ", ".join(sorted(cfg.chats)) or "all")
    stream = events if events is not None else wa.events(
        since=since, types=["message.new"], include_from_me=cfg.include_from_me)
    for ev in stream:
        if wants(cfg, ev):
            retain_with_retry(hs, [retain_item(cfg, ev)], sleep=sleep)
            log.info("retained event %d (%s in %s)", ev.id, ev.payload.get("message_id"), ev.chat_jid)
        cursor.save(ev.id)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    cfg = Config.from_env(dict(os.environ))
    if not cfg.hindsight_url:
        log.warning("HINDSIGHT_URL is not set; idling instead of forwarding")
        signal.pause()
        return
    wa = WhatsAppClient()
    hs = Hindsight(cfg)
    cursor = Cursor(cfg.state_dir / "cursor")
    while True:
        try:
            run(cfg, wa, hs, cursor)
        except BridgeError as e:
            log.warning("bridge error: %s; retrying in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
