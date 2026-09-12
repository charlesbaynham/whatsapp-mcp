"""Forward WhatsApp messages into Hindsight memory. No LLM involved here.

Consumes the bridge's event stream from a persisted cursor and retains each
message into Hindsight, then advances the cursor. A Hindsight outage means
replay on recovery, never loss.

A chat is retained as a *running transcript*, not as isolated messages: each
message is appended to the chat's current Hindsight document (update_mode
"append", which reprocesses only the new chunk), so facts are extracted with
the conversation around them. A document is closed and a new one started when
the chat has been idle for `FORWARDER_SESSION_GAP_DAYS`, or once it holds
`FORWARDER_MAX_MESSAGES`, so a busy chat never grows without bound.

Configuration (environment):
  WHATSAPP_BRIDGE_URL      unix:/run/whatsapp/bridge.sock or http://host:port
  HINDSIGHT_URL            Hindsight API base URL, e.g. https://api.hindsight.vectorize.io
  HINDSIGHT_API_KEY        Bearer token (optional for an unauthenticated local instance)
  HINDSIGHT_BANK           Memory bank id (default: whatsapp)
  HINDSIGHT_RETAIN_PATH    Path template (default: /v1/default/banks/{bank}/memories)
  HINDSIGHT_CONTEXT_EXTRA  Sentence appended to every generated context
  FORWARDER_OWNER_NAME     How the account owner is named in transcripts (default: Me)
  FORWARDER_ACCOUNT_LABEL  Which WhatsApp account this is, named in the context
  FORWARDER_SESSION_GAP_DAYS  Idle gap that starts a new document (default: 7)
  FORWARDER_MAX_MESSAGES   Messages per document before rollover (default: 200)
  FORWARDER_CHATS          Comma-separated chat JIDs to forward; empty = every chat
  FORWARDER_INCLUDE_FROM_ME  Forward your own messages too (default: true)
  FORWARDER_STATE_DIR      Where the cursor and per-chat sessions live
                           (default: $STATE_DIRECTORY or .)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    context_extra: str = ""
    owner_name: str = "Me"
    account_label: str = ""
    session_gap_days: float = 7.0
    max_messages: int = 200
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
            context_extra=env.get("HINDSIGHT_CONTEXT_EXTRA", "").strip(),
            owner_name=env.get("FORWARDER_OWNER_NAME", "Me").strip() or "Me",
            account_label=env.get("FORWARDER_ACCOUNT_LABEL", "").strip(),
            session_gap_days=float(env.get("FORWARDER_SESSION_GAP_DAYS") or 7),
            max_messages=int(env.get("FORWARDER_MAX_MESSAGES") or 200),
            chats=chats,
            include_from_me=env.get("FORWARDER_INCLUDE_FROM_ME", "true").lower() not in ("0", "false", "no"),
            state_dir=Path(env.get("FORWARDER_STATE_DIR") or env.get("STATE_DIRECTORY") or "."),
        )

    @property
    def retain_url(self) -> str:
        return self.hindsight_url + self.retain_path.format(bank=self.bank)

    @property
    def session_gap(self) -> timedelta:
        return timedelta(days=self.session_gap_days)


@dataclass
class Session:
    """The Hindsight document a chat is currently being appended to."""

    document_id: str
    last_timestamp: str = ""
    messages: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {"document_id": self.document_id, "last_timestamp": self.last_timestamp, "messages": self.messages}


class State:
    """Cursor and per-chat sessions, persisted together and atomically.

    They must move as one: the cursor says what has been sent, the sessions
    say what it was appended to, and an append replayed against the wrong
    document duplicates text rather than replacing it.
    """

    def __init__(self, path: Path):
        self.path = path
        self.cursor = 0
        self.sessions: Dict[str, Session] = {}
        self.load()

    def load(self) -> None:
        try:
            raw = json.loads(self.path.read_text())
        except FileNotFoundError:
            return
        except (ValueError, OSError):
            log.warning("state file %s is unreadable; starting from scratch", self.path)
            return
        self.cursor = int(raw.get("cursor") or 0)
        self.sessions = {
            jid: Session(
                document_id=str(s.get("document_id") or ""),
                last_timestamp=str(s.get("last_timestamp") or ""),
                messages=int(s.get("messages") or 0),
            )
            for jid, s in (raw.get("sessions") or {}).items()
            if s.get("document_id")
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "cursor": self.cursor,
            "sessions": {jid: s.as_dict() for jid, s in self.sessions.items()},
        }))
        os.replace(tmp, self.path)


def parse_timestamp(value: str) -> Optional[datetime]:
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def wants(cfg: Config, ev: Event) -> bool:
    if ev.type != "message.new":
        return False
    if cfg.chats and ev.chat_jid not in cfg.chats:
        return False
    if not cfg.include_from_me and ev.payload.get("is_from_me"):
        return False
    return True


def speaker(cfg: Config, msg: Dict[str, Any]) -> str:
    if msg.get("is_from_me"):
        return cfg.owner_name
    return msg.get("sender_name") or msg.get("sender") or "unknown"


def body(msg: Dict[str, Any]) -> str:
    text = (msg.get("content") or "").strip()
    if msg.get("transcript"):
        return f"(voice note) {msg['transcript'].strip()}"
    if msg.get("media_type"):
        note = f"[{msg['media_type']}"
        if msg.get("filename"):
            note += f": {msg['filename']}"
        if msg.get("media_type") == "audio" and msg.get("transcription_status"):
            note += f", transcription {msg['transcription_status']}"
        note += "]"
        return f"{note} {text}".strip()
    return text


def transcript_line(cfg: Config, msg: Dict[str, Any]) -> str:
    """One line of the running transcript, dated so the extractor can place it."""
    ts = parse_timestamp(str(msg.get("timestamp") or ""))
    when = ts.strftime("%Y-%m-%d %H:%M") if ts else "unknown time"
    return f"[{when}] {speaker(cfg, msg)}: {body(msg)}"


def is_group(chat_jid: str) -> bool:
    return chat_jid.endswith("@g.us")


def describe(cfg: Config, msg: Dict[str, Any]) -> str:
    """The context Hindsight extracts facts through — who is talking, and where.

    Hindsight injects this straight into the extraction prompt, so it names
    the participants rather than merely labelling the source "whatsapp".
    """
    chat_jid = str(msg.get("chat_jid") or "")
    chat = str(msg.get("chat_name") or "").strip() or chat_jid or "an unnamed chat"
    account = f" on {cfg.account_label}" if cfg.account_label else ""
    if is_group(chat_jid):
        where = f'WhatsApp group chat "{chat}"{account}, with several participants'
    else:
        other = chat if chat != chat_jid else chat_jid.split("@")[0]
        where = f"WhatsApp conversation{account} between {cfg.owner_name} and {other}"
    parts = [
        f"{where}.",
        f'Each line is "[date time] Speaker: message"; "{cfg.owner_name}" is the owner of this WhatsApp account.',
        "Voice notes appear as their transcript; other attachments appear as a bracketed note.",
    ]
    if cfg.context_extra:
        parts.append(cfg.context_extra)
    return " ".join(parts)


def document_id(chat_jid: str, msg: Dict[str, Any]) -> str:
    ts = parse_timestamp(str(msg.get("timestamp") or "")) or datetime.now(timezone.utc)
    return f"whatsapp:{chat_jid}:{ts.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def session_for(cfg: Config, state: State, ev: Event) -> tuple[Session, bool]:
    """The document this message belongs to, and whether it starts a new one.

    A new document when the chat has been idle longer than the session gap, or
    when the current one is full: a year-long chat must not become one
    ever-growing document, and a fortnight's silence is a new conversation.
    """
    msg = ev.payload
    chat_jid = ev.chat_jid
    current = state.sessions.get(chat_jid)
    if current:
        previous = parse_timestamp(current.last_timestamp)
        now = parse_timestamp(str(msg.get("timestamp") or ""))
        idle = previous and now and (now - previous) >= cfg.session_gap
        if not idle and current.messages < cfg.max_messages:
            return current, False
        log.info("rolling over %s (%s)", chat_jid, "idle" if idle else f"{current.messages} messages")
    return Session(document_id=document_id(chat_jid, msg)), True


def retain_item(cfg: Config, ev: Event, session: Session, started: bool) -> Dict[str, Any]:
    msg = ev.payload
    item: Dict[str, Any] = {
        "content": transcript_line(cfg, msg),
        "context": describe(cfg, msg),
        "document_id": session.document_id,
        "metadata": {
            "source": "whatsapp",
            "chat_jid": str(msg.get("chat_jid") or ""),
            "chat_name": str(msg.get("chat_name") or ""),
            "sender": str(msg.get("sender") or ""),
            "message_id": str(msg.get("message_id") or ""),
            "media_type": str(msg.get("media_type") or ""),
            "event_id": str(ev.id),
        },
        "tags": ["source:whatsapp", f"chat:{msg.get('chat_jid')}"],
    }
    if not started:
        item["update_mode"] = "append"
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
    """Retry forever with exponential backoff. State must not advance until this returns."""
    delay = 2.0
    while True:
        try:
            hs.retain(items)
            return
        except (httpx.HTTPError, RuntimeError) as e:
            log.warning("retain failed (%s); retrying in %.0fs", e, delay)
            sleep(delay)
            delay = min(delay * 2, max_delay)


def forward(cfg: Config, hs: Hindsight, state: State, ev: Event) -> None:
    session, started = session_for(cfg, state, ev)
    retain_with_retry(hs, [retain_item(cfg, ev, session, started)])
    session.messages += 1
    session.last_timestamp = str(ev.payload.get("timestamp") or session.last_timestamp)
    state.sessions[ev.chat_jid] = session
    log.info("retained event %d into %s (%d messages)", ev.id, session.document_id, session.messages)


def run(cfg: Config, wa: WhatsAppClient, hs: Hindsight, state: State, *, events: Optional[Iterable[Event]] = None) -> None:
    log.info("forwarding from event %d to %s (chats: %s)", state.cursor, hs.url, ", ".join(sorted(cfg.chats)) or "all")
    stream = events if events is not None else wa.events(
        since=state.cursor, types=["message.new"], include_from_me=cfg.include_from_me)
    for ev in stream:
        if wants(cfg, ev):
            forward(cfg, hs, state, ev)
        state.cursor = ev.id
        state.save()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    cfg = Config.from_env(dict(os.environ))
    if not cfg.hindsight_url:
        log.warning("HINDSIGHT_URL is not set; idling instead of forwarding")
        signal.pause()
        return
    wa = WhatsAppClient()
    hs = Hindsight(cfg)
    state = State(cfg.state_dir / "state.json")
    while True:
        try:
            run(cfg, wa, hs, state)
        except BridgeError as e:
            log.warning("bridge error: %s; retrying in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
