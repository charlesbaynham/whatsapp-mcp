"""Forward WhatsApp messages into Hindsight memory. No LLM involved here.

Consumes the bridge's event stream from a persisted cursor and retains each
message into Hindsight, then advances the cursor. A Hindsight outage means
replay on recovery, never loss.

A chat is retained as a *running transcript*, not as isolated messages: each
message is appended to the chat's current Hindsight document (update_mode
"append", which reprocesses only the new chunk), so facts are extracted with
the conversation around them. A document grows for as long as the chat does;
append costs the new chunk, not the document, so there is no reason to close
one. Losing the state opens a fresh document per chat and carries on.

Every transcript line names its speaker as the sender's phone number followed
by their name in quotes when the bridge knows one (`447700900123 "Parav
Pandya"`), so the extractor can tell people apart even when two share a name
or one has none; the account owner is named as FORWARDER_OWNER_NAME alone.

`python -m hindsight_forwarder.main reingest [--dry-run]` deletes this
account's WhatsApp documents from the bank and resets the state so the next
run replays the bridge's event log from the beginning. Stop the forwarder
first: a running one would keep appending to documents that no longer exist.

Configuration (environment):
  WHATSAPP_BRIDGE_URL      unix:/run/whatsapp/bridge.sock or http://host:port
  HINDSIGHT_URL            Hindsight API base URL, e.g. https://api.hindsight.vectorize.io
  HINDSIGHT_API_KEY        Bearer token (optional for an unauthenticated local instance)
  HINDSIGHT_BANK           Memory bank id (default: whatsapp)
  HINDSIGHT_RETAIN_PATH    Path template (default: /v1/default/banks/{bank}/memories)
  HINDSIGHT_DOCUMENTS_PATH Path template (default: /v1/default/banks/{bank}/documents)
  HINDSIGHT_CONTEXT_EXTRA  Sentence appended to every generated context
  FORWARDER_OWNER_NAME     How the account owner is named in transcripts (default: Me)
  FORWARDER_ACCOUNT_LABEL  Which WhatsApp account this is, named in the context
  FORWARDER_ACCOUNT        Slug for the same account, tagged as account:<slug>
  FORWARDER_TAGS           Extra comma-separated tags on every memory
  FORWARDER_CHATS          Comma-separated chat JIDs to forward; empty = every chat
  FORWARDER_INCLUDE_FROM_ME  Forward your own messages too (default: true)
  FORWARDER_STATE_DIR      Where the cursor and per-chat open documents live
                           (default: $STATE_DIRECTORY or .)
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set
from urllib.parse import quote

import httpx

from whatsapp_client import BridgeError, Event, WhatsAppClient

log = logging.getLogger("hindsight-forwarder")


@dataclass
class Config:
    hindsight_url: str
    api_key: str = ""
    bank: str = "whatsapp"
    retain_path: str = "/v1/default/banks/{bank}/memories"
    documents_path: str = "/v1/default/banks/{bank}/documents"
    context_extra: str = ""
    owner_name: str = "Me"
    account_label: str = ""
    account: str = ""
    extra_tags: List[str] = field(default_factory=list)
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
            documents_path=env.get("HINDSIGHT_DOCUMENTS_PATH", "/v1/default/banks/{bank}/documents"),
            context_extra=env.get("HINDSIGHT_CONTEXT_EXTRA", "").strip(),
            owner_name=env.get("FORWARDER_OWNER_NAME", "Me").strip() or "Me",
            account_label=env.get("FORWARDER_ACCOUNT_LABEL", "").strip(),
            account=env.get("FORWARDER_ACCOUNT", "").strip(),
            extra_tags=[t.strip() for t in env.get("FORWARDER_TAGS", "").split(",") if t.strip()],
            chats=chats,
            include_from_me=env.get("FORWARDER_INCLUDE_FROM_ME", "true").lower() not in ("0", "false", "no"),
            state_dir=Path(env.get("FORWARDER_STATE_DIR") or env.get("STATE_DIRECTORY") or "."),
        )

    @property
    def retain_url(self) -> str:
        return self.hindsight_url + self.retain_path.format(bank=self.bank)

    @property
    def documents_url(self) -> str:
        return self.hindsight_url + self.documents_path.format(bank=self.bank)

    def tags_for(self, chat_jid: str) -> List[str]:
        """Every memory says where it came from, at three widths.

        A bank shared with other sources needs the source itself tagged, not
        just described: recall filters on tags, and metadata does not filter.
        """
        tags = ["source:whatsapp"]
        if self.account:
            tags.append(f"account:{self.account}")
        tags.extend(self.extra_tags)
        tags.append(f"chat:{chat_jid}")
        return tags


class State:
    """Cursor and per-chat open documents, persisted together and atomically.

    They must move as one: the cursor says what has been sent, the documents
    say what it was appended to, and an append replayed against the wrong
    document duplicates text rather than replacing it.
    """

    def __init__(self, path: Path):
        self.path = path
        self.cursor = 0
        self.documents: Dict[str, str] = {}
        self.load()

    def load(self) -> None:
        try:
            raw = json.loads(self.path.read_text())
        except FileNotFoundError:
            return
        except ValueError:
            log.warning("state file %s is corrupt; starting from scratch", self.path)
            return
        self.cursor = int(raw.get("cursor") or 0)
        self.documents = {jid: str(doc) for jid, doc in (raw.get("documents") or {}).items() if doc}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"cursor": self.cursor, "documents": self.documents}))
        os.replace(tmp, self.path)


def parse_timestamp(value: str) -> Optional[datetime]:
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


FORWARDED_EVENT_TYPES = ["message.new", "poll.vote"]


def wants(cfg: Config, ev: Event) -> bool:
    if ev.type not in FORWARDED_EVENT_TYPES:
        return False
    if cfg.chats and ev.chat_jid not in cfg.chats:
        return False
    if not cfg.include_from_me and ev.payload.get("is_from_me"):
        return False
    return True


def speaker(cfg: Config, msg: Dict[str, Any]) -> str:
    """Who said it: the owner by name; anyone else as `number "Name"`.

    The number is the stable identity (two contacts can share a name, and a
    name can change or be unknown); the quoted name is what the extractor
    should call them. The bridge sends sender_name equal to sender when it
    knows no name, so that case degrades to the bare number.
    """
    if msg.get("is_from_me"):
        return cfg.owner_name
    sender = str(msg.get("sender") or "").strip()
    name = str(msg.get("sender_name") or "").strip()
    if name and name != sender:
        return f'{sender} "{name}"' if sender else name
    return sender or "unknown"


def describe_poll_results(results: Any, total_voters: Any) -> str:
    tally = ", ".join(f"{r.get('option')} {r.get('votes', 0)}" for r in (results or []))
    n = int(total_voters or 0)
    return f"{tally} ({n} voter{'s' if n != 1 else ''})"


def body(msg: Dict[str, Any]) -> str:
    text = (msg.get("content") or "").strip()
    if msg.get("transcript"):
        return f"(voice note) {msg['transcript'].strip()}"
    if msg.get("poll_vote"):
        vote = msg["poll_vote"]
        outcome = describe_poll_results(vote.get("results"), vote.get("total_voters"))
        if not vote.get("selected"):
            return f"withdrew their vote on the poll \"{vote.get('question', '')}\" (now {outcome})"
        chosen = ", ".join(f'"{s}"' for s in vote["selected"])
        return f"voted {chosen} on the poll \"{vote.get('question', '')}\" (now {outcome})"
    if msg.get("poll"):
        poll = msg["poll"]
        line = f"(poll) {poll.get('question', '')} — options: {' / '.join(poll.get('options') or [])}"
        if poll.get("selectable_count") == 0:
            line += " (pick any number)"
        return line
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
        'Each line is "[date time] Speaker: message". A speaker is the sender\'s phone number followed by '
        'their name in quotes when it is known, e.g. 447700900123 "Parav Pandya", or the bare number when it is not; '
        f'"{cfg.owner_name}" is the owner of this WhatsApp account.',
        "Voice notes appear as their transcript; other attachments appear as a bracketed note. "
        "A poll appears as its question and options, and each vote as a line saying who chose what.",
    ]
    if cfg.context_extra:
        parts.append(cfg.context_extra)
    return " ".join(parts)


def new_document_id(chat_jid: str, *, now=None) -> str:
    """A document id no later run can reproduce.

    The random suffix is the whole point: if the id were derived from the chat
    and a message, a wiped state replaying the same events would rebuild the
    previous id and, being a first write, REPLACE that document — deleting a
    history the event log can no longer supply. A fresh id can only ever be
    created, so a lost state costs a seam in the bank, never a deletion.
    """
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"whatsapp:{chat_jid}:{stamp}-{secrets.token_hex(3)}"


def document_for(state: State, ev: Event) -> tuple[str, bool]:
    """The document this message is appended to, and whether this opens it."""
    document = state.documents.get(ev.chat_jid)
    if document:
        return document, False
    document = new_document_id(ev.chat_jid)
    log.info("opening %s for %s", document, ev.chat_jid)
    return document, True


def retain_item(cfg: Config, ev: Event, document: str, opened: bool) -> Dict[str, Any]:
    msg = ev.payload
    item: Dict[str, Any] = {
        "content": transcript_line(cfg, msg),
        "context": describe(cfg, msg),
        "document_id": document,
        "metadata": {
            "source": "whatsapp",
            "chat_jid": str(msg.get("chat_jid") or ""),
            "chat_name": str(msg.get("chat_name") or ""),
            "sender": str(msg.get("sender") or ""),
            "message_id": str(msg.get("message_id") or ""),
            "account": cfg.account,
            "media_type": str(msg.get("media_type") or ""),
            "event_id": str(ev.id),
        },
        "tags": cfg.tags_for(str(msg.get("chat_jid") or "")),
    }
    if not opened:
        item["update_mode"] = "append"
    if msg.get("timestamp"):
        item["timestamp"] = msg["timestamp"]
    return item


class Hindsight:
    def __init__(self, cfg: Config, transport: Optional[httpx.BaseTransport] = None):
        headers = {"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {}
        self.url = cfg.retain_url
        self.documents_url = cfg.documents_url
        self._http = httpx.Client(headers=headers, timeout=60.0, transport=transport)

    def retain(self, items: List[Dict[str, Any]]) -> None:
        resp = self._http.post(self.url, json={"items": items})
        if resp.status_code >= 300:
            raise RuntimeError(f"Hindsight retain failed: HTTP {resp.status_code} {resp.text[:300]}")

    def list_documents(self, q: str, page_size: int = 100) -> Iterator[Dict[str, Any]]:
        """Every document whose id contains q, walking the offset pagination."""
        offset = 0
        while True:
            resp = self._http.get(self.documents_url, params={"q": q, "limit": page_size, "offset": offset})
            if resp.status_code >= 300:
                raise RuntimeError(f"Hindsight list documents failed: HTTP {resp.status_code} {resp.text[:300]}")
            items = resp.json().get("items") or []
            yield from items
            offset += len(items)
            if len(items) < page_size:
                return

    def delete_document(self, document_id: str) -> None:
        resp = self._http.delete(f"{self.documents_url}/{quote(document_id, safe='')}")
        if resp.status_code >= 300 and resp.status_code != 404:
            raise RuntimeError(f"Hindsight delete document failed: HTTP {resp.status_code} {resp.text[:300]}")


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
    document, opened = document_for(state, ev)
    retain_with_retry(hs, [retain_item(cfg, ev, document, opened)])
    state.documents[ev.chat_jid] = document
    log.info("retained event %d into %s", ev.id, document)


def owns_document(cfg: Config, doc: Dict[str, Any]) -> bool:
    """Whether this forwarder wrote doc: a WhatsApp document from this account.

    Two bridges can share a bank (metadata.account tells them apart), and a
    bank can hold documents from other sources entirely; neither is ours to
    delete. A forwarder with no account configured owns every WhatsApp
    document, which is what its own writes were tagged as.
    """
    if not str(doc.get("id") or "").startswith("whatsapp:"):
        return False
    meta = doc.get("document_metadata") or doc.get("metadata") or {}
    if meta.get("source", "whatsapp") != "whatsapp":
        return False
    return not cfg.account or meta.get("account") == cfg.account


def reingest(cfg: Config, hs: Hindsight, state: State, *, dry_run: bool = False) -> List[str]:
    """Delete this account's documents and rewind, so the next run rebuilds them.

    The bridge's event log is the source of truth and is never pruned, so
    replaying it from event 0 recreates every document with whatever the
    forwarder now renders (sender names, say). Each chat opens a fresh
    document id, which is why the old ones must go first: they would
    otherwise linger as duplicates.

    Must run with the forwarder stopped, since a running one would carry on
    appending to the documents this deletes.
    """
    mine = sorted(str(d["id"]) for d in hs.list_documents("whatsapp:") if owns_document(cfg, d))
    for document in mine:
        if dry_run:
            log.info("would delete %s", document)
            continue
        hs.delete_document(document)
        log.info("deleted %s", document)
    if dry_run:
        log.info("dry run: %d documents would be deleted and the cursor reset from %d", len(mine), state.cursor)
        return mine
    state.cursor = 0
    state.documents = {}
    state.save()
    log.info("deleted %d documents; state reset, next run replays the event log from the start", len(mine))
    return mine


def run(cfg: Config, wa: WhatsAppClient, hs: Hindsight, state: State, *, events: Optional[Iterable[Event]] = None) -> None:
    log.info("forwarding from event %d to %s (chats: %s)", state.cursor, hs.url, ", ".join(sorted(cfg.chats)) or "all")
    stream = events if events is not None else wa.events(
        since=state.cursor, types=FORWARDED_EVENT_TYPES, include_from_me=cfg.include_from_me)
    for ev in stream:
        if wants(cfg, ev):
            forward(cfg, hs, state, ev)
        state.cursor = ev.id
        state.save()


def main(argv: Optional[List[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    args = sys.argv[1:] if argv is None else argv
    cfg = Config.from_env(dict(os.environ))
    if args and args[0] == "reingest":
        if not cfg.hindsight_url:
            log.error("HINDSIGHT_URL is not set")
            raise SystemExit(1)
        reingest(cfg, Hindsight(cfg), State(cfg.state_dir / "state.json"), dry_run="--dry-run" in args[1:])
        return
    if args:
        log.error("unknown arguments %s (usage: hindsight_forwarder.main [reingest [--dry-run]])", args)
        raise SystemExit(2)
    if not cfg.hindsight_url:
        log.warning("HINDSIGHT_URL is not set; idling instead of forwarding")
        signal.pause()
        return
    wa = WhatsAppClient()
    hs = Hindsight(cfg)
    state = State(cfg.state_dir / "state.json")
    # Prove the state is writable before consuming a single event: retaining
    # first and failing to record it afterwards replays on restart, and with
    # append that duplicates rather than replaces.
    try:
        state.save()
    except OSError as e:
        log.error("state directory %s is not usable (%s); refusing to forward", cfg.state_dir, e)
        raise SystemExit(1)
    while True:
        try:
            run(cfg, wa, hs, state)
        except BridgeError as e:
            log.warning("bridge error: %s; retrying in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
