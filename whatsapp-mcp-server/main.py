"""WhatsApp MCP server: MCP tools over the whatsapp-bridge REST API.

This process is a thin client. It never opens the message database, never
touches the store directory and never runs ffmpeg; the bridge does all of
that behind its Unix socket (or TCP loopback port on a laptop).
"""

import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from starlette.requests import Request
from starlette.responses import JSONResponse

from mcp.server.fastmcp import FastMCP
from whatsapp_client import BridgeError, BridgeUnavailable, WhatsAppClient

MCP_HOST = os.environ.get("MCP_HOST", "127.0.0.1")
MCP_PORT = int(os.environ.get("MCP_PORT", "8000"))
MCP_TRANSPORT = os.environ.get("MCP_TRANSPORT", "stdio")

# Laptop default is the bridge's TCP loopback port; the hosted deployment sets
# WHATSAPP_BRIDGE_URL=unix:/run/whatsapp/bridge.sock.
wa = WhatsAppClient(os.environ.get("WHATSAPP_BRIDGE_URL", "http://127.0.0.1:8080"))

mcp = FastMCP("whatsapp", host=MCP_HOST, port=MCP_PORT)


# Mean gap the bridge holds between first contacts, for the warning below. The
# bridge's own estimate is what gets reported; this only sets expectations.
NEW_CONTACT_GAP_NOTE = "spaced about 30 minutes apart on average"


def _rough_duration(seconds: float) -> str:
    if seconds < 60:
        return "under a minute"
    if seconds < 3600:
        return f"about {round(seconds / 60)} min"
    return f"about {seconds / 3600:.1f} h"


def _note_new_contact(out: Dict[str, Any]) -> Dict[str, Any]:
    """Add a warning to a send reply the bridge has marked `new_contact`.

    A first contact does not go out after the usual half-minute: the bridge
    holds it in a separate queue, spaced far more widely, because starting
    conversations with a run of strangers is the single strongest spam signal
    WhatsApp acts on. Whoever asked for the send will not expect that, so the
    calling agent is told to say so.
    """
    if not out.get("new_contact"):
        return out
    wait = _rough_duration(float(out.get("estimated_wait_seconds") or 0))
    out["warning"] = (
        "This recipient has never been messaged from this account, so the bridge is "
        f"holding the message in its new-contact queue (first contacts are {NEW_CONTACT_GAP_NOTE}) "
        f"before it joins the normal send queue. Expected to go out in {wait}. "
        "Tell the user the message is queued and roughly when it will be sent; "
        "poll get_send_status with the id if they need to know once it has gone."
    )
    return out


def _result(call, **extra: Any) -> Dict[str, Any]:
    """Run a bridge call and fold any error into a {success, message} dict."""
    try:
        out = call()
    except BridgeError as e:
        return {"success": False, "message": str(e), **extra}
    if isinstance(out, dict):
        return out
    return {"success": True, "message": "ok", **extra}


@mcp.custom_route("/health", methods=["GET"])
async def health(_request: Request) -> JSONResponse:
    """Liveness probe: fails only when the bridge process is unreachable.

    Pairing is an operational state, not a deploy outcome: the deploy loop
    treats a failed health check soon after a deploy as a bad template and
    rolls back, so an unpaired or logged-out bridge must still report
    healthy here rather than triggering a destroy/recreate loop.
    """
    try:
        status = wa.status()
    except BridgeError as e:
        return JSONResponse({"status": "error", "reason": f"bridge unreachable: {e}"}, status_code=503)
    return JSONResponse({
        "status": "ok",
        "paired": bool(status.get("logged_in")),
        "connected": bool(status.get("connected")),
    })


@mcp.tool()
def search_contacts(query: str) -> List[Dict[str, Any]]:
    """Search WhatsApp contacts by name or phone number.

    Args:
        query: Search term to match against contact names or phone numbers
    """
    return wa.search_contacts(query)


@mcp.tool()
def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Dict[str, Any]]:
    """Get WhatsApp messages matching specified criteria with optional context.

    Voice notes carry the spoken text in `transcript` (with
    `transcription_status`) while `content` stays as WhatsApp delivered it,
    so the two are distinguishable; the audio itself is fetchable via
    download_media. A poll has media_type "poll", its question as `content`
    and a `poll` object with the options and the current tally (see
    get_poll_results for the per-voter detail).

    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
    """
    return wa.list_messages(
        after=after, before=before, sender=sender_phone_number, chat_jid=chat_jid, query=query,
        limit=limit, page=page, include_context=include_context,
        context_before=context_before, context_after=context_after,
    )


@mcp.tool()
def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
    unread_only: bool = False
) -> List[Dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.

    Args:
        query: Optional search term to filter chats by name or JID
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
        include_last_message: Whether to include the last message in each chat (default True)
        sort_by: Field to sort results by, either "last_active" or "name" (default "last_active")
        unread_only: If True, only return chats that have unread incoming messages (default False)
    """
    return wa.list_chats(query=query, limit=limit, page=page, include_last_message=include_last_message,
                         sort_by=sort_by, unread_only=unread_only)


@mcp.tool()
def list_unread_chats(limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """List WhatsApp chats that currently have unread incoming messages.

    Args:
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    return wa.list_unread_chats(limit, page)


@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Dict[str, Any]]:
    """Get WhatsApp chat metadata by JID.

    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    return wa.get_chat(chat_jid, include_last_message)


@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Dict[str, Any]]:
    """Get WhatsApp chat metadata by sender phone number.

    Args:
        sender_phone_number: The phone number to search for
    """
    return wa.get_direct_chat_by_phone(sender_phone_number)


@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    return wa.get_contact_chats(jid, limit, page)


@mcp.tool()
def get_last_interaction(jid: str) -> Optional[str]:
    """Get most recent WhatsApp message involving the contact.

    Args:
        jid: The JID of the contact to search for
    """
    msg = wa.get_last_interaction(jid)
    if msg is None:
        return None
    prefix = ""
    if msg.get("media_type"):
        prefix = f"[{msg['media_type']} - Message ID: {msg['id']} - Chat JID: {msg['chat_jid']}] "
    body = msg.get("content") or ""
    if msg.get("transcript"):
        body = f"[voice note] {msg['transcript']}"
    if msg.get("poll"):
        body = _describe_poll(msg["poll"])
    return f"[{msg['timestamp']}] Chat: {msg.get('chat_name') or msg['chat_jid']} From: {msg['sender_name']}: {prefix}{body}\n"


@mcp.tool()
def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> Optional[Dict[str, Any]]:
    """Get context around a specific WhatsApp message.

    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    return wa.get_message_context(message_id, before, after)


@mcp.tool()
def send_message(
    recipient: str,
    message: str,
    block: bool = False
) -> Dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.

    Sending is rate limited and asynchronous: this returns as soon as the bridge
    has queued the message, with `queued: true` and an `id` you can pass to
    get_send_status. The message goes out a short, randomised delay later — that
    spacing is deliberate, it keeps WhatsApp from unlinking the account for
    behaving like a bulk sender.

    ⚠️ A **first contact** — someone this account has never messaged — is held
    much longer: the bridge queues first contacts separately, spaced about 30
    minutes apart on average, before they join the normal queue. The reply then
    carries `new_contact: true`, `estimated_wait_seconds` and a `warning`. Tell
    the user, so they know the message is not going out immediately.

    Sending more than one message? Use send_messages instead: it queues the
    whole batch through the same path, in one tool call.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send
        block: Wait for the message to actually leave and return the send's own
                 outcome instead of queueing (default False). The wait is the rate
                 limit's and can run to minutes, so only pass True when the
                 outcome must be known before doing anything else. Refused
                 outright for a first contact (the wait would be hours):
                 resubmit without block and poll get_send_status instead.

    Returns:
        A dictionary containing success status, a status message, and the
        submission id
    """
    if not recipient:
        return {"success": False, "message": "Recipient must be provided"}
    return _note_new_contact(_result(lambda: wa.send_message(recipient, message, block=block)))


class OutgoingMessage(BaseModel):
    """One message in a batch: who it goes to and what it says."""

    recipient: str = Field(description=(
        "Phone number with country code and no + or other symbols, or a JID "
        '(e.g. "123456789@s.whatsapp.net", or a group JID like "123456789@g.us")'
    ))
    message: str = Field(description="The message text to send")


@mcp.tool()
def send_messages(messages: List[OutgoingMessage]) -> Dict[str, Any]:
    """Queue several WhatsApp messages in one call, to one recipient or many.

    Same path as send_message — each message joins the bridge's send queue and
    goes out behind the usual randomised spacing, in the order given — but as a
    single tool call, so the whole batch is approved once rather than message by
    message. Prefer it whenever you already know every message you want to send.

    The batch is validated before anything is queued: one bad entry rejects the
    lot and nothing goes out. After that each message is submitted in turn, and
    submission stops at the first failure (a full queue or an unreachable
    bridge) rather than leaving a hole in the middle of a conversation — the
    ones already queued still go out, and the rest are reported as not
    submitted so you can retry them.

    ⚠️ Messages to **first contacts** — people this account has never messaged,
    the typical case when the same note goes to everyone from a group — are
    held far longer than the usual spacing: the bridge queues them separately,
    about 30 minutes apart on average, before they join the normal queue. A
    batch of ten strangers therefore takes hours to go out, not minutes. The
    reply says how many of the batch are first contacts and roughly when the
    last message is expected to leave; tell the user, and do not resend
    anything that is merely still queued.

    Args:
        messages: The messages to queue, each with a recipient and text

    Returns:
        `{success, message, queued, new_contacts, estimated_completion_seconds,
        results}`, where `results` has one entry per input message with its
        `recipient`, submission `id`, outcome and, for a first contact,
        `new_contact: true` and its own `estimated_wait_seconds`. A `warning`
        is present whenever any message is a first contact. Pass an id to
        get_send_status to see how that message actually went.
    """
    try:
        # FastMCP hands over parsed models; a direct caller may pass dicts.
        batch = [m if isinstance(m, OutgoingMessage) else OutgoingMessage.model_validate(m)
                 for m in messages]
    except Exception as e:
        return {"success": False, "message": f"Malformed message in batch: {e}", "queued": 0, "results": []}

    if not batch:
        return {"success": False, "message": "No messages to send", "queued": 0, "results": []}
    for i, item in enumerate(batch):
        if not item.recipient:
            return {"success": False, "message": f"Message {i} has no recipient; nothing was queued",
                    "queued": 0, "results": []}
        if not item.message:
            return {"success": False, "message": f"Message {i} to {item.recipient} is empty; nothing was queued",
                    "queued": 0, "results": []}

    results: List[Dict[str, Any]] = []
    queued = 0
    halted = ""
    for item in batch:
        if halted:
            results.append({"recipient": item.recipient, "success": False,
                            "message": f"Not submitted: {halted}"})
            continue
        out = _result(lambda i=item: wa.send_message(i.recipient, i.message))
        ok = bool(out.get("success"))
        entry = {"recipient": item.recipient, "success": ok,
                 "id": out.get("id"), "message": out.get("message", "")}
        if out.get("new_contact"):
            entry["new_contact"] = True
        if "estimated_wait_seconds" in out:
            entry["estimated_wait_seconds"] = out["estimated_wait_seconds"]
        results.append(entry)
        if ok:
            queued += 1
        else:
            halted = out.get("message") or "an earlier message in the batch failed"

    if queued == len(batch):
        summary = f"Queued {queued} message(s); they go out spaced by the rate limit"
    else:
        summary = f"Queued {queued} of {len(batch)} message(s); submission stopped: {halted}"

    new_contacts = sum(1 for r in results if r.get("new_contact"))
    # Each estimate is from submission, moments apart, so the latest one is
    # when the whole batch is expected to have left.
    completion = max((float(r.get("estimated_wait_seconds") or 0) for r in results if r["success"]), default=0.0)
    reply: Dict[str, Any] = {"success": queued == len(batch), "message": summary, "queued": queued,
                             "new_contacts": new_contacts, "estimated_completion_seconds": int(completion),
                             "results": results}
    if new_contacts:
        reply["warning"] = (
            f"{new_contacts} of the {queued} queued message(s) go to first contacts — people this account has "
            f"never messaged — which the bridge holds in a separate queue, {NEW_CONTACT_GAP_NOTE}, before they "
            f"join the normal send queue. The last of the batch is expected to go out in {_rough_duration(completion)}. "
            "Tell the user the messages are queued and roughly how long they will take; do not resend anything "
            "that is merely still queued, and poll get_send_status with an id if they need to know once it has gone."
        )
    return reply


def _describe_poll(poll: Dict[str, Any]) -> str:
    """One line for a poll and its tally, for the text-only tool outputs."""
    text = f"[poll] {poll.get('question', '')} — options: {' / '.join(poll.get('options') or [])}"
    if poll.get("selectable_count") == 0:
        text += " (pick any number)"
    if poll.get("total_voters"):
        tally = ", ".join(f"{r.get('option')} {r.get('votes', 0)}" for r in poll.get("results") or [])
        text += f" — {tally} ({poll['total_voters']} voter{'s' if poll['total_voters'] != 1 else ''})"
    return text


@mcp.tool()
def send_poll(
    recipient: str,
    question: str,
    options: List[str],
    selectable_count: int = 1,
    block: bool = False
) -> Dict[str, Any]:
    """Send a WhatsApp poll to a person or group. For group chats use the JID.

    Sending is rate limited and asynchronous, exactly like send_message: this
    returns as soon as the bridge has queued the poll, with `queued: true` and
    an `id` for get_send_status. Once sent, the poll appears in list_messages
    as a message with media_type "poll" whose `poll` field carries the current
    tally; votes arrive as people cast them, so read the outcome back later
    with get_poll_results (it needs the poll's message id and chat JID, both
    shown by list_messages), or subscribe_chat to be told about each vote.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        question: The poll question (up to 255 characters)
        options: Between 2 and 12 distinct, non-empty answer options (up to 100 characters each)
        selectable_count: How many options a voter may pick: 1 (default) for a single
                 choice, 0 for any number, or any value up to the number of options
        block: Wait for the poll to actually leave and return the send's own outcome
                 (default False). The wait is the rate limit's and can run to minutes.

    Returns:
        A dictionary containing success status, a status message, and the
        submission id
    """
    if not recipient:
        return {"success": False, "message": "Recipient must be provided"}
    if not question or not question.strip():
        return {"success": False, "message": "question must be provided"}
    opts = [o.strip() for o in (options or []) if o and o.strip()]
    if len(opts) < 2:
        return {"success": False, "message": "A poll needs at least 2 non-empty options"}
    if len(set(opts)) != len(opts):
        return {"success": False, "message": "Poll options must be distinct"}
    return _result(lambda: wa.send_poll(recipient, question.strip(), opts,
                                        selectable_count=selectable_count, block=block))


@mcp.tool()
def get_poll_results(chat_jid: str, message_id: str) -> Dict[str, Any]:
    """Read the current outcome of a WhatsApp poll, sent by anyone in the chat.

    Returns the question, options, `results` (one `{option, votes, voters}` per
    option, in poll order), `total_voters`, and `votes` (each voter's current
    selection with a timestamp). WhatsApp lets a voter change or withdraw
    their vote, so this is the state now, not a log; call it again later for
    an updated tally. list_polls / list_messages show which polls exist.

    Args:
        chat_jid: The JID of the chat the poll was sent in
        message_id: The poll's message id, as shown by list_messages or list_polls
    """
    if not chat_jid or not message_id:
        return {"success": False, "message": "chat_jid and message_id must be provided"}
    try:
        out = wa.get_poll(chat_jid, message_id)
    except BridgeError as e:
        return {"success": False, "message": str(e)}
    if out is None:
        return {"success": False, "message": f"No poll {message_id} in chat {chat_jid}"}
    return out


@mcp.tool()
def list_polls(chat_jid: Optional[str] = None, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """List WhatsApp polls, newest first, each with its current tally in `poll`.

    Args:
        chat_jid: Optional chat JID to restrict the list to one chat
        limit: Maximum number of polls to return (default 20)
        page: Page number for pagination (default 0)
    """
    return wa.list_polls(chat_jid=chat_jid, limit=limit, page=page)


@mcp.tool()
def get_send_status(send_id: str) -> Dict[str, Any]:
    """Look up how a queued send went, by the id send_message returned.

    `state` is queued (still waiting behind the rate limit), sent, or failed;
    `message` carries the bridge's own reason on a failure. `new_contact: true`
    marks a first contact, which waits in the bridge's separate new-contact
    queue (spaced about 30 minutes apart on average) before the normal one, so
    "queued" can last hours for those. The queue is durable — a bridge restart
    resumes a submission still queued, spacing intact, rather than losing it —
    but finished sends are kept only for a bounded time (the newest 500, or a
    week), so an unknown id means it has aged out, not that it failed.

    Args:
        send_id: The id returned by send_message, send_file or send_audio_message
    """
    if not send_id:
        return {"success": False, "message": "send_id must be provided"}
    return _result(lambda: wa.send_status(send_id))


@mcp.tool()
def get_reachout_timelock() -> Dict[str, Any]:
    """Check WhatsApp's "reach-out time-lock" — the rate limit on first-contact
    sends that returns error 463. Established chats are never affected by it.

    Asks WhatsApp directly, rather than reporting only the last passively
    observed state. `active` (bool), `enforcement_type`, `ends` and
    `checked_at` (RFC 3339 or null) describe it; while active, sends to anyone
    not already in a chat with this account are refused before they reach
    WhatsApp.
    """
    return _result(lambda: wa.reachout_timelock())


@mcp.tool()
def mark_chat_read(chat_jid: str, send_receipt: bool = False) -> Dict[str, Any]:
    """Mark a WhatsApp chat as read. This is the ONLY way read state changes in this
    server: listing or reading messages (list_messages, get_chat, list_chats, etc.)
    never marks anything as read as a side effect.

    send_receipt=False (default): clears the chat's unread count and syncs that
    state to your other WhatsApp devices WITHOUT notifying the sender.
    send_receipt=True: sends real WhatsApp read receipts (blue ticks) that the
    sender WILL see. Only pass True when the user explicitly asks for that.

    Args:
        chat_jid: The JID of the chat to mark as read
        send_receipt: Whether to send real read receipts visible to the sender (default False)

    Returns:
        A dictionary with success status, a status message, marked_count (how many
        messages were marked read), and receipt_sent (whether a receipt was sent)
    """
    if not chat_jid:
        return {"success": False, "message": "chat_jid must be provided", "marked_count": 0, "receipt_sent": False}
    return _result(lambda: wa.mark_chat_read(chat_jid, send_receipt), marked_count=0, receipt_sent=False)


@mcp.tool()
def send_file(recipient: str, media_path: str, block: bool = False) -> Dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.

    Sending is rate limited and asynchronous: this returns as soon as the bridge
    has queued the message, with `queued: true` and an `id` you can pass to
    get_send_status. The message goes out a short, randomised delay later — that
    spacing is deliberate, it keeps WhatsApp from unlinking the account for
    behaving like a bulk sender. A first contact (someone this account has never
    messaged) is held much longer — see send_message — and the reply then
    carries `new_contact: true` and a `warning` to pass on to the user.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document). Either a
                 file this server can read (it is uploaded to the bridge) or a path inside the
                 bridge's store directory, e.g. one returned by download_media.
        block: Wait for the message to actually leave and return the send's own
                 outcome instead of queueing (default False). The wait is the rate
                 limit's and can run to minutes, so only pass True when the
                 outcome must be known before doing anything else. Refused
                 outright for a first contact (the wait would be hours):
                 resubmit without block and poll get_send_status instead.

    Returns:
        A dictionary containing success status, a status message, and the
        submission id
    """
    if not recipient or not media_path:
        return {"success": False, "message": "recipient and media_path must be provided"}
    return _note_new_contact(_result(lambda: wa.send_file(recipient, path=media_path, block=block)))


@mcp.tool()
def send_audio_message(recipient: str, media_path: str, block: bool = False) -> Dict[str, Any]:
    """Send any audio file as a WhatsApp voice message to the specified recipient. For group messages use the JID. The bridge converts it to Opus .ogg with ffmpeg if needed; if that fails, use send_file instead.

    Sending is rate limited and asynchronous: this returns as soon as the bridge
    has queued the message, with `queued: true` and an `id` you can pass to
    get_send_status. The message goes out a short, randomised delay later — that
    spacing is deliberate, it keeps WhatsApp from unlinking the account for
    behaving like a bulk sender. A first contact (someone this account has never
    messaged) is held much longer — see send_message — and the reply then
    carries `new_contact: true` and a `warning` to pass on to the user.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send. Either a file this server can read
                 (it is uploaded to the bridge) or a path inside the bridge's store directory.
        block: Wait for the message to actually leave and return the send's own
                 outcome instead of queueing (default False). The wait is the rate
                 limit's and can run to minutes, so only pass True when the
                 outcome must be known before doing anything else. Refused
                 outright for a first contact (the wait would be hours):
                 resubmit without block and poll get_send_status instead.

    Returns:
        A dictionary containing success status, a status message, and the
        submission id
    """
    if not recipient or not media_path:
        return {"success": False, "message": "recipient and media_path must be provided"}
    return _note_new_contact(_result(lambda: wa.send_file(recipient, path=media_path, voice_note=True, block=block)))


@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> Dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message

    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    try:
        out = wa.download_media(message_id, chat_jid)
    except BridgeError as e:
        return {"success": False, "message": f"Failed to download media: {e}"}
    if out.get("success"):
        return {"success": True, "message": "Media downloaded successfully", "file_path": out.get("path")}
    return {"success": False, "message": out.get("message", "Failed to download media")}


@mcp.tool()
def subscribe_chat(
    chat_jid: str,
    url: str,
    bearer_token: str = "",
    kind: str = "claude_routine",
    headers: Optional[Dict[str, str]] = None,
    include_from_me: bool = False,
    debounce_seconds: int = 0,
    ttl_seconds: int = 0,
    max_per_hour: int = 60,
) -> Dict[str, Any]:
    """Push new WhatsApp messages in a chat to a URL as they arrive, instead of polling.

    Once subscribed, every new incoming message in the chat (and outgoing
    messages too, if include_from_me=True) causes the bridge to POST a JSON
    body ({"text": <human-readable summary + JSON>, "events": [...]}) with
    an `Authorization: Bearer <bearer_token>` header to `url`. Voice notes
    are delivered once, after transcription, with the transcript inline.

    Two kinds of target:
    - kind="claude_routine" (default): wire this to a Claude Code Routine's
      API fire endpoint, e.g.
      https://api.anthropic.com/v1/claude_code/routines/trig_.../fire
      Paste that fire URL as `url` and the routine's trigger bearer token as
      `bearer_token`. The bridge automatically adds the anthropic-version and
      anthropic-beta headers the fire endpoint requires, so you don't need to
      pass those in `headers`. IMPORTANT: the routine's own prompt must
      explicitly opt in to acting on the <routine-fire-payload> block it
      receives (e.g. "when a <routine-fire-payload> is present, read the new
      WhatsApp messages in it and act on them") -- a routine whose prompt
      doesn't mention the fire payload will ignore it. Also note that every
      POST starts a brand-new routine run, so for a busy chat set
      debounce_seconds (e.g. 30-120) to avoid firing a run per message.
    - kind="generic": POSTs the same body to any other URL/webhook receiver,
      using `headers` for any extra headers it needs beyond the bearer token.

    chat_jid may be a specific chat's JID, or "*" to subscribe to messages
    across all chats.

    Reliability and limits:
    - Each delivery is a single attempt -- there is no immediate retry. After
      a failed attempt (any failure type: bad status code, timeout, connection
      error, etc.) the subscription backs off for 5 minutes, holding and
      batching any new messages that arrive in the meantime into the next
      attempt. Three consecutive failed attempts auto-disable the
      subscription; any successful delivery resets the failure count back to
      0. A disabled subscription's record shows disabled_reason explaining
      why, and it stops firing until you call enable_subscription to revive
      it.
    - max_per_hour caps how many times this subscription can fire in a
      rolling hour, so a very busy chat can't run away with your quota.
    - ttl_seconds (0 = never expires, max 30 days) sets an expiry after which
      the subscription stops firing on its own. For a bounded task (e.g. "let
      me know about replies in this chat for the next hour") always set
      ttl_seconds so nothing keeps firing into a dead session forever.

    Args:
        chat_jid: The JID of the chat to watch, or "*" for all chats
        url: The endpoint to POST new-message events to
        bearer_token: Sent as `Authorization: Bearer <token>`. Never echoed back
                 by list_subscriptions or this call's response -- store it yourself
                 if you need it again.
        kind: "claude_routine" (default) or "generic"
        headers: Optional extra headers to send with each POST
        include_from_me: Whether to also fire for messages you sent (default False)
        debounce_seconds: Minimum gap between fires for this subscription; recommended
                 for busy chats so a burst of messages triggers one fire, not many
        ttl_seconds: How long this subscription stays active, in seconds (0 = no
                 expiry, max 30 days = 2592000). Recommended for bounded tasks.
        max_per_hour: Maximum number of fires allowed per rolling hour (default 60)

    Returns:
        A dictionary with success status, a message, and (on success) the created
        subscription record (with the bearer token masked)
    """
    if not chat_jid or not url:
        return {"success": False, "message": "chat_jid and url must be provided"}
    try:
        sub = wa.create_webhook(
            chat_jid=chat_jid, url=url, bearer_token=bearer_token, kind=kind, headers=headers or {},
            include_from_me=include_from_me, debounce_seconds=debounce_seconds,
            ttl_seconds=ttl_seconds, max_per_hour=max_per_hour)
    except BridgeError as e:
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "Subscription created", "subscription": _mask(sub)}


def _mask(sub: Dict[str, Any]) -> Dict[str, Any]:
    """Never surface the raw bearer token, even if a future bridge echoed it."""
    return {k: v for k, v in sub.items() if k != "bearer_token"}


@mcp.tool()
def unsubscribe_chat(subscription_id: int) -> Dict[str, Any]:
    """Remove a chat subscription created by subscribe_chat, stopping further pushes.

    Args:
        subscription_id: The id of the subscription to remove (from subscribe_chat
                 or list_subscriptions)

    Returns:
        A dictionary with success status and a message
    """
    try:
        wa.delete_webhook(subscription_id)
    except BridgeError as e:
        if e.status == 404:
            return {"success": False, "message": f"Subscription {subscription_id} not found"}
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "Subscription deleted"}


@mcp.tool()
def enable_subscription(subscription_id: int) -> Dict[str, Any]:
    """Re-enable a subscription that the bridge auto-disabled (e.g. after 3
    consecutive delivery failures) or that has expired.

    Use list_subscriptions first to check disabled_reason and confirm why it
    was disabled before reviving it -- if the underlying problem (bad URL,
    revoked token, expired ttl) isn't fixed, it will likely just fail again.

    Args:
        subscription_id: The id of the subscription to re-enable

    Returns:
        A dictionary with success status, a message, and (on success) the
        updated subscription record
    """
    try:
        sub = wa.enable_webhook(subscription_id)
    except BridgeError as e:
        if e.status == 404:
            return {"success": False, "message": f"Subscription {subscription_id} not found"}
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "Subscription enabled", "subscription": _mask(sub)}


@mcp.tool()
def list_subscriptions() -> List[Dict[str, Any]]:
    """List all active and inactive chat subscriptions (webhooks).

    Bearer tokens are never returned -- each record instead includes
    bearer_token_hint (the last 4 characters) so you can recognize which
    subscription is which without exposing the secret.

    Each delivery is a single attempt with no immediate retry; after a failed
    attempt the subscription backs off for 5 minutes (batching any new
    messages into the next attempt) before trying again. The bridge
    auto-disables a subscription after 3 consecutive failed attempts (any
    failure type counts; a success resets the counter back to 0), or once it
    passes its expires_at (if ttl_seconds was set). Check enabled,
    disabled_reason, and consecutive_failures to see subscription health, and
    expires_at / max_per_hour for its limits. Use enable_subscription to
    revive a disabled subscription.

    Returns:
        A list of subscription records: id, chat_jid, url, bearer_token_hint,
        kind, headers, include_from_me, debounce_seconds, max_per_hour,
        enabled, disabled_reason, consecutive_failures, expires_at,
        created_at, last_fired_at, last_status, last_error
    """
    try:
        return [_mask(s) for s in wa.list_webhooks()]
    except BridgeError:
        return []


@mcp.tool()
def test_subscription(subscription_id: int) -> Dict[str, Any]:
    """Send a one-off test event to a subscription's URL to verify it's wired correctly.

    Useful after subscribe_chat to confirm the URL and bearer token actually
    work (e.g. that a Claude Code Routine fire URL accepts the request) before
    relying on it for live messages.

    Args:
        subscription_id: The id of the subscription to test

    Returns:
        A dictionary with success status, the HTTP status code returned by the
        target URL, and an error message if it failed
    """
    try:
        return wa.test_webhook(subscription_id)
    except BridgeError as e:
        if e.status == 404:
            return {"success": False, "message": f"Subscription {subscription_id} not found"}
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport=MCP_TRANSPORT)
