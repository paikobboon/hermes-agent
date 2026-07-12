"""
LINE Messaging API platform adapter for Hermes Agent.

A bundled platform plugin that runs an aiohttp webhook server, accepts LINE
webhook events (signature-verified), and relays messages to/from the agent
via the standard ``BasePlatformAdapter`` interface.

Design highlights
-----------------

**Reply token preferred, Push fallback.** LINE's reply token is single-use
and expires roughly 60 seconds after the inbound event. We try Reply first
(it's free) and fall back to the metered Push API when the token is absent,
expired, or rejected by the API.

**Slow-LLM postback button (optional).** When the LLM is still running past
``slow_response_threshold`` seconds (default 45, leaving 15s margin on the
60s reply-token TTL), we burn the original reply token to send a Template
Buttons bubble — the user taps it later to receive the cached answer via a
*fresh* reply token (also free). State machine: PENDING → READY → DELIVERED,
with ERROR for cancelled runs. Set the threshold to 0 to disable the
button and always Push-fallback instead.

**Three-allowlist gating.** Separate allowlists for users (U-prefixed),
groups (C-prefixed), and rooms (R-prefixed). ``LINE_ALLOW_ALL_USERS=true``
is a dev-only escape hatch.

**Media via public HTTPS.** LINE's Messaging API does *not* accept
binary uploads — images, audio, and video must be reachable HTTPS URLs.
We register registered tempfiles under ``/line/media/<token>/<filename>``
served by the same aiohttp app, with an allowed-roots traversal guard.
``LINE_PUBLIC_URL`` (e.g. ``https://my-tunnel.example.com``) overrides
the host:port construction so URLs are reachable when bind is 0.0.0.0
or behind a reverse proxy.

**5-message batching.** LINE accepts at most 5 message objects per
Reply/Push call; longer responses are smart-chunked at 4500 chars
(LINE per-bubble limit is 5000) and batched.

Synthesis credits
-----------------

This file is a synthesis of seven open community PRs adding LINE support
to Hermes Agent. It deliberately ports the *strongest* idea from each into
a single plugin-form module that requires zero core edits:

* PR #18153 (leepoweii)   — Template Buttons postback cache state machine,
  Markdown URL preservation, system-message bypass.
* PR #8398  (yuga-hashimoto) — media URL serving with traversal guard,
  send_voice / send_video, ``LINE_PUBLIC_URL`` env, macOS ``/tmp`` root.
* PR #16832 (jethac)      — config wiring style, voice/image tests.
* PR #21023 (perng)       — plugin-form skeleton (the only one already
  modeled on ``ADDING_A_PLATFORM.md``), reply→push fallback at 50s TTL,
  loading-animation indicator, source dispatcher.
* PR #14942 (soichiyo)    — Cloudflare-tunnel operating model (docs only).
* PR #14988 (David-0x221Eight) — text-first scope discipline.
* PR #6676  (liyoungc)    — Push-only mode (used as the ``threshold=0``
  fallback path here).
"""

from __future__ import annotations

import asyncio
import base64
import enum
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import random
import re
import secrets
import tempfile
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote as _urlquote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy / function-level imports for gateway internals are NOT used here —
# the plugin discovery flow imports adapter.py late enough that gateway is
# already loaded.
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_bytes,
    cache_media_from_bytes,
)
from gateway.config import Platform
from gateway import rich_sent_store
# Active-profile-aware cache resolution — same helper base.py uses for the
# image/audio/video caches (see IMAGE_CACHE_DIR). Used by _flex_cache_dir.
from hermes_constants import get_hermes_dir


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
LINE_LOADING_URL = "https://api.line.me/v2/bot/chat/loading/start"
LINE_CONTENT_URL_FMT = "https://api-data.line.me/v2/bot/message/{message_id}/content"
LINE_BOT_INFO_URL = "https://api.line.me/v2/bot/info"

# Identity resolution endpoints (sender display names + group titles).
LINE_GROUP_MEMBER_URL_FMT = (
    "https://api.line.me/v2/bot/group/{group_id}/member/{user_id}"
)
LINE_ROOM_MEMBER_URL_FMT = (
    "https://api.line.me/v2/bot/room/{room_id}/member/{user_id}"
)
LINE_PROFILE_URL_FMT = "https://api.line.me/v2/bot/profile/{user_id}"
LINE_GROUP_SUMMARY_URL_FMT = "https://api.line.me/v2/bot/group/{group_id}/summary"

# LINE Messaging API hard limits
LINE_PER_BUBBLE_CHARS = 5000  # Hard limit per text message object
LINE_SAFE_BUBBLE_CHARS = 4500  # Conservative limit for chunking
LINE_MAX_MESSAGES_PER_CALL = 5  # API rejects >5 messages per Reply/Push
LINE_REPLY_TOKEN_TTL_SECONDS = 50  # Conservative cap below LINE's ~60s
LINE_RECENT_MESSAGE_CACHE_SIZE = 200

# Webhook hardening
WEBHOOK_BODY_MAX_BYTES = 1_048_576  # 1 MiB — webhooks are tiny JSON
DEFAULT_WEBHOOK_PORT = 8646
DEFAULT_WEBHOOK_PATH = "/line/webhook"
DEFAULT_MEDIA_PATH_PREFIX = "/line/media"

# Curated safe LINE sticker palette Lucky can reference with
# STICKER:<packageId>:<stickerId>. These are free official LINE sticker sets:
# 446 classic warm Brown/Cony faces (e.g. 1988), 789 friendly cheer/thanks
# variants, and 11537 CHOCO & FRIENDS-style happy/love reactions.
LINE_SAFE_STICKERS: Tuple[Tuple[str, str, str], ...] = (
    ("446", "1988", "warm happy"),
    ("446", "1990", "love"),
    ("789", "10855", "thanks"),
    ("789", "10863", "cheer"),
    ("11537", "52002734", "big smile"),
    ("11537", "52002738", "heart"),
)

# Slow-LLM postback button defaults
DEFAULT_SLOW_RESPONSE_THRESHOLD = 45.0  # seconds; 0 disables
DEFAULT_PENDING_REPLY_TEXT = (
    "🤔 Still thinking. Tap below to fetch the answer when it's ready."
)
DEFAULT_BUTTON_LABEL = "Get answer"
DEFAULT_DELIVERED_TEXT = "Already replied ✅"
DEFAULT_INTERRUPTED_TEXT = "Run was interrupted before completion."

# Media defaults
# LINE clients fetch originalContentUrl when the recipient taps the image,
# which can be hours after send — a short TTL turns older images into
# "can't open" errors. Env-tunable per profile (lucky runs 86400 = 24h,
# matching the gateway's 24h image-cache cleanup). FORK DELTA 2026-07-12.
MEDIA_TOKEN_TTL_SECONDS = 1800  # upstream default; see LINE_MEDIA_TTL_SECONDS env
LINE_IMAGE_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per LINE docs
LINE_AV_MAX_BYTES = 200 * 1024 * 1024  # 200 MB for voice/video

# Sender/chat identity resolution
SENDER_NAME_CACHE_TTL_SECONDS = 6 * 3600  # 6h; display names rarely change
SENDER_NAME_HTTP_TIMEOUT_SECONDS = 10.0  # bounded — resolution must not hang dispatch

# Media→text coalescing. LINE has no image captions, so a user sends an image
# and then explains it in a follow-up text 1–5s later. Without coalescing the
# adapter dispatches two disjoint turns and the agent answers the bare image
# before the caption lands. A MEDIA message opens a short buffer; TEXT (and
# further media) from the same sender merge into one MessageEvent.
DEFAULT_COALESCE_MEDIA_GRACE = 4.0  # seconds a lone media waits for a caption
DEFAULT_COALESCE_IDLE = 2.5         # window extension per merged message
DEFAULT_COALESCE_MAX_AGE = 7.0      # hard cap from the first buffered event

# Inbound message types that open/extend a coalescing buffer. Audio (voice
# notes), stickers, and locations are self-contained turns — they never buffer.
_COALESCE_MEDIA_TYPES: Set[str] = {"image", "video", "file"}

# Map LINE webhook message types to the normalized MessageType the gateway
# routes on. LINE has no separate "voice" type — audio messages are recorded
# voice clips, so they map to VOICE (which the gateway sends through STT),
# mirroring how Telegram/WhatsApp classify voice notes. Anything unknown
# falls back to TEXT.
_LINE_MESSAGE_TYPES = {
    "text": MessageType.TEXT,
    "image": MessageType.PHOTO,
    "video": MessageType.VIDEO,
    "audio": MessageType.VOICE,
    "file": MessageType.DOCUMENT,
    "location": MessageType.LOCATION,
    "sticker": MessageType.STICKER,
}

# A 1×1 transparent PNG used as fallback video preview thumbnail when no
# explicit preview is supplied — LINE requires ``previewImageUrl`` for
# video messages. Sourced from the Python stdlib (no Pillow dependency).
_FALLBACK_PNG_PREVIEW = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000d49444154789c63000100000005000100377a7ff20000000049454e"
    "44ae426082"
)


# ---------------------------------------------------------------------------
# Markdown stripping (URL-preserving)
# ---------------------------------------------------------------------------

_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
_MD_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_MD_ITAL_RE = re.compile(r"(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)")
_MD_CODE_INLINE_RE = re.compile(r"`([^`]+)`")
_MD_CODE_BLOCK_RE = re.compile(r"```[a-zA-Z0-9_+-]*\n?(.*?)```", re.DOTALL)
_MD_HEADING_RE = re.compile(r"^#{1,6}\s+", re.MULTILINE)
_MD_BULLET_RE = re.compile(r"^[\s]*[-*+]\s+", re.MULTILINE)
_STICKER_MARKER_RE = re.compile(r"(?<!\S)STICKER:([^\s:]+)(?::([^\s:]+))?")
# FORK DELTA 2026-07-09: generic FLEXFILE:<token> marker. An agent reply
# containing ``FLEXFILE:abc123`` has that text replaced by a cached LINE Flex
# message loaded from ``<profile>/cache/flex/<token>.json`` and sent on the
# normal (free) reply path, exactly like STICKER. Generic, not Splitwise-
# specific. The token charset excludes ``/`` and ``.`` so it cannot express
# path traversal; the path is still range-checked in ``_load_flex_message``.
_FLEXFILE_MARKER_RE = re.compile(r"(?<!\S)FLEXFILE:([A-Za-z0-9_-]{1,64})")
_FLEX_FILE_MAX_BYTES = 60 * 1024  # 60 KB cap on a cached flex message file


def strip_markdown_preserving_urls(text: str) -> str:
    """Strip Markdown that LINE can't render, but keep URLs usable.

    LINE's text bubble has zero Markdown support — bold, italics, code
    fences, headings, and bullet markers all render as literal characters.
    URLs *are* auto-linked by the client, but only when they appear bare
    (not inside ``[label](url)`` syntax). This converts ``[label](url)``
    to ``label (url)`` so the URL remains tappable, then strips the rest.

    Source: PR #18153 (leepoweii) — adapted to keep code-block content
    visible (LINE users frequently want command snippets to land as
    plain text, not be eaten by the fence).
    """
    if not text:
        return text

    # Code blocks first — keep the inner content, drop the fences.
    def _unfence(m: re.Match) -> str:
        return m.group(1).rstrip("\n")
    text = _MD_CODE_BLOCK_RE.sub(_unfence, text)

    # Inline code: keep content, drop backticks.
    text = _MD_CODE_INLINE_RE.sub(r"\1", text)

    # Markdown links → "label (url)"
    text = _MD_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2)})", text)

    # Bold/italic markers — strip.
    text = _MD_BOLD_RE.sub(r"\1", text)
    text = _MD_ITAL_RE.sub(r"\1", text)

    # Headings (#, ##) and bullet markers — strip the prefix only.
    text = _MD_HEADING_RE.sub("", text)
    text = _MD_BULLET_RE.sub("• ", text)

    return text


def split_for_line(text: str, max_chars: int = LINE_SAFE_BUBBLE_CHARS) -> List[str]:
    """Split ``text`` into LINE-sized bubbles, preferring paragraph/line breaks.

    Returns at most ``LINE_MAX_MESSAGES_PER_CALL`` chunks; longer text is
    truncated with an ellipsis on the final chunk to keep the response
    deliverable in a single Reply/Push call.
    """
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    remaining = text
    while remaining and len(chunks) < LINE_MAX_MESSAGES_PER_CALL:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            remaining = ""
            break
        # Try to break on the latest paragraph or newline within budget.
        cut = remaining.rfind("\n\n", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = remaining.rfind("\n", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = remaining.rfind(" ", 0, max_chars)
        if cut <= 0:
            cut = max_chars
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()

    if remaining:
        # Truncate gracefully — caller already burned its 5-bubble budget.
        if chunks:
            tail = chunks[-1]
            if len(tail) > max_chars - 1:
                tail = tail[: max_chars - 1]
            chunks[-1] = tail.rstrip() + "…"
        else:
            chunks.append(remaining[: max_chars - 1] + "…")
    return chunks


# ---------------------------------------------------------------------------
# Webhook signature verification
# ---------------------------------------------------------------------------

def verify_line_signature(body: bytes, signature: str, channel_secret: str) -> bool:
    """Verify a LINE webhook's ``X-Line-Signature`` header.

    LINE signs the *raw* request body with HMAC-SHA256 keyed by the
    channel secret, then base64-encodes the digest. Constant-time
    comparison defends against timing oracles.
    """
    if not signature or not channel_secret or body is None:
        return False
    try:
        digest = hmac.new(
            channel_secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).digest()
        expected = base64.b64encode(digest).decode("utf-8")
    except Exception:
        return False
    return hmac.compare_digest(expected, signature)


# ---------------------------------------------------------------------------
# Cache state machine — slow-LLM postback flow
# ---------------------------------------------------------------------------

class State(enum.Enum):
    PENDING = "pending"  # button sent, LLM still running
    READY = "ready"      # LLM done, response cached, waiting for postback tap
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    ERROR = "error"      # LLM raised / interrupted; cached error text waiting


@dataclass
class _CacheEntry:
    state: State
    payload: Any = None
    chat_id: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class RequestCache:
    """In-memory cache for slow-LLM postback retrieval.

    PRs #18153 originally combined two TTLs — one for PENDING (24h) and
    a shorter one for READY/DELIVERED/ERROR (1h). We keep the same model
    here.
    """

    def __init__(
        self,
        ttl_seconds: int = 3600,
        pending_ttl_seconds: int = 86400,
    ) -> None:
        self._entries: Dict[str, _CacheEntry] = {}
        self._ttl = ttl_seconds
        self._pending_ttl = pending_ttl_seconds

    def register_pending(self, chat_id: str) -> str:
        rid = str(uuid.uuid4())
        self._entries[rid] = _CacheEntry(state=State.PENDING, chat_id=chat_id)
        return rid

    def get(self, request_id: str) -> Optional[_CacheEntry]:
        return self._entries.get(request_id)

    def set_ready(self, request_id: str, payload: Any) -> None:
        entry = self._entries.get(request_id)
        if entry is None:
            return
        # FORK DELTA 2026-07-09 (lucky image-drop fix): a slow turn emits its
        # text and its image as two separate adapter sends, each calling
        # set_ready on the same pending button id. The original guard fired
        # only on PENDING, so the second payload (the image) hit a READY
        # entry and was silently dropped -- a tap then delivered text only.
        # Accumulate instead so one press delivers the whole answer.
        # DELIVERING/DELIVERED/ERROR stay terminal (no resurrection).
        # Pinned by test_set_ready_accumulates_multiple_payloads.
        if entry.state is State.PENDING:
            entry.state = State.READY
            entry.payload = payload
            entry.updated_at = time.time()
            return
        if entry.state is State.READY:
            entry.payload = (
                _normalize_cached_payload(entry.payload)
                + _normalize_cached_payload(payload)
            )[:LINE_MAX_MESSAGES_PER_CALL]
            entry.updated_at = time.time()
            return

    def set_error(self, request_id: str, message: str) -> None:
        entry = self._entries.get(request_id)
        if entry is None or entry.state is not State.PENDING:
            return
        entry.state = State.ERROR
        entry.payload = message
        entry.updated_at = time.time()

    def mark_delivered(self, request_id: str) -> None:
        entry = self._entries.get(request_id)
        if entry is None or entry.state not in {State.READY, State.ERROR, State.DELIVERING}:
            return
        entry.state = State.DELIVERED
        entry.updated_at = time.time()

    def claim_delivery(self, request_id: str) -> Optional[Tuple[State, Any]]:
        entry = self._entries.get(request_id)
        if entry is None or entry.state not in {State.READY, State.ERROR}:
            return None
        previous_state = entry.state
        payload = entry.payload
        entry.state = State.DELIVERING
        entry.updated_at = time.time()
        return previous_state, payload

    def release_delivery_claim(self, request_id: str, previous_state: State) -> None:
        entry = self._entries.get(request_id)
        if entry is None or entry.state is not State.DELIVERING:
            return
        entry.state = previous_state
        entry.updated_at = time.time()

    def find_pending_for_chat(self, chat_id: str) -> Optional[str]:
        for rid, entry in self._entries.items():
            if entry.state is State.PENDING and entry.chat_id == chat_id:
                return rid
        return None

    def prune(self) -> int:
        now = time.time()
        removed = 0
        for rid in list(self._entries.keys()):
            entry = self._entries[rid]
            if entry.state is State.PENDING:
                if now - entry.created_at > self._pending_ttl:
                    del self._entries[rid]
                    removed += 1
            else:
                if now - entry.updated_at > self._ttl:
                    del self._entries[rid]
                    removed += 1
        return removed


# ---------------------------------------------------------------------------
# Inbound dedup
# ---------------------------------------------------------------------------

class _MessageDeduplicator:
    """Bounded LRU of LINE webhook event IDs to ignore at-least-once retries.

    Optionally persists the seen-set to disk so redeliveries that straddle a
    gateway restart (LINE webhook-redelivery is at-least-once) are still
    recognized by the fresh process.
    """

    def __init__(self, max_size: int = 1000, persist_path: Optional[str] = None) -> None:
        self._seen: Dict[str, float] = {}
        self._max = max_size
        self._persist_path = persist_path
        if persist_path:
            try:
                with open(persist_path, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict):
                    self._seen = {str(k): float(v) for k, v in loaded.items()}
            except (FileNotFoundError, ValueError, OSError):
                pass  # first run or unreadable state: start empty

    def _persist(self) -> None:
        if not self._persist_path:
            return
        try:
            tmp = self._persist_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._seen, fh)
            os.replace(tmp, self._persist_path)
        except OSError:
            pass  # persistence is best-effort; never break dispatch

    def is_duplicate(self, event_id: str) -> bool:
        if not event_id:
            return False
        if event_id in self._seen:
            return True
        if len(self._seen) >= self._max:
            # Drop the oldest 10% so we don't trim on every insert.
            cutoff = sorted(self._seen.values())[len(self._seen) // 10 or 1]
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        self._seen[event_id] = time.time()
        self._persist()
        return False


# ---------------------------------------------------------------------------
# Media→text coalescing
# ---------------------------------------------------------------------------

# Signature of the callable the coalescer invokes to emit a finished turn.
# Kept LINE-shaped but adapter-agnostic so the coalescer is unit-testable
# without a live gateway: pass any async callable and inspect what it receives.
CoalesceDispatch = Callable[..., Awaitable[None]]


@dataclass
class _CoalesceBuffer:
    """One in-flight (chat_id, user_id) turn awaiting possible follow-ups."""

    key: Tuple[str, str]
    latest_event: Dict[str, Any]      # freshest raw event → freshest replyToken
    message_id: str                    # id of the FIRST buffered media message
    placeholder: str                   # original ``[image]`` text, used if no caption
    media_urls: List[str] = field(default_factory=list)
    media_types: List[str] = field(default_factory=list)
    texts: List[str] = field(default_factory=list)  # real captions, newline-joined
    created_at: float = field(default_factory=time.monotonic)
    timer: Optional["asyncio.Task[None]"] = None
    timer_token: int = 0               # guards against stale timer flushes


class _MediaCoalescer:
    """Sliding-window batcher that fuses a media message with the text that
    explains it into a single dispatched turn.

    Rules (see module patch notes):

    * Only a MEDIA message opens a buffer, held ``grace`` seconds.
    * TEXT from the same sender while a buffer is open merges in and extends
      the window by ``idle`` (captions join with newlines).
    * Further MEDIA from the same sender merges its urls/types and extends too.
    * ``max_age`` from the first buffered event is a hard flush cap.
    * TEXT with no open buffer dispatches immediately (zero added latency).
    * A different sender or chat never merges — separate key, separate buffer.

    Concurrency: a single adapter-level lock guards all buffer state (LINE
    traffic is low, so one lock is simpler and provably correct). Timers run
    as tasks; a per-buffer token makes a woken timer a no-op if it was
    superseded by a merge, so we never double-flush or leak a stale flush.
    """

    def __init__(
        self,
        dispatch: CoalesceDispatch,
        *,
        grace: float,
        idle: float,
        max_age: float,
    ) -> None:
        self._dispatch = dispatch
        self._grace = max(0.0, grace)
        self._idle = max(0.0, idle)
        self._max_age = max(0.0, max_age)
        self._buffers: Dict[Tuple[str, str], _CoalesceBuffer] = {}
        self._lock = asyncio.Lock()

    def has_buffer(self, key: Tuple[str, str]) -> bool:
        return key in self._buffers

    async def submit_media(
        self,
        key: Tuple[str, str],
        raw_event: Dict[str, Any],
        text: str,
        media_urls: List[str],
        media_types: List[str],
        message_id: str,
    ) -> None:
        """Open a new buffer for a lone media message, or merge into an open one."""
        flush_event: Optional[Dict[str, Any]] = None
        async with self._lock:
            buf = self._buffers.get(key)
            if buf is None:
                buf = _CoalesceBuffer(
                    key=key,
                    latest_event=raw_event,
                    message_id=message_id,
                    placeholder=text,
                    media_urls=list(media_urls),
                    media_types=list(media_types),
                )
                self._buffers[key] = buf
                # The initial grace window is still bounded by the hard cap so
                # a grace > max_age misconfiguration can't defeat the cap.
                self._schedule(key, min(self._grace, self._max_age))
            else:
                buf.media_urls.extend(media_urls)
                buf.media_types.extend(media_types)
                buf.latest_event = raw_event
                flush_event = self._extend_or_flush(key)
        if flush_event is not None:
            await self._safe_dispatch(flush_event)

    async def submit_text(
        self,
        key: Tuple[str, str],
        raw_event: Dict[str, Any],
        text: str,
        media_urls: List[str],
        media_types: List[str],
        message_id: str,
    ) -> None:
        """Merge a caption into an open buffer, or dispatch the text immediately."""
        dispatch_kwargs: Optional[Dict[str, Any]] = None
        async with self._lock:
            buf = self._buffers.get(key)
            if buf is None:
                # No media pending — plain text must gain zero latency.
                dispatch_kwargs = dict(
                    raw_event=raw_event,
                    text=text,
                    media_urls=list(media_urls),
                    media_types=list(media_types),
                    message_id=message_id,
                )
            else:
                if text:
                    buf.texts.append(text)
                buf.latest_event = raw_event
                dispatch_kwargs = self._extend_or_flush(key)
        if dispatch_kwargs is not None:
            await self._safe_dispatch(dispatch_kwargs)

    async def flush_all(self) -> None:
        """Immediately flush every pending buffer — for adapter shutdown."""
        pending: List[Dict[str, Any]] = []
        async with self._lock:
            for key in list(self._buffers.keys()):
                buf = self._buffers.pop(key)
                self._cancel_timer(buf)
                pending.append(self._build_flush_kwargs(buf))
        for kwargs in pending:
            await self._safe_dispatch(kwargs)

    async def _safe_dispatch(self, kwargs: Dict[str, Any]) -> None:
        """Dispatch outside the lock; downstream failures are logged, never
        raised. Timer flushes run in detached tasks, where an uncaught
        exception is a silently dropped user turn — the exact failure class
        this patch exists to remove."""
        try:
            await self._dispatch(**kwargs)
        except Exception:
            logger.exception("LINE: coalesced dispatch failed")

    # -- internals (all callers below hold ``self._lock``) ------------------

    def _extend_or_flush(self, key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
        """Reschedule the buffer's timer, or return flush kwargs if the hard
        cap is already reached. Caller dispatches the returned kwargs outside
        the lock."""
        buf = self._buffers[key]
        now = time.monotonic()
        hard_remaining = (buf.created_at + self._max_age) - now
        if hard_remaining <= 0:
            self._cancel_timer(buf)
            del self._buffers[key]
            return self._build_flush_kwargs(buf)
        self._schedule(key, min(self._idle, hard_remaining))
        return None

    def _schedule(self, key: Tuple[str, str], delay: float) -> None:
        buf = self._buffers[key]
        self._cancel_timer(buf)
        buf.timer_token += 1
        token = buf.timer_token
        buf.timer = asyncio.create_task(self._flush_after(key, max(0.0, delay), token))

    @staticmethod
    def _cancel_timer(buf: _CoalesceBuffer) -> None:
        if buf.timer is not None and not buf.timer.done():
            buf.timer.cancel()
        buf.timer = None

    async def _flush_after(self, key: Tuple[str, str], delay: float, token: int) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        kwargs: Optional[Dict[str, Any]] = None
        async with self._lock:
            buf = self._buffers.get(key)
            if buf is None or buf.timer_token != token:
                return  # superseded by a merge or already flushed
            del self._buffers[key]
            kwargs = self._build_flush_kwargs(buf)
        await self._safe_dispatch(kwargs)

    def _build_flush_kwargs(self, buf: _CoalesceBuffer) -> Dict[str, Any]:
        text = "\n".join(buf.texts) if buf.texts else buf.placeholder
        return dict(
            raw_event=buf.latest_event,
            text=text,
            media_urls=list(buf.media_urls),
            media_types=list(buf.media_types),
            message_id=buf.message_id,
        )


# ---------------------------------------------------------------------------
# Source / chat-id resolution
# ---------------------------------------------------------------------------

def _resolve_chat(source: Dict[str, Any]) -> Tuple[str, str]:
    """Return ``(chat_id, chat_type)`` from a LINE event ``source`` block.

    LINE sources are one of:
      * ``{"type": "user",  "userId":  "U..."}``  → 1:1 DM
      * ``{"type": "group", "groupId": "C...", "userId": "U..."}``  → group chat
      * ``{"type": "room",  "roomId":  "R...", "userId": "U..."}``  → multi-user room

    Source: PR #21023 (perng), unchanged.
    """
    src_type = (source or {}).get("type", "")
    if src_type == "group":
        return source.get("groupId", ""), "group"
    if src_type == "room":
        return source.get("roomId", ""), "room"
    if src_type == "user":
        return source.get("userId", ""), "dm"
    return "", "dm"


def _allowed_for_source(
    source: Dict[str, Any],
    *,
    allow_all: bool,
    user_ids: Set[str],
    group_ids: Set[str],
    room_ids: Set[str],
) -> bool:
    """Three-list gate — credit PR #18153."""
    if allow_all:
        return True
    src_type = (source or {}).get("type", "")
    if src_type == "user":
        uid = source.get("userId", "")
        return bool(uid) and uid in user_ids
    if src_type == "group":
        gid = source.get("groupId", "")
        return bool(gid) and gid in group_ids
    if src_type == "room":
        rid = source.get("roomId", "")
        return bool(rid) and rid in room_ids
    return False


# ---------------------------------------------------------------------------
# LINE Reply / Push HTTP client
# ---------------------------------------------------------------------------

class _LineClient:
    """Thin async wrapper around the LINE Messaging API.

    We use ``aiohttp`` directly to avoid a ``line-bot-sdk`` dependency
    (the SDK pulls in its own httpx pin and the ergonomic gain is small
    for the four endpoints we actually call).
    """

    def __init__(self, channel_access_token: str, *, timeout: float = 15.0) -> None:
        self._token = channel_access_token
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {channel_access_token}",
            "Content-Type": "application/json",
        }

    async def reply(
        self,
        reply_token: str,
        messages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.post(
                LINE_REPLY_URL,
                headers=self._headers,
                json={"replyToken": reply_token, "messages": messages},
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    raise RuntimeError(f"LINE reply {resp.status}: {body[:200]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    return {}
                return data if isinstance(data, dict) else {}

    async def push(
        self,
        chat_id: str,
        messages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.post(
                LINE_PUSH_URL,
                headers=self._headers,
                json={"to": chat_id, "messages": messages},
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    raise RuntimeError(f"LINE push {resp.status}: {body[:200]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    return {}
                return data if isinstance(data, dict) else {}

    async def loading(self, chat_id: str, seconds: int = 60) -> None:
        """Loading indicator (DM only). LINE rejects this for groups/rooms."""
        if not chat_id or not chat_id.startswith("U"):
            return
        import aiohttp
        # LINE caps loadingSeconds in 5-step increments, max 60.
        clamped = max(5, min(60, (seconds // 5) * 5 or 5))
        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                await session.post(
                    LINE_LOADING_URL,
                    headers=self._headers,
                    json={"chatId": chat_id, "loadingSeconds": clamped},
                )
        except Exception as exc:  # best-effort; never raise
            logger.debug("LINE loading indicator failed: %s", exc)

    async def fetch_content(self, message_id: str) -> bytes:
        """Download an inbound media message's binary content."""
        import aiohttp
        url = LINE_CONTENT_URL_FMT.format(message_id=message_id)
        timeout = aiohttp.ClientTimeout(total=30.0)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.get(url, headers={"Authorization": f"Bearer {self._token}"}) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"LINE content {resp.status}")
                return await resp.read()

    async def get_bot_user_id(self) -> Optional[str]:
        """Fetch this channel's own userId so we can filter self-messages."""
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=10.0)
        try:
            async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
                async with session.get(LINE_BOT_INFO_URL, headers=self._headers) as resp:
                    if resp.status >= 400:
                        return None
                    data = await resp.json()
                    return data.get("userId")
        except Exception:
            return None

    async def get_json(
        self, url: str, *, timeout: float = SENDER_NAME_HTTP_TIMEOUT_SECONDS
    ) -> Optional[Dict[str, Any]]:
        """GET a LINE JSON endpoint. Fail-open: any error/non-2xx → ``None``.

        Used for identity lookups (member profiles, group summaries) where a
        failure must degrade to the raw id rather than block message handling.
        """
        import aiohttp
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        try:
            async with aiohttp.ClientSession(timeout=client_timeout, trust_env=True) as session:
                async with session.get(url, headers=self._headers) as resp:
                    if resp.status >= 400:
                        return None
                    return await resp.json()
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------

def _text_message(text: str) -> Dict[str, Any]:
    """Build a LINE text message object, capped to per-bubble max."""
    if len(text) > LINE_PER_BUBBLE_CHARS:
        text = text[: LINE_PER_BUBBLE_CHARS - 1] + "…"
    return {"type": "text", "text": text}


def _image_message(original_url: str, preview_url: Optional[str] = None) -> Dict[str, Any]:
    return {
        "type": "image",
        "originalContentUrl": original_url,
        "previewImageUrl": preview_url or original_url,
    }


def _generate_line_preview(
    src_path: str, *, max_dim: int = 1024, max_bytes: int = 1_000_000
) -> Optional[str]:
    """Small JPEG preview for LINE's previewImageUrl (LINE caps it at 1 MB).
    The LINE MOBILE app renders this thumbnail and fails on an oversized
    preview (desktop loads the original instead), so a multi-MB PNG reused
    as the preview shows blank on phones. Returns a temp .jpg path
    (<= max_bytes), or None on any failure so the caller falls back to the
    original URL. FORK DELTA 2026-07-09.
    """
    try:
        from PIL import Image
        import tempfile
    except Exception:
        return None
    try:
        with Image.open(src_path) as im:
            im = im.convert("RGB")
            im.thumbnail((max_dim, max_dim), Image.LANCZOS)
            fd, tmp = tempfile.mkstemp(suffix=".jpg", prefix="line_preview_")
            os.close(fd)
            for quality, dim in ((85, max_dim), (70, max_dim), (60, 512), (50, 384)):
                if dim != max_dim:
                    im.thumbnail((dim, dim), Image.LANCZOS)
                im.save(tmp, format="JPEG", quality=quality, optimize=True)
                if os.path.getsize(tmp) <= max_bytes:
                    return tmp
            return tmp
    except Exception:
        return None


def _audio_message(url: str, duration_ms: int = 1000) -> Dict[str, Any]:
    return {
        "type": "audio",
        "originalContentUrl": url,
        "duration": int(duration_ms),
    }


def _video_message(url: str, preview_url: str) -> Dict[str, Any]:
    return {
        "type": "video",
        "originalContentUrl": url,
        "previewImageUrl": preview_url,
    }


def _sticker_message(package_id: str, sticker_id: str) -> Dict[str, Any]:
    return {
        "type": "sticker",
        "packageId": package_id,
        "stickerId": sticker_id,
    }


def _append_text_messages(messages: List[Dict[str, Any]], text: str) -> None:
    cleaned = strip_markdown_preserving_urls(text).strip()
    if not cleaned:
        return
    for chunk in split_for_line(cleaned):
        if len(messages) >= LINE_MAX_MESSAGES_PER_CALL:
            return
        messages.append(_text_message(chunk))


def _media_token_store_path() -> Optional[str]:
    """Where persisted media-serving tokens live (``<profile>/state/line_media_tokens.json``).

    ``HERMES_PROFILE_DIR`` override (explicit deployments / tests) wins; else
    the lucky profile when present (mirrors the ``_MessageDeduplicator``
    persist-path idiom in ``__init__``). ``None`` → in-memory only.
    FORK DELTA 2026-07-12.
    """
    override = os.environ.get("HERMES_PROFILE_DIR")
    if override:
        return os.path.join(override, "state", "line_media_tokens.json")
    lucky = os.path.expanduser("~/.hermes/profiles/lucky")
    if os.path.isdir(lucky):
        return os.path.join(lucky, "state", "line_media_tokens.json")
    return None


def _flex_cache_dir() -> Path:
    """Resolve the active profile's Flex-message cache dir (``<profile>/cache/flex``).

    Mirrors how ``gateway.platforms.base`` resolves the image/audio/video caches
    via ``get_hermes_dir`` (active-profile-aware). Resolution order:
    ``HERMES_PROFILE_DIR`` env override (explicit deployments / tests), then the
    canonical ``get_hermes_dir("cache/flex", "flex_cache")``, then a lucky-profile
    fallback. FORK DELTA 2026-07-09.
    """
    override = os.environ.get("HERMES_PROFILE_DIR")
    if override:
        return Path(override) / "cache" / "flex"
    try:
        return get_hermes_dir("cache/flex", "flex_cache")
    except Exception:  # pragma: no cover - defensive fallback only
        return Path.home() / ".hermes" / "profiles" / "lucky" / "cache" / "flex"


def _load_flex_message(token: str) -> Optional[Dict[str, Any]]:
    """Load + validate a cached LINE Flex message for a ``FLEXFILE:<token>`` marker.

    Reads ``<flex-cache>/<token>.json``. ``token`` is already constrained to
    ``[A-Za-z0-9_-]{1,64}`` by ``_FLEXFILE_MARKER_RE`` (no ``/``, ``.``, or
    traversal), but we still resolve the path and confirm it stays inside the
    flex cache dir before reading — defense in depth, mirroring the cache
    traversal guard in ``gateway.platforms.base``. Enforces a 60 KB size cap and
    validates the payload shape (``type == "flex"`` + dict ``contents`` + str
    ``altText``). Returns the message dict, or ``None`` when the file is missing,
    oversized, unreadable, malformed, or escapes the cache dir — the caller then
    leaves the literal marker text in place. FORK DELTA 2026-07-09.
    """
    cache_dir = _flex_cache_dir()
    try:
        path = (cache_dir / f"{token}.json").resolve()
        if not path.is_relative_to(cache_dir.resolve()):
            logger.warning(
                "LINE: FLEXFILE token %r escaped flex cache dir; refusing", token
            )
            return None
        if not path.is_file():
            return None
        if path.stat().st_size > _FLEX_FILE_MAX_BYTES:
            logger.warning(
                "LINE: FLEXFILE %s exceeds %d-byte cap; skipping",
                path.name,
                _FLEX_FILE_MAX_BYTES,
            )
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("LINE: failed to load FLEXFILE %r: %s", token, exc)
        return None

    if (
        isinstance(data, dict)
        and data.get("type") == "flex"
        and isinstance(data.get("contents"), dict)
        and isinstance(data.get("altText"), str)
    ):
        return data
    logger.warning(
        "LINE: FLEXFILE %r is not a valid flex message payload; leaving as text",
        token,
    )
    return None


def _messages_from_text_payload(content: str) -> List[Dict[str, Any]]:
    """Build LINE messages from text plus inline ``STICKER:pkg:id`` and
    ``FLEXFILE:<token>`` markers.

    Both marker families are collected and processed strictly left-to-right so a
    single reply can interleave text, stickers, and cached Flex bubbles. An
    invalid marker — a malformed STICKER, or a FLEXFILE that does not resolve to
    a valid cached message — is left in place as literal text. Output is capped
    at ``LINE_MAX_MESSAGES_PER_CALL``. FORK DELTA 2026-07-09 adds FLEXFILE.
    """
    text = str(content or "")
    messages: List[Dict[str, Any]] = []
    last = 0

    # STICKER and FLEXFILE spans never overlap (both require a start-of-token
    # boundary via ``(?<!\S)`` and use distinct literal prefixes), so a single
    # position-sorted sweep interleaves them correctly with the surrounding text.
    markers = [("sticker", m) for m in _STICKER_MARKER_RE.finditer(text)]
    markers += [("flex", m) for m in _FLEXFILE_MARKER_RE.finditer(text)]
    markers.sort(key=lambda item: item[1].start())

    for kind, match in markers:
        if kind == "sticker":
            package_id = match.group(1) or ""
            sticker_id = match.group(2) or ""
            if not (package_id.isdigit() and sticker_id.isdigit()):
                logger.warning(
                    "LINE: malformed STICKER marker %r; leaving as text",
                    match.group(0),
                )
                continue
            marker_message = _sticker_message(package_id, sticker_id)
        else:  # flex
            marker_message = _load_flex_message(match.group(1))
            if marker_message is None:
                logger.warning(
                    "LINE: unresolved FLEXFILE marker %r; leaving as text",
                    match.group(0),
                )
                continue

        _append_text_messages(messages, text[last:match.start()])
        if len(messages) >= LINE_MAX_MESSAGES_PER_CALL:
            return messages[:LINE_MAX_MESSAGES_PER_CALL]
        messages.append(marker_message)
        last = match.end()

    _append_text_messages(messages, text[last:])
    return messages[:LINE_MAX_MESSAGES_PER_CALL]


def _messages_from_prebuilt_payload(
    messages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Normalize pre-built messages, expanding text through the shared builder."""
    normalized: List[Dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if message.get("type") == "text":
            normalized.extend(_messages_from_text_payload(str(message.get("text") or "")))
        else:
            normalized.append(message)
    return normalized

def _normalize_cached_payload(payload: Any) -> List[Dict[str, Any]]:
    """Coerce a cached postback payload -- plain text (str) or a prebuilt
    message list -- into a list of LINE message dicts, so multiple slow-turn
    sends (text + image) merge into one delivery instead of clobbering.
    FORK DELTA 2026-07-09.
    """
    if isinstance(payload, list):
        return _messages_from_prebuilt_payload(
            [m for m in payload if isinstance(m, dict)]
        )
    if payload is None:
        return []
    return _messages_from_text_payload(str(payload))


def _sent_message_ids(response: Any) -> List[str]:
    if not isinstance(response, dict):
        return []
    sent_messages = response.get("sentMessages") or []
    if not isinstance(sent_messages, list):
        return []
    ids: List[str] = []
    for sent in sent_messages:
        if not isinstance(sent, dict):
            continue
        sent_id = str(sent.get("id") or "").strip()
        if sent_id:
            ids.append(sent_id)
    return ids


def _recent_text_for_outbound_message(message: Dict[str, Any]) -> str:
    msg_type = str(message.get("type") or "")
    if msg_type == "text":
        return str(message.get("text") or "").strip()
    if msg_type == "sticker":
        return "[sticker]"
    if msg_type == "image":
        return "[image]"
    if msg_type == "video":
        return "[video]"
    if msg_type == "audio":
        return "[audio]"
    return f"[{msg_type}]" if msg_type else "[message]"


def build_postback_button_message(
    text: str, button_label: str, request_id: str
) -> Dict[str, Any]:
    """Template Buttons message — the slow-LLM postback bubble.

    From PR #18153 (leepoweii). Template Buttons stay tappable from chat
    history, unlike Quick Reply chips which are dismissed the moment any
    new message arrives in the chat.

    LINE limits: ``text`` ≤ 160 chars, ``altText`` ≤ 400 chars.
    """
    truncated = text if len(text) <= 160 else text[:157] + "..."
    alt = text if len(text) <= 400 else text[:397] + "..."
    return {
        "type": "template",
        "altText": alt,
        "template": {
            "type": "buttons",
            "text": truncated,
            "actions": [
                {
                    "type": "postback",
                    "label": button_label[:20] or DEFAULT_BUTTON_LABEL,
                    "data": json.dumps(
                        {"action": "show_response", "request_id": request_id}
                    ),
                    "displayText": button_label[:300] or DEFAULT_BUTTON_LABEL,
                }
            ],
        },
    }


# Prefixes the gateway uses for system busy-acks (interrupting / queued /
# steered). When the postback cache has a PENDING entry we *bypass* the
# cache for these so they reach the user as visible bubbles instead of
# being silently swallowed. From PR #18153.
_SYSTEM_BYPASS_PREFIXES: Tuple[str, ...] = (
    "⚡ Interrupting",
    "⏳ Queued",
    "⏩ Steered",
    "💾",  # background-review summary
)


def _is_system_bypass(content: str) -> bool:
    if not content:
        return False
    return any(content.startswith(p) for p in _SYSTEM_BYPASS_PREFIXES)


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _csv_set(value: str) -> Set[str]:
    if not value:
        return set()
    return {x.strip() for x in value.split(",") if x.strip()}


def _truthy_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def _float_setting(env_name: str, extra_value: Any, default: float) -> float:
    """Resolve a float from env, then the ``extra`` config, then ``default``.

    Any unparseable value at either layer falls back to ``default`` rather
    than raising — a bad knob must never take the adapter down.
    """
    raw = os.getenv(env_name)
    if raw is None:
        raw = extra_value
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _text_setting(env_name: str, extra_value: Any, default: str) -> str:
    raw = os.getenv(env_name)
    if raw is None:
        raw = extra_value
    if raw is None:
        return default
    value = str(raw).strip()
    return value or default


def _text_choices(
    env_name: str,
    plural_value: Any,
    singular_value: Any,
    default: str,
) -> List[str]:
    raw = os.getenv(env_name)
    if raw is not None:
        value = str(raw).strip()
        return [value or default]

    for candidate in (plural_value, singular_value):
        choices = _coerce_text_choices(candidate)
        if choices:
            return choices
    return [default]


def _coerce_text_choices(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    value = str(raw).strip()
    return [value] if value else []


def _choose_text(choices: List[str], default: str) -> str:
    if not choices:
        return default
    if len(choices) == 1:
        return choices[0]
    return random.choice(choices)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class LineAdapter(BasePlatformAdapter):
    """LINE Messaging API gateway adapter."""

    # LINE has its own message-edit story (none) — we always send fresh
    # bubbles, never edit, so REQUIRES_EDIT_FINALIZE stays False.

    def __init__(self, config, **kwargs):
        platform = Platform("line")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        # Credentials
        self.channel_access_token = (
            os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
            or extra.get("channel_access_token", "")
        )
        self.channel_secret = (
            os.getenv("LINE_CHANNEL_SECRET")
            or extra.get("channel_secret", "")
        )

        # Webhook server
        self.webhook_host = os.getenv("LINE_HOST") or extra.get("host", "0.0.0.0")
        try:
            self.webhook_port = int(
                os.getenv("LINE_PORT") or extra.get("port", DEFAULT_WEBHOOK_PORT)
            )
        except (TypeError, ValueError):
            self.webhook_port = DEFAULT_WEBHOOK_PORT
        self.webhook_path = extra.get("webhook_path", DEFAULT_WEBHOOK_PATH)

        # Public base URL — required for media sending when bind isn't
        # publicly reachable.
        self.public_base_url = (
            os.getenv("LINE_PUBLIC_URL")
            or extra.get("public_url", "")
            or ""
        ).rstrip("/")

        # Three-allowlist gating
        self.allow_all = _truthy_env(
            "LINE_ALLOW_ALL_USERS", bool(extra.get("allow_all_users", False))
        )
        self.allowed_users = _csv_set(
            os.getenv("LINE_ALLOWED_USERS", "")
        ) | set(extra.get("allowed_users", []))
        self.allowed_groups = _csv_set(
            os.getenv("LINE_ALLOWED_GROUPS", "")
        ) | set(extra.get("allowed_groups", []))
        self.allowed_rooms = _csv_set(
            os.getenv("LINE_ALLOWED_ROOMS", "")
        ) | set(extra.get("allowed_rooms", []))

        # Slow-LLM postback button threshold
        try:
            self.slow_response_threshold = float(
                os.getenv("LINE_SLOW_RESPONSE_THRESHOLD")
                or extra.get("slow_response_threshold", DEFAULT_SLOW_RESPONSE_THRESHOLD)
            )
        except (TypeError, ValueError):
            self.slow_response_threshold = DEFAULT_SLOW_RESPONSE_THRESHOLD

        # Per-profile slow-response copy; plural YAML list keys take precedence.
        # LINE button labels hard-cap at 20 chars when each chosen label is sent.
        self.pending_reply_texts = _text_choices(
            "LINE_PENDING_REPLY_TEXT",
            extra.get("pending_reply_texts"),
            extra.get("pending_reply_text"),
            DEFAULT_PENDING_REPLY_TEXT,
        )
        self.pending_reply_text = self.pending_reply_texts[0]
        self.pending_button_labels = _text_choices(
            "LINE_PENDING_BUTTON_LABEL",
            extra.get("pending_button_labels"),
            extra.get("pending_button_label"),
            DEFAULT_BUTTON_LABEL,
        )
        self.pending_button_label = self.pending_button_labels[0]
        self.delivered_texts = _text_choices(
            "LINE_DELIVERED_TEXT",
            extra.get("delivered_texts"),
            extra.get("delivered_text"),
            DEFAULT_DELIVERED_TEXT,
        )
        self.delivered_text = self.delivered_texts[0]
        self.interrupted_texts = _text_choices(
            "LINE_INTERRUPTED_TEXT",
            extra.get("interrupted_texts"),
            extra.get("interrupted_text"),
            DEFAULT_INTERRUPTED_TEXT,
        )
        self.interrupted_text = self.interrupted_texts[0]

        # Sender/chat identity resolution (group chats need "who is speaking").
        self.sender_names = _truthy_env(
            "LINE_SENDER_NAMES", bool(extra.get("sender_names", True))
        )

        # Media→text coalescing knobs.
        self.coalesce_media = _truthy_env(
            "LINE_COALESCE_MEDIA", bool(extra.get("coalesce_media", True))
        )
        self.coalesce_media_grace = _float_setting(
            "LINE_COALESCE_MEDIA_GRACE",
            extra.get("coalesce_media_grace"),
            DEFAULT_COALESCE_MEDIA_GRACE,
        )
        self.coalesce_idle = _float_setting(
            "LINE_COALESCE_IDLE",
            extra.get("coalesce_idle"),
            DEFAULT_COALESCE_IDLE,
        )
        self.coalesce_max_age = _float_setting(
            "LINE_COALESCE_MAX_AGE",
            extra.get("coalesce_max_age"),
            DEFAULT_COALESCE_MAX_AGE,
        )

        # Runtime state
        self._client: Optional[_LineClient] = None
        self._app = None  # aiohttp.web.Application
        self._runner = None  # aiohttp.web.AppRunner
        self._site = None  # aiohttp.web.TCPSite
        self._reply_tokens: Dict[str, Tuple[str, float]] = {}  # chat_id → (token, expiry)
        self._cache = RequestCache()
        _dedup_state = os.path.join(os.path.expanduser("~/.hermes/profiles/lucky/state"), "line_dedup.json") if os.path.isdir(os.path.expanduser("~/.hermes/profiles/lucky")) else None
        if _dedup_state:
            os.makedirs(os.path.dirname(_dedup_state), exist_ok=True)
        self._dedup = _MessageDeduplicator(persist_path=_dedup_state)
        self._bot_user_id: Optional[str] = None
        self._lock_key: Optional[str] = None
        self._recent_message_texts: "OrderedDict[str, str]" = OrderedDict()

        # Media state
        self._media_tokens: Dict[str, Tuple[str, float]] = {}  # token → (path, expiry)
        self._media_temp_paths: Set[str] = set()
        # TTL is env-tunable: a token that dies before the family taps the
        # image renders it unopenable (LINE fetches on tap, not at send).
        # FORK DELTA 2026-07-12.
        try:
            self._media_ttl = float(os.getenv("LINE_MEDIA_TTL_SECONDS") or MEDIA_TOKEN_TTL_SECONDS)
        except (TypeError, ValueError):
            self._media_ttl = MEDIA_TOKEN_TTL_SECONDS
        self._media_token_store = _media_token_store_path()
        self._load_media_tokens()

        # Pending-button slot per chat — ensures one outstanding postback
        # button per chat at a time. Postback cache request_id keyed by chat_id.
        self._pending_buttons: Dict[str, str] = {}
        self._pending_delivery_tokens: Dict[str, Tuple[str, str, float]] = {}

        # Identity-resolution cache: key → (resolved_name, expiry). Keys are
        # namespaced by lookup kind (member/profile/group summary).
        self._name_cache: Dict[str, Tuple[str, float]] = {}

        # Media→text coalescer. Emits finished turns through _dispatch_coalesced.
        self._coalescer = _MediaCoalescer(
            self._dispatch_coalesced,
            grace=self.coalesce_media_grace,
            idle=self.coalesce_idle,
            max_age=self.coalesce_max_age,
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.channel_access_token or not self.channel_secret:
            self._set_fatal_error(
                "config_missing",
                "LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET must be set",
                retryable=False,
            )
            return False

        # Prevent two profiles from running on the same channel access token.
        try:
            from gateway.status import acquire_scoped_lock
            # Use a hash of the token so we don't write the secret to disk.
            tok_hash = hashlib.sha256(self.channel_access_token.encode()).hexdigest()[:16]
            if not acquire_scoped_lock("line", tok_hash):
                self._set_fatal_error(
                    "lock_conflict",
                    "LINE channel already in use by another profile",
                    retryable=False,
                )
                return False
            self._lock_key = tok_hash
        except ImportError:
            self._lock_key = None

        self._client = _LineClient(self.channel_access_token)

        # Best-effort: fetch our own bot userId for self-message filtering.
        # If the call fails (offline tests, transient 5xx) we fall back to
        # not filtering self-events; the cost is minor (LINE doesn't
        # actually echo our own messages back).
        try:
            self._bot_user_id = await self._client.get_bot_user_id()
        except Exception as exc:
            logger.debug("LINE: get_bot_user_id failed: %s", exc)
            self._bot_user_id = None

        # Spin up the aiohttp webhook server.
        try:
            from aiohttp import web
        except ImportError:
            self._set_fatal_error(
                "missing_dep",
                "aiohttp is required for the LINE adapter — install with `pip install aiohttp`",
                retryable=False,
            )
            return False

        self._app = web.Application(client_max_size=WEBHOOK_BODY_MAX_BYTES)
        self._app.router.add_post(self.webhook_path, self._handle_webhook)
        # Public health probe — useful for tunnel/proxy verification.
        self._app.router.add_get(f"{self.webhook_path}/health", self._handle_health)
        # Media serving endpoint.
        self._app.router.add_get(
            f"{DEFAULT_MEDIA_PATH_PREFIX}/{{token}}/{{filename}}",
            self._handle_media,
        )

        self._runner = web.AppRunner(self._app)
        try:
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self.webhook_host, self.webhook_port)
            await self._site.start()
        except OSError as exc:
            self._set_fatal_error(
                "bind_failed",
                f"Could not bind LINE webhook on {self.webhook_host}:{self.webhook_port}: {exc}",
                retryable=True,
            )
            return False

        self._mark_connected()
        logger.info(
            "LINE: webhook listening on %s:%s%s%s",
            self.webhook_host,
            self.webhook_port,
            self.webhook_path,
            f" (public: {self.public_base_url})" if self.public_base_url else "",
        )
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

        # Flush any pending coalescing buffers before teardown so a buffered
        # media+caption turn is never silently dropped on shutdown.
        try:
            await self._coalescer.flush_all()
        except Exception as exc:
            logger.debug("LINE: coalescer flush on disconnect failed: %s", exc)

        if self._site is not None:
            try:
                await self._site.stop()
            except Exception:
                pass
            self._site = None
        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except Exception:
                pass
            self._runner = None
        self._app = None

        # Persist tokens, then drop only EXPIRED temp previews: LINE clients
        # still fetch a just-sent image after a restart, so live tokens and
        # their preview files must survive teardown (rehydrated by
        # _load_media_tokens on the next start). FORK DELTA 2026-07-12.
        self._save_media_tokens()
        _now = time.time()
        for path in list(self._media_temp_paths):
            _live = any(
                p == path and exp > _now for p, exp in self._media_tokens.values()
            )
            if _live:
                continue
            try:
                os.unlink(path)
            except OSError:
                pass
            self._media_temp_paths.discard(path)

        if self._lock_key:
            try:
                from gateway.status import release_scoped_lock
                release_scoped_lock("line", self._lock_key)
            except Exception:
                pass
            self._lock_key = None

    # ------------------------------------------------------------------
    # Webhook handlers
    # ------------------------------------------------------------------

    async def _handle_health(self, request) -> Any:
        from aiohttp import web
        return web.json_response({"status": "ok", "platform": "line"})

    async def _handle_webhook(self, request) -> Any:
        from aiohttp import web

        # Body cap defends against memory-exhaustion via crafted Content-Length
        # (aiohttp's client_max_size only applies to certain body modes).
        try:
            body = await request.read()
        except Exception as exc:
            logger.debug("LINE: read failed: %s", exc)
            return web.Response(status=400, text="bad request")
        if len(body) > WEBHOOK_BODY_MAX_BYTES:
            return web.Response(status=413, text="payload too large")

        signature = request.headers.get("X-Line-Signature", "")
        if not verify_line_signature(body, signature, self.channel_secret):
            return web.Response(status=401, text="invalid signature")

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return web.Response(status=400, text="bad json")

        events = payload.get("events", []) or []
        for event in events:
            try:
                await self._dispatch_event(event)
            except Exception:
                logger.exception("LINE: dispatch_event failed")

        return web.Response(status=200, text="ok")

    async def _dispatch_event(self, event: Dict[str, Any]) -> None:
        event_type = event.get("type")
        source = event.get("source") or {}
        webhook_event_id = event.get("webhookEventId", "") or ""

        # Dedup retries (LINE webhooks may be re-delivered).
        if webhook_event_id and self._dedup.is_duplicate(webhook_event_id):
            logger.debug("LINE: ignoring duplicate webhook event %s", webhook_event_id)
            return

        # Filter our own messages (self-echo).
        sender_user_id = source.get("userId", "")
        if self._bot_user_id and sender_user_id == self._bot_user_id:
            return

        # Allowlist gate.
        if not _allowed_for_source(
            source,
            allow_all=self.allow_all,
            user_ids=self.allowed_users,
            group_ids=self.allowed_groups,
            room_ids=self.allowed_rooms,
        ):
            logger.info("LINE: rejecting unauthorized source %s", source)
            return

        if event_type == "message":
            await self._handle_message_event(event)
        elif event_type == "postback":
            await self._handle_postback_event(event)
        elif event_type in {"follow", "unfollow", "join", "leave"}:
            logger.info("LINE: lifecycle event %s from %s", event_type, source)
        else:
            logger.debug("LINE: ignoring event type %r", event_type)

    async def _handle_message_event(self, event: Dict[str, Any]) -> None:
        msg = event.get("message") or {}
        msg_type = msg.get("type", "")
        message_id = msg.get("id", "")
        reply_token = event.get("replyToken", "")
        source = event.get("source") or {}
        chat_id, chat_type = _resolve_chat(source)
        user_id = source.get("userId", "") or chat_id

        # Stash the reply token for outbound use.
        if chat_id and reply_token:
            self._reply_tokens[chat_id] = (
                reply_token,
                time.time() + LINE_REPLY_TOKEN_TTL_SECONDS,
            )

        # Handle media inbound — fetch the binary, cache it, and surface a
        # vision-tool-friendly local path on the MessageEvent.
        media_urls: List[str] = []
        media_types: List[str] = []
        text = ""

        if msg_type == "text":
            text = msg.get("text", "") or ""
        elif msg_type in {"image", "audio", "video", "file"}:
            local_path = await self._download_media(message_id, msg_type)
            if local_path:
                media_urls.append(local_path)
                media_types.append(msg_type)
            text = f"[{msg_type}]"
        elif msg_type == "sticker":
            keywords = msg.get("keywords") or []
            text = f"[sticker: {', '.join(keywords)}]" if keywords else "[sticker]"
        elif msg_type == "location":
            title = msg.get("title", "")
            address = msg.get("address", "")
            text = f"[location: {title} {address}]".strip()
        else:
            text = f"[unsupported message type: {msg_type}]"

        # Best-effort typing indicator (DM only).
        if chat_type == "dm" and self._client:
            asyncio.create_task(self._client.loading(chat_id))

        # Route through the media→text coalescer when enabled. MEDIA opens or
        # extends a buffer; TEXT merges into an open buffer (else dispatches
        # immediately). Self-contained turns (sticker/location/audio/unknown)
        # bypass the buffer entirely and never disturb a pending one.
        if self.coalesce_media:
            key = (chat_id, user_id)
            if msg_type in _COALESCE_MEDIA_TYPES:
                await self._coalescer.submit_media(
                    key, event, text, media_urls, media_types, message_id
                )
                return
            if msg_type == "text":
                await self._coalescer.submit_text(
                    key, event, text, media_urls, media_types, message_id
                )
                return

        await self._dispatch_coalesced(
            raw_event=event,
            text=text,
            media_urls=media_urls,
            media_types=media_types,
            message_id=message_id,
        )

    async def _dispatch_coalesced(
        self,
        *,
        raw_event: Dict[str, Any],
        text: str,
        media_urls: List[str],
        media_types: List[str],
        message_id: str,
    ) -> None:
        """Build the final ``MessageEvent`` (resolving sender/chat identity)
        and hand it to the gateway. Single builder for every dispatch path —
        immediate, coalesced flush, and shutdown flush."""
        msg = raw_event.get("message") or {}
        msg_type = msg.get("type", "")
        source = raw_event.get("source") or {}
        chat_id, chat_type = _resolve_chat(source)
        user_id = source.get("userId", "") or chat_id

        user_name = await self._resolve_sender_name(source, user_id)
        chat_name = await self._resolve_chat_name(source, chat_id, chat_type)

        # A coalesced image+caption must route as PHOTO (so vision fires), not
        # as the TEXT type of whichever event happened to arrive last.
        if media_types:
            message_type = _LINE_MESSAGE_TYPES.get(media_types[0], MessageType.TEXT)
        else:
            message_type = _LINE_MESSAGE_TYPES.get(msg_type, MessageType.TEXT)

        source_obj = self.build_source(
            chat_id=chat_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
            chat_name=chat_name,
        )

        reply_to_id, reply_to_text = self._resolve_quote_context(chat_id, msg)

        event_obj = MessageEvent(
            text=text,
            message_type=message_type,
            source=source_obj,
            raw_message=raw_event,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=reply_to_id,
            reply_to_text=reply_to_text,
        )

        self._remember_recent_message_text(chat_id, message_id, text)
        await self.handle_message(event_obj)

    def _recent_message_key(self, chat_id: str, message_id: str) -> str:
        return f"{chat_id}:{message_id}"

    def _remember_recent_message_text(
        self,
        chat_id: str,
        message_id: str,
        text: str,
    ) -> None:
        if not chat_id or not message_id or not text:
            return
        key = self._recent_message_key(chat_id, message_id)
        self._recent_message_texts[key] = text[:2000]
        self._recent_message_texts.move_to_end(key)
        while len(self._recent_message_texts) > LINE_RECENT_MESSAGE_CACHE_SIZE:
            self._recent_message_texts.popitem(last=False)

    def _remember_sent_message_texts(
        self,
        chat_id: str,
        response: Any,
        messages: List[Dict[str, Any]],
    ) -> None:
        for sent_id, message in zip(_sent_message_ids(response), messages):
            sent_text = _recent_text_for_outbound_message(message)
            self._remember_recent_message_text(chat_id, sent_id, sent_text)

    def _resolve_quote_context(
        self,
        chat_id: str,
        msg: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str]]:
        quoted_id = str(msg.get("quotedMessageId") or "").strip()
        if not quoted_id:
            return None, None

        quoted_text: Optional[str] = None
        try:
            quoted_text = rich_sent_store.lookup(chat_id, quoted_id)
        except Exception as exc:
            logger.debug("LINE: rich quote lookup failed for %s: %s", quoted_id, exc)

        if not quoted_text:
            quoted_text = self._recent_message_texts.get(
                self._recent_message_key(chat_id, quoted_id)
            )

        if not quoted_text:
            quoted_text = "[quoted an earlier message]"
        return quoted_id, quoted_text

    # ------------------------------------------------------------------
    # Sender / chat identity resolution
    # ------------------------------------------------------------------

    def _prune_name_cache(self) -> None:
        """Drop expired identity entries opportunistically (mirrors the
        _media_tokens eviction idiom — no background sweeper)."""
        now = time.time()
        for cache_key in list(self._name_cache.keys()):
            if self._name_cache[cache_key][1] <= now:
                self._name_cache.pop(cache_key, None)

    async def _cached_lookup(self, cache_key: str, url: str, field_name: str) -> Optional[str]:
        """Return ``field_name`` from ``url`` (via cache), or ``None`` on any
        miss/error. Successful lookups are cached for the TTL; failures are
        not cached, so a transient error retries on the next message."""
        self._prune_name_cache()
        cached = self._name_cache.get(cache_key)
        if cached is not None and cached[1] > time.time():
            return cached[0]
        if not self._client:
            return None
        data = await self._client.get_json(url)
        if not data:
            return None
        value = data.get(field_name)
        if not value:
            return None
        self._name_cache[cache_key] = (value, time.time() + SENDER_NAME_CACHE_TTL_SECONDS)
        return value

    async def _resolve_sender_name(self, source: Dict[str, Any], user_id: str) -> str:
        """Resolve a sender's LINE display name, fail-open to the raw id.

        In group/room chats the raw ``U…`` id tells the agent nothing about
        who is speaking. We resolve it via the member/profile API, cache the
        result ~6h, and on any error fall back to the id so message handling
        is never blocked.
        """
        if not self.sender_names or not user_id:
            return user_id
        src_type = (source or {}).get("type", "")
        if src_type == "group":
            group_id = source.get("groupId", "")
            if not group_id:
                return user_id
            cache_key = f"member:group:{group_id}:{user_id}"
            url = LINE_GROUP_MEMBER_URL_FMT.format(group_id=group_id, user_id=user_id)
        elif src_type == "room":
            room_id = source.get("roomId", "")
            if not room_id:
                return user_id
            cache_key = f"member:room:{room_id}:{user_id}"
            url = LINE_ROOM_MEMBER_URL_FMT.format(room_id=room_id, user_id=user_id)
        else:
            cache_key = f"profile:{user_id}"
            url = LINE_PROFILE_URL_FMT.format(user_id=user_id)
        try:
            name = await self._cached_lookup(cache_key, url, "displayName")
        except Exception as exc:  # defensive — resolution never blocks handling
            logger.debug("LINE: sender-name resolution failed for %s: %s", user_id, exc)
            return user_id
        if name:
            return name
        logger.debug("LINE: no display name for %s; using raw id", user_id)
        return user_id

    async def _resolve_chat_name(
        self, source: Dict[str, Any], chat_id: str, chat_type: str
    ) -> str:
        """Resolve a human chat title, fail-open to ``chat_id``.

        Only group chats expose a summary endpoint (``groupName``); rooms are
        anonymous and DMs have no group title, so those keep the id.
        """
        if not self.sender_names or chat_type != "group":
            return chat_id
        group_id = (source or {}).get("groupId", "") or chat_id
        if not group_id:
            return chat_id
        cache_key = f"group_summary:{group_id}"
        url = LINE_GROUP_SUMMARY_URL_FMT.format(group_id=group_id)
        try:
            name = await self._cached_lookup(cache_key, url, "groupName")
        except Exception as exc:
            logger.debug("LINE: chat-name resolution failed for %s: %s", group_id, exc)
            return chat_id
        return name or chat_id

    async def _handle_postback_event(self, event: Dict[str, Any]) -> None:
        """User tapped the slow-LLM postback button — deliver cached payload."""
        postback = event.get("postback") or {}
        data = postback.get("data", "") or ""
        reply_token = event.get("replyToken", "")
        source = event.get("source") or {}
        chat_id, _ = _resolve_chat(source)

        try:
            parsed = json.loads(data)
        except (TypeError, json.JSONDecodeError):
            return

        if parsed.get("action") != "show_response":
            return
        request_id = parsed.get("request_id", "")
        if not request_id:
            return

        entry = self._cache.get(request_id)
        if not self._client or not reply_token or not entry:
            return

        if entry.state in {State.READY, State.ERROR}:
            await self._deliver_cached_response(request_id, chat_id, reply_token)
        elif entry.state is State.DELIVERED:
            try:
                await self._client.reply(
                    reply_token, [_text_message(self._select_delivered_text())]
                )
            except Exception:
                pass
        elif entry.state is State.DELIVERING:
            return
        elif entry.state is State.PENDING:
            # Pai's pull model (2026-07-09): every tap while the answer is
            # not ready just replies the predefined "not done yet" text via
            # THIS tap's own fresh reply token. No token stashing, no
            # proactive push later -- the user taps again to check, and when
            # the entry is READY the branch above delivers text+image in one
            # free reply. Pinned by test_pending_tap_always_replies_not_done.
            try:
                await self._client.reply(
                    reply_token, [_text_message(self._select_pending_reply_text())]
                )
            except Exception:
                pass

    def _register_pending_delivery_token(
        self,
        request_id: str,
        chat_id: str,
        reply_token: str,
    ) -> bool:
        if request_id in self._pending_delivery_tokens:
            return False
        self._pending_delivery_tokens[request_id] = (
            chat_id,
            reply_token,
            time.time() + LINE_REPLY_TOKEN_TTL_SECONDS,
        )
        return True

    async def _deliver_registered_pending_response(self, request_id: str) -> bool:
        delivery = self._pending_delivery_tokens.get(request_id)
        if not delivery:
            return False
        chat_id, reply_token, expires_at = delivery
        usable_reply_token = reply_token if time.time() < expires_at else ""
        return await self._deliver_cached_response(
            request_id, chat_id, usable_reply_token
        )

    async def _deliver_cached_response(
        self,
        request_id: str,
        chat_id: str,
        reply_token: str,
    ) -> bool:
        claim = self._cache.claim_delivery(request_id)
        if claim is None:
            return False
        previous_state, payload = claim
        messages = self._messages_for_cached_payload(payload, previous_state)
        if not messages:
            messages = [_text_message("")]

        # A reactive answer is FREE-reply-only: pushing behind the button's back
        # defeats the button (Pai's rule, 2026-07-08). Without a live reply token
        # we keep the answer cached (READY) so the next press delivers it free.
        if not reply_token:
            logger.info("LINE cached-hold (no token; awaiting next tap, never push) chat=%s n=%d", chat_id, len(messages))
            self._cache.release_delivery_claim(request_id, previous_state)
            return False
        logger.info("LINE SEND site=cached kind=reply chat=%s n=%d", chat_id, len(messages))
        try:
            response = await self._client.reply(reply_token, messages)
            self._remember_sent_message_texts(chat_id, response, messages)
            self._cache.mark_delivered(request_id)
            self._pending_buttons.pop(chat_id, None)
            self._pending_delivery_tokens.pop(request_id, None)
            return True
        except Exception as exc:
            logger.warning(
                "LINE: postback reply failed (%s); keeping cached for next press (no push)",
                exc,
            )
            self._cache.release_delivery_claim(request_id, previous_state)
            return False

    def _messages_for_cached_payload(
        self,
        payload: Any,
        state: State,
    ) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return _messages_from_prebuilt_payload([
                message for message in payload[:LINE_MAX_MESSAGES_PER_CALL]
                if isinstance(message, dict)
            ])[:LINE_MAX_MESSAGES_PER_CALL]
        text = (
            str(payload or self._select_interrupted_text())
            if state is State.ERROR
            else str(payload or "")
        )
        return _messages_from_text_payload(text)

    async def _download_media(self, message_id: str, msg_type: str) -> Optional[str]:
        if not self._client or not message_id:
            return None
        try:
            data = await self._client.fetch_content(message_id)
        except Exception as exc:
            logger.warning("LINE: failed to fetch %s content for %s: %s", msg_type, message_id, exc)
            return None
        ext = {
            "image": ".jpg",
            "audio": ".m4a",
            "video": ".mp4",
            "file": ".bin",
        }.get(msg_type, ".bin")
        try:
            if msg_type == "image":
                return cache_image_from_bytes(data, ext=ext)
            return cache_media_from_bytes(data, ext=ext, media_type=msg_type)
        except Exception as exc:
            logger.warning("LINE: failed to cache %s payload: %s", msg_type, exc)
            return None

    # ------------------------------------------------------------------
    # Outbound send (text)
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")

        # System busy-acks (interrupting / queued / steered) bypass the
        # postback cache and route directly to LINE so they reach the user
        # as visible bubbles. Source: PR #18153.
        if _is_system_bypass(content):
            return await self._send_text_chunks(chat_id, content, force_push=False)

        # If the chat has a PENDING postback button outstanding, route the
        # response into the cache for the user to fetch via tap.
        pending_rid = self._pending_buttons.get(chat_id)
        if pending_rid:
            self._cache.set_ready(pending_rid, content)
            if await self._deliver_registered_pending_response(pending_rid):
                return SendResult(success=True, message_id=pending_rid)
            # The button is the delivery contract: if the answer can't go out as
            # a FREE reply yet (no press / token expired), keep it cached behind
            # the live button and never push. The press delivers it. (Pai, 2026-07-08)
            entry = self._cache.get(pending_rid)
            if entry is not None and entry.state in {State.READY, State.ERROR}:
                return SendResult(success=True, message_id=pending_rid)
            # Stale button (already delivered): clear so we don't swallow this
            # fresh answer, then fall through to the normal path.
            logger.warning(
                "LINE: stale pending-button rid=%s for chat %s; clearing",
                pending_rid, chat_id,
            )
            self._cache.mark_delivered(pending_rid)
            self._pending_buttons.pop(chat_id, None)
            self._pending_delivery_tokens.pop(pending_rid, None)

        return await self._send_text_chunks(chat_id, content, force_push=False)

    async def _send_text_chunks(
        self,
        chat_id: str,
        content: str,
        *,
        force_push: bool,
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")

        messages = _messages_from_text_payload(content)
        if not messages:
            return SendResult(success=True, message_id=None)

        token, used_reply = self._consume_reply_token(chat_id)
        logger.info("LINE SEND site=text kind=%s chat=%s n=%d", "reply" if (used_reply and not force_push) else "push", chat_id, len(messages))
        if used_reply and not force_push:
            try:
                response = await self._client.reply(token, messages)
                self._remember_sent_message_texts(chat_id, response, messages)
                return SendResult(success=True, message_id=token)
            except Exception as exc:
                logger.info("LINE: reply token rejected (%s); falling back to push", exc)
                # fall through to push

        try:
            response = await self._client.push(chat_id, messages)
            self._remember_sent_message_texts(chat_id, response, messages)
            return SendResult(success=True, message_id=None)
        except Exception as exc:
            logger.error("LINE: push send failed: %s", exc)
            return SendResult(success=False, error=str(exc))

    def _consume_reply_token(self, chat_id: str) -> Tuple[str, bool]:
        """Consume a stashed reply token if present and unexpired.

        Returns ``(token, used_reply)``.
        """
        entry = self._reply_tokens.pop(chat_id, None)
        if not entry:
            return "", False
        token, expires_at = entry
        if not token or time.time() >= expires_at:
            return "", False
        return token, True

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Trigger LINE's loading-animation indicator (DM only)."""
        if self._client and chat_id:
            await self._client.loading(chat_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Best-effort chat info derived from the chat_id prefix.

        LINE's chat-info APIs are limited and per-source-type — instead of
        chasing them we infer from the well-known ID prefixes:
        ``U`` = user (1:1), ``C`` = group, ``R`` = room. The agent only
        needs ``name`` + ``type`` from this method.
        """
        prefix = (chat_id or "")[:1]
        chat_type = {"U": "dm", "C": "group", "R": "channel"}.get(prefix, "dm")
        return {"name": chat_id or "", "type": chat_type}

    def format_message(self, content: str) -> str:
        """Strip Markdown that LINE can't render. URLs are preserved."""
        return strip_markdown_preserving_urls(content)

    def _select_pending_reply_text(self) -> str:
        return _choose_text(self.pending_reply_texts, DEFAULT_PENDING_REPLY_TEXT)

    def _select_pending_button_label(self) -> str:
        return _choose_text(self.pending_button_labels, DEFAULT_BUTTON_LABEL)

    def _select_delivered_text(self) -> str:
        return _choose_text(self.delivered_texts, DEFAULT_DELIVERED_TEXT)

    def _select_interrupted_text(self) -> str:
        return _choose_text(self.interrupted_texts, DEFAULT_INTERRUPTED_TEXT)

    # ------------------------------------------------------------------
    # Slow-LLM postback button — driven by _keep_typing
    # ------------------------------------------------------------------

    async def _keep_typing(self, chat_id: str, *args, **kwargs) -> None:
        """Override the base loop to fire the postback button at threshold.

        We intentionally keep the base implementation behind us: it's
        responsible for the typing-indicator heartbeat, while *this*
        wrapper layers in the slow-LLM postback bubble at threshold.
        """
        if (
            self.slow_response_threshold <= 0
            or not self._client
            or not chat_id
        ):
            await super()._keep_typing(chat_id, *args, **kwargs)
            return

        async def _fire_postback() -> None:
            try:
                await asyncio.sleep(self.slow_response_threshold)
            except asyncio.CancelledError:
                raise
            # Only fire if we still have a usable reply token. If the agent
            # already responded, _consume_reply_token has cleared it.
            if chat_id not in self._reply_tokens:
                return
            if chat_id in self._pending_buttons:
                return
            rid = self._cache.register_pending(chat_id)
            self._pending_buttons[chat_id] = rid
            token, used = self._consume_reply_token(chat_id)
            if not used:
                self._pending_buttons.pop(chat_id, None)
                return
            msg = build_postback_button_message(
                self._select_pending_reply_text(),
                self._select_pending_button_label(),
                rid,
            )
            try:
                await self._client.reply(token, [msg])
                logger.info("LINE: sent slow-LLM postback button for chat %s (rid=%s)", chat_id, rid)
            except Exception as exc:
                logger.warning("LINE: postback button send failed: %s", exc)
                self._pending_buttons.pop(chat_id, None)

        post_task = asyncio.create_task(_fire_postback())
        try:
            await super()._keep_typing(chat_id, *args, **kwargs)
        finally:
            if not post_task.done():
                post_task.cancel()
                try:
                    await post_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def interrupt_session_activity(self, session_key: str, chat_id: str) -> None:
        """Resolve any orphan PENDING postback so the button doesn't loop."""
        await super().interrupt_session_activity(session_key, chat_id)
        rid = self._pending_buttons.pop(chat_id, None)
        if rid:
            self._cache.set_error(rid, self._select_interrupted_text())
            await self._deliver_registered_pending_response(rid)

    # ------------------------------------------------------------------
    # Outbound media (image / voice / video)
    # ------------------------------------------------------------------

    def _load_media_tokens(self) -> None:
        """Rehydrate persisted media-serving tokens so restarts don't break sent images.

        The LINE app fetches ``originalContentUrl`` when a family member taps
        the image — often long after a safe-restart. Losing the in-memory
        token map turned every earlier image into a 404 ("can't open").
        Store format: ``{token: [path, expiry, is_temp]}`` (legacy 2-field
        entries tolerated). Expired or missing-file entries are dropped.
        FORK DELTA 2026-07-12.
        """
        path = self._media_token_store
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                return
            now = time.time()
            for token, entry in data.items():
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                file_p, expires_at = str(entry[0]), float(entry[1])
                if expires_at <= now or not os.path.isfile(file_p):
                    continue
                self._media_tokens[token] = (file_p, expires_at)
                if len(entry) >= 3 and entry[2]:
                    self._media_temp_paths.add(file_p)
        except Exception as exc:
            logger.warning("LINE: failed to load media token store: %s", exc)

    def _save_media_tokens(self) -> None:
        """Persist the media token map (atomic write; best-effort). FORK DELTA 2026-07-12."""
        path = self._media_token_store
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                token: [p, exp, p in self._media_temp_paths]
                for token, (p, exp) in self._media_tokens.items()
            }
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, path)
        except Exception as exc:
            logger.warning("LINE: failed to persist media token store: %s", exc)

    def _register_media(self, file_path: str, *, cleanup: bool = False) -> str:
        """Register a local file for HTTPS serving; return the URL token."""
        # Evict expired tokens first.
        now = time.time()
        for token in list(self._media_tokens.keys()):
            path, exp = self._media_tokens[token]
            if now > exp:
                self._media_tokens.pop(token, None)
                if path in self._media_temp_paths:
                    self._media_temp_paths.discard(path)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass

        resolved = str(Path(file_path).resolve())
        token = secrets.token_urlsafe(32)
        self._media_tokens[token] = (resolved, now + self._media_ttl)
        if cleanup:
            self._media_temp_paths.add(resolved)
        self._save_media_tokens()
        return token

    def _media_url(self, token: str, filename: str) -> str:
        """Build the public HTTPS URL for a media token. PR #8398 style."""
        if self.public_base_url:
            base = self.public_base_url
        else:
            host = self.webhook_host
            port = self.webhook_port
            if port == 443:
                base = f"https://{host}"
            else:
                base = f"https://{host}:{port}"
        safe_name = _urlquote(filename, safe="")
        return f"{base}{DEFAULT_MEDIA_PATH_PREFIX}/{token}/{safe_name}"

    async def _handle_media(self, request) -> Any:
        """Serve a registered local file over HTTPS for LINE's media URLs.

        Defence-in-depth: even though ``_register_media`` is only called
        from trusted internal code, we recheck the resolved path against
        an allowed-roots set before serving. Sources allowed:
        ``tempfile.gettempdir()``, ``/tmp`` (which resolves to
        ``/private/tmp`` on macOS), and ``HERMES_HOME``. PR #8398.
        """
        from aiohttp import web

        token = request.match_info["token"]
        entry = self._media_tokens.get(token)
        if not entry:
            return web.Response(status=404, text="not found")

        file_path, expires_at = entry
        if time.time() > expires_at:
            self._media_tokens.pop(token, None)
            return web.Response(status=410, text="gone")

        path = Path(file_path)
        if not path.exists() or not path.is_file():
            return web.Response(status=404, text="not found")

        try:
            from hermes_constants import get_hermes_home
            hermes_home = Path(get_hermes_home()).resolve()
        except Exception:
            hermes_home = Path.home().joinpath(".hermes").resolve()

        allowed_roots = {
            Path(tempfile.gettempdir()).resolve(),
            Path("/tmp").resolve(),  # → /private/tmp on macOS
            hermes_home,
        }
        resolved = path.resolve()
        if not any(_is_relative_to(resolved, r) for r in allowed_roots):
            logger.warning("LINE: refusing to serve outside allowed roots: %s", resolved)
            return web.Response(status=403, text="forbidden")

        content_type, _ = mimetypes.guess_type(str(path))
        return web.FileResponse(
            path,
            headers={"Content-Type": content_type or "application/octet-stream"},
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        path = Path(image_path)
        if not path.exists() or not path.is_file():
            return SendResult(success=False, error=f"image file not found: {image_path}")
        if path.stat().st_size > LINE_IMAGE_MAX_BYTES:
            return SendResult(success=False, error="image exceeds 10 MB LINE limit")
        if not self._mark_media_egress_allowed(chat_id, str(path)):
            return SendResult(success=True)
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")
        if not self.public_base_url and self.webhook_host == "0.0.0.0":
            return SendResult(
                success=False,
                error="LINE_PUBLIC_URL must be set to send images "
                "(LINE only accepts publicly reachable HTTPS URLs)",
            )

        token = self._register_media(str(path.resolve()))
        url = self._media_url(token, path.name)
        if not url.lower().startswith("https://"):
            return SendResult(success=False, error=f"LINE image URL must be HTTPS: {url}")
        # LINE caps previewImageUrl at 1 MB; a full multi-MB PNG reused as the
        # preview renders BLANK on the LINE mobile app (desktop loads the
        # original directly). Generate a small JPEG thumbnail for the preview
        # so phones render it; fall back to the original URL if Pillow can't.
        preview_url = url
        _preview_path = _generate_line_preview(str(path.resolve()))
        if _preview_path:
            _preview_token = self._register_media(_preview_path, cleanup=True)
            preview_url = self._media_url(_preview_token, os.path.basename(_preview_path))
        msgs: List[Dict[str, Any]] = [_image_message(url, preview_url)]
        if caption:
            msgs.append(_text_message(caption))
        return await self._send_messages(chat_id, msgs)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        duration_ms: int = 1000,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        path = Path(audio_path)
        if not path.exists() or not path.is_file():
            return SendResult(success=False, error=f"audio file not found: {audio_path}")
        if path.stat().st_size > LINE_AV_MAX_BYTES:
            return SendResult(success=False, error="audio exceeds 200 MB LINE limit")
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")
        if not self.public_base_url and self.webhook_host == "0.0.0.0":
            return SendResult(
                success=False,
                error="LINE_PUBLIC_URL must be set to send audio",
            )

        token = self._register_media(str(path.resolve()))
        url = self._media_url(token, path.name)
        return await self._send_messages(chat_id, [_audio_message(url, duration_ms)])

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        preview_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        path = Path(video_path)
        if not path.exists() or not path.is_file():
            return SendResult(success=False, error=f"video file not found: {video_path}")
        if path.stat().st_size > LINE_AV_MAX_BYTES:
            return SendResult(success=False, error="video exceeds 200 MB LINE limit")
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")
        if not self.public_base_url and self.webhook_host == "0.0.0.0":
            return SendResult(
                success=False,
                error="LINE_PUBLIC_URL must be set to send video",
            )

        # LINE requires a previewImageUrl. Use one if supplied, otherwise
        # write a stdlib 1×1 PNG to /tmp and serve it. PR #8398.
        if preview_path and Path(preview_path).is_file():
            preview_token = self._register_media(str(Path(preview_path).resolve()))
            preview_filename = Path(preview_path).name
        else:
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            try:
                tmp.write(_FALLBACK_PNG_PREVIEW)
                tmp.flush()
                tmp.close()
                preview_token = self._register_media(tmp.name, cleanup=True)
                preview_filename = "preview.png"
            except Exception:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
                raise

        video_token = self._register_media(str(path.resolve()))
        video_url = self._media_url(video_token, path.name)
        preview_url = self._media_url(preview_token, preview_filename)
        return await self._send_messages(chat_id, [_video_message(video_url, preview_url)])

    async def _send_messages(
        self,
        chat_id: str,
        messages: List[Dict[str, Any]],
    ) -> SendResult:
        """Send already-built message objects, batched at 5/call."""
        if not self._client:
            return SendResult(success=False, error="LINE adapter not connected")
        messages = _messages_from_prebuilt_payload(messages)
        if not messages:
            return SendResult(success=True, message_id=None)

        pending_rid = self._pending_buttons.get(chat_id)
        if pending_rid:
            self._cache.set_ready(
                pending_rid,
                messages[:LINE_MAX_MESSAGES_PER_CALL],
            )
            if await self._deliver_registered_pending_response(pending_rid):
                return SendResult(success=True, message_id=pending_rid)
            # Live button (answer READY, awaiting a press): keep cached, never
            # push — the press is the delivery contract (Pai, 2026-07-08).
            entry = self._cache.get(pending_rid)
            if entry is not None and entry.state in {State.READY, State.ERROR}:
                return SendResult(success=True, message_id=pending_rid)
            # Stale button rid (no registered press, tokens expired, or the
            # payload claim was already spent): do NOT swallow the send.
            # Tombstone the cached payload so a very late press of the old
            # button can't replay it, clear the stale entry, and fall through
            # to the normal reply/push path. (2026-07-06: a leftover rid
            # silently ate two real replies — "sent" was logged, nothing
            # reached the chat.)
            logger.warning(
                "LINE: pending-button delivery unavailable for rid=%s; "
                "clearing stale entry and sending via reply/push",
                pending_rid,
            )
            self._cache.mark_delivered(pending_rid)
            self._pending_buttons.pop(chat_id, None)
            self._pending_delivery_tokens.pop(pending_rid, None)

        first_batch = messages[:LINE_MAX_MESSAGES_PER_CALL]
        rest = messages[LINE_MAX_MESSAGES_PER_CALL:]

        # First batch: try reply token, fall back to push.
        token, used_reply = self._consume_reply_token(chat_id)
        logger.info("LINE SEND site=prebuilt kind=%s chat=%s n=%d", "reply" if used_reply else "push", chat_id, len(messages))
        if used_reply:
            try:
                response = await self._client.reply(token, first_batch)
                self._remember_sent_message_texts(chat_id, response, first_batch)
            except Exception as exc:
                logger.info("LINE: reply token rejected (%s); falling back to push", exc)
                try:
                    response = await self._client.push(chat_id, first_batch)
                    self._remember_sent_message_texts(chat_id, response, first_batch)
                except Exception as exc2:
                    return SendResult(success=False, error=str(exc2))
        else:
            try:
                response = await self._client.push(chat_id, first_batch)
                self._remember_sent_message_texts(chat_id, response, first_batch)
            except Exception as exc:
                return SendResult(success=False, error=str(exc))

        # Subsequent batches: always push (reply token is single-use).
        while rest:
            batch = rest[:LINE_MAX_MESSAGES_PER_CALL]
            rest = rest[LINE_MAX_MESSAGES_PER_CALL:]
            try:
                response = await self._client.push(chat_id, batch)
                self._remember_sent_message_texts(chat_id, response, batch)
            except Exception as exc:
                logger.warning("LINE: push for follow-up batch failed: %s", exc)
                return SendResult(success=False, error=str(exc))

        return SendResult(success=True, message_id=None)


def _is_relative_to(child: Path, parent: Path) -> bool:
    """Backport for Path.is_relative_to (Python 3.9+) — defensive against
    cwd-resolution differences across CI runners."""
    try:
        return child.resolve().is_relative_to(parent.resolve())
    except (AttributeError, ValueError):
        try:
            child.resolve().relative_to(parent.resolve())
            return True
        except ValueError:
            return False


# ---------------------------------------------------------------------------
# Plugin entry-point hooks
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    """Plugin gate: require credentials AND aiohttp at runtime."""
    if not os.getenv("LINE_CHANNEL_ACCESS_TOKEN"):
        return False
    if not os.getenv("LINE_CHANNEL_SECRET"):
        return False
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    has_token = bool(
        os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or extra.get("channel_access_token")
    )
    has_secret = bool(
        os.getenv("LINE_CHANNEL_SECRET") or extra.get("channel_secret")
    )
    return has_token and has_secret


def is_connected(config) -> bool:
    """Surface in ``hermes status`` even before the adapter is instantiated."""
    return validate_config(config)


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Auto-seed PlatformConfig.extra from env-only setups.

    Lets ``hermes status`` reflect a LINE configuration that lives entirely
    in ``.env`` without a ``platforms.line`` block in ``config.yaml``.
    Mirrors the IRC plugin's pattern.
    """
    if not (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") and os.getenv("LINE_CHANNEL_SECRET")):
        return None
    seeded: Dict[str, Any] = {}
    if os.getenv("LINE_PORT"):
        try:
            seeded["port"] = int(os.environ["LINE_PORT"])
        except ValueError:
            pass
    if os.getenv("LINE_HOST"):
        seeded["host"] = os.environ["LINE_HOST"]
    if os.getenv("LINE_PUBLIC_URL"):
        seeded["public_url"] = os.environ["LINE_PUBLIC_URL"]
    if os.getenv("LINE_HOME_CHANNEL"):
        seeded["home_channel"] = os.environ["LINE_HOME_CHANNEL"]
    return seeded or {}


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process push delivery for cron jobs running detached from the gateway.

    Without this hook ``deliver=line`` cron jobs fail with ``no live adapter``
    when cron runs as its own process. We always Push (reply tokens require
    an inbound webhook event we don't have in this path).

    ``thread_id`` is accepted for signature parity but ignored — LINE has
    no native thread primitive on the channel-side API. ``media_files``
    likewise: cron-side media delivery requires a publicly-reachable URL,
    which the standalone path can't construct without binding the webhook
    server, so we send a text reference instead.
    """
    extra = getattr(pconfig, "extra", {}) or {}
    token = (
        os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        or extra.get("channel_access_token", "")
    )
    if not token or not chat_id:
        return {"error": "LINE standalone send: missing token or chat_id"}

    messages = _messages_from_text_payload(message or "")
    if not messages:
        return {"success": True, "message_id": None}
    if media_files:
        # Tack on a hint so the recipient knows media was generated but not delivered.
        messages.append(_text_message(f"[{len(media_files)} attachment(s) generated; not deliverable from cron]"))
        messages = messages[:LINE_MAX_MESSAGES_PER_CALL]

    client = _LineClient(token)
    try:
        await client.push(chat_id, messages)
        return {"success": True, "message_id": None}
    except Exception as exc:
        return {"error": str(exc)}


def interactive_setup() -> None:
    """Minimal stdin wizard for ``hermes setup line``.

    Mirrors the irc/teams style: prompts for the two required vars, plus
    one optional public URL. Writes to ``~/.hermes/.env`` via ``hermes_cli.config``.
    """
    print()
    print("LINE Messaging API setup")
    print("------------------------")
    print("Create a Messaging API channel at https://developers.line.biz/console/")
    print("then copy the values below.")
    print()

    try:
        from hermes_cli.config import get_env_var, set_env_var
    except ImportError:
        print("hermes_cli.config not available; set LINE_* vars manually in ~/.hermes/.env")
        return

    def _prompt(var: str, prompt: str, *, secret: bool = False) -> None:
        existing = get_env_var(var) if callable(get_env_var) else None
        suffix = " [keep current]" if existing else ""
        try:
            if secret:
                from hermes_cli.secret_prompt import masked_secret_prompt
                value = masked_secret_prompt(f"{prompt}{suffix}: ")
            else:
                value = input(f"{prompt}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if value:
            set_env_var(var, value)

    _prompt("LINE_CHANNEL_ACCESS_TOKEN", "Channel access token", secret=True)
    _prompt("LINE_CHANNEL_SECRET", "Channel secret", secret=True)
    _prompt("LINE_PUBLIC_URL", "Public HTTPS base URL (optional, e.g. https://my-tunnel.example.com)")
    _prompt("LINE_ALLOWED_USERS", "Allowed user IDs (comma-separated; blank=skip)")
    print("Done. Set the webhook URL in the LINE console to "
          "<your-public-url>/line/webhook and enable 'Use webhook'.")


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    ctx.register_platform(
        name="line",
        label="LINE",
        adapter_factory=lambda cfg: LineAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["LINE_CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_SECRET"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="LINE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="LINE_ALLOWED_USERS",
        allow_all_env="LINE_ALLOW_ALL_USERS",
        # LINE per-bubble cap is 5000; smart-chunker uses 4500.
        max_message_length=LINE_SAFE_BUBBLE_CHARS,
        emoji="💚",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via LINE Messaging API. LINE does NOT render "
            "Markdown — text bubbles show ** and # literally. Bare URLs are "
            "auto-linked, but \\[label\\](url) syntax is not. Each text bubble "
            "is capped at 5000 characters and at most 5 bubbles are sent per "
            "reply, so keep responses concise. Image/audio/video sending "
            "requires LINE_PUBLIC_URL configured to a publicly reachable HTTPS "
            "host. Slow responses surface a postback button the user taps "
            "to fetch the reply via a fresh free token."
        ),
    )
