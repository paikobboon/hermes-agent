"""Tests for the LINE adapter fork patches.

Covers the two patch sets added to ``adapter.py``:

* Patch 1 — sender display-name / group-title resolution with a TTL cache
  and fail-open fallback to the raw LINE id.
* Patch 2 — media→text coalescing: a media message opens a short sliding
  window into which the sender's follow-up caption merges, producing a
  single ``MessageEvent`` instead of two disjoint turns.

The gateway internals the adapter imports at module load
(``gateway.platforms.base``, ``gateway.config``) do not exist off-gateway, so
we inject minimal fakes into ``sys.modules`` *before* importing the adapter.
LINE HTTP is never touched: media download is monkeypatched, and identity
lookups go through a fake client. No network, no real webhook server.

Run:
    .venv/bin/python -m pytest test_line_adapter_patch.py -v
"""

from __future__ import annotations

import asyncio
import enum
import importlib.util
import sys
import time
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fake gateway modules — installed before importing the adapter under test.
# ---------------------------------------------------------------------------

class _FakeMessageType(enum.Enum):
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    VOICE = "voice"
    DOCUMENT = "document"
    LOCATION = "location"
    STICKER = "sticker"


@dataclass
class _FakeMessageEvent:
    text: str
    message_type: Any
    source: Any
    raw_message: Any
    message_id: str
    media_urls: List[str] = field(default_factory=list)
    media_types: List[str] = field(default_factory=list)
    reply_to_message_id: Optional[str] = None
    reply_to_text: Optional[str] = None


@dataclass
class _FakeSendResult:
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


def _fake_cache_image_from_bytes(data: bytes, ext: str = ".bin") -> str:
    return f"/fake/cached{ext}"


def _fake_cache_media_from_bytes(data: bytes, ext: str = ".bin", *, media_type: str = "file") -> str:
    return f"/fake/cached_media{ext}"


class _FakeBasePlatformAdapter:
    """Minimal stand-in for the real base adapter.

    Records every dispatched ``MessageEvent`` on ``self.handled`` — tests read
    that list to assert what the coalescer/dispatch path produced.
    """

    def __init__(self, config: Any = None, platform: Any = None) -> None:
        self.config = config
        self.platform = platform
        self.handled: List[Any] = []

    async def handle_message(self, event: Any) -> None:
        self.handled.append(event)

    def build_source(self, **kwargs: Any) -> Dict[str, Any]:
        return dict(kwargs)

    def _set_fatal_error(self, *args: Any, **kwargs: Any) -> None:
        self._fatal = (args, kwargs)

    def _mark_connected(self) -> None:
        self._connected = True

    def _mark_disconnected(self) -> None:
        self._connected = False

    async def _keep_typing(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def interrupt_session_activity(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakePlatform:
    def __init__(self, name: str) -> None:
        self.name = name


def _install_fake_gateway() -> None:
    base_mod = types.ModuleType("gateway.platforms.base")
    base_mod.BasePlatformAdapter = _FakeBasePlatformAdapter
    base_mod.MessageEvent = _FakeMessageEvent
    base_mod.MessageType = _FakeMessageType
    base_mod.SendResult = _FakeSendResult
    base_mod.cache_image_from_bytes = _fake_cache_image_from_bytes
    base_mod.cache_media_from_bytes = _fake_cache_media_from_bytes

    config_mod = types.ModuleType("gateway.config")
    config_mod.Platform = _FakePlatform

    platforms_mod = types.ModuleType("gateway.platforms")
    platforms_mod.base = base_mod
    gateway_mod = types.ModuleType("gateway")
    gateway_mod.platforms = platforms_mod
    gateway_mod.config = config_mod
    rich_sent_store_mod = types.ModuleType("gateway.rich_sent_store")
    rich_sent_store_mod.lookup = lambda *a, **k: None
    gateway_mod.rich_sent_store = rich_sent_store_mod

    # Force the fakes in even if a real ``gateway`` is importable (e.g. when
    # run inside the hermes-agent tree) — the adapter-under-test must bind to
    # the recording base class so tests can inspect dispatched events.
    sys.modules["gateway"] = gateway_mod
    sys.modules["gateway.platforms"] = platforms_mod
    sys.modules["gateway.platforms.base"] = base_mod
    sys.modules["gateway.config"] = config_mod
    sys.modules["gateway.rich_sent_store"] = rich_sent_store_mod


def _load_adapter_module():
    # Isolate the fake-gateway install: save any real gateway modules,
    # install fakes, load the adapter (which binds its imports to the fakes
    # at import time), then restore sys.modules so the fakes never leak into
    # other test modules collected in the same session (see this dir's
    # conftest anti-pattern note).
    _modnames = [
        "gateway",
        "gateway.platforms",
        "gateway.platforms.base",
        "gateway.config",
        "gateway.rich_sent_store",
    ]
    _saved = {name: sys.modules.get(name) for name in _modnames}
    try:
        _install_fake_gateway()
        # Load the real LINE plugin adapter by its actual path.
        path = Path(__file__).resolve().parents[2] / "plugins" / "platforms" / "line" / "adapter.py"
        spec = importlib.util.spec_from_file_location("line_adapter_under_test", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        for name, mod in _saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


adapter_mod = _load_adapter_module()
MessageType = adapter_mod.MessageType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**extra: Any):
    return types.SimpleNamespace(extra=dict(extra))


def _make_adapter(**extra: Any):
    """Build a LineAdapter with no real client and a stubbed media download."""
    adapter = adapter_mod.LineAdapter(_make_config(**extra))
    adapter._client = None

    async def _fake_download(message_id: str, msg_type: str) -> str:
        return f"/fake/{message_id}.jpg"

    adapter._download_media = _fake_download
    return adapter


def _group_source(group_id: str, user_id: str) -> Dict[str, Any]:
    return {"type": "group", "groupId": group_id, "userId": user_id}


def _image_event(msg_id: str, group_id: str, user_id: str, reply_token: str = "rt") -> Dict[str, Any]:
    return {
        "type": "message",
        "replyToken": reply_token,
        "webhookEventId": f"wh-{msg_id}",
        "source": _group_source(group_id, user_id),
        "message": {"type": "image", "id": msg_id},
    }


def _text_event(text: str, group_id: str, user_id: str, msg_id: str = "t1", reply_token: str = "rt") -> Dict[str, Any]:
    return {
        "type": "message",
        "replyToken": reply_token,
        "webhookEventId": f"wh-{msg_id}",
        "source": _group_source(group_id, user_id),
        "message": {"type": "text", "id": msg_id, "text": text},
    }


class _FakeLineClient:
    """Counts identity lookups and returns a canned JSON payload."""

    def __init__(self, response: Optional[Dict[str, Any]]) -> None:
        self.response = response
        self.calls = 0
        self.urls: List[str] = []

    async def get_json(self, url: str, *, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        self.calls += 1
        self.urls.append(url)
        return self.response


# ---------------------------------------------------------------------------
# Patch 2 — coalescing
# ---------------------------------------------------------------------------

async def test_image_then_text_merges_to_single_dispatch():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.25,
        coalesce_idle=0.25,
        coalesce_max_age=2.0,
    )
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    await adapter._handle_message_event(_text_event("here is the x-ray, thoughts?", "C1", "U1"))

    # Nothing dispatched yet — still inside the window.
    assert adapter.handled == []

    await asyncio.sleep(0.45)

    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "here is the x-ray, thoughts?"
    assert evt.media_urls == ["/fake/m1.jpg"]
    assert evt.media_types == ["image"]
    assert evt.message_type == MessageType.PHOTO
    assert evt.message_id == "m1"
    assert adapter._coalescer._buffers == {}


async def test_plain_text_dispatches_immediately():
    adapter = _make_adapter(sender_names=False, coalesce_media=True)
    await adapter._handle_message_event(_text_event("just a question", "C1", "U1"))

    # No sleep: plain text must gain zero latency.
    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "just a question"
    assert evt.media_urls == []
    assert evt.message_type == MessageType.TEXT
    assert adapter._coalescer._buffers == {}


async def test_two_senders_in_group_never_merge():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.25,
        coalesce_idle=0.25,
        coalesce_max_age=2.0,
    )
    # U1 opens a media buffer; U2 sends unrelated text.
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    await adapter._handle_message_event(_text_event("unrelated", "C1", "U2", msg_id="t2"))

    # U2's text dispatched immediately and carries no media from U1.
    assert len(adapter.handled) == 1
    u2 = adapter.handled[0]
    assert u2.text == "unrelated"
    assert u2.media_urls == []
    # U1's buffer is untouched and still pending.
    assert adapter._coalescer.has_buffer(("C1", "U1"))

    await asyncio.sleep(0.4)

    # U1's buffer flushes on its own with the bare image placeholder.
    assert len(adapter.handled) == 2
    u1 = adapter.handled[1]
    assert u1.text == "[image]"
    assert u1.media_urls == ["/fake/m1.jpg"]
    assert u1.message_type == MessageType.PHOTO


async def test_max_age_cap_flushes_under_continuous_merging():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=1.0,   # long — would not fire on its own in time
        coalesce_idle=1.0,          # long — every merge would keep extending
        coalesce_max_age=0.3,       # the hard cap that must win
    )
    key = ("C1", "U1")
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))

    start = time.monotonic()
    captions = 0
    for i in range(30):
        if not adapter._coalescer.has_buffer(key):
            break
        await adapter._handle_message_event(_text_event(f"c{i}", "C1", "U1", msg_id=f"t{i}"))
        captions += 1
        await asyncio.sleep(0.03)
    elapsed = time.monotonic() - start

    # The cap fired despite continuous merges within the idle window.
    assert len(adapter.handled) == 1
    assert elapsed < 0.6
    evt = adapter.handled[0]
    assert evt.media_types == ["image"]
    # Several captions were merged before the cap flushed them together.
    assert "\n" in evt.text
    assert evt.text.split("\n")[0] == "c0"
    assert adapter._coalescer._buffers == {}


async def test_grace_expiry_flushes_bare_image():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.15,
        coalesce_idle=0.15,
        coalesce_max_age=2.0,
    )
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    assert adapter.handled == []

    await asyncio.sleep(0.3)

    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "[image]"      # no caption arrived → placeholder kept
    assert evt.media_urls == ["/fake/m1.jpg"]
    assert evt.media_types == ["image"]
    assert evt.message_type == MessageType.PHOTO


async def test_coalesce_disabled_restores_immediate_dispatch():
    adapter = _make_adapter(sender_names=False, coalesce_media=False)
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))

    # Legacy behavior: the image dispatches immediately as its own turn.
    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "[image]"
    assert evt.media_urls == ["/fake/m1.jpg"]
    assert evt.message_type == MessageType.PHOTO
    # Coalescer never engaged.
    assert adapter._coalescer._buffers == {}


async def test_additional_media_merges_into_buffer():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.25,
        coalesce_idle=0.25,
        coalesce_max_age=2.0,
    )
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    await adapter._handle_message_event(_image_event("m2", "C1", "U1"))
    await adapter._handle_message_event(_text_event("both of these", "C1", "U1", msg_id="t1"))

    await asyncio.sleep(0.45)

    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "both of these"
    assert evt.media_urls == ["/fake/m1.jpg", "/fake/m2.jpg"]
    assert evt.media_types == ["image", "image"]
    assert evt.message_id == "m1"  # id of the FIRST buffered media


async def test_disconnect_flushes_pending_buffer():
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=5.0,   # long enough that only shutdown flushes it
        coalesce_idle=5.0,
        coalesce_max_age=10.0,
    )
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    await adapter._handle_message_event(_text_event("explain this", "C1", "U1"))
    assert adapter.handled == []  # still buffered

    # Teardown with no live server/client — flush must still happen.
    await adapter.disconnect()

    assert len(adapter.handled) == 1
    evt = adapter.handled[0]
    assert evt.text == "explain this"
    assert evt.media_urls == ["/fake/m1.jpg"]
    assert adapter._coalescer._buffers == {}


async def test_audio_does_not_open_buffer():
    """Voice notes are self-contained — they must dispatch immediately and
    never open a coalescing buffer."""
    adapter = _make_adapter(sender_names=False, coalesce_media=True)

    async def _fake_download(message_id: str, msg_type: str) -> str:
        return f"/fake/{message_id}.m4a"

    adapter._download_media = _fake_download

    audio_event = {
        "type": "message",
        "replyToken": "rt",
        "source": _group_source("C1", "U1"),
        "message": {"type": "audio", "id": "a1"},
    }
    await adapter._handle_message_event(audio_event)

    assert len(adapter.handled) == 1
    assert adapter.handled[0].message_type == MessageType.VOICE
    assert adapter._coalescer._buffers == {}


# ---------------------------------------------------------------------------
# Patch 1 — identity resolution
# ---------------------------------------------------------------------------

async def test_display_name_cache_hit_expiry_and_failopen():
    adapter = _make_adapter(sender_names=True)
    client = _FakeLineClient({"displayName": "Alice"})
    adapter._client = client
    source = _group_source("C1", "U1")

    # First call hits the API.
    name1 = await adapter._resolve_sender_name(source, "U1")
    assert name1 == "Alice"
    assert client.calls == 1
    assert client.urls[0] == adapter_mod.LINE_GROUP_MEMBER_URL_FMT.format(
        group_id="C1", user_id="U1"
    )

    # Second call is served from cache — no new API hit.
    name2 = await adapter._resolve_sender_name(source, "U1")
    assert name2 == "Alice"
    assert client.calls == 1

    # Force the cache entry to expire → next call re-fetches.
    cache_key = "member:group:C1:U1"
    value, _ = adapter._name_cache[cache_key]
    adapter._name_cache[cache_key] = (value, time.time() - 1)
    name3 = await adapter._resolve_sender_name(source, "U1")
    assert name3 == "Alice"
    assert client.calls == 2

    # API error (None payload) for a fresh user → fail-open to the raw id.
    client.response = None
    name4 = await adapter._resolve_sender_name(_group_source("C1", "U2"), "U2")
    assert name4 == "U2"


async def test_sender_names_disabled_returns_raw_id():
    adapter = _make_adapter(sender_names=False)
    adapter._client = _FakeLineClient({"displayName": "Alice"})
    name = await adapter._resolve_sender_name(_group_source("C1", "U1"), "U1")
    assert name == "U1"
    # Disabled → no API call at all.
    assert adapter._client.calls == 0


async def test_dm_uses_profile_endpoint():
    adapter = _make_adapter(sender_names=True)
    client = _FakeLineClient({"displayName": "Bob"})
    adapter._client = client
    source = {"type": "user", "userId": "Uxyz"}
    name = await adapter._resolve_sender_name(source, "Uxyz")
    assert name == "Bob"
    assert client.urls[0] == adapter_mod.LINE_PROFILE_URL_FMT.format(user_id="Uxyz")


async def test_group_chat_name_resolves_summary():
    adapter = _make_adapter(sender_names=True)
    client = _FakeLineClient({"groupName": "Family Care Group"})
    adapter._client = client
    source = _group_source("C1", "U1")
    chat_name = await adapter._resolve_chat_name(source, "C1", "group")
    assert chat_name == "Family Care Group"
    assert client.urls[0] == adapter_mod.LINE_GROUP_SUMMARY_URL_FMT.format(group_id="C1")

    # DM/room have no group title → keep the id, no API call.
    client2 = _FakeLineClient({"groupName": "nope"})
    adapter._client = client2
    assert await adapter._resolve_chat_name({"type": "user", "userId": "U1"}, "U1", "dm") == "U1"
    assert client2.calls == 0


async def test_dispatch_exception_in_timer_flush_is_logged_not_lost(caplog):
    """A raising downstream handler must not kill the timer task silently,
    must not leave buffer state stuck, and must be visible in logs — a
    swallowed exception here is a silently dropped user turn."""
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.1,
        coalesce_idle=0.1,
        coalesce_max_age=1.0,
    )

    async def _boom(event_obj):
        raise RuntimeError("downstream failure")

    adapter.handle_message = _boom  # type: ignore[assignment]

    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    await asyncio.sleep(0.3)  # let the grace timer flush into the raising handler

    # Buffer must be cleared (no stuck state for that sender)...
    assert not adapter._coalescer.has_buffer(("C1", "U1"))
    # ...the exception must be logged, not swallowed silently...
    assert any("coalesced dispatch failed" in r.message for r in caplog.records)
    # ...and the coalescer must still work for the next message.
    async def _ok(event_obj):
        adapter.handled.append(event_obj)
    adapter.handle_message = _ok  # type: ignore[assignment]
    await adapter._handle_message_event(_text_event("m2", "C1", "U1", "hello"))
    assert len(adapter.handled) == 1


def _sticker_event(msg_id: str, group_id: str, user_id: str) -> Dict[str, Any]:
    return {
        "type": "message",
        "replyToken": "rt-sticker",
        "webhookEventId": f"wh-{msg_id}",
        "source": _group_source(group_id, user_id),
        "message": {"type": "sticker", "id": msg_id, "keywords": ["ok"]},
    }


async def test_sticker_during_open_buffer_bypasses_and_preserves_it():
    """A self-contained turn (sticker) from the same sender must dispatch
    immediately AND leave the pending media buffer intact — the claimed
    'never disturb' behavior."""
    adapter = _make_adapter(
        sender_names=False,
        coalesce_media=True,
        coalesce_media_grace=0.4,
        coalesce_idle=0.4,
        coalesce_max_age=2.0,
    )
    await adapter._handle_message_event(_image_event("m1", "C1", "U1"))
    assert adapter._coalescer.has_buffer(("C1", "U1"))

    await adapter._handle_message_event(_sticker_event("s1", "C1", "U1"))
    # Sticker dispatched immediately, buffer untouched.
    assert len(adapter.handled) == 1
    assert adapter.handled[0].text == "[sticker: ok]"
    assert adapter._coalescer.has_buffer(("C1", "U1"))

    # Caption still merges into the surviving buffer.
    await adapter._handle_message_event(_text_event("คือรูปแผลป๊า", "C1", "U1", "t9"))
    await asyncio.sleep(0.9)
    assert len(adapter.handled) == 2
    merged = adapter.handled[1]
    assert merged.text == "คือรูปแผลป๊า"
    assert merged.media_urls == ["/fake/m1.jpg"]
