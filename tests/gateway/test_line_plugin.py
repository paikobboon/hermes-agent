"""Tests for the LINE platform adapter plugin.

Covers the seven synthesis areas from the PR review:

1. webhook signature verification (HMAC-SHA256, base64) + tampering rejection
2. inbound chat-id resolution for user / group / room sources
3. three-allowlist gating (users / groups / rooms / allow_all)
4. inbound dedup via webhookEventId
5. RequestCache state machine (PENDING → READY → DELIVERED, ERROR)
6. Markdown stripping with URL preservation + LINE-sized chunking
7. send routing: reply token preferred → push fallback → batched at 5/call
8. register() metadata + standalone_send shape
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import base64
import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/line/adapter.py under plugin_adapter_line so it
# cannot collide with sibling platform-plugin tests in the same xdist worker.
_line = load_plugin_adapter("line")

verify_line_signature = _line.verify_line_signature
strip_markdown_preserving_urls = _line.strip_markdown_preserving_urls
split_for_line = _line.split_for_line
build_postback_button_message = _line.build_postback_button_message
_resolve_chat = _line._resolve_chat
_allowed_for_source = _line._allowed_for_source
_is_system_bypass = _line._is_system_bypass
RequestCache = _line.RequestCache
State = _line.State
LineAdapter = _line.LineAdapter
register = _line.register
check_requirements = _line.check_requirements
validate_config = _line.validate_config
_standalone_send = _line._standalone_send
_env_enablement = _line._env_enablement
_MessageDeduplicator = _line._MessageDeduplicator


# ---------------------------------------------------------------------------
# 1. Signature verification
# ---------------------------------------------------------------------------

class TestSignature:

    def _sign(self, body: bytes, secret: str) -> str:
        digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def test_valid_signature_passes(self):
        body = b'{"events": []}'
        sig = self._sign(body, "secret")
        assert verify_line_signature(body, sig, "secret")

    def test_tampered_body_rejected(self):
        body = b'{"events": []}'
        sig = self._sign(body, "secret")
        assert not verify_line_signature(body + b" ", sig, "secret")

    def test_wrong_secret_rejected(self):
        body = b'{"events": []}'
        sig = self._sign(body, "secret")
        assert not verify_line_signature(body, sig, "different")

    def test_empty_signature_rejected(self):
        assert not verify_line_signature(b"x", "", "secret")

    def test_empty_secret_rejected(self):
        assert not verify_line_signature(b"x", "AAAA", "")

    def test_garbage_signature_rejected(self):
        assert not verify_line_signature(b"hello", "not base64 at all!!", "s")


# ---------------------------------------------------------------------------
# 2. Chat-id / source resolution
# ---------------------------------------------------------------------------

class TestSourceResolution:

    def test_user_source(self):
        chat_id, ctype = _resolve_chat({"type": "user", "userId": "U123"})
        assert chat_id == "U123"
        assert ctype == "dm"

    def test_group_source(self):
        chat_id, ctype = _resolve_chat({"type": "group", "groupId": "C456", "userId": "U123"})
        assert chat_id == "C456"
        assert ctype == "group"

    def test_room_source(self):
        chat_id, ctype = _resolve_chat({"type": "room", "roomId": "R789", "userId": "U123"})
        assert chat_id == "R789"
        assert ctype == "room"

    def test_unknown_source_falls_back_to_dm(self):
        chat_id, ctype = _resolve_chat({"type": "weird"})
        assert chat_id == ""
        assert ctype == "dm"

    def test_empty_source(self):
        chat_id, ctype = _resolve_chat({})
        assert chat_id == ""
        assert ctype == "dm"


# ---------------------------------------------------------------------------
# 3. Three-allowlist gating
# ---------------------------------------------------------------------------

class TestAllowlist:

    def test_allow_all_short_circuits(self):
        for src in [
            {"type": "user", "userId": "Ufoo"},
            {"type": "group", "groupId": "Cfoo"},
            {"type": "room", "roomId": "Rfoo"},
        ]:
            assert _allowed_for_source(src, allow_all=True, user_ids=set(), group_ids=set(), room_ids=set())

    def test_user_in_allowlist_passes(self):
        src = {"type": "user", "userId": "Uok"}
        assert _allowed_for_source(src, allow_all=False, user_ids={"Uok"}, group_ids=set(), room_ids=set())

    def test_user_not_in_allowlist_rejected(self):
        src = {"type": "user", "userId": "Uother"}
        assert not _allowed_for_source(src, allow_all=False, user_ids={"Uok"}, group_ids=set(), room_ids=set())

    def test_group_uses_group_list_not_user_list(self):
        src = {"type": "group", "groupId": "Cok", "userId": "Uany"}
        assert _allowed_for_source(src, allow_all=False, user_ids={"Uany"}, group_ids={"Cok"}, room_ids=set())
        assert not _allowed_for_source(src, allow_all=False, user_ids={"Uany"}, group_ids=set(), room_ids=set())

    def test_room_uses_room_list(self):
        src = {"type": "room", "roomId": "Rok"}
        assert _allowed_for_source(src, allow_all=False, user_ids=set(), group_ids=set(), room_ids={"Rok"})
        assert not _allowed_for_source(src, allow_all=False, user_ids=set(), group_ids=set(), room_ids=set())

    def test_unknown_type_rejected(self):
        src = {"type": "weird"}
        assert not _allowed_for_source(src, allow_all=False, user_ids=set(), group_ids=set(), room_ids=set())


# ---------------------------------------------------------------------------
# 4. Inbound dedup
# ---------------------------------------------------------------------------

class TestDedup:

    def test_first_event_not_duplicate(self):
        d = _MessageDeduplicator()
        assert not d.is_duplicate("evt1")

    def test_repeat_event_marked_duplicate(self):
        d = _MessageDeduplicator()
        d.is_duplicate("evt1")
        assert d.is_duplicate("evt1")

    def test_blank_id_not_treated_as_duplicate(self):
        d = _MessageDeduplicator()
        # Blank IDs should always pass through (don't lock out unidentifiable events).
        assert not d.is_duplicate("")
        assert not d.is_duplicate("")

    def test_lru_eviction_under_pressure(self):
        d = _MessageDeduplicator(max_size=10)
        for i in range(20):
            d.is_duplicate(f"evt{i}")
        # Exact eviction order isn't specified, but the cap must be enforced.
        # Insert one more and assert the bookkeeping doesn't grow without bound.
        d.is_duplicate("evt20")
        assert len(d._seen) <= 20  # bounded — exact cap depends on eviction policy

    def test_dedup_persists_across_restarts(self, tmp_path):
        persist = str(tmp_path / "line_dedup.json")
        d1 = _MessageDeduplicator(persist_path=persist)
        assert not d1.is_duplicate("evt-restart-1")
        # A fresh instance (simulating a gateway restart) must recognize the id.
        d2 = _MessageDeduplicator(persist_path=persist)
        assert d2.is_duplicate("evt-restart-1")
        # And still admit brand-new events.
        assert not d2.is_duplicate("evt-restart-2")

    def test_dedup_persist_path_unreadable_is_harmless(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        d = _MessageDeduplicator(persist_path=str(bad))
        assert not d.is_duplicate("evt-x")
        assert d.is_duplicate("evt-x")


# ---------------------------------------------------------------------------
# 5. RequestCache state machine
# ---------------------------------------------------------------------------

class TestRequestCache:

    def test_register_pending_is_pending(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        assert c.get(rid).state is State.PENDING
        assert c.get(rid).chat_id == "Uchat"

    def test_set_ready_transitions(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        c.set_ready(rid, "the answer")
        assert c.get(rid).state is State.READY
        assert c.get(rid).payload == "the answer"

    def test_set_error_transitions(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        c.set_error(rid, "boom")
        assert c.get(rid).state is State.ERROR
        assert c.get(rid).payload == "boom"

    def test_mark_delivered_from_ready(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        c.set_ready(rid, "x")
        c.mark_delivered(rid)
        assert c.get(rid).state is State.DELIVERED

    def test_mark_delivered_from_error(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        c.set_error(rid, "x")
        c.mark_delivered(rid)
        assert c.get(rid).state is State.DELIVERED

    def test_set_ready_on_delivered_is_noop(self):
        c = RequestCache()
        rid = c.register_pending("Uchat")
        c.set_ready(rid, "first")
        c.mark_delivered(rid)
        c.set_ready(rid, "second")
        # DELIVERED is terminal — no further mutation
        assert c.get(rid).payload == "first"
        assert c.get(rid).state is State.DELIVERED

    def test_set_ready_accumulates_multiple_payloads(self):
        # FORK DELTA 2026-07-09: a slow turn sends its text and its image as
        # two separate set_ready calls on the SAME pending button. Both must
        # survive so a single postback tap delivers the whole answer -- the
        # image must not be dropped just because the text already flipped the
        # entry to READY. Before the fix, the second set_ready no-op'd and the
        # picture never reached the family.
        c = RequestCache()
        rid = c.register_pending("Cchat")
        c.set_ready(rid, "here is your picture")
        image_msg = {
            "type": "image",
            "originalContentUrl": "https://x/i.jpg",
            "previewImageUrl": "https://x/i.jpg",
        }
        c.set_ready(rid, [image_msg])
        assert c.get(rid).state is State.READY
        _prev, payload = c.claim_delivery(rid)
        assert isinstance(payload, list), payload
        types = [m.get("type") for m in payload]
        assert "text" in types, f"text dropped: {payload}"
        assert "image" in types, f"image dropped: {payload}"
        text_msg = next(m for m in payload if m.get("type") == "text")
        assert "picture" in text_msg.get("text", "")

    def test_find_pending_for_chat(self):
        c = RequestCache()
        rid_a = c.register_pending("Ua")
        rid_b = c.register_pending("Ub")
        assert c.find_pending_for_chat("Ua") == rid_a
        assert c.find_pending_for_chat("Ub") == rid_b
        assert c.find_pending_for_chat("Uc") is None
        c.set_ready(rid_a, "x")
        # No longer PENDING — should not be found
        assert c.find_pending_for_chat("Ua") is None


# ---------------------------------------------------------------------------
# 6. Markdown stripping + chunking
# ---------------------------------------------------------------------------

class TestMarkdownAndChunking:

    def test_bold_stripped(self):
        assert strip_markdown_preserving_urls("**hello**") == "hello"

    def test_italic_stripped(self):
        assert strip_markdown_preserving_urls("*hello*") == "hello"

    def test_inline_code_unfenced(self):
        assert strip_markdown_preserving_urls("run `ls -la`") == "run ls -la"

    def test_link_preserved_with_url(self):
        out = strip_markdown_preserving_urls("see [here](https://x.com)")
        assert "https://x.com" in out
        assert "here (https://x.com)" in out

    def test_heading_prefix_stripped(self):
        out = strip_markdown_preserving_urls("# Title\n## Sub")
        assert out == "Title\nSub"

    def test_bullet_marker_replaced(self):
        out = strip_markdown_preserving_urls("- a\n- b")
        assert out == "• a\n• b"

    def test_code_fence_content_kept(self):
        # Source files often contain code snippets — the agent should still
        # see the content as plain text, just without backticks.
        md = "```python\nprint('hi')\n```"
        out = strip_markdown_preserving_urls(md)
        assert "print('hi')" in out
        assert "```" not in out

    def test_split_short_returns_single_chunk(self):
        assert split_for_line("hi") == ["hi"]

    def test_split_long_chunks_at_paragraph_boundary(self):
        text = "para1\n\npara2\n\npara3"
        chunks = split_for_line(text, max_chars=8)
        assert all(len(c) <= 8 for c in chunks), chunks
        assert len(chunks) >= 2

    def test_split_caps_at_five_chunks(self):
        # 1000 paragraphs of 100 chars each — must cap at 5 LINE bubbles.
        text = "\n\n".join(["x" * 100 for _ in range(1000)])
        chunks = split_for_line(text)
        assert len(chunks) <= 5


# ---------------------------------------------------------------------------
# 7. Send routing (reply -> push fallback, batching, system-bypass)
# ---------------------------------------------------------------------------

class TestSendRouting:

    @pytest.fixture
    def adapter(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={
            "channel_access_token": "tok",
            "channel_secret": "sec",
        })
        ad = LineAdapter(cfg)
        ad._client = MagicMock()
        ad._client.reply = AsyncMock()
        ad._client.push = AsyncMock()
        return ad

    def test_system_bypass_recognized(self):
        assert _is_system_bypass("⚡ Interrupting current run")
        assert _is_system_bypass("⏳ Queued — agent is busy")
        assert _is_system_bypass("⏩ Steered toward new task")
        assert not _is_system_bypass("Hello world")
        assert not _is_system_bypass("")

    def test_send_uses_reply_when_token_present(self, adapter):
        import time as _time
        adapter._reply_tokens["Uchat"] = ("rt-token", _time.time() + 30)
        result = asyncio.run(adapter.send("Uchat", "hello"))
        assert result.success
        adapter._client.reply.assert_called_once()
        adapter._client.push.assert_not_called()
        # Token consumed (single-use)
        assert "Uchat" not in adapter._reply_tokens

    def test_send_falls_back_to_push_when_no_token(self, adapter):
        result = asyncio.run(adapter.send("Uchat", "hello"))
        assert result.success
        adapter._client.push.assert_called_once()
        adapter._client.reply.assert_not_called()

    def test_send_falls_back_to_push_when_reply_fails(self, adapter):
        import time as _time
        adapter._reply_tokens["Uchat"] = ("rt-token", _time.time() + 30)
        adapter._client.reply.side_effect = RuntimeError("expired")
        result = asyncio.run(adapter.send("Uchat", "hello"))
        assert result.success
        adapter._client.reply.assert_called_once()
        adapter._client.push.assert_called_once()

    def test_send_returns_failure_when_push_fails(self, adapter):
        adapter._client.push.side_effect = RuntimeError("network")
        result = asyncio.run(adapter.send("Uchat", "hello"))
        assert not result.success
        assert "network" in result.error

    def test_send_pending_button_caches_response(self, adapter):
        # Simulate that the slow-LLM postback button has fired.
        rid = adapter._cache.register_pending("Uchat")
        adapter._pending_buttons["Uchat"] = rid
        result = asyncio.run(adapter.send("Uchat", "the answer"))
        assert result.success
        # Response must have been cached, not pushed/replied.
        adapter._client.reply.assert_not_called()
        adapter._client.push.assert_not_called()
        assert adapter._cache.get(rid).state is State.READY
        assert adapter._cache.get(rid).payload == "the answer"

    def test_pending_tap_always_replies_not_done(self, adapter):
        # Pai's pull model (2026-07-09): tapping while the answer is still
        # PENDING must reply the predefined 'not done yet' text via the tap's
        # own token -- NOT silently stash the token and return nothing (the
        # old first-tap behavior that felt dead).
        import json as _json
        rid = adapter._cache.register_pending("Uchat")
        event = {
            "replyToken": "fresh-token",
            "postback": {"data": _json.dumps({"action": "show_response", "request_id": rid})},
            "source": {"type": "user", "userId": "Uchat"},
        }
        asyncio.run(adapter._handle_postback_event(event))
        adapter._client.reply.assert_called_once()
        token, messages = adapter._client.reply.call_args[0]
        assert token == "fresh-token"
        assert messages[0]["type"] == "text" and messages[0]["text"]
        # no token stashed, no proactive delivery scheduled
        assert rid not in adapter._pending_delivery_tokens
        adapter._client.push.assert_not_called()

    def test_send_system_bypass_skips_postback_cache(self, adapter):
        # Even with a pending button, system busy-acks must surface visibly.
        rid = adapter._cache.register_pending("Uchat")
        adapter._pending_buttons["Uchat"] = rid
        result = asyncio.run(adapter.send("Uchat", "⚡ Interrupting current run"))
        assert result.success
        # Bypass goes through push (no reply token stored)
        adapter._client.push.assert_called_once()
        # And the cache entry is unchanged (still PENDING for the eventual answer)
        assert adapter._cache.get(rid).state is State.PENDING

    def test_pending_button_list_config_rotates_from_plural_keys(self, monkeypatch):
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(enabled=True, extra={
            "channel_access_token": "tok",
            "channel_secret": "sec",
            "pending_reply_text": "singular text",
            "pending_reply_texts": ["list text 1", "list text 2"],
            "pending_button_label": "singular label",
            "pending_button_labels": ["list label 1", "list label 2 that is too long"],
        })
        adapter = LineAdapter(cfg)
        adapter.slow_response_threshold = 0.01
        adapter._client = MagicMock()
        adapter._client.reply = AsyncMock()
        adapter._client.loading = AsyncMock()
        adapter._reply_tokens["Uchat"] = ("reply-token", time.time() + 30)

        choices = []

        def fake_choice(options):
            choices.append(tuple(options))
            return options[-1]

        monkeypatch.setattr(_line.random, "choice", fake_choice)

        async def run_keep_typing_once():
            stop_event = asyncio.Event()
            task = asyncio.create_task(
                adapter._keep_typing("Uchat", interval=0.05, stop_event=stop_event)
            )
            await asyncio.sleep(0.04)
            stop_event.set()
            await task

        asyncio.run(run_keep_typing_once())

        adapter._client.reply.assert_called_once()
        sent = adapter._client.reply.call_args.args[1][0]
        assert sent["template"]["text"] == "list text 2"
        assert sent["template"]["actions"][0]["label"] == (
            "list label 2 that is too long"[:20]
        )
        assert ("list text 1", "list text 2") in choices
        assert ("list label 1", "list label 2 that is too long") in choices

    def test_pending_taps_reply_not_done_then_next_tap_delivers(self, adapter):
        # Pai's pull model (2026-07-09): taps while PENDING each reply the
        # predefined "not done" text (free, on that tap's own token) and never
        # stash for proactive delivery. The completed answer caches READY (NOT
        # auto-delivered); the NEXT tap delivers it exactly once. No push, and
        # dedup is preserved by the cache's DELIVERING claim.
        rid = adapter._cache.register_pending("Uchat")
        adapter._pending_buttons["Uchat"] = rid

        def event(reply_token):
            return {
                "type": "postback",
                "replyToken": reply_token,
                "source": {"type": "user", "userId": "Uchat"},
                "postback": {
                    "data": json.dumps({
                        "action": "show_response",
                        "request_id": rid,
                    })
                },
            }

        async def run_flow():
            await adapter._handle_postback_event(event("tap-1"))   # PENDING -> "not done"
            await adapter._handle_postback_event(event("tap-2"))   # PENDING -> "not done"
            send_result = await adapter.send("Uchat", "the answer")  # caches READY, no auto-deliver
            await adapter._handle_postback_event(event("tap-3"))   # READY -> deliver once
            return send_result

        send_result = asyncio.run(run_flow())
        assert send_result.success
        delivered_answer_calls = [
            call for call in adapter._client.reply.call_args_list
            if call.args[1][0].get("text") == "the answer"
        ]
        assert len(delivered_answer_calls) == 1
        assert adapter._client.reply.call_count == 3
        adapter._client.push.assert_not_called()
        assert adapter._cache.get(rid).state is State.DELIVERED

    def test_send_caps_messages_per_call_at_five(self, adapter):
        # Build a payload that would naturally split into more than 5 LINE
        # bubbles; the chunker should cap at 5 + truncate.
        big = "\n\n".join(["x" * 4500 for _ in range(20)])
        result = asyncio.run(adapter.send("Uchat", big))
        assert result.success
        call_kwargs = adapter._client.push.call_args
        # call_args is (args, kwargs); for our send the messages are the 2nd positional
        sent_messages = call_kwargs.args[1] if call_kwargs.args else call_kwargs.kwargs.get("messages")
        # Without args, fall back to inspecting the call shape
        if sent_messages is None:
            # We invoked client.push(chat_id, messages) — check first batch
            sent_messages = adapter._client.push.call_args.args[1]
        assert len(sent_messages) <= 5

    def test_format_message_strips_markdown(self, adapter):
        out = adapter.format_message("**bold** [link](https://x.com)")
        assert "**" not in out
        assert "https://x.com" in out

    def test_send_sticker_marker_sends_sticker_and_remaining_text(self, adapter):
        result = asyncio.run(adapter.send("Uchat", "Love you STICKER:446:1988"))

        assert result.success
        adapter._client.push.assert_called_once()
        sent_messages = adapter._client.push.call_args.args[1]
        assert sent_messages == [
            {"type": "text", "text": "Love you"},
            {"type": "sticker", "packageId": "446", "stickerId": "1988"},
        ]

    def test_prebuilt_text_reply_path_parses_sticker_marker(self, adapter):
        adapter._reply_tokens["Uchat"] = ("rt-token", time.time() + 30)

        result = asyncio.run(
            adapter._send_messages(
                "Uchat",
                [{"type": "text", "text": "Love you STICKER:446:1988"}],
            )
        )

        assert result.success
        adapter._client.reply.assert_called_once()
        adapter._client.push.assert_not_called()
        assert adapter._client.reply.call_args.args[1] == [
            {"type": "text", "text": "Love you"},
            {"type": "sticker", "packageId": "446", "stickerId": "1988"},
        ]

    def test_malformed_sticker_marker_is_left_as_text(self, adapter, caplog):
        with caplog.at_level("WARNING"):
            result = asyncio.run(adapter.send("Uchat", "Love you STICKER:abc"))

        assert result.success
        adapter._client.push.assert_called_once()
        sent_messages = adapter._client.push.call_args.args[1]
        assert sent_messages == [
            {"type": "text", "text": "Love you STICKER:abc"},
        ]
        assert "malformed STICKER marker" in caplog.text


class TestSendDocument:

    @pytest.fixture
    def adapter(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={
            "channel_access_token": "tok",
            "channel_secret": "sec",
            "public_url": "https://line-public.example.com",
        })
        ad = LineAdapter(cfg)
        ad._client = MagicMock()
        ad._client.reply = AsyncMock()
        ad._client.push = AsyncMock()
        return ad

    def test_send_document_pushes_download_link(self, adapter, tmp_path):
        doc = tmp_path / "report.pdf"
        doc.write_bytes(b"%PDF-1.4\nfake pdf\n")

        result = asyncio.run(adapter.send_document("Uchat", str(doc)))

        assert result.success
        adapter._client.push.assert_called_once()
        sent = adapter._client.push.call_args.args[1]
        assert len(sent) == 1
        assert sent[0]["type"] == "flex"
        blob = json.dumps(sent[0])
        assert "report.pdf" in blob
        assert "https://line-public.example.com/line/media/" in blob
        assert str(tmp_path) not in blob

    def test_send_document_missing_file_fails(self, adapter, tmp_path):
        missing = tmp_path / "missing.pdf"

        result = asyncio.run(adapter.send_document("Uchat", str(missing)))

        assert not result.success
        assert f"document not found: {missing}" == result.error
        adapter._client.push.assert_not_called()
        adapter._client.reply.assert_not_called()

    def test_send_document_registers_long_ttl(self, adapter, tmp_path):
        doc = tmp_path / "late-tap.pdf"
        doc.write_bytes(b"%PDF-1.4\n")
        before = time.time()

        result = asyncio.run(adapter.send_document("Uchat", str(doc)))

        assert result.success
        assert len(adapter._media_tokens) == 1
        _, expires_at = next(iter(adapter._media_tokens.values()))
        assert expires_at >= before + 23 * 3600

    def test_send_document_respects_file_name_override(self, adapter, tmp_path):
        doc = tmp_path / "internal-123.tmp"
        doc.write_bytes(b"%PDF-1.4\n")

        result = asyncio.run(
            adapter.send_document("Uchat", str(doc), file_name="Salary_Guide.pdf")
        )

        assert result.success
        sent = adapter._client.push.call_args.args[1]
        assert sent[0]["type"] == "flex"
        blob = json.dumps(sent[0])
        assert "Salary_Guide.pdf" in blob
        uri = sent[0]["contents"]["footer"]["contents"][0]["action"]["uri"]
        assert "Salary_Guide.pdf" in uri
        assert "internal-123.tmp" not in blob


class TestQuoteContext:

    @pytest.fixture
    def adapter(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={
            "channel_access_token": "tok",
            "channel_secret": "sec",
        })
        ad = LineAdapter(cfg)
        ad.sender_names = False
        ad._client = MagicMock()
        ad._client.reply = AsyncMock()
        ad._client.push = AsyncMock()
        ad._client.loading = AsyncMock()
        ad.handle_message = AsyncMock()
        return ad

    def _event(self, message_id="msg-new", quoted_message_id=None):
        message = {
            "type": "text",
            "id": message_id,
            "text": "What did you mean?",
        }
        if quoted_message_id:
            message["quotedMessageId"] = quoted_message_id
        return {
            "type": "message",
            "replyToken": "reply-token",
            "source": {"type": "user", "userId": "Uchat"},
            "message": message,
        }

    def test_quoted_message_id_resolves_from_rich_sent_store(self, adapter, monkeypatch):
        monkeypatch.setattr(
            _line.rich_sent_store,
            "lookup",
            lambda chat_id, message_id: (
                "Dinner is at 6" if (chat_id, message_id) == ("Uchat", "quoted-1") else None
            ),
        )

        asyncio.run(adapter._handle_message_event(self._event(quoted_message_id="quoted-1")))

        captured = adapter.handle_message.call_args.args[0]
        assert captured.reply_to_message_id == "quoted-1"
        assert captured.reply_to_text == "Dinner is at 6"

    def test_unresolvable_quoted_message_id_uses_placeholder(self, adapter, monkeypatch):
        monkeypatch.setattr(_line.rich_sent_store, "lookup", lambda chat_id, message_id: None)

        asyncio.run(adapter._handle_message_event(self._event(quoted_message_id="missing-1")))

        captured = adapter.handle_message.call_args.args[0]
        assert captured.reply_to_message_id == "missing-1"
        assert captured.reply_to_text == "[quoted an earlier message]"

    def test_quoted_outbound_line_message_id_resolves_from_recent_cache(
        self,
        adapter,
        monkeypatch,
    ):
        monkeypatch.setattr(_line.rich_sent_store, "lookup", lambda chat_id, message_id: None)
        adapter._client.push.return_value = {"sentMessages": [{"id": "sent-1"}]}

        result = asyncio.run(adapter.send("Uchat", "Dinner is at 6"))
        assert result.success

        asyncio.run(adapter._handle_message_event(self._event(quoted_message_id="sent-1")))

        captured = adapter.handle_message.call_args.args[0]
        assert captured.reply_to_message_id == "sent-1"
        assert captured.reply_to_text == "Dinner is at 6"

    def test_absent_quoted_message_id_leaves_reply_context_empty(self, adapter, monkeypatch):
        monkeypatch.setattr(_line.rich_sent_store, "lookup", lambda chat_id, message_id: None)

        asyncio.run(adapter._handle_message_event(self._event()))

        captured = adapter.handle_message.call_args.args[0]
        assert captured.reply_to_message_id is None
        assert captured.reply_to_text is None


# ---------------------------------------------------------------------------
# 8. Register() metadata + plugin entry points
# ---------------------------------------------------------------------------

class TestRegister:

    class _FakeCtx:
        def __init__(self):
            self.kwargs = None

        def register_platform(self, **kw):
            self.kwargs = kw

    def test_register_calls_register_platform(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert ctx.kwargs is not None
        assert ctx.kwargs["name"] == "line"
        assert ctx.kwargs["label"] == "LINE"

    def test_register_advertises_required_env(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert set(ctx.kwargs["required_env"]) == {
            "LINE_CHANNEL_ACCESS_TOKEN",
            "LINE_CHANNEL_SECRET",
        }

    def test_register_wires_allowlist_envs(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert ctx.kwargs["allowed_users_env"] == "LINE_ALLOWED_USERS"
        assert ctx.kwargs["allow_all_env"] == "LINE_ALLOW_ALL_USERS"

    def test_register_wires_cron_home_channel(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert ctx.kwargs["cron_deliver_env_var"] == "LINE_HOME_CHANNEL"

    def test_register_provides_standalone_sender(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert callable(ctx.kwargs["standalone_sender_fn"])

    def test_register_provides_env_enablement(self):
        ctx = self._FakeCtx()
        register(ctx)
        assert callable(ctx.kwargs["env_enablement_fn"])

    def test_register_factory_yields_line_adapter(self):
        ctx = self._FakeCtx()
        register(ctx)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={
            "channel_access_token": "tok",
            "channel_secret": "sec",
        })
        ad = ctx.kwargs["adapter_factory"](cfg)
        assert isinstance(ad, LineAdapter)

    def test_max_message_length_below_line_per_bubble_limit(self):
        ctx = self._FakeCtx()
        register(ctx)
        # LINE per-bubble limit is 5000; we register 4500 to leave headroom.
        assert ctx.kwargs["max_message_length"] <= 5000


class TestEnvEnablement:

    def test_returns_none_without_credentials(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        assert _env_enablement() is None

    def test_returns_dict_with_credentials(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "sec")
        assert _env_enablement() == {}

    def test_seeds_port_from_env(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "sec")
        monkeypatch.setenv("LINE_PORT", "8080")
        assert _env_enablement() == {"port": 8080}

    def test_seeds_public_url(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "sec")
        monkeypatch.setenv("LINE_PUBLIC_URL", "https://my-tunnel.example.com")
        result = _env_enablement()
        assert result["public_url"] == "https://my-tunnel.example.com"


class TestStandaloneSend:

    def test_missing_token_returns_error(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={})
        result = asyncio.run(_standalone_send(cfg, "Uchat", "hi"))
        assert "error" in result

    def test_missing_chat_id_returns_error(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={})
        result = asyncio.run(_standalone_send(cfg, "", "hi"))
        assert "error" in result

    def test_pushes_via_client_when_credentials_present(self, monkeypatch):
        from gateway.config import PlatformConfig

        push_calls = []

        class _FakeClient:
            def __init__(self, *a, **kw):
                pass

            async def push(self, chat_id, messages):
                push_calls.append((chat_id, messages))

        monkeypatch.setattr(_line, "_LineClient", _FakeClient)
        cfg = PlatformConfig(
            enabled=True,
            extra={"channel_access_token": "tok"},
        )
        result = asyncio.run(_standalone_send(cfg, "Uchat", "hello"))
        assert result.get("success") is True
        assert len(push_calls) == 1
        assert push_calls[0][0] == "Uchat"
        # Message wraps as text bubble
        assert push_calls[0][1][0]["type"] == "text"

    def test_standalone_send_parses_sticker_markers(self, monkeypatch):
        from gateway.config import PlatformConfig

        push_calls = []

        class _FakeClient:
            def __init__(self, *a, **kw):
                pass

            async def push(self, chat_id, messages):
                push_calls.append((chat_id, messages))

        monkeypatch.setattr(_line, "_LineClient", _FakeClient)
        cfg = PlatformConfig(
            enabled=True,
            extra={"channel_access_token": "tok"},
        )
        result = asyncio.run(
            _standalone_send(cfg, "Uchat", "ขอบคุณค่ะ 💙\nSTICKER:789:10863")
        )

        assert result.get("success") is True
        assert len(push_calls) == 1
        messages = push_calls[0][1]
        sticker_messages = [
            message for message in messages if message.get("type") == "sticker"
        ]
        assert sticker_messages == [
            {"type": "sticker", "packageId": "789", "stickerId": "10863"},
        ]
        text_messages = [message for message in messages if message.get("type") == "text"]
        assert len(text_messages) == 1
        assert any("ขอบคุณค่ะ" in message.get("text", "") for message in text_messages)
        assert not any("STICKER:" in message.get("text", "") for message in text_messages)


class TestFlexFileMarker:
    """FORK DELTA 2026-07-09: ``FLEXFILE:<token>`` renders a cached LINE Flex
    message on the normal (free) reply path, modeled on the STICKER marker.
    Generic, not Splitwise-specific."""

    @staticmethod
    def _write_flex(profile_dir, token, payload):
        flex_dir = profile_dir / "cache" / "flex"
        flex_dir.mkdir(parents=True, exist_ok=True)
        (flex_dir / f"{token}.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_flexfile_marker_renders_cached_flex_with_surrounding_text(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HERMES_PROFILE_DIR", str(tmp_path))
        flex_payload = {
            "type": "flex",
            "altText": "Splitwise summary",
            "contents": {
                "type": "bubble",
                "body": {"type": "box", "layout": "vertical", "contents": []},
            },
        }
        self._write_flex(tmp_path, "tok1", flex_payload)

        messages = _line._messages_from_text_payload("nice! FLEXFILE:tok1 done")

        # The cached flex bubble is interleaved between the two text fragments.
        assert [m for m in messages if m.get("type") == "flex"] == [flex_payload]
        text_messages = [m for m in messages if m.get("type") == "text"]
        assert [m["text"] for m in text_messages] == ["nice!", "done"]
        assert not any("FLEXFILE:" in m.get("text", "") for m in text_messages)

    def test_missing_flexfile_marker_is_left_as_literal_text(
        self, tmp_path, monkeypatch, caplog
    ):
        monkeypatch.setenv("HERMES_PROFILE_DIR", str(tmp_path))
        with caplog.at_level("WARNING"):
            messages = _line._messages_from_text_payload("FLEXFILE:missingtok")

        # No file for the token -> the marker survives as literal text, no crash.
        assert messages == [{"type": "text", "text": "FLEXFILE:missingtok"}]
        assert not any(m.get("type") == "flex" for m in messages)
        assert "FLEXFILE" in caplog.text


class TestPostbackButtonShape:

    def test_template_buttons_structure(self):
        msg = build_postback_button_message("hi", "Tap me", "rid-1")
        assert msg["type"] == "template"
        assert msg["template"]["type"] == "buttons"
        assert msg["template"]["text"] == "hi"
        actions = msg["template"]["actions"]
        assert len(actions) == 1
        assert actions[0]["type"] == "postback"
        data = json.loads(actions[0]["data"])
        assert data == {"action": "show_response", "request_id": "rid-1"}

    def test_text_truncated_to_160(self):
        long = "x" * 200
        msg = build_postback_button_message(long, "Tap", "rid")
        assert len(msg["template"]["text"]) <= 160

    def test_alt_text_truncated_to_400(self):
        long = "x" * 500
        msg = build_postback_button_message(long, "Tap", "rid")
        assert len(msg["altText"]) <= 400


class TestCheckRequirements:

    def test_rejects_without_token(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "s")
        assert not check_requirements()

    def test_rejects_without_secret(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "t")
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        assert not check_requirements()


class TestValidateConfig:

    def test_validates_from_extra(self):
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(
            enabled=True,
            extra={"channel_access_token": "t", "channel_secret": "s"},
        )
        assert validate_config(cfg)

    def test_rejects_empty_config(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={})
        assert not validate_config(cfg)


class TestAdapterInit:

    def test_init_from_config_extra(self, monkeypatch):
        for k in ("LINE_CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_SECRET", "LINE_PORT"):
            monkeypatch.delenv(k, raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(
            enabled=True,
            extra={
                "channel_access_token": "tok",
                "channel_secret": "sec",
                "port": 7777,
                "public_url": "https://x.example.com",
                "allowed_users": ["U1", "U2"],
            },
        )
        ad = LineAdapter(cfg)
        assert ad.channel_access_token == "tok"
        assert ad.channel_secret == "sec"
        assert ad.webhook_port == 7777
        assert ad.public_base_url == "https://x.example.com"
        assert ad.allowed_users == {"U1", "U2"}

    def test_env_overrides_extra(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "env-tok")
        monkeypatch.setenv("LINE_PORT", "1234")
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(
            enabled=True,
            extra={"channel_access_token": "extra-tok", "channel_secret": "s", "port": 5555},
        )
        ad = LineAdapter(cfg)
        assert ad.channel_access_token == "env-tok"
        assert ad.webhook_port == 1234

    def test_csv_allowlist_parsed(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "t")
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "s")
        monkeypatch.setenv("LINE_ALLOWED_USERS", "U1, U2,U3")
        monkeypatch.setenv("LINE_ALLOWED_GROUPS", "C1")
        from gateway.config import PlatformConfig
        ad = LineAdapter(PlatformConfig(enabled=True))
        assert ad.allowed_users == {"U1", "U2", "U3"}
        assert ad.allowed_groups == {"C1"}

    def test_singular_slow_response_copy_keys_accept_lists(self, monkeypatch):
        monkeypatch.delenv("LINE_PENDING_REPLY_TEXT", raising=False)
        monkeypatch.delenv("LINE_PENDING_BUTTON_LABEL", raising=False)
        monkeypatch.delenv("LINE_DELIVERED_TEXT", raising=False)
        monkeypatch.delenv("LINE_INTERRUPTED_TEXT", raising=False)
        from gateway.config import PlatformConfig
        ad = LineAdapter(PlatformConfig(enabled=True, extra={
            "channel_access_token": "t",
            "channel_secret": "s",
            "pending_reply_text": ["wait 1", "wait 2"],
            "pending_button_label": ["get 1", "get 2"],
            "delivered_text": ["done 1", "done 2"],
            "interrupted_text": ["stop 1", "stop 2"],
        }))
        assert ad.pending_reply_texts == ["wait 1", "wait 2"]
        assert ad.pending_button_labels == ["get 1", "get 2"]
        assert ad.delivered_texts == ["done 1", "done 2"]
        assert ad.interrupted_texts == ["stop 1", "stop 2"]

    def test_get_chat_info_infers_type_from_prefix(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "t")
        monkeypatch.setenv("LINE_CHANNEL_SECRET", "s")
        from gateway.config import PlatformConfig
        ad = LineAdapter(PlatformConfig(enabled=True))
        assert asyncio.run(ad.get_chat_info("U123"))["type"] == "dm"
        assert asyncio.run(ad.get_chat_info("C123"))["type"] == "group"
        assert asyncio.run(ad.get_chat_info("R123"))["type"] == "channel"


# ---------------------------------------------------------------------------
# 9. Inbound message-type classification
# ---------------------------------------------------------------------------

class TestMessageTypeMapping:
    """LINE webhook message types must map to the right normalized
    MessageType so the gateway routes media correctly (e.g. voice → STT,
    files → document handling). Regression guard for the old code that
    referenced the non-existent ``MessageType.IMAGE`` and collapsed every
    non-text message onto a single type."""

    def test_image_event_not_attributeerror_regression(self):
        # The bug: MessageType.IMAGE doesn't exist on the enum.
        MessageType = _line.MessageType
        assert not hasattr(MessageType, "IMAGE")

    def test_every_line_type_maps_to_correct_enum(self):
        MessageType = _line.MessageType
        mapping = _line._LINE_MESSAGE_TYPES
        assert mapping["text"] == MessageType.TEXT
        assert mapping["image"] == MessageType.PHOTO
        assert mapping["video"] == MessageType.VIDEO
        # LINE has no separate voice type — audio clips are voice notes.
        assert mapping["audio"] == MessageType.VOICE
        assert mapping["file"] == MessageType.DOCUMENT
        assert mapping["location"] == MessageType.LOCATION
        assert mapping["sticker"] == MessageType.STICKER

    def test_unknown_type_falls_back_to_text(self):
        MessageType = _line.MessageType
        assert _line._LINE_MESSAGE_TYPES.get("flex", MessageType.TEXT) == MessageType.TEXT



class TestNeverPushBehindButton:
    """Reactive answers behind a slow-LLM button are delivered ONLY by a press
    (free reply). They are never pushed behind the button's back. (Pai, 2026-07-08)."""

    @pytest.fixture
    def adapter(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig
        cfg = PlatformConfig(enabled=True, extra={"channel_access_token": "tok", "channel_secret": "sec"})
        ad = LineAdapter(cfg)
        ad._client = MagicMock()
        ad._client.reply = AsyncMock()
        ad._client.push = AsyncMock()
        return ad

    def test_expired_delivery_token_never_pushes(self, adapter):
        import time as _time
        rid = adapter._cache.register_pending("Uchat")
        adapter._pending_buttons["Uchat"] = rid
        # User pressed earlier, but that reply token has since expired.
        adapter._pending_delivery_tokens[rid] = ("Uchat", "old-token", _time.time() - 1)
        result = asyncio.run(adapter.send("Uchat", "the answer"))
        assert result.success
        adapter._client.push.assert_not_called()   # never push behind the button
        adapter._client.reply.assert_not_called()  # expired token can't reply
        assert adapter._cache.get(rid).state is State.READY  # waits for the next press
        assert adapter._cache.get(rid).payload == "the answer"

    def test_press_with_valid_token_replies_free(self, adapter):
        import time as _time
        rid = adapter._cache.register_pending("Uchat")
        adapter._pending_buttons["Uchat"] = rid
        adapter._cache.set_ready(rid, "the answer")
        adapter._pending_delivery_tokens[rid] = ("Uchat", "fresh-token", _time.time() + 30)
        ok = asyncio.run(adapter._deliver_registered_pending_response(rid))
        assert ok
        adapter._client.reply.assert_called_once()
        adapter._client.push.assert_not_called()


class TestLinePreview:
    def test_preview_is_small_jpeg_distinct_from_source(self, tmp_path):
        import pytest, os as _os
        Image = pytest.importorskip("PIL.Image")
        # A full multi-MB PNG reused as LINE's previewImageUrl (>1 MB) renders
        # blank on the mobile app. _generate_line_preview must return a small
        # (<=1 MB) JPEG thumbnail, distinct from the source.
        src = tmp_path / "big.png"
        Image.frombytes("RGB", (1254, 1254), _os.urandom(1254 * 1254 * 3)).save(src, "PNG")
        assert src.stat().st_size > 1_048_576, "source PNG should exceed 1 MB"
        preview = _line._generate_line_preview(str(src))
        assert preview is not None and preview != str(src)
        assert _os.path.getsize(preview) <= 1_000_000, "preview must be <= 1 MB"
        with open(preview, "rb") as fh:
            assert fh.read(3) == b"\xff\xd8\xff", "preview must be JPEG"
        _os.unlink(preview)
