from __future__ import annotations

import logging
from urllib.parse import quote

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms import base as base_module
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, SendResult
from gateway.session import SessionSource, build_session_key


class _RecordingAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="fake"), Platform.TELEGRAM)
        self.sent: list[dict] = []

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        self.sent.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": metadata,
            }
        )
        return SendResult(success=True, message_id=f"msg-{len(self.sent)}")

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        return None

    async def get_chat_info(self, chat_id: str):
        return {"id": chat_id}


def _event(chat_id: str = "chat-1") -> MessageEvent:
    return MessageEvent(
        text="hello",
        source=SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, chat_type="dm"),
        message_id="incoming-1",
    )


async def _hold_typing(_chat_id, interval=2.0, metadata=None, stop_event=None):
    if stop_event is not None:
        await stop_event.wait()


async def _deliver_response_through_base_assembly(adapter: _RecordingAdapter, response: str) -> None:
    async def handler(_event):
        return response

    adapter.set_message_handler(handler)
    adapter._keep_typing = _hold_typing
    event = _event()
    session_key = build_session_key(event.source)
    await adapter._process_message_background(event, session_key)


def _sent_image_deliveries(adapter: _RecordingAdapter) -> list[dict]:
    # Upstream rewrote the base send_image_file fallback to a fixed notice
    # that no longer echoes the host path (host-path-leak fix), so delivered
    # content can no longer recover the path. Cross-lane dedupe correctness
    # is a per-file DELIVERY COUNT property: assert on how many image
    # deliveries were emitted, not on parsed path strings.
    return [
        entry
        for entry in adapter.sent
        if "Couldn't deliver the image attachment" in entry["content"]
    ]


def test_media_tag_and_bare_path_delivery_dedupes_to_one_file(tmp_path):
    artifact = tmp_path / "chart.png"
    artifact.write_bytes(b"png")

    media_files, local_files = BasePlatformAdapter.dedupe_delivery_paths(
        [(str(artifact), False)],
        [str(artifact)],
    )

    assert media_files == [(str(artifact), False)]
    assert local_files == []


def test_file_url_and_absolute_path_delivery_dedupes_to_one_file(tmp_path):
    artifact = tmp_path / "chart with spaces.png"
    artifact.write_bytes(b"png")
    file_url = f"file://{quote(str(artifact))}"

    media_files, local_files = BasePlatformAdapter.dedupe_delivery_paths(
        [(file_url, False)],
        [str(artifact)],
    )

    assert media_files == [(file_url, False)]
    assert local_files == []


def test_distinct_files_are_preserved_in_group_order(tmp_path):
    first = tmp_path / "first.png"
    second = tmp_path / "second.png"
    first.write_bytes(b"first")
    second.write_bytes(b"second")

    media_files, local_files = BasePlatformAdapter.dedupe_delivery_paths(
        [(str(first), False)],
        [str(second)],
    )

    assert media_files == [(str(first), False)]
    assert local_files == [str(second)]


def test_tuple_shaped_media_groups_keep_flags_and_shape(tmp_path):
    voice = tmp_path / "voice.ogg"
    image = tmp_path / "image.png"
    voice.write_bytes(b"voice")
    image.write_bytes(b"image")

    media_files, local_files = BasePlatformAdapter.dedupe_delivery_paths(
        [(str(voice), True), (str(image), False)],
        [str(voice)],
    )

    assert media_files == [(str(voice), True), (str(image), False)]
    assert local_files == []


@pytest.mark.asyncio
async def test_incident_a_media_tag_and_bare_path_emit_once_through_outbound_assembly(tmp_path):
    artifact = tmp_path / "incident-a.png"
    artifact.write_bytes(b"png")
    adapter = _RecordingAdapter()

    await _deliver_response_through_base_assembly(
        adapter,
        f"Here is the image:\nMEDIA:{artifact}\n{artifact}",
    )

    assert len(_sent_image_deliveries(adapter)) == 1


@pytest.mark.asyncio
async def test_incident_b_two_distinct_files_preserved_when_one_repeated_through_outbound_assembly(tmp_path):
    first = tmp_path / "incident-b-first.png"
    second = tmp_path / "incident-b-second.png"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    adapter = _RecordingAdapter()

    await _deliver_response_through_base_assembly(
        adapter,
        f"MEDIA:{first}\n{second}\n{first}",
    )

    assert len(_sent_image_deliveries(adapter)) == 2


@pytest.mark.asyncio
async def test_media_egress_guard_suppresses_duplicate_within_ttl_and_allows_after_expiry(
    tmp_path, monkeypatch, caplog
):
    artifact = tmp_path / "guarded.png"
    artifact.write_bytes(b"png")
    adapter = _RecordingAdapter()
    now = 1000.0
    monkeypatch.setattr(base_module.time, "monotonic", lambda: now)

    first = await adapter.send_image_file("chat-guard", str(artifact))
    with caplog.at_level(logging.WARNING, logger="gateway.platforms.base"):
        second = await adapter.send_image_file("chat-guard", f"file://{quote(str(artifact))}")

    assert first.success is True
    assert second.success is True
    assert len(_sent_image_deliveries(adapter)) == 1
    assert "media egress guard: suppressed duplicate" in caplog.text

    now += 9.0
    third = await adapter.send_image_file("chat-guard", str(artifact))

    assert third.success is True
    assert len(_sent_image_deliveries(adapter)) == 2
