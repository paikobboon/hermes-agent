"""Pinning tests for deliver_response_with_media() — the queued-follow-up
"first response" lane must ship media natively, not leak raw markdown.

Lucky group incident 2026-07-12 22:43: a queued follow-up sent the agent's
first response via raw adapter.send(), bypassing outbound media assembly —
the family received "[label](sandbox:/…/img.png)" as literal text and no
image. FORK DELTA 2026-07-12.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from gateway.run import deliver_response_with_media


def _adapter_with_assembly(cleaned_text):
    adapter = MagicMock()
    adapter.assemble_and_send_media = AsyncMock(return_value=cleaned_text)
    adapter.send = AsyncMock()
    return adapter


class TestDeliverResponseWithMedia:

    def test_media_assembly_runs_and_cleaned_text_is_sent(self):
        adapter = _adapter_with_assembly("caption text")
        asyncio.run(deliver_response_with_media(
            adapter, "C123", "caption text\n[img](sandbox:/tmp/x.png)", metadata={"m": 1}
        ))
        adapter.assemble_and_send_media.assert_awaited_once_with(
            "C123", "caption text\n[img](sandbox:/tmp/x.png)", metadata={"m": 1}
        )
        adapter.send.assert_awaited_once_with("C123", "caption text", metadata={"m": 1})

    def test_image_only_response_sends_no_empty_text(self):
        adapter = _adapter_with_assembly("   ")
        asyncio.run(deliver_response_with_media(adapter, "C123", "/tmp/x.png"))
        adapter.assemble_and_send_media.assert_awaited_once()
        adapter.send.assert_not_awaited()

    def test_adapter_without_assembly_falls_back_to_plain_send(self):
        adapter = MagicMock(spec=["send"])
        adapter.send = AsyncMock()
        asyncio.run(deliver_response_with_media(adapter, "C123", "hello"))
        adapter.send.assert_awaited_once_with("C123", "hello", metadata=None)
