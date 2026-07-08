# FORK_DELTAS — paikobboon/hermes-agent · branch `fork/line-group-upgrades`

> Ledger of every behavior this fork carries on top of upstream (NousResearch).
> Doctrine (LifeOS upstream-upgrade-safety): each delta gets a lineage note, a
> re-apply anchor (a stable string to find/re-locate it after rebases), and a
> pinning test. `scripts/ops/upgrade-hermes.sh` (in the lucky-profile repo)
> rebases this branch onto upstream and REFUSES to proceed unless the pinning
> tests pass. Update this ledger in the same commit as any new delta.
>
> Upstream is fetch-only: push URLs on `origin` are deliberately dead, and
> `gh pr/issue create` + `*nousresearch*` are deny-listed in every profile.

## Deltas (2026-07-06/07 hardening wave)

| Commit | What | Re-apply anchor | Pinning test |
|---|---|---|---|
| `47815c890` | Slow-response strings configurable per profile (pending/button/delivered/interrupted; env + `extra:` keys) | `DEFAULT_PENDING_REPLY_TEXT` constants + `_text_setting(` reads in `plugins/platforms/line/adapter.py` | `test_line_plugin.py` (config-string cases) |
| `aadaff3d3` | Rotating copy (plural list keys, `random.choice` per send) + postback double-press dedupe (per-rid `DELIVERING` claim) | `pending_reply_texts` / `DELIVERING` in `plugins/platforms/line/adapter.py` | `test_line_plugin.py::` rotation + double-press cases |
| `41b97b1f4` | Stale pending-button rid can no longer swallow sends: honor delivery result, tombstone payload, fall through to reply/push, WARN | `"pending-button delivery unavailable for rid"` in `plugins/platforms/line/adapter.py` `_send_messages` | covered via double-press/pending cases; live WARNING observed 2026-07-07 00:13 |
| `7f6afaa84` | Cross-lane media dedupe — one file, one delivery. Shared `dedupe_delivery_paths(*groups)` (strip `file://`, URL-decode, expanduser, realpath; first-wins; shape-preserving) applied at base.py outbound assembly (×2), run.py queued path (×2), weixin.py direct send | `def dedupe_delivery_paths` in `gateway/platforms/base.py` | `tests/gateway/test_media_dedupe.py` (4 cases) |
| `local` | Media egress guard — per-chat 90s TTL invariant closes duplicate image delivery at the outbound boundary; LINE local image sends share the guard before public URL registration; dedupe removals log `cross-lane dedupe: N -> M` | `media egress guard` in `gateway/platforms/base.py` | `tests/gateway/test_media_dedupe.py` incident A/B assembly replays + TTL boundary case |
| `local` | Cron/scheduled platform delivery now runs the shared interactive media assembly before the final text send: generated image/file paths are attached natively, markdown image wrappers are stripped, cleaned text is sent only when non-empty, and raw oversized output still gets an audit copy | `assemble_and_send_media` call in `DeliveryRouter._deliver_to_platform` (`gateway/delivery.py`) | `tests/gateway/test_delivery.py::test_cron_delivery_attaches_bare_image_path_and_sends_cleaned_text`; `tests/gateway/test_delivery.py::test_cron_delivery_attaches_markdown_image_once_and_strips_wrapper` |
| `local` | Proactive cross-chat sends are mirrored into the target session so Lucky remembers what she sent there: `hermes send` has a guarded CLI fallback that skips already-mirrored tool sends, `line:U/C/R...` targets parse as explicit IDs, and cron mirrors confirmed live-adapter/standalone deliveries after cleaning `MEDIA:` tags | `_mirror_successful_send(` in `hermes_cli/send_cmd.py`; `platform_name == "line"` in `tools/send_message_tool.py`; `_mirror_cron_delivery(` in `cron/scheduler.py` | `tests/hermes_cli/test_send_cmd.py::test_successful_explicit_send_fallback_mirrors_target_session`; `tests/hermes_cli/test_send_cmd.py::test_successful_send_does_not_double_mirror_when_tool_already_did_it`; `tests/tools/test_send_message_target_parse.py::test_line_user_id_target_is_explicit`; `tests/cron/test_scheduler.py::TestDeliverResultWrapping::test_successful_delivery_mirrors_to_target_session`; `tests/cron/test_scheduler.py::TestDeliverResultWrapping::test_failed_delivery_does_not_mirror_to_target_session` |
| `local` | LINE sticker send without a new agent tool: outbound text may include `STICKER:<packageId>:<stickerId>`, valid numeric markers are stripped into native LINE sticker messages, malformed markers remain text with a warning, and Lucky has a curated safe sticker palette constant. Fixed follow-up: pre-built text messages on the primary reply/push path now converge through `_messages_from_text_payload`, so normal agent replies, pushes, cached-button delivery, and CLI sends share the same text-to-LINE-message builder instead of leaking raw markers. | `_messages_from_text_payload` / `_messages_from_prebuilt_payload` / `LINE_SAFE_STICKERS` in `plugins/platforms/line/adapter.py` | `tests/gateway/test_line_plugin.py::TestSendRouting::test_send_sticker_marker_sends_sticker_and_remaining_text`; `tests/gateway/test_line_plugin.py::TestSendRouting::test_prebuilt_text_reply_path_parses_sticker_marker`; `tests/gateway/test_line_plugin.py::TestSendRouting::test_malformed_sticker_marker_is_left_as_text` |
| `local` | LINE standalone out-of-process send lane missed the STICKER convergence sweep: `hermes send -t line:...` now uses the shared text payload builder so valid `STICKER:<packageId>:<stickerId>` markers become native sticker messages instead of leaking as literal text. | `_standalone_send` payload build in `plugins/platforms/line/adapter.py` | `tests/gateway/test_line_plugin.py::TestStandaloneSend::test_standalone_send_parses_sticker_markers` |
| `local` | LINE quoted-reply context: inbound `message.quotedMessageId` populates `reply_to_message_id` and `reply_to_text` from `rich_sent_store` or a bounded recent-message cache, with a placeholder when unresolved so Lucky sees the user is replying to an earlier message. Fixed follow-up: successful LINE reply/push responses cache outbound `sentMessages[].id` back to the sent text or media placeholder, so quoting Lucky's own messages resolves too. | `_resolve_quote_context` / `_remember_sent_message_texts` in `plugins/platforms/line/adapter.py` | `tests/gateway/test_line_plugin.py::TestQuoteContext::test_quoted_message_id_resolves_from_rich_sent_store`; `tests/gateway/test_line_plugin.py::TestQuoteContext::test_quoted_outbound_line_message_id_resolves_from_recent_cache`; `tests/gateway/test_line_plugin.py::TestQuoteContext::test_unresolvable_quoted_message_id_uses_placeholder`; `tests/gateway/test_line_plugin.py::TestQuoteContext::test_absent_quoted_message_id_leaves_reply_context_empty` |

## Pre-wave deltas (Pai-era, before 2026-07-06)

The branch also carries earlier LINE work (sender display-name resolution,
media/text coalescing, webhook-event dedupe persistence, sticker/coalesce
tests, group upgrades). Snapshot them any time with:

    git log --oneline origin/main..fork/line-group-upgrades

When touching any of these, add them to the table above with an anchor + test.

## Test infra note

`pytest` lives in `./.venv` (NOT `./venv`, which is the runtime venv and has no
network access for pip). Run the suite as:

    PYTHONPATH=$(ls -d .venv/lib/python*/site-packages) ./venv/bin/python -m pytest tests/gateway/test_line_plugin.py tests/gateway/test_media_dedupe.py -q

(or simply `./.venv/bin/pytest` if its interpreter matches). 85 tests green as
of 2026-07-07.

| `df internal` | Codex keepalive TLS-reset bypass — chatgpt.com uses SDK-default transport (NousResearch#12952) | `chatgpt.com` in _build_keepalive_http_client (run_agent.py) | manual — verify plain httpx.Client for chatgpt base_url |
| `local` | LINE inbound video/audio-file/document discard fix — non-image media was cached via image-only `cache_image_from_bytes`, which refuses non-image bytes, so a valid MP4 was fetched then thrown away and the family saw a bare `[video]` with no file/metadata. New `cache_media_from_bytes` saves non-image bytes (size still validated); adapter routes images→image cache, video/audio/file→media cache | `cache_media_from_bytes` in `gateway/platforms/base.py`; `_download_media` in `plugins/platforms/line/adapter.py` | `tests/gateway/test_media_download_retry.py::TestCacheMediaFromBytes` (2 cases) |
