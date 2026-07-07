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
