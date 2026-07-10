# LINE File Delivery Attempt — 2026-07-10 (archived, not live)

Pai's order: revert from live, keep on this archive branch in case we need it one day.

## The problem it solved
Karn asked Lucky to cut pages from a 235-page salary-guide PDF. Lucky cut it correctly
(95-page PDF in `cache/`) but delivered only a local filesystem path into LINE chat —
nothing usable arrived. Session: `20260710_151733_c531df33` in Lucky's `state.db`.

Three stacked causes:
1. **LINE's Messaging API has no file message type for bots** — LINE FAQ, verbatim:
   "Q: Can I send PDF files with the Messaging API? A: You can't send PDF files with
   the Messaging API." (developers.line.biz/en/faq). Bots send text/image/video/audio/
   sticker/flex only. Users can send files TO bots; never the reverse.
2. The LINE adapter had no `send_document` override → gateway fallback can't deliver files.
3. Lucky's `lucky-line-operations` skill documented `MEDIA:` for images only, so the
   model pasted a raw host path.

## What was built (this branch, working and test-pinned)
Hermes fork commits `30c25c33b..3c5afe01f` on top of `91d1992bc`:
- `LineAdapter.send_document` (`plugins/platforms/line/adapter.py`): serves the file
  from the adapter's existing HTTPS media server (public Tailscale Funnel `:8443/line`,
  256-bit `secrets.token_urlsafe(32)` tokens, 24h TTL via new `ttl` kwarg on
  `_register_media`), and sends a **Flex file card**: 📄 filename, human size,
  เปิดไฟล์ button, plus a **first-page preview hero** for PDFs (rendered by shelling
  out to system python3 + PyMuPDF — the gateway venv has no fitz; see
  `_generate_pdf_page_preview`, best-effort, never blocks the card).
- Egress guard, size cap (200 MB), missing-file/HTTPS validation, `LINE SEND
  site=document ... url=...` ops log line, platform_hint updated to forbid host
  paths in chat text.
- Pinning tests: `tests/gateway/test_line_plugin.py::TestSendDocument` (5 cases).
  Full sweep at archive time: 100 passed line-plugin / 347 passed line+media+delivery+cron.
- FORK_DELTAS.md row (revert removed it from the working branch; it exists here).

Companion profile-repo work (lucky-profile `archive/file-delivery-20260710`):
skill sections teaching `MEDIA:<path>` for files, forbidding host paths, and an
explicit-ask-only page-images recipe (python3+fitz render → MEDIA image tags).

**Live-verified before revert:** synthetic signed webhook → Lucky consulted the skill,
tagged the file, LINE accepted the flex push, and the card's URL served
`200 application/pdf` at the exact byte size (9,590,259). Karn received and thanked.

## Why it was reverted
Pai's requirement was **real PDF file bubbles**, not tap-to-open cards/links. That is
impossible from a bot (FAQ above). The only real-file route is automating Lucky's own
OA Manager chat console (chat.line.biz — operators CAN send files per LINE help
center), which Pai declined as ToS-gray risk on the family care OA (2026-07-10).
With file bubbles off the table, Pai chose to carry nothing new live.

Manual escape hatch that always works: Pai sends any file as Lucky by hand from the
LINE Official Account app.

## How to restore
1. Hermes fork: `git cherry-pick 30c25c33b ad7e6447a 24a782635 978d2aa88 3c5afe01f`
   onto `fork/line-group-upgrades` (or merge this branch), re-run:
   `PYTHONPATH=$(ls -d .venv/lib/python*/site-packages) ./venv/bin/python -m pytest
   tests/gateway/test_line_plugin.py -q`
2. lucky-profile: merge `archive/file-delivery-20260710` (SKILL.md sections), deploy
   via `./deploy-lucky.sh`.
3. Restart: `bash ~/.hermes/profiles/lucky/scripts/ops/safe-restart.sh`.
4. Re-add the FORK_DELTAS row.
Requirements: `LINE_PUBLIC_URL` set (Funnel public on `:8443/line`), system python3
with PyMuPDF for the hero preview (degrades gracefully without).
