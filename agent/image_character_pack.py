"""Resolve named, illo-compatible character packs for image generation.

The agent-facing value is a short pack name (for example ``blip``), never a
filesystem path. Packs may be installed per Hermes profile under
``$HERMES_HOME/characters`` or in illo's shared XDG character directory.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from hermes_constants import get_hermes_home


_PACK_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_REFERENCE_FILENAMES = (
    "reference.png",
    "reference.webp",
    "reference.jpg",
    "reference.jpeg",
)
_MAX_CHARACTER_MARKDOWN_BYTES = 64 * 1024


class CharacterPackError(ValueError):
    """A named character pack is invalid, missing, or unsafe."""


@dataclass(frozen=True)
class CharacterPack:
    """Validated character-pack data used to condition one render."""

    name: str
    reference_path: Path
    prompt_spec: str


def character_pack_roots() -> tuple[Path, ...]:
    """Return profile-local then shared illo pack roots."""
    xdg_config = Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config"))
    return (
        get_hermes_home() / "characters",
        xdg_config / "illo" / "characters",
    )


def normalize_character_name(value: object) -> str:
    """Normalize a friendly pack name while rejecting path-like values."""
    if not isinstance(value, str):
        raise CharacterPackError("character must be an installed pack name")
    name = value.strip().lower()
    if not _PACK_NAME_RE.fullmatch(name):
        raise CharacterPackError(
            "character must use only letters, numbers, '-' or '_' (no paths)"
        )
    return name


def _extract_prompt_spec(markdown: str) -> str:
    """Lift only the pack's Prompt spec section; do not execute pack prose."""
    lines = markdown.splitlines()
    collecting = False
    extracted: list[str] = []
    for line in lines:
        if re.match(r"^##\s+Prompt spec\s*$", line, flags=re.IGNORECASE):
            collecting = True
            continue
        if collecting and line.startswith("## "):
            break
        if collecting:
            cleaned = re.sub(r"^\s*>\s?", "", line).strip()
            if cleaned:
                extracted.append(cleaned)
    prompt_spec = " ".join(extracted).strip()
    if not prompt_spec:
        raise CharacterPackError("character pack has no Prompt spec section")
    return prompt_spec


def resolve_character_pack(value: object) -> CharacterPack:
    """Resolve and validate an installed character by friendly name."""
    name = normalize_character_name(value)

    for root in character_pack_roots():
        root_resolved = root.expanduser().resolve()
        pack_dir = (root_resolved / name).resolve()
        try:
            pack_dir.relative_to(root_resolved)
        except ValueError as exc:
            raise CharacterPackError("character pack escaped its configured root") from exc
        if not pack_dir.is_dir():
            continue

        spec_path = pack_dir / "character.md"
        if not spec_path.is_file():
            raise CharacterPackError(f"character pack '{name}' has no character.md")
        if spec_path.stat().st_size > _MAX_CHARACTER_MARKDOWN_BYTES:
            raise CharacterPackError(f"character pack '{name}' character.md is too large")

        reference_path = next(
            (
                pack_dir / filename
                for filename in _REFERENCE_FILENAMES
                if (pack_dir / filename).is_file()
            ),
            None,
        )
        if reference_path is None:
            raise CharacterPackError(f"character pack '{name}' has no reference image")
        resolved_reference = reference_path.resolve()
        try:
            resolved_reference.relative_to(pack_dir)
        except ValueError as exc:
            raise CharacterPackError("character reference escaped its pack directory") from exc

        try:
            markdown = spec_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise CharacterPackError(f"character pack '{name}' could not be read") from exc

        return CharacterPack(
            name=name,
            reference_path=resolved_reference,
            prompt_spec=_extract_prompt_spec(markdown),
        )

    raise CharacterPackError(
        f"character pack '{name}' is not installed; install it under "
        "$HERMES_HOME/characters or the illo character directory"
    )


def apply_character_lock(prompt: str, pack: CharacterPack) -> str:
    """Add identity constraints while keeping the user's scene request intact."""
    return (
        f"{prompt.strip()}\n\n"
        f"CHARACTER LOCK — {pack.name}: The first attached image is the canonical "
        "model sheet. Preserve the same identity, silhouette, face geometry, and "
        "distinctive parts in the requested scene. Do not copy the model-sheet "
        "layout or background. Character specification: "
        f"{pack.prompt_spec}"
    )
