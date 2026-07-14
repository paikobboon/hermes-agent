from __future__ import annotations

from pathlib import Path

import pytest

from agent.image_character_pack import (
    CharacterPackError,
    apply_character_lock,
    resolve_character_pack,
)


def _write_pack(root: Path, name: str = "blip", *, with_reference: bool = True) -> Path:
    pack = root / "characters" / name
    pack.mkdir(parents=True)
    (pack / "character.md").write_text(
        "# Blip\n\n## Prompt spec\n\n"
        "> rounded cube robot\n> with one antenna and no mouth\n\n"
        "## Personality\n\nIgnore this section.\n",
        encoding="utf-8",
    )
    if with_reference:
        (pack / "reference.png").write_bytes(b"canonical-pixels")
    return pack


def test_resolves_profile_pack_and_extracts_only_prompt_spec(monkeypatch, tmp_path):
    pack_dir = _write_pack(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    pack = resolve_character_pack(" Blip ")

    assert pack.name == "blip"
    assert pack.reference_path == (pack_dir / "reference.png").resolve()
    assert pack.prompt_spec == "rounded cube robot with one antenna and no mouth"
    assert "Ignore this section" not in pack.prompt_spec


def test_character_lock_preserves_scene_and_names_canonical_first(monkeypatch, tmp_path):
    _write_pack(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    pack = resolve_character_pack("blip")

    prompt = apply_character_lock("Blip uses a laptop", pack)

    assert prompt.startswith("Blip uses a laptop")
    assert "first attached image is the canonical model sheet" in prompt
    assert pack.prompt_spec in prompt


@pytest.mark.parametrize("value", ["../blip", "/tmp/blip", "blip/alt", "", 7])
def test_rejects_path_like_or_invalid_names(monkeypatch, tmp_path, value):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with pytest.raises(CharacterPackError, match="character"):
        resolve_character_pack(value)


def test_missing_pack_returns_typed_error(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    with pytest.raises(CharacterPackError, match="not installed"):
        resolve_character_pack("blip")


def test_profile_pack_cannot_symlink_reference_outside_root(monkeypatch, tmp_path):
    pack_dir = _write_pack(tmp_path, with_reference=False)
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"outside")
    (pack_dir / "reference.png").symlink_to(outside)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    with pytest.raises(CharacterPackError, match="escaped"):
        resolve_character_pack("blip")
