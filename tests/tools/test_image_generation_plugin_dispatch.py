from __future__ import annotations

import json
import pytest

from agent import image_gen_registry
from agent.image_gen_provider import ImageGenProvider


@pytest.fixture(autouse=True)
def _reset_registry():
    image_gen_registry._reset_for_tests()
    yield
    image_gen_registry._reset_for_tests()


class _FakeCodexProvider(ImageGenProvider):
    @property
    def name(self) -> str:
        return "codex"

    def generate(self, prompt, aspect_ratio="landscape", **kwargs):
        return {
            "success": True,
            "image": "/tmp/codex-test.png",
            "model": "gpt-5.2-codex",
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            "provider": "codex",
        }


class _CapturingCodexProvider(_FakeCodexProvider):
    def __init__(self):
        self.calls = []

    def generate(self, prompt, aspect_ratio="landscape", **kwargs):
        self.calls.append({
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            **kwargs,
        })
        return super().generate(prompt, aspect_ratio, **kwargs)


class TestPluginDispatch:
    def test_dispatch_routes_to_codex_provider(self, monkeypatch, tmp_path):
        from tools import image_generation_tool
        from agent import image_gen_registry as registry_module
        from hermes_cli import plugins as plugins_module

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        (tmp_path / "config.yaml").write_text("image_gen:\n  provider: codex\n")
        image_gen_registry.register_provider(_FakeCodexProvider())

        monkeypatch.setattr(image_generation_tool, "_read_configured_image_provider", lambda: "codex")
        monkeypatch.setattr(plugins_module, "_ensure_plugins_discovered", lambda: None)
        monkeypatch.setattr(registry_module, "get_provider", lambda name: _FakeCodexProvider() if name == "codex" else None)

        dispatched = image_generation_tool._dispatch_to_plugin_provider("draw cat", "square")
        payload = json.loads(dispatched)

        assert payload["success"] is True
        assert payload["provider"] == "codex"
        assert payload["image"] == "/tmp/codex-test.png"
        assert payload["aspect_ratio"] == "square"

    def test_named_character_resolves_canonical_reference_first(self, monkeypatch, tmp_path):
        from tools import image_generation_tool
        from agent import image_gen_registry as registry_module
        from hermes_cli import plugins as plugins_module

        pack_dir = tmp_path / "characters" / "blip"
        pack_dir.mkdir(parents=True)
        (pack_dir / "character.md").write_text(
            "# Blip\n\n## Prompt spec\n\n> rounded robot with one antenna\n",
            encoding="utf-8",
        )
        canonical = pack_dir / "reference.png"
        canonical.write_bytes(b"canonical")
        style_reference = tmp_path / "style.png"
        style_reference.write_bytes(b"style")

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        provider = _CapturingCodexProvider()
        monkeypatch.setattr(image_generation_tool, "_read_configured_image_provider", lambda: "codex")
        monkeypatch.setattr(image_generation_tool, "_read_configured_image_model", lambda: None)
        monkeypatch.setattr(plugins_module, "_ensure_plugins_discovered", lambda *args, **kwargs: None)
        monkeypatch.setattr(registry_module, "get_provider", lambda name: provider if name == "codex" else None)
        monkeypatch.setattr(image_generation_tool, "_postprocess_image_generate_result", lambda raw, task_id=None: raw)

        raw = image_generation_tool._handle_image_generate({
            "prompt": "Blip explains LifeOS",
            "aspect_ratio": "landscape",
            "character": "Blip",
            "reference_image_urls": [str(style_reference)],
        })
        payload = json.loads(raw)

        assert payload["success"] is True
        assert payload["character"] == "blip"
        assert provider.calls[0]["reference_image_urls"] == [
            str(canonical.resolve()),
            str(style_reference),
        ]
        assert provider.calls[0]["prompt"].startswith("Blip explains LifeOS")
        assert "CHARACTER LOCK — blip" in provider.calls[0]["prompt"]

    def test_named_character_rejects_paths_before_dispatch(self, monkeypatch, tmp_path):
        from tools import image_generation_tool

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        raw = image_generation_tool._handle_image_generate({
            "prompt": "draw it",
            "character": "../../secret",
        })
        payload = json.loads(raw)

        assert payload["success"] is False
        assert payload["error_type"] == "invalid_character"

    def test_dispatch_reports_missing_registered_provider(self, monkeypatch, tmp_path):
        from tools import image_generation_tool
        from hermes_cli import plugins as plugins_module

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        (tmp_path / "config.yaml").write_text("image_gen:\n  provider: missing-codex\n")

        monkeypatch.setattr(image_generation_tool, "_read_configured_image_provider", lambda: "missing-codex")
        monkeypatch.setattr(plugins_module, "_ensure_plugins_discovered", lambda: None)

        dispatched = image_generation_tool._dispatch_to_plugin_provider("draw cat", "landscape")
        payload = json.loads(dispatched)

        assert payload["success"] is False
        assert payload["error_type"] == "provider_not_registered"
        assert "image_gen.provider='missing-codex'" in payload["error"]

    def test_dispatch_force_refreshes_plugins_when_provider_initially_missing(self, monkeypatch, tmp_path):
        from tools import image_generation_tool
        from hermes_cli import plugins as plugins_module
        from agent import image_gen_registry as registry_module

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        (tmp_path / "config.yaml").write_text("image_gen:\n  provider: codex\n")

        monkeypatch.setattr(image_generation_tool, "_read_configured_image_provider", lambda: "codex")

        calls = []
        provider_state = {"provider": None}

        def fake_ensure_plugins_discovered(force=False):
            calls.append(force)
            if force:
                provider_state["provider"] = _FakeCodexProvider()

        monkeypatch.setattr(plugins_module, "_ensure_plugins_discovered", fake_ensure_plugins_discovered)
        monkeypatch.setattr(registry_module, "get_provider", lambda name: provider_state["provider"])

        dispatched = image_generation_tool._dispatch_to_plugin_provider("draw hammy", "portrait")
        payload = json.loads(dispatched)

        assert calls == [False, True]
        assert payload["success"] is True
        assert payload["provider"] == "codex"
        assert payload["aspect_ratio"] == "portrait"
