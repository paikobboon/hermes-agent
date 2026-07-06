from __future__ import annotations

from urllib.parse import quote

from gateway.platforms.base import BasePlatformAdapter


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
