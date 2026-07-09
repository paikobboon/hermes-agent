"""Pin: the background-review fork cannot create/mutate skills (Pai 2026-07-10).

Its autonomous skill creation produced a rogue skill that broke a live feature;
skill mutation is now human-only. Foreground writes stay allowed.
"""
from tools.skill_provenance import set_current_write_origin
import tools.skill_manager_tool as sm


def test_background_review_cannot_create_or_mutate_skills():
    # foreground / assistant context: preflight does not block create
    set_current_write_origin("assistant_tool")
    assert sm._background_review_preflight("create", "any-skill") is None

    # background-review fork: every mutation is refused, fail-closed
    set_current_write_origin("background_review")
    for action in ("create", "edit", "patch", "delete", "write_file", "remove_file"):
        r = sm._background_review_preflight(action, "any-skill")
        assert isinstance(r, dict), (action, r)
        assert r.get("success") is False and r.get("_fail_closed") is True, (action, r)
    # reset so we don't leak the origin into other tests
    set_current_write_origin("assistant_tool")
