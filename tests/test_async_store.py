"""Tests for the asynchronous, Redux-style :class:`StonedogRemembers` store.

These focus on the branches that the basic lifecycle tests don't reach:
the event stream, load-failure fallbacks, processor start/stop guards, and
the action-processor's error paths (missing path, unknown type, failed apply).
"""

import asyncio
import json

import pytest

from stonedog_remembers import StonedogRemembers


async def _drain_event(store, timeout=1.0):
    """Wait for and return the next emitted event."""
    return await asyncio.wait_for(store.subscribe_events().get(), timeout)


@pytest.mark.asyncio
async def test_load_initial_state_missing_file(tmp_path):
    """A missing state file logs a warning and yields an empty state."""
    store = StonedogRemembers(str(tmp_path / "does_not_exist.json"))
    await store.load_initial_state()
    assert store.get_current_state() == {}


@pytest.mark.asyncio
async def test_load_initial_state_invalid_json(tmp_path):
    """Malformed JSON falls back to an empty state instead of raising."""
    bad = tmp_path / "bad.json"
    bad.write_text("{ this is not json ")
    store = StonedogRemembers(str(bad))
    await store.load_initial_state()
    assert store.get_current_state() == {}


@pytest.mark.asyncio
async def test_load_initial_state_unexpected_error(tmp_path):
    """A non-IO/JSON error while loading is swallowed to an empty state."""
    # Point the store at a directory: open(..., 'r') raises IsADirectoryError,
    # which is neither FileNotFoundError nor JSONDecodeError -> generic branch.
    store = StonedogRemembers(str(tmp_path))
    await store.load_initial_state()
    assert store.get_current_state() == {}


@pytest.mark.asyncio
async def test_dispatch_emits_state_changed_event(tmp_path):
    """A successful SET_STATE mutates state and emits a STATE_CHANGED event."""
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"user": {"theme": "dark"}}))

    store = StonedogRemembers(str(state_file))
    await store.load_initial_state()
    events = store.subscribe_events()
    store.start_processing()

    await store.dispatch(
        {
            "type": StonedogRemembers.ACTION_TYPE_SET_STATE,
            "path": "user.theme",
            "value": "light",
        }
    )

    event = await asyncio.wait_for(events.get(), 1.0)
    assert event["type"] == StonedogRemembers.EVENT_TYPE_STATE_CHANGED
    assert event["path"] == "user.theme"
    assert event["old_value"] == "dark"
    assert event["new_value"] == "light"
    assert store.get_current_state()["user"]["theme"] == "light"

    await store.stop_processing()


@pytest.mark.asyncio
async def test_dispatch_missing_path_is_ignored(tmp_path):
    """A SET_STATE action without 'path' is skipped and emits no event."""
    store = StonedogRemembers(str(tmp_path / "s.json"))
    await store.load_initial_state()
    events = store.subscribe_events()
    store.start_processing()

    await store.dispatch({"type": StonedogRemembers.ACTION_TYPE_SET_STATE, "value": 1})
    await asyncio.sleep(0.05)

    assert events.empty()
    assert store.get_current_state() == {}
    await store.stop_processing()


@pytest.mark.asyncio
async def test_dispatch_failed_apply_emits_no_event(tmp_path):
    """A set that can't be applied leaves state unchanged and emits nothing."""
    state_file = tmp_path / "s.json"
    state_file.write_text(json.dumps({"items": [1, 2]}))

    store = StonedogRemembers(str(state_file))
    await store.load_initial_state()
    events = store.subscribe_events()
    store.start_processing()

    # list index out of range -> set_nested_value returns False
    await store.dispatch(
        {
            "type": StonedogRemembers.ACTION_TYPE_SET_STATE,
            "path": "items.9",
            "value": "x",
        }
    )
    await asyncio.sleep(0.05)

    assert events.empty()
    assert store.get_current_state() == {"items": [1, 2]}
    await store.stop_processing()


@pytest.mark.asyncio
async def test_dispatch_unknown_action_type_is_ignored(tmp_path):
    """An unrecognized action type produces no state change and no event."""
    store = StonedogRemembers(str(tmp_path / "s.json"))
    await store.load_initial_state()
    events = store.subscribe_events()
    store.start_processing()

    await store.dispatch({"type": "NOT_A_REAL_ACTION", "path": "a", "value": 1})
    await asyncio.sleep(0.05)

    assert events.empty()
    assert store.get_current_state() == {}
    await store.stop_processing()


@pytest.mark.asyncio
async def test_start_processing_is_idempotent(tmp_path):
    """Calling start_processing twice does not spawn a second processor task."""
    store = StonedogRemembers(str(tmp_path / "s.json"))
    await store.load_initial_state()
    store.start_processing()
    first_task = store._processing_task
    store.start_processing()  # already running -> warning branch, no new task
    assert store._processing_task is first_task
    await store.stop_processing()


@pytest.mark.asyncio
async def test_stop_processing_without_start_is_noop(tmp_path):
    """Stopping a store that was never started is safe."""
    store = StonedogRemembers(str(tmp_path / "s.json"))
    await store.load_initial_state()
    await store.stop_processing()  # no task -> should not raise


@pytest.mark.asyncio
async def test_get_current_state_returns_copy(tmp_path):
    """get_current_state hands back a deep copy; mutating it is harmless."""
    state_file = tmp_path / "s.json"
    state_file.write_text(json.dumps({"a": {"b": 1}}))
    store = StonedogRemembers(str(state_file))
    await store.load_initial_state()

    snapshot = store.get_current_state()
    snapshot["a"]["b"] = 999
    assert store.get_current_state()["a"]["b"] == 1


def test_rozremembers_is_a_deprecated_alias():
    # The class shipped as RozRemembers while the library was roz-remembers.
    # It must stay importable and be the *same object*, so isinstance checks and
    # the ACTION_TYPE_*/EVENT_TYPE_* class attributes behave identically.
    from stonedog_remembers import RozRemembers, StonedogRemembers

    assert RozRemembers is StonedogRemembers
