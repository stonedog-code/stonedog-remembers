"""stonedog-remembers: a small, message-driven state management library for Python.

Two front ends share the same dot-path engine:

* :class:`StonedogRemembers` — the original asyncio, Redux-style store driven by an
  action queue and an event queue. Best for long-running async applications.
* :class:`Store` — a synchronous facade with the same dot-path semantics. Best
  for synchronous code (e.g. the card-sorter device loop) that just needs a
  central, observable bag of runtime state without an event loop.
"""

import asyncio
import copy
import json
import logging
from typing import Any, Callable, Dict, List, Optional

# A library must never configure the root logger (that's the application's job,
# e.g. via stonedog-logs). Attach a NullHandler so our own records don't emit
# "No handlers could be found" warnings when the app hasn't configured logging.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__all__ = [
    "StonedogRemembers",
    "RozRemembers",
    "Store",
    "get_nested_value",
    "set_nested_value",
]


# ---------------------------------------------------------------------------
# Shared dot-path engine (used by both the async store and the sync Store)
# ---------------------------------------------------------------------------


def get_nested_value(path: str, data: Dict[str, Any]) -> Any:
    """Return the value at a dot-notation ``path`` (e.g. ``"job.bins"``).

    Supports dict keys and numeric list indices. Returns ``None`` when any
    segment of the path is missing or not traversable.
    """
    parts = path.split(".")
    temp_data: Any = data
    for part in parts:
        if isinstance(temp_data, dict):
            temp_data = temp_data.get(part)
        elif isinstance(temp_data, list) and part.isdigit():
            idx = int(part)
            if 0 <= idx < len(temp_data):
                temp_data = temp_data[idx]
            else:
                return None
        else:
            return None
        if temp_data is None:
            break
    return temp_data


def set_nested_value(path: str, value: Any, data: Dict[str, Any]) -> bool:
    """Set ``value`` at a dot-notation ``path`` within ``data`` in place.

    Intermediate dicts are auto-created. Returns ``True`` on success, ``False``
    if a segment cannot be traversed/assigned (e.g. list index out of range).
    """
    parts = path.split(".")
    temp_data: Any = data

    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            if isinstance(temp_data, dict):
                temp_data[part] = value
            elif isinstance(temp_data, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(temp_data):
                    temp_data[idx] = value
                else:
                    logger.warning(
                        f"Cannot set list index out of range for path: {path}"
                    )
                    return False
            else:
                logger.warning(
                    f"Cannot set value on non-container at path segment: {path}"
                )
                return False
        else:
            if isinstance(temp_data, dict):
                if part not in temp_data or not isinstance(
                    temp_data[part], (dict, list)
                ):
                    temp_data[part] = {}
                temp_data = temp_data[part]
            elif isinstance(temp_data, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(temp_data):
                    if not isinstance(temp_data[idx], (dict, list)):
                        temp_data[idx] = {}
                    temp_data = temp_data[idx]
                else:
                    logger.warning(
                        f"Cannot traverse list index out of range for path: {path}"
                    )
                    return False
            else:
                logger.warning(
                    f"Invalid path traversal: segment '{part}' is not a container in path '{path}'"
                )
                return False
    return True


class StonedogRemembers:
    """
    A generic, message-driven state management library inspired by Redux.
    It manages a central state, processes incoming actions/commands, and
    emits events whenever the state changes. External logic (sagas/listeners)
    can subscribe to these events and dispatch new actions.
    """

    ACTION_TYPE_SET_STATE = "SET_STATE"
    EVENT_TYPE_STATE_CHANGED = "STATE_CHANGED"

    def __init__(self, initial_state_file_path: str):
        self._initial_state_file_path = initial_state_file_path
        self._state: Dict[str, Any] = {}

        self._action_queue = asyncio.Queue()
        self._event_queue = asyncio.Queue()

        self._processing_task: Optional[asyncio.Task] = None

    async def load_initial_state(self):
        try:
            with open(self._initial_state_file_path, "r") as f:
                self._state = json.load(f)
            logger.info(
                f"Initial state loaded successfully from: {self._initial_state_file_path}"
            )
        except FileNotFoundError:
            logger.warning(
                f"Initial state file not found: {self._initial_state_file_path}. Starting with an empty state."
            )
            self._state = {}
        except json.JSONDecodeError:
            logger.error(
                f"Invalid JSON in initial state file: {self._initial_state_file_path}. Starting with an empty state."
            )
            self._state = {}
        except Exception as e:
            logger.error(f"Unexpected error loading initial state: {e}", exc_info=True)
            self._state = {}

    def start_processing(self):
        if self._processing_task is None or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._action_processor())
            logger.info("StonedogRemembers action processor started.")
        else:
            logger.warning("StonedogRemembers action processor already running.")

    async def stop_processing(self):
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
            logger.info("StonedogRemembers action processor stopped.")

    async def _action_processor(self):
        logger.info("Action processor is running, waiting for actions...")
        while True:
            try:
                action = await self._action_queue.get()
                logger.debug(f"Processing action: {action}")

                action_type = action.get("type")

                if action_type == self.ACTION_TYPE_SET_STATE:
                    path = action.get("path")
                    value = action.get("value")

                    if path is None:
                        logger.warning(f"SET_STATE action missing 'path': {action}")
                        self._action_queue.task_done()
                        continue

                    old_value_at_path = self._get_nested_value(
                        path, current_data=self._state
                    )

                    temp_state = copy.deepcopy(self._state)

                    applied_successfully = self._set_nested_value(
                        path, value, current_data=temp_state
                    )

                    if applied_successfully:
                        self._state = temp_state

                        await self._event_queue.put(
                            {
                                "type": self.EVENT_TYPE_STATE_CHANGED,
                                "path": path,
                                "old_value": old_value_at_path,
                                "new_value": self._get_nested_value(
                                    path, current_data=self._state
                                ),
                                "action_source": action,
                            }
                        )
                        logger.debug(
                            f"State updated for path '{path}'. Emitted {self.EVENT_TYPE_STATE_CHANGED} event."
                        )
                    else:
                        logger.warning(
                            f"Failed to apply state change for path '{path}' with value '{value}'."
                        )
                else:
                    logger.warning(
                        f"Unknown action type received: {action_type}. Action: {action}"
                    )

                self._action_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Action processor task cancelled.")
                break
            except Exception as e:
                logger.error(
                    f"Unhandled exception in action processor: {e}", exc_info=True
                )
                self._action_queue.task_done()

    def _get_nested_value(self, path: str, current_data: Dict[str, Any]) -> Any:
        return get_nested_value(path, current_data)

    def _set_nested_value(
        self, path: str, value: Any, current_data: Dict[str, Any]
    ) -> bool:
        return set_nested_value(path, value, current_data)

    async def dispatch(self, action: Dict[str, Any]):
        await self._action_queue.put(action)
        logger.debug(f"Action dispatched: {action['type']}")

    def subscribe_events(self) -> asyncio.Queue:
        return self._event_queue

    def get_current_state(self) -> Dict[str, Any]:
        return copy.deepcopy(self._state)


# Type alias for synchronous subscriber callbacks.
Subscriber = Callable[[Dict[str, Any]], None]


class Store:
    """A synchronous, observable state container with dot-path access.

    Designed for synchronous code that wants centralized, predictable runtime
    state without an asyncio event loop. Mirrors :class:`StonedogRemembers`'
    dot-path semantics and ``STATE_CHANGED`` event shape.

    Example::

        store = Store({"job": {"bins": 10, "sorted": 0}})
        store.set("job.sorted", 1)
        store.get("job.sorted")            # -> 1
        store.subscribe(lambda e: print(e["path"], e["new_value"]))
    """

    EVENT_TYPE_STATE_CHANGED = "STATE_CHANGED"

    def __init__(
        self,
        initial_state: Optional[Dict[str, Any]] = None,
        *,
        state_file: Optional[str] = None,
    ) -> None:
        self._state: Dict[str, Any] = {}
        self._subscribers: List[Subscriber] = []
        self._state_file = state_file
        if state_file:
            self.load(state_file)
        elif initial_state is not None:
            self._state = copy.deepcopy(initial_state)

    def set(self, path: str, value: Any) -> bool:
        """Set ``value`` at ``path``; notify subscribers on success.

        Returns ``True`` if the change was applied. The state is mutated on a
        deep copy first so a failed traversal never leaves it half-updated.
        """
        old_value = get_nested_value(path, self._state)
        candidate = copy.deepcopy(self._state)
        if not set_nested_value(path, value, candidate):
            logger.warning(f"Store.set failed for path '{path}'.")
            return False
        self._state = candidate
        self._notify(
            {
                "type": self.EVENT_TYPE_STATE_CHANGED,
                "path": path,
                "old_value": old_value,
                "new_value": get_nested_value(path, self._state),
            }
        )
        return True

    def get(self, path: str, default: Any = None) -> Any:
        """Return the value at ``path`` or ``default`` if it is missing."""
        value = get_nested_value(path, self._state)
        return default if value is None else value

    def get_state(self) -> Dict[str, Any]:
        """Return a deep copy of the entire state."""
        return copy.deepcopy(self._state)

    def subscribe(self, callback: Subscriber) -> Callable[[], None]:
        """Register ``callback`` for ``STATE_CHANGED`` events.

        Returns an unsubscribe function.
        """
        self._subscribers.append(callback)

        def _unsubscribe() -> None:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

        return _unsubscribe

    def _notify(self, event: Dict[str, Any]) -> None:
        for callback in list(self._subscribers):
            try:
                callback(event)
            except Exception as exc:  # a bad subscriber must not break the store
                logger.error(f"Store subscriber raised: {exc}", exc_info=True)

    def load(self, path: str) -> None:
        """Replace the state with JSON loaded from ``path`` (empty on failure)."""
        try:
            with open(path, "r") as f:
                self._state = json.load(f)
            logger.info(f"Store loaded state from: {path}")
        except FileNotFoundError:
            logger.warning(f"Store state file not found: {path}. Starting empty.")
            self._state = {}
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in store state file: {path}. Starting empty.")
            self._state = {}

    def save(self, path: Optional[str] = None) -> None:
        """Persist the current state as JSON to ``path`` (or the ``state_file``)."""
        target = path or self._state_file
        if not target:
            raise ValueError(
                "Store.save requires a path or a state_file set at construction."
            )
        with open(target, "w") as f:
            json.dump(self._state, f, indent=2, default=str)
        logger.debug(f"Store saved state to: {target}")


# ---------------------------------------------------------------------------
# Backwards compatibility
# ---------------------------------------------------------------------------

#: Deprecated alias for :class:`StonedogRemembers`.
#:
#: The class was named ``RozRemembers`` while this library was published as
#: ``roz-remembers``. It is kept as a plain alias -- not a subclass -- so
#: ``isinstance`` checks and ``ACTION_TYPE_*``/``EVENT_TYPE_*`` class attributes
#: behave identically either way. Prefer ``StonedogRemembers`` in new code.
RozRemembers = StonedogRemembers
