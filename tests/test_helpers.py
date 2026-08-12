"""Edge-case tests for the shared dot-path engine and Store persistence.

Covers list-index traversal, out-of-range / non-container failures, and the
Store's load-failure fallbacks that the happy-path tests don't reach.
"""

from stonedog_remembers import Store, get_nested_value, set_nested_value

# --- get_nested_value --------------------------------------------------------


def test_get_list_index():
    data = {"items": [{"name": "a"}, {"name": "b"}]}
    assert get_nested_value("items.1.name", data) == "b"


def test_get_list_index_out_of_range_returns_none():
    assert get_nested_value("items.5", {"items": [1, 2]}) is None


def test_get_through_non_container_returns_none():
    # "a" is an int, so "a.b" cannot be traversed.
    assert get_nested_value("a.b", {"a": 1}) is None


def test_get_missing_dict_key_returns_none():
    assert get_nested_value("nope", {"a": 1}) is None


# --- set_nested_value --------------------------------------------------------


def test_set_list_index_in_place():
    data = {"items": [1, 2, 3]}
    assert set_nested_value("items.1", 99, data) is True
    assert data["items"] == [1, 99, 3]


def test_set_traverses_into_list_index():
    data = {"items": [{"n": 1}, {"n": 2}]}
    assert set_nested_value("items.0.n", 42, data) is True
    assert data["items"][0]["n"] == 42


def test_set_final_list_index_out_of_range_fails():
    data = {"items": [1, 2]}
    assert set_nested_value("items.9", "x", data) is False
    assert data == {"items": [1, 2]}


def test_set_traverse_list_index_out_of_range_fails():
    data = {"items": [1, 2]}
    assert set_nested_value("items.9.deep", "x", data) is False
    assert data == {"items": [1, 2]}


def test_set_final_on_non_container_fails():
    # Trying to set "a.b" where "a" traverses into an int at the last step.
    data = {"a": [1]}  # list, but the last segment is not a digit
    assert set_nested_value("a.b", "x", data) is False


def test_set_traverse_through_non_container_fails():
    # Descend into a list with a non-digit segment -> cannot traverse.
    data = {"a": [1]}
    assert set_nested_value("a.b.c", "x", data) is False
    assert data == {"a": [1]}


def test_set_overwrites_scalar_intermediate_with_dict():
    # An existing scalar in an intermediate position is replaced by a dict.
    data = {"a": 1}
    assert set_nested_value("a.b", 2, data) is True
    assert data == {"a": {"b": 2}}


def test_set_replaces_scalar_at_list_index_during_traversal():
    data = {"items": [7]}  # items[0] is a scalar; traversal turns it into a dict
    assert set_nested_value("items.0.deep", "v", data) is True
    assert data["items"][0] == {"deep": "v"}


# --- Store persistence fallbacks --------------------------------------------


def test_store_load_missing_file_starts_empty(tmp_path):
    store = Store(state_file=str(tmp_path / "missing.json"))
    assert store.get_state() == {}


def test_store_load_invalid_json_starts_empty(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json")
    store = Store(state_file=str(bad))
    assert store.get_state() == {}


def test_store_save_uses_state_file_from_construction(tmp_path):
    path = tmp_path / "s.json"
    path.write_text("{}")
    store = Store(state_file=str(path))
    store.set("a.b", 1)
    store.save()  # no arg -> uses the state_file
    assert '"b": 1' in path.read_text()


def test_store_save_serializes_non_json_types(tmp_path):
    # default=str lets non-JSON values (e.g. a set) serialize without error.
    path = tmp_path / "s.json"
    store = Store({"tags": {1, 2}})
    store.save(str(path))
    assert path.exists()
