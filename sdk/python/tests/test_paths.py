from stratum_sdk.paths import fs_route, ref_route, tree_route


def test_fs_route_normalizes_dot_segments() -> None:
    assert fs_route("../secret.txt") == "fs/secret.txt"


def test_tree_route_normalizes_path() -> None:
    assert tree_route("/a/./b/../c") == "tree/a/c"


def test_ref_route_double_encodes_dot_segments() -> None:
    assert ref_route("agent/a/../b") == "vcs/refs/agent/a/%252E%252E/b"


def test_ref_route_leading_slash_preserved_in_segments() -> None:
    assert ref_route("/leading") == "vcs/refs//leading"
