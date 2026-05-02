"""Route construction aligned with sdk/typescript/src/paths.ts."""

from __future__ import annotations

from urllib.parse import quote


def _strip_leading_slash(value: str) -> str:
    return value.lstrip("/")


def normalize_route_path(path: str) -> str:
    parts: list[str] = []
    for part in _strip_leading_slash(path).split("/"):
        if part == "" or part == ".":
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def encode_path_segments(path: str) -> str:
    return "/".join(quote(segment, safe="") for segment in path.split("/") if segment)


def fs_route(path: str) -> str:
    """Filesystem route under ``fs/<path>`` with normalized dots and encoded segments."""
    return _path_route("fs", path)


def tree_route(path: str) -> str:
    """Tree route under ``tree/<path>`` with normalized dots and encoded segments."""
    return _path_route("tree", path)


def _path_route(prefix: str, path: str) -> str:
    route_path = normalize_route_path(path)
    if route_path == "":
        return prefix
    return f"{prefix}/{encode_path_segments(route_path)}"


def _encode_ref_name_segment(segment: str) -> str:
    if segment == ".":
        return "%252E"
    if segment == "..":
        return "%252E%252E"
    return quote(segment, safe="")


def _encode_ref_name_segments(name: str) -> str:
    return "/".join(_encode_ref_name_segment(segment) for segment in name.split("/"))


def ref_route(name: str) -> str:
    """VCS ref update route; preserves ref path shape with dot-only escaping."""
    return f"vcs/refs/{_encode_ref_name_segments(name)}"


def encode_route_segment(value: str) -> str:
    return quote(value, safe="")
