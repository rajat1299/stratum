import json
import tomllib
from pathlib import Path

import stratum_sdk

SDK_ROOT = Path(__file__).resolve().parents[2]
PYTHON_ROOT = Path(__file__).resolve().parents[1]


def _matrix_entry(name: str) -> dict[str, object]:
    matrix = json.loads((SDK_ROOT / "version-matrix.json").read_text())
    for package in matrix["packages"]:
        if package["name"] == name:
            return package
    raise AssertionError(f"missing matrix entry: {name}")


def test_python_package_boundary_matches_beta_matrix() -> None:
    matrix = _matrix_entry("stratum-sdk")
    pyproject = tomllib.loads((PYTHON_ROOT / "pyproject.toml").read_text())

    project = pyproject["project"]
    assert project["name"] == "stratum-sdk"
    assert project["version"] == matrix["version"] == "0.0.0b0"
    assert stratum_sdk.__version__ == matrix["version"]
    assert project["readme"] == "README.md"
    assert project["requires-python"] == ">=3.11"
    assert project["license"]["text"] == "MIT"
    assert "httpx>=0.27,<1" in project["dependencies"]

    wheel = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]
    sdist = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"]
    assert wheel["packages"] == ["src/stratum_sdk"]
    assert "src/stratum_sdk" in sdist["include"]
    assert "tests" in sdist["include"]
    assert "README.md" in sdist["include"]
    assert "LICENSE" in sdist["include"]
    assert (PYTHON_ROOT / "src" / "stratum_sdk" / "py.typed").is_file()
