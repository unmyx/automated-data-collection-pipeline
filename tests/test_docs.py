"""Checks that keep the portfolio documentation honest and internally consistent.

The documentation is a deliverable in its own right (PLAN section 18), so it gets
the same treatment as code: relative links must resolve, anchors must exist,
no machine-specific path may leak in, the captured samples must be present, and
every command the CLI actually exposes must be documented in the README.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import typer
from pydantic import AliasChoices

from adcp.cli import app
from adcp.config import Settings

pytestmark = [pytest.mark.unit, pytest.mark.smoke]

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Every Markdown file that is part of the portfolio surface.
DOC_FILES = [*sorted((REPO_ROOT / "docs").glob("*.md")), REPO_ROOT / "README.md"]

SAMPLE_FILES = (
    "collection-summary.txt",
    "idempotent-rerun.txt",
    "observations.sql.txt",
    "partial-failure.txt",
    "rejection-result.txt",
    "retry-log.jsonl",
    "watermarks.sql.txt",
)

_LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
_HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*$", re.MULTILINE)

#: Mojibake that appears when UTF-8 text is round-tripped through a legacy
#: single-byte encoding - a real defect that shipped in the README once. Written
#: with escapes so this detector does not match its own source line.
_MOJIBAKE = re.compile("\u00c2|\u00c3|\u00e2\u20ac|\u00ef\u00bb\u00bf")

#: Environment names documented but deliberately not ``Settings`` fields: they are
#: read by Compose, by the test suite, or by CI directly.
_NON_SETTING_ENV = frozenset(
    {
        "ADCP_LIVE_API_TESTS",
        "ADCP_TEST_ALLOW_ANY_DATABASE",
        "ADCP_TEST_DATABASE_URL",
        "ADCP_TEST_USE_TESTCONTAINERS",
        "ADCP_UPPER_SNAKE",  # placeholder in the PLAN naming-convention table
    },
)

#: Windows drive paths and POSIX home directories that would leak a developer's
#: machine into a public repository.
_LOCAL_PATH = re.compile(
    r"[A-Za-z]:\\(?:Users|Projects)\\"
    r"|[A-Za-z]:/(?:Users|Projects)/"
    r"|/Users/[A-Za-z0-9._-]+/"
    r"|/home/[a-z0-9._-]+/",
)


def _slug(heading: str) -> str:
    """Approximate GitHub's heading anchor, enough to validate our own links."""
    text = heading.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return text.replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    """Every anchor the file exposes, including GitHub's duplicate suffixes."""
    seen: dict[str, int] = {}
    anchors: set[str] = set()
    for heading in _HEADING.findall(path.read_text(encoding="utf-8")):
        slug = _slug(heading)
        anchors.add(slug)
        seen[slug] = seen.get(slug, 0) + 1
        if seen[slug] > 1:
            anchors.add(f"{slug}-{seen[slug] - 1}")
    return anchors


def _relative_links(path: Path) -> list[str]:
    links: list[str] = []
    for raw in _LINK.findall(path.read_text(encoding="utf-8")):
        target = raw.split()[0].strip().strip("<>")
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        links.append(target)
    return links


@pytest.mark.parametrize("doc", DOC_FILES, ids=lambda path: path.name)
def test_relative_links_resolve(doc: Path) -> None:
    missing: list[str] = []
    broken_anchors: list[str] = []
    for target in _relative_links(doc):
        file_part, _, anchor = target.partition("#")
        destination = doc if not file_part else (doc.parent / file_part).resolve()
        if file_part and not destination.exists():
            missing.append(target)
            continue
        if anchor and destination.suffix == ".md" and anchor not in _anchors(destination):
            broken_anchors.append(target)

    assert not missing, f"{doc.name} links to paths that do not exist: {missing}"
    assert not broken_anchors, f"{doc.name} links to anchors that do not exist: {broken_anchors}"


@pytest.mark.parametrize("doc", DOC_FILES, ids=lambda path: path.name)
def test_docs_contain_no_machine_specific_paths(doc: Path) -> None:
    match = _LOCAL_PATH.search(doc.read_text(encoding="utf-8"))
    assert match is None, (
        f"{doc.name} contains a machine-specific path: {match and match.group()!r}"
    )


@pytest.mark.parametrize("doc", DOC_FILES, ids=lambda path: path.name)
def test_docs_contain_no_encoding_damage(doc: Path) -> None:
    text = doc.read_text(encoding="utf-8")
    match = _MOJIBAKE.search(text)
    assert match is None, f"{doc.name} contains mojibake: {match and match.group()!r}"
    assert not text.startswith("\ufeff"), f"{doc.name} starts with a byte-order mark"


def test_documented_environment_variables_actually_exist() -> None:
    """Every ADCP_* name in the docs must be a real setting or a known extra."""
    prefix = str(Settings.model_config.get("env_prefix", "ADCP_"))
    known = set(_NON_SETTING_ENV)
    for name, field in Settings.model_fields.items():
        alias = field.validation_alias
        if isinstance(alias, AliasChoices):
            known.update(str(choice) for choice in alias.choices)
        elif isinstance(alias, str):
            known.add(alias)
        else:
            known.add(f"{prefix}{name.upper()}")

    token = re.compile(r"\bADCP_[A-Z0-9_]+\b")
    documents = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").glob("*.md"))]
    unknown: set[str] = set()
    for document in documents:
        unknown.update(set(token.findall(document.read_text(encoding="utf-8"))) - known)

    assert not unknown, f"documented but non-existent settings: {sorted(unknown)}"


@pytest.mark.parametrize("name", SAMPLE_FILES)
def test_captured_samples_exist_and_are_not_empty(name: str) -> None:
    sample = REPO_ROOT / "docs" / "samples" / name
    assert sample.is_file(), f"docs/samples/{name} is missing"
    assert sample.read_text(encoding="utf-8").strip(), f"docs/samples/{name} is empty"


def _command_names(typer_app: typer.Typer) -> set[str]:
    names: set[str] = set()
    for command in typer_app.registered_commands:
        names.add(command.name or (command.callback.__name__ if command.callback else ""))
    for group in typer_app.registered_groups:
        names.add(group.name or "")
        if group.typer_instance is not None:
            names.update(_command_names(group.typer_instance))
    return {name for name in names if name}


def test_readme_documents_every_cli_command() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    undocumented = sorted(name for name in _command_names(app) if name not in readme)
    assert not undocumented, f"CLI commands are missing from the README: {undocumented}"


def test_docs_have_no_unfinished_milestone_markers() -> None:
    """The shipped docs must not read as a work in progress."""
    offenders: list[str] = []
    for doc in DOC_FILES:
        for number, line in enumerate(doc.read_text(encoding="utf-8").splitlines(), start=1):
            stripped = line.strip().lower()
            if stripped.startswith("> **status: m") or "not yet implemented" in stripped:
                offenders.append(f"{doc.name}:{number}: {line.strip()}")
    assert not offenders, offenders
