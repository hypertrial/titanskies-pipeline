"""Pinned geography source download, cache, and offline acquisition."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import requests

ARTIFACT_MANIFEST_VERSION = "1"
ARTIFACT_MANIFEST_NAME = "tempo_geography_artifacts.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_source_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    required = {"manifest_version", "geometry_version", "sources"}
    if required - set(manifest):
        raise ValueError("Geography source manifest is incomplete")
    for source in manifest["sources"]:
        missing = {
            "id",
            "version",
            "url",
            "filename",
            "sha256",
            "attribution",
            "license",
        } - set(source)
        if missing:
            raise ValueError(
                f"Geography source {source.get('id', '<unknown>')} is incomplete"
            )
        if len(source["sha256"]) != 64:
            raise ValueError(f"Geography source {source['id']} has an invalid SHA-256")
    return manifest


def acquire_source(
    source: Mapping[str, str],
    *,
    source_cache: Path,
    offline: bool,
) -> Path:
    source_cache.mkdir(parents=True, exist_ok=True)
    destination = source_cache / source["filename"]
    if destination.exists() and sha256_file(destination) == source["sha256"]:
        return destination
    if destination.exists() and offline:
        raise ValueError(f"Cached geography source failed checksum: {source['id']}")
    if offline:
        raise FileNotFoundError(
            f"Offline geography source is not cached: {source['id']} ({destination})"
        )

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=source_cache
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with requests.get(source["url"], stream=True, timeout=(30, 300)) as response:
            response.raise_for_status()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        actual = sha256_file(temporary)
        if actual != source["sha256"]:
            raise ValueError(
                f"Downloaded geography source failed checksum: {source['id']} "
                f"(expected {source['sha256']}, got {actual})"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def acquire_all_sources(
    manifest: Mapping[str, Any],
    *,
    source_cache: Path,
    offline: bool,
) -> dict[str, Path]:
    return {
        source["id"]: acquire_source(source, source_cache=source_cache, offline=offline)
        for source in manifest["sources"]
    }


def safe_extract(archive: Path, destination: Path) -> Path:
    archive_checksum = sha256_file(archive)
    destination = destination / archive_checksum
    marker = destination / ".complete"
    if marker.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{archive_checksum}.", dir=destination.parent)
    )
    try:
        with zipfile.ZipFile(archive) as zipped:
            for member in zipped.infolist():
                resolved = (temporary / member.filename).resolve()
                if not resolved.is_relative_to(temporary.resolve()):
                    raise ValueError(
                        f"Unsafe path in geography archive: {member.filename}"
                    )
            zipped.extractall(temporary)
        (temporary / ".complete").touch()
        try:
            os.replace(temporary, destination)
        except FileExistsError:
            pass
    finally:
        if temporary.exists():
            import shutil

            shutil.rmtree(temporary)
    return destination


def find_file(root: Path, *suffixes: str) -> Path:
    matches = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and any(path.name.casefold().endswith(suffix.casefold()) for suffix in suffixes)
    )
    if not matches:
        raise FileNotFoundError(f"None of {suffixes!r} found below {root}")
    return matches[0]


__all__ = [
    "ARTIFACT_MANIFEST_NAME",
    "ARTIFACT_MANIFEST_VERSION",
    "acquire_all_sources",
    "acquire_source",
    "find_file",
    "load_source_manifest",
    "safe_extract",
    "sha256_file",
]
