from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class SourceArtifact:
    path: Path
    repo_path: str
    source_kind: str
    source_id: str | None
    title: str
    created_at: str | None
    body: str
    external_url: str | None
    provenance_pointer: str | None
    frontmatter: dict[str, object]


@dataclass
class SourceEvidence:
    label: str
    path: str
    status: str
    cleaned_text: str
    fetched_summary: str | None
    detected_url: str | None
    source_kind: str = "capture"
    source_id: str | None = None
    title: str | None = None
    external_url: str | None = None
    provenance_pointer: str | None = None
    tags: set[str] = field(default_factory=set)
    excluded_from_body: bool = False
    created_at: str | None = None
    raw_content: str | None = None


@dataclass(frozen=True)
class SourceCitation:
    label: str
    path: str
    status: str
    source_kind: str
    source_id: str | None = None
    external_url: str | None = None


@dataclass(frozen=True)
class FetchResultLike:
    summary: str | None
    status: str


def _string_frontmatter_value(frontmatter: dict[str, object], key: str) -> str | None:
    value = frontmatter.get(key)
    return value if isinstance(value, str) and value else None


def read_source_artifact(
    path: Path,
    *,
    split_frontmatter: Callable[[str], tuple[dict[str, object], str, bool]],
    normalize_repo_path: Callable[[str | Path], str],
) -> SourceArtifact:
    text = path.read_text(encoding="utf-8")
    frontmatter, body, has_frontmatter = split_frontmatter(text)
    if not has_frontmatter:
        raise ValueError("source artifact missing frontmatter")
    if not body.strip():
        raise ValueError("source artifact body is empty")

    title = _string_frontmatter_value(frontmatter, "title")
    if title is None:
        title = path.stem

    return SourceArtifact(
        path=path,
        repo_path=normalize_repo_path(path),
        source_kind=_string_frontmatter_value(frontmatter, "source_kind") or "capture",
        source_id=_string_frontmatter_value(frontmatter, "source_id"),
        title=title,
        created_at=_string_frontmatter_value(frontmatter, "created_at"),
        body=body,
        external_url=_string_frontmatter_value(frontmatter, "external_url"),
        provenance_pointer=_string_frontmatter_value(frontmatter, "provenance_pointer"),
        frontmatter=frontmatter,
    )


def content_body(artifact: SourceArtifact) -> str:
    synthetic_heading = f"# {artifact.title}\n\n"
    if artifact.body.startswith(synthetic_heading):
        return artifact.body[len(synthetic_heading) :]
    return artifact.body


def source_artifact_to_evidence(
    artifact: SourceArtifact,
    *,
    citation_path: str | None = None,
    fetcher: Callable[[str], FetchResultLike] | None = None,
    extract_first_url: Callable[[str], str | None],
    is_google_search_url: Callable[[str], bool],
    clean_source_text: Callable[[str, str], str],
    detect_source_tags: Callable[[str, str, str | None, str | None], set[str]],
    should_exclude_from_body: Callable[[set[str]], bool],
) -> SourceEvidence:
    body = content_body(artifact)
    url = artifact.external_url or extract_first_url(body)
    fetched_summary = None
    status = "local_only"
    if url:
        if is_google_search_url(url):
            status = "fetch_skipped"
        elif fetcher is not None:
            fetch_result = fetcher(url)
            fetched_summary = fetch_result.summary
            status = fetch_result.status

    cleaned_text = clean_source_text(body, artifact.title)
    tags = detect_source_tags(artifact.title, cleaned_text, url, fetched_summary)
    return SourceEvidence(
        label=artifact.external_url or url or artifact.title,
        path=citation_path or f"../{artifact.repo_path}",
        status=status,
        cleaned_text=cleaned_text,
        fetched_summary=fetched_summary,
        detected_url=url,
        source_kind=artifact.source_kind,
        source_id=artifact.source_id,
        created_at=artifact.created_at,
        title=artifact.title,
        external_url=artifact.external_url or url,
        provenance_pointer=artifact.provenance_pointer,
        tags=tags,
        excluded_from_body=should_exclude_from_body(tags),
        raw_content=body,
    )


def evidence_to_citation(evidence: SourceEvidence) -> SourceCitation:
    return SourceCitation(
        label=evidence.external_url or evidence.detected_url or evidence.label,
        path=evidence.path,
        status=evidence.status,
        source_kind=evidence.source_kind,
        source_id=evidence.source_id,
        external_url=evidence.external_url,
    )
