from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9']*")
_WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]")
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_HEADING_RE = re.compile(r"^#\s+(.+)$", re.MULTILINE)
_SKIP_SLUGS = {"index", "log", "review"}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True)
class WikiSearchDocument:
    slug: str
    title: str
    path: str
    text: str


@dataclass(frozen=True)
class WikiSearchResult:
    slug: str
    title: str
    path: str
    score: float
    matched_terms: tuple[str, ...]
    snippet: str


@dataclass
class WikiSearchIndex:
    documents: list[WikiSearchDocument]
    document_frequency: dict[str, int]
    vectors: dict[str, dict[str, float]]
    norms: dict[str, float]
    document_tokens: dict[str, set[str]] = field(default_factory=dict)


def _tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for token in _TOKEN_RE.findall(text.lower()):
        token = token.strip("'")
        if len(token) < 2 or token in _STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _plain_text(markdown: str) -> str:
    text = _WIKILINK_RE.sub(lambda match: match.group(1).replace("-", " "), markdown)
    text = _MARKDOWN_LINK_RE.sub(lambda match: match.group(1), text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"^[#>*\-\s]+", "", text, flags=re.MULTILINE)
    return re.sub(r"\s+", " ", text).strip()


def _title_from_markdown(slug: str, markdown: str, api: ModuleType) -> str:
    heading = _HEADING_RE.search(markdown)
    if heading:
        return heading.group(1).strip()
    return api.bw.page_title(slug)


def _weighted_text(title: str, markdown: str) -> str:
    plain = _plain_text(markdown)
    link_text = " ".join(match.group(1).replace("-", " ") for match in _WIKILINK_RE.finditer(markdown))
    return " ".join([title, title, title, link_text, plain])


def _vectorize(tokens: list[str], document_count: int, document_frequency: dict[str, int]) -> dict[str, float]:
    if not tokens:
        return {}
    term_counts: dict[str, int] = {}
    for token in tokens:
        term_counts[token] = term_counts.get(token, 0) + 1
    max_count = max(term_counts.values())
    vector: dict[str, float] = {}
    for token, count in term_counts.items():
        tf = 0.5 + 0.5 * (count / max_count)
        idf = math.log((1 + document_count) / (1 + document_frequency.get(token, 0))) + 1
        vector[token] = tf * idf
    return vector


def _norm(vector: dict[str, float]) -> float:
    return math.sqrt(sum(weight * weight for weight in vector.values()))


def _snippet(text: str, query_terms: set[str], *, max_chars: int = 180) -> str:
    if not text:
        return ""
    lowered = text.lower()
    first_match = min((lowered.find(term) for term in query_terms if lowered.find(term) >= 0), default=0)
    start = max(0, first_match - 55)
    end = min(len(text), start + max_chars)
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet += "..."
    return snippet


def build_wiki_search_index(api: ModuleType, *, wiki_root: Path | None = None) -> WikiSearchIndex:
    root = wiki_root or api.WIKI_ROOT
    documents: list[WikiSearchDocument] = []
    token_lists: dict[str, list[str]] = {}
    document_frequency: dict[str, int] = {}

    for page_path in sorted(root.glob("*.md")):
        slug = page_path.stem
        if slug in _SKIP_SLUGS:
            continue
        markdown = page_path.read_text(encoding="utf-8")
        title = _title_from_markdown(slug, markdown, api)
        text = _weighted_text(title, markdown)
        tokens = _tokenize(text)
        document = WikiSearchDocument(
            slug=slug,
            title=title,
            path=api.normalize_repo_path(page_path),
            text=_plain_text(markdown),
        )
        documents.append(document)
        token_lists[slug] = tokens
        for token in set(tokens):
            document_frequency[token] = document_frequency.get(token, 0) + 1

    vectors = {
        document.slug: _vectorize(token_lists[document.slug], len(documents), document_frequency)
        for document in documents
    }
    norms = {slug: _norm(vector) for slug, vector in vectors.items()}
    document_tokens = {slug: set(tokens) for slug, tokens in token_lists.items()}
    return WikiSearchIndex(documents, document_frequency, vectors, norms, document_tokens)


def search_wiki(
    api: ModuleType,
    query: str,
    *,
    top_k: int = 5,
    index: WikiSearchIndex | None = None,
    wiki_root: Path | None = None,
) -> list[WikiSearchResult]:
    normalized_query = query.strip()
    if not normalized_query:
        return []

    search_index = index or build_wiki_search_index(api, wiki_root=wiki_root)
    query_tokens = _tokenize(normalized_query)
    if not query_tokens:
        return []

    query_vector = _vectorize(query_tokens, len(search_index.documents), search_index.document_frequency)
    query_norm = _norm(query_vector)
    if query_norm == 0:
        return []

    query_terms = set(query_tokens)
    documents_by_slug = {document.slug: document for document in search_index.documents}
    scored: list[WikiSearchResult] = []
    for slug, vector in search_index.vectors.items():
        document_norm = search_index.norms.get(slug, 0)
        if document_norm == 0:
            continue
        dot = sum(query_weight * vector.get(token, 0.0) for token, query_weight in query_vector.items())
        if dot <= 0:
            continue
        document = documents_by_slug[slug]
        score = dot / (query_norm * document_norm)
        matched_terms = tuple(sorted(query_terms & search_index.document_tokens.get(slug, set())))
        scored.append(
            WikiSearchResult(
                slug=document.slug,
                title=document.title,
                path=document.path,
                score=score,
                matched_terms=matched_terms,
                snippet=_snippet(document.text, query_terms),
            )
        )

    scored.sort(key=lambda result: (-result.score, result.title.lower(), result.slug))
    return scored[:top_k]


def save_wiki_search_index(index: WikiSearchIndex, path: Path) -> None:
    payload = {
        "version": 1,
        "documents": [document.__dict__ for document in index.documents],
        "document_frequency": index.document_frequency,
        "vectors": index.vectors,
        "norms": index.norms,
        "document_tokens": {slug: sorted(tokens) for slug, tokens in index.document_tokens.items()},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def load_wiki_search_index(path: Path) -> WikiSearchIndex:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise ValueError(f"unsupported wiki search index version: {payload.get('version')}")
    documents = [WikiSearchDocument(**document) for document in payload["documents"]]
    document_tokens = {slug: set(tokens) for slug, tokens in payload.get("document_tokens", {}).items()}
    return WikiSearchIndex(
        documents=documents,
        document_frequency=dict(payload["document_frequency"]),
        vectors={slug: {term: float(weight) for term, weight in vector.items()} for slug, vector in payload["vectors"].items()},
        norms={slug: float(norm) for slug, norm in payload["norms"].items()},
        document_tokens=document_tokens,
    )


def build_search_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search local wiki pages with a lightweight TF-IDF index.")
    parser.add_argument("query", nargs="*", help="Search query.")
    parser.add_argument("--top-k", type=int, default=5, help="Number of ranked results to return.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable output.")
    parser.add_argument("--write-index", type=Path, default=None, help="Write the generated index to this JSON file.")
    parser.add_argument("--read-index", type=Path, default=None, help="Search an existing JSON index instead of rebuilding.")
    return parser


def search_main(api: ModuleType, argv: list[str] | None = None) -> int:
    args = build_search_parser().parse_args(argv)
    query = " ".join(args.query).strip()
    if not query:
        raise SystemExit("query is required")
    index = load_wiki_search_index(args.read_index) if args.read_index else build_wiki_search_index(api)
    if args.write_index:
        save_wiki_search_index(index, args.write_index)
    results = search_wiki(api, query, top_k=args.top_k, index=index)
    if args.json:
        print(json.dumps([result.__dict__ for result in results], ensure_ascii=False, separators=(",", ":")))
        return 0
    for index_number, result in enumerate(results, start=1):
        terms = ", ".join(result.matched_terms)
        print(f"{index_number}. {result.title} ({result.slug}) score={result.score:.3f}")
        if terms:
            print(f"   terms: {terms}")
        if result.snippet:
            print(f"   {result.snippet}")
    return 0


if __name__ == "__main__":  # pragma: no cover - direct script execution path
    try:
        from scripts import vault_pipeline as vp
    except ModuleNotFoundError:
        import vault_pipeline as vp

    raise SystemExit(search_main(vp, sys.argv[1:]))
