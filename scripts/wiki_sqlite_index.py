from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


DB_SCHEMA_VERSION = 1
SYSTEM_SLUGS = {"index", "log", "review", "catalog"}
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9']*")
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]")
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
STOPWORDS = {
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
class IndexedWikiPage:
    slug: str
    title: str
    path: str
    page_type: str
    shape: str
    content_hash: str
    source_count: int
    headings: str
    links: tuple[str, ...]
    sources: str
    body: str
    snippet: str


@dataclass(frozen=True)
class SQLiteWikiSearchResult:
    slug: str
    title: str
    path: str
    score: float
    matched_terms: tuple[str, ...]
    snippet: str
    why: str
    text_score: float
    pagerank_score: float


def default_db_path(api: ModuleType) -> Path:
    return api.ROOT / "state" / "vault.db"


def _tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for token in TOKEN_RE.findall(text.lower()):
        token = token.strip("'")
        if len(token) < 2 or token in STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _plain_text(markdown: str) -> str:
    text = WIKILINK_RE.sub(lambda match: match.group(1).replace("-", " "), markdown)
    text = MARKDOWN_LINK_RE.sub(lambda match: match.group(1), text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"^[#>*\-\s]+", "", text, flags=re.MULTILINE)
    return re.sub(r"\s+", " ", text).strip()


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


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_text(page: object) -> str:
    parts: list[str] = []
    for source in page.sources.values():
        for value in (
            source.label,
            source.title,
            source.external_url,
            source.detected_url,
            source.fetched_summary,
        ):
            if value:
                parts.append(str(value))
    return " ".join(parts)


def _page_from_path(api: ModuleType, path: Path, page_types: dict[str, str]) -> IndexedWikiPage:
    markdown = path.read_text(encoding="utf-8")
    parsed = api.bw.parse_page_file(path, page_type=page_types.get(path.stem))
    page = api.bw.parsed_page_to_page(parsed)
    headings = " ".join(match.group(2).strip() for match in HEADING_RE.finditer(markdown))
    links = tuple(api.bw.ordered_unique(sorted(page.connections)))
    sources = _source_text(page)
    plain = _plain_text(markdown)
    title = page.title or api.bw.page_title(path.stem)
    body = " ".join(
        [
            title,
            title,
            title,
            headings,
            " ".join(link.replace("-", " ") for link in links),
            sources,
            plain,
        ]
    )
    return IndexedWikiPage(
        slug=page.slug,
        title=title,
        path=api.normalize_repo_path(path),
        page_type=page.page_type,
        shape=api.bw.page_shape(page),
        content_hash=_content_hash(markdown),
        source_count=len(page.sources),
        headings=headings,
        links=links,
        sources=sources,
        body=body,
        snippet=plain,
    )


def crawl_wiki_pages(api: ModuleType, *, wiki_root: Path | None = None) -> list[IndexedWikiPage]:
    root = wiki_root or api.WIKI_ROOT
    page_types = api.bw.load_existing_page_types(root / api.bw.CATALOG_PATH)
    pages: list[IndexedWikiPage] = []
    for page_path in sorted(root.glob("*.md")):
        if page_path.stem in SYSTEM_SLUGS:
            continue
        pages.append(_page_from_path(api, page_path, page_types))
    return pages


def _compute_pagerank(pages: list[IndexedWikiPage], *, damping: float = 0.85, iterations: int = 30) -> dict[str, float]:
    slugs = [page.slug for page in pages]
    if not slugs:
        return {}
    known = set(slugs)
    outgoing = {
        page.slug: tuple(slug for slug in page.links if slug in known and slug != page.slug)
        for page in pages
    }
    count = len(slugs)
    scores = {slug: 1.0 / count for slug in slugs}
    base = (1.0 - damping) / count
    for _ in range(iterations):
        next_scores = {slug: base for slug in slugs}
        dangling_score = sum(scores[slug] for slug, targets in outgoing.items() if not targets)
        dangling_share = damping * dangling_score / count
        for slug in slugs:
            next_scores[slug] += dangling_share
        for slug, targets in outgoing.items():
            if not targets:
                continue
            share = damping * scores[slug] / len(targets)
            for target in targets:
                next_scores[target] += share
        scores = next_scores
    return scores


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE pages (
            slug TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            path TEXT NOT NULL,
            page_type TEXT NOT NULL,
            shape TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            source_count INTEGER NOT NULL,
            headings TEXT NOT NULL,
            links_json TEXT NOT NULL,
            sources TEXT NOT NULL,
            body TEXT NOT NULL,
            snippet TEXT NOT NULL
        );
        CREATE TABLE links (
            source_slug TEXT NOT NULL,
            target_slug TEXT NOT NULL,
            PRIMARY KEY (source_slug, target_slug)
        );
        CREATE INDEX links_target_slug_idx ON links(target_slug);
        CREATE TABLE page_rank (
            slug TEXT PRIMARY KEY,
            score REAL NOT NULL
        );
        CREATE VIRTUAL TABLE pages_fts USING fts5(
            slug UNINDEXED,
            title,
            headings,
            links,
            sources,
            body,
            tokenize='unicode61'
        );
        """
    )


def rebuild_sqlite_index(
    api: ModuleType,
    *,
    db_path: Path | None = None,
    wiki_root: Path | None = None,
) -> dict[str, object]:
    target = db_path or default_db_path(api)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    for suffix in ("-wal", "-shm"):
        sidecar = target.with_name(target.name + suffix)
        if sidecar.exists():
            sidecar.unlink()

    pages = crawl_wiki_pages(api, wiki_root=wiki_root)
    pagerank = _compute_pagerank(pages)
    with closing(_connect(target)) as connection:
        _create_schema(connection)
        connection.execute("INSERT INTO metadata(key, value) VALUES (?, ?)", ("schema_version", str(DB_SCHEMA_VERSION)))
        connection.execute("INSERT INTO metadata(key, value) VALUES (?, ?)", ("page_count", str(len(pages))))
        for page in pages:
            links_text = " ".join(link.replace("-", " ") for link in page.links)
            connection.execute(
                """
                INSERT INTO pages(
                    slug, title, path, page_type, shape, content_hash, source_count,
                    headings, links_json, sources, body, snippet
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    page.slug,
                    page.title,
                    page.path,
                    page.page_type,
                    page.shape,
                    page.content_hash,
                    page.source_count,
                    page.headings,
                    json.dumps(list(page.links), ensure_ascii=False, separators=(",", ":")),
                    page.sources,
                    page.body,
                    page.snippet,
                ),
            )
            connection.execute(
                "INSERT INTO pages_fts(slug, title, headings, links, sources, body) VALUES (?, ?, ?, ?, ?, ?)",
                (page.slug, page.title, page.headings, links_text, page.sources, page.body),
            )
            connection.execute(
                "INSERT INTO page_rank(slug, score) VALUES (?, ?)",
                (page.slug, pagerank.get(page.slug, 0.0)),
            )
            for target_slug in page.links:
                if target_slug in pagerank and target_slug != page.slug:
                    connection.execute(
                        "INSERT OR IGNORE INTO links(source_slug, target_slug) VALUES (?, ?)",
                        (page.slug, target_slug),
                    )
        connection.commit()
        page_count = connection.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        link_count = connection.execute("SELECT COUNT(*) FROM links").fetchone()[0]
    return {"db_path": str(target), "pages": int(page_count), "links": int(link_count), "schema_version": DB_SCHEMA_VERSION}


def _fts_query(tokens: list[str]) -> str:
    return " OR ".join(f'"{token}"' for token in tokens)


def _normalise_text_scores(rows: list[sqlite3.Row]) -> dict[str, float]:
    if not rows:
        return {}
    ranks = [float(row["rank"]) for row in rows]
    best = min(ranks)
    worst = max(ranks)
    if best == worst:
        return {str(row["slug"]): 1.0 for row in rows}
    return {str(row["slug"]): (worst - float(row["rank"])) / (worst - best) for row in rows}


def _graph_reason(
    connection: sqlite3.Connection,
    *,
    slug: str,
    pagerank_score: float,
    seed_slugs: set[str],
) -> str:
    reasons: list[str] = []
    inbound_from_seed = connection.execute(
        "SELECT source_slug FROM links WHERE target_slug = ? ORDER BY source_slug LIMIT 3",
        (slug,),
    ).fetchall()
    outbound_to_seed = connection.execute(
        "SELECT target_slug FROM links WHERE source_slug = ? ORDER BY target_slug LIMIT 3",
        (slug,),
    ).fetchall()
    inbound_matches = [str(row["source_slug"]) for row in inbound_from_seed if str(row["source_slug"]) in seed_slugs]
    outbound_matches = [str(row["target_slug"]) for row in outbound_to_seed if str(row["target_slug"]) in seed_slugs]
    if inbound_matches:
        reasons.append("linked from " + ", ".join(inbound_matches))
    if outbound_matches:
        reasons.append("links to " + ", ".join(outbound_matches))
    if pagerank_score > 0:
        reasons.append(f"PageRank {pagerank_score:.6f}")
    return "; ".join(reasons) if reasons else "lexical match"


def search_sqlite_index(
    api: ModuleType,
    query: str,
    *,
    db_path: Path | None = None,
    top_k: int = 5,
    candidate_limit: int = 50,
) -> list[SQLiteWikiSearchResult]:
    _ = api
    target = db_path or default_db_path(api)
    normalized_query = query.strip()
    if not normalized_query or not target.exists():
        return []
    tokens = _tokenize(normalized_query)
    if not tokens:
        return []

    with closing(_connect(target)) as connection:
        rows = connection.execute(
            """
            SELECT p.slug, p.title, p.path, p.snippet, p.body, bm25(pages_fts) AS rank, pr.score AS pagerank
            FROM pages_fts
            JOIN pages p ON p.slug = pages_fts.slug
            JOIN page_rank pr ON pr.slug = p.slug
            WHERE pages_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (_fts_query(tokens), candidate_limit),
        ).fetchall()
        text_scores = _normalise_text_scores(rows)
        max_pagerank = max((float(row["pagerank"]) for row in rows), default=0.0)
        query_terms = set(tokens)
        seed_slugs = {str(row["slug"]) for row in rows[: max(1, min(5, len(rows)))]}
        results: list[SQLiteWikiSearchResult] = []
        for row in rows:
            slug = str(row["slug"])
            text_score = text_scores.get(slug, 0.0)
            pagerank_score = float(row["pagerank"])
            pagerank_component = pagerank_score / max_pagerank if max_pagerank > 0 else 0.0
            score = (0.82 * text_score) + (0.18 * pagerank_component)
            body = str(row["body"])
            matched_terms = tuple(sorted(term for term in query_terms if term in set(_tokenize(body))))
            results.append(
                SQLiteWikiSearchResult(
                    slug=slug,
                    title=str(row["title"]),
                    path=str(row["path"]),
                    score=score,
                    matched_terms=matched_terms,
                    snippet=_snippet(str(row["snippet"]), query_terms),
                    why=_graph_reason(connection, slug=slug, pagerank_score=pagerank_score, seed_slugs=seed_slugs),
                    text_score=text_score,
                    pagerank_score=pagerank_score,
                )
            )
    results.sort(key=lambda result: (-result.score, result.title.lower(), result.slug))
    return results[:top_k]
