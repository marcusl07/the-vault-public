from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


@dataclass(frozen=True)
class ReviewItem:
    reason: str
    affected_pages: list[str]
    source_paths: list[str]
    next_action: str
    status: str = "open"


@dataclass(frozen=True)
class ReviewResolution:
    reason: str
    affected_pages: list[str]


@dataclass
class OperationalEffects:
    jsonl_events: list[dict[str, object]] = field(default_factory=list)
    state_events: list[dict[str, object]] = field(default_factory=list)
    wiki_log_entries: list[str] = field(default_factory=list)
    review_items: list[ReviewItem] = field(default_factory=list)
    review_resolutions: list[ReviewResolution] = field(default_factory=list)

    def extend(self, other: "OperationalEffects") -> "OperationalEffects":
        self.jsonl_events.extend(other.jsonl_events)
        self.state_events.extend(other.state_events)
        self.wiki_log_entries.extend(other.wiki_log_entries)
        self.review_items.extend(other.review_items)
        self.review_resolutions.extend(other.review_resolutions)
        return self

    def to_dict(self) -> dict[str, object]:
        return {
            "jsonl_events": list(self.jsonl_events),
            "state_events": list(self.state_events),
            "wiki_log_entries": list(self.wiki_log_entries),
            "review_items": [item.__dict__.copy() for item in self.review_items],
            "review_resolutions": [resolution.__dict__.copy() for resolution in self.review_resolutions],
        }

    @classmethod
    def jsonl_event(cls, payload: dict[str, object]) -> "OperationalEffects":
        return cls(jsonl_events=[payload])

    @classmethod
    def state_event(cls, payload: dict[str, object]) -> "OperationalEffects":
        return cls(state_events=[payload])

    @classmethod
    def ingest_log(
        cls,
        title: str,
        *,
        date: str,
        router_decision: object | None = None,
        deferred_items: list[str] | None = None,
    ) -> "OperationalEffects":
        entry = f'## [{date}] ingest | Capture: "{title}"'
        if router_decision is not None:
            entry += f" | Router: {router_decision.action}"
        if deferred_items:
            entry += f" | Deferred: {len(deferred_items)}"
        return cls(wiki_log_entries=[entry])

    @classmethod
    def bootstrap_log(
        cls,
        *,
        date: str,
        processed_sources: int,
        changed_pages: int,
        failed_sources: int,
    ) -> "OperationalEffects":
        return cls(
            wiki_log_entries=[
                (
                    f"## [{date}] bootstrap | pipeline | Sources: {processed_sources} | "
                    f"Changed pages: {changed_pages} | Failed sources: {failed_sources}"
                )
            ]
        )

    @classmethod
    def query_log(cls, summary: str, *, date: str) -> "OperationalEffects":
        return cls(wiki_log_entries=[f"## [{date}] query | {summary}"])

    @classmethod
    def review_item(
        cls,
        *,
        reason: str,
        affected_pages: list[str],
        source_paths: list[str],
        next_action: str,
        status: str = "open",
    ) -> "OperationalEffects":
        return cls(review_items=[ReviewItem(reason, affected_pages, source_paths, next_action, status)])

    @classmethod
    def review_resolution(cls, *, reason: str, affected_pages: list[str]) -> "OperationalEffects":
        return cls(review_resolutions=[ReviewResolution(reason, affected_pages)])


@dataclass(frozen=True)
class AppliedOperationalEffects:
    jsonl_events: int = 0
    state_events: int = 0
    wiki_log_entries: int = 0
    review_items: int = 0
    review_resolutions: int = 0


class OperationalSink:
    def __init__(self, api: ModuleType):
        self.api = api

    def apply(
        self,
        effects: OperationalEffects,
        *,
        log_path: Path | None = None,
        state_path: Path | None = None,
    ) -> AppliedOperationalEffects:
        for payload in effects.jsonl_events:
            self.api.notes_impl.append_jsonl_event(
                payload,
                log_path or self.api.JSONL_LOG_PATH,
                timestamp=self.api.utc_timestamp,
                fsync=self.api.os.fsync,
            )
        for payload in effects.state_events:
            self.api.notes_impl.append_jsonl_event(
                payload,
                state_path or self.api.STATE_EVENTS_PATH,
                timestamp=self.api.utc_timestamp,
                fsync=self.api.os.fsync,
            )
        for entry in effects.wiki_log_entries:
            self._append_wiki_log_entry(entry)
        for item in effects.review_items:
            self._append_review_backlog_item(item)
        resolved = 0
        for resolution in effects.review_resolutions:
            resolved += self._resolve_review_backlog_entries(resolution)
        return AppliedOperationalEffects(
            jsonl_events=len(effects.jsonl_events),
            state_events=len(effects.state_events),
            wiki_log_entries=len(effects.wiki_log_entries),
            review_items=len(effects.review_items),
            review_resolutions=resolved,
        )

    def _append_wiki_log_entry(self, entry: str) -> None:
        log_path = self.api.WIKI_ROOT / "log.md"
        existing = log_path.read_text(encoding="utf-8") if log_path.exists() else "# Wiki Log\n"
        self.api.atomic_write_text(log_path, existing.rstrip() + "\n\n" + entry + "\n")

    def _append_review_backlog_item(self, item: ReviewItem) -> None:
        review_path = self.api.WIKI_ROOT / "review.md"
        existing = review_path.read_text(encoding="utf-8") if review_path.exists() else "# Wiki Review Backlog\n"
        page_links = ", ".join(f"[[{slug}]]" for slug in self.api.bw.ordered_unique(item.affected_pages)) or "None"
        source_links = ", ".join(f"[{path}]({path})" for path in self.api.bw.ordered_unique(item.source_paths)) or "None"
        entry = "\n".join(
            [
                f"## [{self.api._today_date()}] {item.status} | {item.reason}",
                f"- Affected pages: {page_links}",
                f"- Source artifacts: {source_links}",
                f"- Next action: {item.next_action}",
            ]
        )
        self.api.atomic_write_text(review_path, existing.rstrip() + "\n\n" + entry + "\n")

    def _resolve_review_backlog_entries(self, resolution: ReviewResolution) -> int:
        review_path = self.api.WIKI_ROOT / "review.md"
        if not review_path.exists():
            return 0
        lines = review_path.read_text(encoding="utf-8").splitlines()
        resolved = 0
        updated_lines: list[str] = []
        current_section: list[str] = []

        def flush_section(section: list[str]) -> None:
            nonlocal resolved
            if not section:
                return
            heading = section[0]
            if not heading.startswith("## ["):
                updated_lines.extend(section)
                return
            heading_parts = heading.split(" | ", 1)
            heading_reason = heading_parts[1] if len(heading_parts) == 2 else ""
            matches_reason = heading_reason == resolution.reason
            has_affected_pages = any(
                line.startswith("- Affected pages:") and all(f"[[{slug}]]" in line for slug in resolution.affected_pages)
                for line in section[1:]
            )
            if matches_reason and has_affected_pages and " open | " in heading:
                section = [heading.replace(" open | ", " resolved | ", 1), *section[1:]]
                resolved += 1
            updated_lines.extend(section)

        for line in lines:
            if line.startswith("## ["):
                flush_section(current_section)
                current_section = [line]
            elif current_section:
                current_section.append(line)
            else:
                updated_lines.append(line)
        flush_section(current_section)
        if resolved:
            self.api.atomic_write_text(review_path, "\n".join(updated_lines) + "\n")
        return resolved
