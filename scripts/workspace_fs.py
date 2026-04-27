from __future__ import annotations

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import tempfile
from typing import Callable, Iterator


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_prefix = f".{hashlib.sha1(path.name.encode('utf-8')).hexdigest()[:12]}."
    fd, tmp_name = tempfile.mkstemp(prefix=safe_prefix, suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


@contextmanager
def temporary_workspace(
    snapshot: Callable[[], tuple[object, ...]],
    configure: Callable[..., None],
    root: Path,
    restore_kwargs: Callable[[tuple[object, ...]], dict[str, object]] | None = None,
    *configure_args: object,
    **configure_kwargs: object,
) -> Iterator[None]:
    original = snapshot()
    configure(root, *configure_args, **configure_kwargs)
    try:
        yield
    finally:
        restored_root = original[0]
        configure(restored_root, **(restore_kwargs(original) if restore_kwargs else {}))
