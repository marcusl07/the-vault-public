from __future__ import annotations

from scripts import vault_pipeline as vp


def main(argv: list[str] | None = None) -> int:
    return vp.run_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
