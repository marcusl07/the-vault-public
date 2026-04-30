from __future__ import annotations

from scripts import vault_pipeline as vp


def discover(argv: list[str] | None = None) -> object:
    return vp.discover(argv)


def process(plan: object) -> object:
    return vp.process(plan)


def write_outputs(result: object) -> int:
    return vp.write_outputs(result)


def main(argv: list[str] | None = None) -> int:
    plan = discover(argv)
    result = process(plan)
    return write_outputs(result)


if __name__ == "__main__":
    raise SystemExit(main())
