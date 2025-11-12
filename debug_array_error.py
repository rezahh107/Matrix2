"""اسکریپت کمکی برای اجرای تخصیص نمونه و ممیزی سریع."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from app.infra import cli
from app.infra.audit_allocations import audit_allocations


def _print_summary(report: dict[str, dict[str, object]]) -> None:
    """چاپ خلاصهٔ ممیزی برای استفادهٔ توسعه‌دهندگان."""

    print("=== Audit Summary ===")
    for key, payload in report.items():
        count = int(payload.get("count", 0))  # type: ignore[arg-type]
        print(f"{key}: {count}")
        samples = payload.get("samples")  # type: ignore[assignment]
        if isinstance(samples, Sequence) and samples:
            preview = list(samples[:3])  # type: ignore[index]
            print(f"  samples: {preview}")


def main(argv: Sequence[str] | None = None) -> int:
    """اجرای تخصیص CLI با امکان ممیزی خروجی."""

    parser = argparse.ArgumentParser(description="Debug allocator pipeline")
    parser.add_argument("--students", required=True, help="مسیر فایل دانش‌آموزان")
    parser.add_argument("--pool", required=True, help="مسیر استخر منتورها")
    parser.add_argument("--output", required=True, help="مسیر Excel خروجی")
    parser.add_argument(
        "--policy",
        default="config/policy.json",
        help="مسیر فایل policy.json (پیش‌فرض: config/policy.json)",
    )
    parser.add_argument(
        "--capacity-column",
        default=None,
        help="نام ستون ظرفیت باقی‌مانده (در صورت نیاز به override)",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="پس از اجرا، ممیزی خروجی را چاپ کن",
    )

    args = parser.parse_args(argv)

    cli_args = [
        "allocate",
        "--students",
        str(args.students),
        "--pool",
        str(args.pool),
        "--output",
        str(args.output),
        "--policy",
        str(args.policy),
    ]
    if args.capacity_column:
        cli_args.extend(["--capacity-column", str(args.capacity_column)])

    exit_code = cli.main(cli_args)
    if exit_code != 0:
        return exit_code

    if args.audit:
        report = audit_allocations(Path(args.output))
        _print_summary(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
