"""سازگاری عقب‌رو برای ماژول مرجع مدارس."""

from __future__ import annotations

from pathlib import Path
import pandas as pd

from app.infra.references.schools import (
    get_school_reference_frames,
    import_school_crosswalk_from_excel,
    import_school_report_from_excel,
)

__all__ = [
    "import_school_report_from_excel",
    "import_school_crosswalk_from_excel",
    "get_school_reference_frames",
]
