# 📘 Rebuild Blueprint — نسخه ادغامی نهایی (Python/PySide6, Policy-First)

## 0) هدف‌ها و اصول غیرقابل‌مذاکره

- **Policy-First & SSoT**: کل قوانین از `config/policy.json` خوانده شود؛ هیچ هاردکدی در Core.
- **Determinism**: هر اجرای یکسان ← خروجی یکسان (sort پایدار، natural-sort، نرمال‌سازی dtype).
- **DDD سبک (Domain/Application/Infra)**: Domain بدون I/O/Qt؛ تمام فایل/GUI در Infra.
- **Small Batches + Two-Key Rule**: هر تغییر کوچک + امکان برگشت سریع.
- **Performance Budget**: 10K دانش‌آموز ≤ 60s، 100K ≤ 5m، RAM ≤ 2GB.

------

## 1) معماری هدف و ساختار دایرکتوری

```
app/
├─ core/                      # Domain (بدون I/O/Qt)
│  ├─ allocate_students.py    # موتور تخصیص (خالص)
│  ├─ build_matrix.py         # Matrix Builder (خالص)
│  ├─ policy_loader.py        # Loader + cache
│  └─ common/
│     ├─ types.py             # TypedDict/Dataclass قراردادها
│     ├─ ids.py               # premap + natural sort + inject
│     └─ utils.py             # توابع خالص (normalize, safe_cast,…)
├─ infra/                     # I/O, Excel, Logging, CLI/GUI bridges
│  ├─ io_utils.py             # write_xlsx_atomic (fallback)
│  ├─ reporting.py            # Exporters (xlsx/md/json/html)
│  └─ cli.py                  # اجرای headless
├─ ui/                        # PySide6 فقط UI
│  ├─ main_window.py
│  └─ task_runner.py          # Threading/Signals bridge
config/
└─ tests/
   ├─ fixtures/               # mini_pool.csv, students.json, …
   ├─ unit/                   # ranking/trace/types
   ├─ integration/            # E2E با فایل‌های واقعی
   └─ perf/                   # بنچمارک
```

------

## 2) Policy Management (Schema + Loader)

### 2.1 `config/policy.json` (مینیمال کافی)

```json
{
  "version": "1.0.3",
  "normal_statuses": [1, 0],
  "school_statuses": [1],
  "join_keys": ["کدرشته","جنسیت","دانش آموز فارغ","مرکز گلستان صدرا","مالی حکمت بنیاد","کد مدرسه"],
  "ranking": ["min_occupancy_ratio","min_allocations_new","min_mentor_id"]
}
```

### 2.2 `app/core/policy_loader.py`

```python
from __future__ import annotations
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import json
from typing import Dict, List

@dataclass(frozen=True)
class PolicyConfig:
    version: str
    normal_statuses: List[int]
    school_statuses: List[int]
    join_keys: List[str]
    ranking: List[str]

def _validate_policy(data: Dict) -> None:
    req = ["version","normal_statuses","school_statuses","join_keys","ranking"]
    miss = [k for k in req if k not in data]
    if miss: raise ValueError(f"Policy keys missing: {miss}")
    if len(data["join_keys"]) != 6: raise ValueError("join_keys must be 6")

@lru_cache(maxsize=4)
def load_policy(path: str | Path = "config/policy.json") -> PolicyConfig:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    _validate_policy(data)
    return PolicyConfig(**data)
```

------

## 3) قرارداد داده‌ها و نوع‌ها (Domain Contracts)

### 3.1 `app/core/common/types.py`

```python
from __future__ import annotations
from typing import TypedDict, Literal, Dict, List, Optional, Any

class JoinKeys(TypedDict):
    کدرشته: int; جنسیت: int; دانش_آموز_فارغ: int
    مرکز_گلستان_صدرا: int; مالی_حکمت_بنیاد: int; کد_مدرسه: int

class StudentRow(TypedDict, total=False):
    student_id: str
    کدرشته: int; جنسیت: int; دانش_آموز_فارغ: int
    مرکز_گلستان_صدرا: int; مالی_حکمت_بنیاد: int; کد_مدرسه: int
    نام: str

class MentorRow(TypedDict, total=False):
    پشتیبان: str
    کد_کارمندی_پشتیبان: str
    occupancy_ratio: float
    allocations_new: int
    remaining_capacity: int
    covered_now: int
    special_limit: int

AllocationErrorLiteral = Literal["ELIGIBILITY_NO_MATCH","CAPACITY_FULL","DATA_MISSING","INTERNAL_ERROR"]

class AllocationLogRecord(TypedDict, total=False):
    row_index: int; student_id: str
    allocation_status: Literal["success","failed"]
    mentor_selected: Optional[str]; mentor_id: Optional[str]
    occupancy_ratio: Optional[float]
    join_keys: JoinKeys; candidate_count: int
    selection_reason: Optional[str]; tie_breakers: Dict[str, Any]
    error_type: Optional[AllocationErrorLiteral]
    detailed_reason: Optional[str]; suggested_actions: List[str]
```

> نکته: در DataFrame نام ستون‌ها همان نسخه‌ی با فاصله بماند؛ در TypedDict برای سادگی تست‌ها از آندرلاین استفاده شده.

------

## 4) پیش‌نقشه و رتبه‌بندی (premap + natural sort + stable)

### 4.1 `app/core/common/ids.py`

```python
from __future__ import annotations
import re
import pandas as pd
from typing import Dict

def _norm(s): 
    return "" if s is None else str(s).replace("\u200c","").strip()

def mentor_id_natural_key(s: str | None) -> tuple[str, int]:
    s = _norm(s); m = re.search(r"^(\D*?)(\d+)$", s)
    return (s, 0) if not m else (m.group(1), int(m.group(2)))

def build_mentor_id_map(matrix_df: pd.DataFrame) -> Dict[str, str]:
    need = {"پشتیبان","کد کارمندی پشتیبان"}
    if not need.issubset(matrix_df.columns): 
        raise KeyError(f"Missing columns: {need - set(matrix_df.columns)}")
    df = matrix_df[list(need)].dropna()
    df = df[df["پشتیبان"].astype(str).str.strip()!=""]
    df = df[df["کد کارمندی پشتیبان"].astype(str).str.strip()!=""]
    out: Dict[str,str] = {}
    for _,r in df.iterrows(): out[_norm(r["پشتیبان"])] = _norm(r["کد کارمندی پشتیبان"])
    return out

def inject_mentor_id(pool: pd.DataFrame, id_map: Dict[str,str]) -> pd.DataFrame:
    if "پشتیبان" not in pool.columns: return pool
    if "کد کارمندی پشتیبان" not in pool.columns: pool = pool.copy(); pool["کد کارمندی پشتیبان"] = ""
    mask = pool["کد کارمندی پشتیبان"].astype(str).str.strip().eq("")
    pool.loc[mask, "کد کارمندی پشتیبان"] = pool.loc[mask,"پشتیبان"].map(lambda n: id_map.get(_norm(n),""))
    return pool

def ensure_ranking_columns(pool: pd.DataFrame) -> pd.DataFrame:
    for c in ("occupancy_ratio","allocations_new"): 
        if c not in pool.columns: raise KeyError(f"Missing: {c}")
    pool = pool.copy()
    pool["mentor_id_str"] = pool["کد کارمندی پشتیبان"].astype(str).str.strip()
    return pool
```

### 4.2 هسته‌ی رتبه‌بندی (Policy-aware)

```python
def apply_ranking_policy(candidate_pool: pd.DataFrame) -> pd.DataFrame:
    pool = ensure_ranking_columns(candidate_pool)
    # sort پایدار و دترمینستیک
    return pool.sort_values(
        by=["occupancy_ratio","allocations_new","mentor_id_str"],
        ascending=[True, True, True],
        kind="stable"
    )
```

------

## 5) موتور تخصیص: ۷ تابع حیاتی + تریس ۸‌مرحله

> پیاده‌سازی‌ها باید دقیقاً با Policy §10/§12 هم‌راستا باشند. (این‌ها در allocate_students.py پس از خط 600 قرار می‌گیرند.)

**توابع:**

1. `analyze_candidate_capacity_detailed(df) → dict`
2. `get_top_candidates_preview(sorted_df, top_n=5) → list[dict]`
3. `create_success_log_record(student, idx, mentor_row, trace) → AllocationLogRecord`
4. `create_error_log_record(student, idx, trace) → AllocationLogRecord`
5. `calculate_detailed_metrics(logs) → dict`
6. `generate_allocation_summary(logs) → dict`
7. `generate_output_files(import_rows, logs, stats, out_import, out_log) → None`

**استاندارد تریس (۸ مرحله حذف/فیلتر):**

1. نوع/گروه (Normal/School/Dual)
2. گروه/کراس‌واک (bucket/synonym)
3. **جنسیت**
4. **وضعیت دانش‌آموز فارغ**
5. **مرکز گلستان صدرا**
6. **مالی حکمت بنیاد**
7. **کد مدرسه**
8. **Gate ظرفیت** (covered_now/special_limit)

برای هر مرحله: `{"filter":"name","before":N,"after":M,"drop_reason":"...", "keys":join_keys}`

> این دقیقاً نیاز §12 را پوشش می‌دهد و چرایی انتخاب/رد را مستند می‌کند.

------

## 6) I/O اتمیک و سازگاری پانداس

### 6.1 `app/infra/io_utils.py` (fallback امن)

```python
from __future__ import annotations
from pathlib import Path
import os, tempfile, pandas as pd

def write_xlsx_atomic(sheets: dict[str, pd.DataFrame], filepath: str | Path) -> None:
    path = Path(filepath); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    try:
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as w:
            for name, df in sheets.items():
                df = df.copy()
                df.to_excel(w, sheet_name=str(name)[:31], index=False)
        os.replace(tmp.name, str(path))
    finally:
        try: os.unlink(tmp.name)
        except FileNotFoundError: pass
```

### 6.2 قوانین سازگاری پانداس

- **No `inplace=True`** روی view؛ همیشه `df = df.assign(...)` یا `df[col] = df[col].fillna(0)`
- ورودی/خروجی Excel فقط با **openpyxl**.
- نوع‌های کلیدی را **int** و شناورها را **float** normalize کن (قبل از sort/merge).

------

## 7) Concurrency/GUI: قرارداد ثابت و پاک‌سازی

### 7.1 قرارداد Progress/Cancel (bridge)

- در **Domain**: `progress(pct:int, msg:str)` یک **callable** است (نه Signal).
- در **UI**: سیگنال را wrap کن: `lambda p,m: self.progress.emit(p,m)`

### 7.2 `ui/task_runner.py::run()`

```python
def run(self):
    try:
        result = self.task_func(
            lambda pct, msg: self.progress.emit(pct, msg),
            self.check_cancel, *self.args, **self.kwargs
        )
        self.finished.emit(TaskResult(success=True, data=result))
    except Exception as e:
        self.finished.emit(TaskResult(False, error=str(e), traceback=traceback.format_exc()))
```

### 7.3 `ui/main_window.py::closeEvent`

- ترتیب امن: `runner.cancel()` → `thread.quit()` → `wait(2000)` → در صورت نیاز `terminate()` → ذخیره تنظیمات.

### 7.4 Headless/CI

- اگر DISPLAY نیست: `QT_QPA_PLATFORM=offscreen` و **CLI** را پیش‌فرض کن.
- امکان اجرای کامل سناریوها بدون PySide6 (برای CI).

------

## 8) Diagnostics & Fixtures

### 8.1 `scripts/diagnostic.py` (env doctor خلاصه)

- Python≥3.9، pandas/numpy/openpyxl نسخه‌ها
- Qt/GL presence (libGL)
- دسترسی نوشتن روی مسیر خروجی
- گزارش HTML ساده با پیشنهاد رفع

### 8.2 Fixtures (حداقل)

- `fixtures/mini_pool.csv` (۳ منتور با occupancy/tie برابر)
- `fixtures/students_small.xlsx` (۱۰ دانش‌آموز)
- `fixtures/test_tie_break.xlsx`, `capacity_scenarios.xlsx`
- `policy_v1.0.3.json` و «policy_variant» برای تست سریع تغییر قوانین

------

## 9) Testing Strategy (DoD)

- **Unit**:
  - ranking (natural/stable)
  - trace filters (۸ مرحله)
  - types/normalization
- **Golden Tests**: خروجی `allocation_log.xlsx`/`import_to_sabt.xlsx` با dataset کوچک
- **Integration (E2E)**: matrix+students+capacity واقعی → فایل‌های خروجی صحیح
- **Performance**: 10K ≤ 60s؛ logِ زمان و حافظه.
- **Linters/Typing**: flake8/mypy اختیاری؛ حداقل PEP8 و type hints در Core.

------

## 10) CI/CD مینیمال (GitHub Actions)

- ماتریس پایتون 3.9/3.10/3.11
- نصب `openpyxl`, (بدون PySide6 در CI)
- اجرای unit/integration/perf-smoke
- تولید artifacts: فایل‌های خروجی نمونه

*(Docker اختیاری: پایه slim + libgl1-mesa-glx برای محیط GUI؛ در CI headless کافیست.)*

------

## 11) نقشه‌راه اجرا (۲ هفتهٔ بهینه)

**هفته ۱**

1. `types.py` + `ids.py` (premap/ensure/natural)
2. PolicyLoader و اتصال به allocate/build
3. اصلاح ranking + حذف inplace + دترمینیسم
4. پیاده‌سازی ۷ تابع + تریس ۸‌مرحله
5. io_utils اتمیک + golden tests

**هفته ۲**

1. Diagnostic + Fixtures تکمیلی
2. CLI headless + UI bridge پاکسازی + closeEvent
3. Integration/Perf Tests + CI پایه
4. مستندات و Release

------

## 12) ریسک‌ها و راهکارها

- **libGL/Qt**: Headless پیش‌فرض + مسیر CLI
- **ناپایداری sort در pandas قدیمی**: قفل نسخه در `requirements-lock.txt`
- **mentor_id ناسازگار (EMP-2 vs EMP-010)**: natural-key اجباری
- **I/O شکست**: `write_xlsx_atomic` + replace و cleanup

------

## 13) پرامپت‌های مهندسی (کپسول‌های قابل‌اجرا)

### P-A) «PolicyLoader + Contracts»

**ROLE**: Senior Python + Config
 **TASK**: پیاده‌سازی `policy_loader.py` و `types.py` مطابق بالا
 **CONSTRAINTS**: بدون وابستگی خارجی؛ `@lru_cache`؛ ولیدیشن دستی
 **OUTPUT**: فایل‌ها + تست unit برای validate_policy
 **VERIFY**: load → join_keys==6 → ranking 3 مرحله

### P-B) «Premap + Ranking»

**ROLE**: Algo Engineer
 **TASK**: `ids.py` و `apply_ranking_policy` (stable/natural)
 **CONSTRAINTS**: عدم تغییر دادهٔ اصلی؛ ستون مشتق `mentor_id_str`
 **OUTPUT**: تابع‌ها + تست tie-break
 **VERIFY**: EMP-001 ≺ EMP-002 ≺ EMP-010 در تساوی دو معیار اول

### P-C) «Allocation 7-Pack + Trace»

**ROLE**: Data Systems
 **TASK**: پیاده‌سازی ۷ تابع + تریس ۸ مرحله
 **CONSTRAINTS**: فارسی، دترمینیسم، بدون I/O در Domain
 **OUTPUT**: کد + golden test (۱۰ دانش‌آموز)
 **VERIFY**: نرخ تخصیص ≥80%، فایل‌های خروجی بازشدنی

### P-D) «Atomic I/O + No-inplace»

**ROLE**: Infra
 **TASK**: `write_xlsx_atomic` و حذف تمام `inplace=True`
 **VERIFY**: بدون FutureWarning؛ فایل‌ها اتمیک جایگزین شوند

### P-E) «UI Bridge + Headless CLI»

**ROLE**: PySide6/Concurrency
 **TASK**: قرارداد progress callable؛ closeEvent؛ CLI headless
 **VERIFY**: بستن وسط تسک بدون crash؛ CI headless سبز

------

## 14) چک‌لیست پذیرش نهایی (Project DoD)

-  Core بدون I/O/Qt؛ Infra مسئول فایل/UI
-  Policy از JSON؛ تغییر policy بدون تغییر کد
-  رتبه‌بندی پایدار و طبیعی (۳ معیار)
-  Trace ۸‌مرحله برای هر تصمیم (success/fail)
-  Excelهای خروجی با `write_xlsx_atomic`
-  تست‌های Unit/Golden/Integration/Perf سبز
-  CI اجرا و آرتیفکت نمونه تولید شود
-  مسیر Headless کامل و مستند
-  هیچ `inplace=True` در Core وجود ندارد

------

### اسنیپت‌های کلیدی (برای استفاده سریع)

**Natural Sort و Premap**: در بخش 4.1
 **Ranking پایدار**: در بخش 4.2
 **Atomic Excel**: در بخش 6.1
 **Policy Loader**: در بخش 2.2
 **Bridge Progress & closeEvent**: در بخش 7

------

## جمع‌بندی

این «نسخه ادغامی نهایی» تمام بهترین روش‌ها و نقدهای مطرح‌شده را یکپارچه کرده و مسیر بازنویسی را **اجرایی، دترمینیستیک، Policy-First و مقیاس‌پذیر** می‌کند. همین الان با **P-A و P-B** شروع کن؛ سپس **P-C** (۷ تابع + Trace) را اجرا و با **P-D/E** پروژه را پایدار و قابل‌استقرار کن.