"""پل Thread-safe بین PySide6 و Core با تزریق progress."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple

from PySide6.QtCore import QThread, Signal

ProgressFn = Callable[[int, str], None]

__all__ = ["Worker", "WorkerCancelled", "ProgressFn"]


class WorkerCancelled(RuntimeError):
    """استثناء داخلی برای اعلام لغو امن."""


@dataclass(slots=True)
class _Invocation:
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class Worker(QThread):
    """اجرای تابع طولانی در Thread جداگانه با پشتیبانی progress و لغو.

    مثال::

        >>> from PySide6.QtCore import QCoreApplication
        >>> app = QCoreApplication([])
        >>> def dummy(progress: ProgressFn) -> None:
        ...     progress(50, "half")
        ...
        >>> worker = Worker(dummy)
        >>> worker.start()  # doctest: +SKIP
    """

    progress = Signal(int, str)
    finished = Signal(bool, object)

    def __init__(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._invocation = _Invocation(func=func, args=args, kwargs=kwargs)
        self._cancelled = False

    def request_cancel(self) -> None:
        """علامت‌گذاری برای لغو؛ در نخستین progress بعدی اعمال می‌شود."""

        self._cancelled = True

    def is_cancelled(self) -> bool:
        """آیا لغو درخواست شده است؟"""

        return self._cancelled

    def _progress_hook(self, pct: int, message: str) -> None:
        """پراکندن سیگنال progress با رعایت لغو."""

        if self._cancelled:
            raise WorkerCancelled("Cancelled")
        self.progress.emit(int(pct), str(message))

    def run(self) -> None:  # noqa: D401 - پیاده‌سازی QThread
        """اجرای تابع با تزریق progress و مدیریت خطا/لغو."""

        invocation = self._invocation
        kwargs = dict(invocation.kwargs)
        if "progress" not in kwargs:
            kwargs["progress"] = self._progress_hook
        try:
            invocation.func(*invocation.args, **kwargs)
        except WorkerCancelled:
            self.finished.emit(False, None)
        except Exception as exc:  # pragma: no cover - خطا برای UI bubble می‌شود
            self.finished.emit(False, exc)
        else:
            self.finished.emit(True, None)
