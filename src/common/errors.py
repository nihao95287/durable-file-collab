"""统一错误码与结构化错误对象（docs/Interfaces.md §0.3）。

约定：所有接口失败时抛出本模块的异常，异常至少带 code / message / retryable，
不得用裸字符串表示失败原因。调用方用 ``except QueueError as exc`` 统一兜底。
"""

from __future__ import annotations

from typing import Any


class QueueError(Exception):
    """所有契约错误的基类。"""

    code: str = "InternalError"
    retryable: bool = False

    def __init__(
        self,
        message: str = "",
        *,
        code: str | None = None,
        retryable: bool | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message or self.code)
        if code is not None:
            self.code = code
        if retryable is not None:
            self.retryable = retryable
        self.message = message or self.code
        self.detail = detail or {}

    def to_dict(self) -> dict[str, Any]:
        """HTTP 层直接把这个结构放进响应体的 error 字段。"""

        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.detail:
            payload["detail"] = self.detail
        return payload

    def __str__(self) -> str:  # pragma: no cover - 便于调试
        return f"{self.code}: {self.message}"


class IllegalTransition(QueueError):
    """状态机非法流转（C-02）。"""

    code = "IllegalTransition"
    retryable = False


class TaskNotFound(QueueError):
    code = "TaskNotFound"
    retryable = False


class ChunkChecksumMismatch(QueueError):
    code = "ChunkChecksumMismatch"
    retryable = True


class ClaimInvalid(QueueError):
    code = "ClaimInvalid"
    retryable = True


class RetryExhausted(QueueError):
    code = "RetryExhausted"
    retryable = False


class LeaseHeldByOther(QueueError):
    code = "LeaseHeldByOther"
    retryable = True


class LeaseExpired(QueueError):
    code = "LeaseExpired"
    retryable = True


class PermissionDenied(QueueError):
    code = "PermissionDenied"
    retryable = False


class Unauthorized(QueueError):
    code = "Unauthorized"
    retryable = False


class InvalidArgument(QueueError):
    code = "InvalidArgument"
    retryable = False


class ObjectNotFound(QueueError):
    code = "ObjectNotFound"
    retryable = False


ERROR_TYPES: dict[str, type[QueueError]] = {
    cls.code: cls
    for cls in (
        QueueError,
        IllegalTransition,
        TaskNotFound,
        ChunkChecksumMismatch,
        ClaimInvalid,
        RetryExhausted,
        LeaseHeldByOther,
        LeaseExpired,
        PermissionDenied,
        Unauthorized,
        InvalidArgument,
        ObjectNotFound,
    )
}


def error_from_code(code: str, message: str = "") -> QueueError:
    """按契约错误码构造异常，供 stub 与测试使用。"""

    return ERROR_TYPES.get(code, QueueError)(message, code=code)
