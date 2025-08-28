# validation.py
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence


###############################################################################
#                           К А С Т О М Н А Я   О Ш И Б К А
###############################################################################


@dataclass(slots=True, frozen=True)
class DataParsingError(ValueError):
    """
    Универсальное исключение для ошибок парсинга / валидации данных.

    Поля намеренно сделаны максимально общими – чтобы ошибку удобно
    агрегировать в продуктах (Sentry, Prometheus, лог-пайплайны):
    -------------------------------------------------------------------------
    •   code            – короткий машинный код ошибки («missing_key» и т. п.)
    •   message         – человекочитаемое описание
    •   entity_id       – идентификатор сущности (SKU, offer_id, order_id …)
    •   field_path      – «путь» до проблемного места (пример: "states.price")
    •   expected        – что ожидали (тип/значение/кол-во …)   – *опционально*
    •   actual          – что получили фактически                 – *опционально*
    •   payload_sample  – обрезок данных (≤500 симв.) для контекста  – *опционально*
    """

    code: str
    message: str
    entity_id: str | int | None = None
    field_path: str | None = None
    expected: Any | None = None
    actual: Any | None = None
    payload_sample: str | None = field(default=None, repr=False)

    # ↓ формируем текст исключения – пригодится, если кто-то печатает str(exc)
    def __str__(self) -> str:  # noqa: DunderStr
        parts = [f"[{self.code}] {self.message}"]
        if self.entity_id is not None:
            parts.append(f"entity_id={self.entity_id!r}")
        if self.field_path is not None:
            parts.append(f"field_path={self.field_path}")
        if self.expected is not None:
            parts.append(f"expected={self.expected!r}")
        if self.actual is not None:
            parts.append(f"actual={self.actual!r}")
        return "; ".join(parts)

    @staticmethod
    def _sample(obj: Any, limit: int = 500) -> str | None:  # noqa: D401
        """~500 символов из объекта, пригодных для логов (JSON, repr, str)."""
        try:
            txt = json.dumps(obj, ensure_ascii=False)[:limit]
        except Exception:
            txt = repr(obj)[:limit]
        return txt


###############################################################################
#                            P U B L I C   A P I
###############################################################################


class Validator:
    """Коллекция stateless-валидаторов – прямо бросают `DataParsingError`."""

    # ---------- key helpers --------------------------------------------------

    @staticmethod
    def get_key(
        mapping: Mapping[str, Any],
        key: str,
        *,
        entity_id: str | int | None = None,
        field_path: str | None = None,
        required: bool = True,
    ) -> Any:
        """
        Вернуть значение `key` или (если required=True) бросить ошибку.

        field_path – логический/человеческий путь до уровня, где искали ключ.
        """
        if key in mapping:
            return mapping[key]

        if required:
            raise DataParsingError(
                code="missing_key",
                message=f"Key {key!r} is required but not found",
                entity_id=entity_id,
                field_path=f"{field_path}.{key}" if field_path else key,
                payload_sample=DataParsingError._sample(mapping),
            )

    # ---------- list helpers -------------------------------------------------

    @staticmethod
    def require_non_empty_list(
        seq: Sequence[Any] | None,
        *,
        entity_id: str | int | None = None,
        field_path: str | None = None,
    ) -> Sequence[Any]:
        """Убедиться, что `seq` – непустой список/кортеж; иначе – ошибка."""
        if isinstance(seq, (list, tuple)) and len(seq) > 0:
            return seq

        raise DataParsingError(
            code="empty_list",
            message="List is empty or not provided",
            entity_id=entity_id,
            field_path=field_path,
            expected="non-empty list",
            actual=type(seq).__name__,
            payload_sample=DataParsingError._sample(seq),
        )

    @staticmethod
    def require_list_length(
        seq: Sequence[Any] | None,
        *,
        length: int | Iterable[int],
        entity_id: str | int | None = None,
        field_path: str | None = None,
    ) -> Sequence[Any]:
        """
        Проверить, что длина списка = length (int) или входит в набор length (Iterable).

        Параметр `length`:
           • int           – точное значение
           • Iterable[int] – допустимый диапазон/набор размеров
        """
        valid_lengths = {length} if isinstance(length, int) else set(length)
        seq_len = len(seq) if isinstance(seq, Sequence) else None

        if seq_len in valid_lengths:
            return seq  # type: ignore[return-value]

        raise DataParsingError(
            code="invalid_list_length",
            message=f"List length {seq_len} not in {sorted(valid_lengths)}",
            entity_id=entity_id,
            field_path=field_path,
            expected=f"length in {sorted(valid_lengths)}",
            actual=seq_len,
            payload_sample=DataParsingError._sample(seq),
        )
