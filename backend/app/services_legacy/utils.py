# utils.py
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

# ─────────── Константы ────────────────────────────────────────────────
PRICE_RE      = re.compile(r"\d+")
NARROW_SPACE  = "\u2009"
MONTHS_RU     = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# ─────────── Утилиты ─────────────────────────────────────────────────
def digits_only(text: str) -> str:
    """Оставляет в строке только цифры и точку."""
    return re.sub(r"[^0-9.]", "", text.replace(" ", "")
                                   .replace(NARROW_SPACE, "")
                                   .replace("\xa0", ""))

def clean_price(text: Optional[str]) -> Optional[int]:
    """Превращает «790 ₽» → 790 или None."""
    if not text:
        return None
    m = PRICE_RE.search(digits_only(text))
    return int(m.group()) if m else None

def parse_delivery(label: Optional[str]) -> Optional[date]:
    """
    «Сегодня/Завтра/Послезавтра/2 августа» → date.
    Вычисляет текущую дату при каждом вызове, поэтому всегда актуальна.
    """
    if not label:
        return None

    today = date.today()
    txt = label.lower().strip()

    if txt == "сегодня":
        return today
    if txt == "завтра":
        return today + timedelta(days=1)
    if txt == "послезавтра":
        return today + timedelta(days=2)

    m = re.match(r"(\d{1,2})\s+([а-яё]+)", txt)
    if m:
        day, month_name = int(m.group(1)), m.group(2)
        month = MONTHS_RU.get(month_name)
        if month:
            year = today.year
            parsed = date(year, month, day)
            # если дата «проскочила» в прошлое — значит имели в виду следующий год
            if parsed < today - timedelta(days=2):
                parsed = date(year + 1, month, day)
            return parsed
    return None

COMMON_WIDGET_META = ["lexemes", "params"]

def clear_widget_meta(node: Any) -> None:
    """Рекурсивно удаляет ключи 'lexemes' и 'params' из словарей внутри node."""
    if isinstance(node, dict):
        for k in COMMON_WIDGET_META:
            node.pop(k, None)
        for v in node.values():
            clear_widget_meta(v)
    elif isinstance(node, list):
        for item in node:
            clear_widget_meta(item)

def collect_raw_widgets(json_page: Dict[str, Any], prefix: str, clear_meta=True) -> List[Dict[str, Any]]:
    """
    Вернуть список всех widgetStates-элементов, чьи ключи начинаются с `prefix`.
    """
    widgets: List[Dict[str, Any]] = []
    for k, raw in json_page.get("widgetStates", {}).items():
        if not k.startswith(prefix):
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if clear_meta:
            clear_widget_meta(obj)
        widgets.append(obj)
    return widgets
