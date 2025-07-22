"""
Определяет тип, регион, год, форму и прочие свойства чая по названию товара.
Используется в скрейпере и при последующей обработке данных.
"""

from __future__ import annotations

import re
from typing import Dict, Any

from app.core.config import TEA_KEYWORDS


def classify_tea_type(name: str) -> Dict[str, Any]:
    """
    Берёт `name` (рус./англ.) и возвращает словарь с распознанными полями:
    `tea_type`, `tea_region`, `tea_year`, `tea_grade`, `is_pressed`.

    Поля отсутствуют, если не удалось уверенно их определить.
    """
    name_lower = name.lower()
    result: Dict[str, Any] = {}

    # ───── вид чая ─────
    if any(t in name_lower for t in ("пуэр", "пуер", "pu-erh", "pu erh", "puer")):
        result["tea_type"] = "пуэр"
    elif any(t in name_lower for t in ("улун", "оолонг", "oolong")):
        result["tea_type"] = "улун"
    elif any(t in name_lower for t in ("зелёный", "зеленый", "green")):
        result["tea_type"] = "зелёный"
    elif any(t in name_lower for t in ("чёрный", "черный", "black")):
        result["tea_type"] = "чёрный"
    elif any(t in name_lower for t in ("белый", "white")):
        result["tea_type"] = "белый"

    # ───── форма (прессованный / рассыпной) ─────
    if any(t in name_lower for t in ("блин", "блинчик", "плитка", "прессованный", "cake", "brick")):
        result["is_pressed"] = True
    elif any(t in name_lower for t in ("рассыпной", "листовой", "loose")):
        result["is_pressed"] = False

    # ───── градация пуэра ─────
    if result.get("tea_type") == "пуэр":
        if any(t in name_lower for t in ("шэн", "шен", "sheng", "сырой")):
            result["tea_grade"] = "шэн"
        elif any(t in name_lower for t in ("шу", "shou", "shu", "готовый")):
            result["tea_grade"] = "шу"

    # ───── регион ─────
    for region in TEA_KEYWORDS["regions"]:
        if region in name_lower:
            result["tea_region"] = region
            break

    # ───── год ─────
    year_match = re.search(r"\b(19|20)\d{2}\b", name_lower)
    if year_match:
        result["tea_year"] = year_match.group(0)
    else:
        for y in TEA_KEYWORDS["years"]:
            if y in name_lower:
                result["tea_year"] = y
                break

    return result