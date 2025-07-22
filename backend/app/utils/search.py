"""
Генератор поисковых запросов для Ozon. Используется
в / search/queries  и при автоматическом запуске задач.
"""

from __future__ import annotations

import random
from typing import List

from app.core.config import TEA_KEYWORDS


def generate_search_queries(limit: int = 50, shuffle: bool = True) -> List[str]:
    """
    Возвращает список уникальных поисковых фраз (max `limit`).
    Принцип:
      1. 10 базовых терминов
      2. Комбинации `base_term + form`
      3. Комбинации `base_term + region`
      4. Популярные «ручные» запросы для пуэра
    """
    queries: List[str] = []

    # 1. первые 10 базовых
    queries.extend(TEA_KEYWORDS["base_terms"][:10])

    # 2. формы (3 первых)  +   3. регионы (3 первых)
    for base in TEA_KEYWORDS["base_terms"][:5]:
        queries.extend(f"{base} {form}" for form in TEA_KEYWORDS["forms"][:3])
        queries.extend(f"{base} {reg}" for reg in TEA_KEYWORDS["regions"][:3])

    # 4. пул «ручных» запросов
    queries += [
        "пуэр блин 357",
        "шэн пуэр",
        "шу пуэр",
        "пуэр юннань",
        "китайский пуэр",
        "пуэр чай блинчик",
        "пуэр плитка",
        "пуэр рассыпной",
    ]

    # удаляем дубликаты, случайно перемешиваем и обрезаем до лимита
    unique = list(dict.fromkeys(queries))
    if shuffle:
        random.shuffle(unique)

    return unique[:limit]