"""
Обёртка над сервисами решения капч. Пока что возвращает «заглушку»,
но интерфейс уже готов для реальных API-запросов.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Dict, Any

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class CaptchaSolver:
    """Храним счётчик решённых капч и сведения о сервисах из настроек."""

    def __init__(self) -> None:
        settings = get_settings()
        self.services: Dict[str, Dict[str, str | None]] = {
            "capsolver": {
                "api_key": settings.CAPSOLVER_API_KEY,
                "client_key": settings.CAPSOLVER_CLIENT_KEY,
                "endpoint": "https://api.capsolver.com/createTask",
            },
            "2captcha": {
                "api_key": settings.TWOCAPTCHA_API_KEY,
                "endpoint": "https://2captcha.com/in.php",
            },
        }
        self.solve_count: int = 0

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def solve_captcha(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> Dict[str, Any]:
        """
        «Решает» капчу; сейчас только имитирует задержку и возвращает фиктивный
        токен. Здесь же можно добавить настоящий вызов API выбранного сервиса.
        """
        self.solve_count += 1
        logger.info(
            "Solving %s captcha for %s (attempt #%d)",
            captcha_type,
            page_url,
            self.solve_count,
        )

        # имитируем задержку сети / решения капчи
        await asyncio.sleep(random.uniform(8, 15))

        # псевдослучайно выбираем сервис для демонстрации
        service_name = random.choice(list(self.services))
        return {
            "success": True,
            "solution": "stubbed_solution_token",
            "service": service_name,
        }


# ------------------------------------------------------------------------- #
# Глобальный экземпляр
# ------------------------------------------------------------------------- #

captcha_solver = CaptchaSolver()