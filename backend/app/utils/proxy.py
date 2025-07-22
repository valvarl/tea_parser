"""
Пул прокси-серверов с простым round-robin и «баном» неработающих адресов.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class ProxyPool:
    """Итеративно выдаёт прокси из списка и позволяет пометить их как «плохие»."""

    def __init__(self, proxies: Optional[List[str]] = None) -> None:
        self.proxies: List[str] = proxies or []
        self.current_index: int = 0
        self.failed_proxies: set[int] = set()

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    def get_proxy(self) -> Optional[str]:
        """
        Возвращает очередной прокси-адрес либо `None`, если список пустой.
        Прокси, ранее помеченные `mark_failed`, временно пропускаются.
        """
        if not self.proxies:
            return None

        available = [
            p for i, p in enumerate(self.proxies) if i not in self.failed_proxies
        ]
        if not available:                           # все упали → сброс
            self.failed_proxies.clear()
            available = self.proxies
            logger.debug("ProxyPool reset — все прокси были забанены")

        proxy = available[self.current_index % len(available)]
        self.current_index += 1
        return proxy

    def mark_failed(self, proxy: str) -> None:
        """Помечает прокси как «нерабочий», чтобы временно не использовать."""
        try:
            idx = self.proxies.index(proxy)
        except ValueError:
            return
        self.failed_proxies.add(idx)
        logger.debug("Proxy marked as failed: %s", proxy)


# ------------------------------------------------------------------------- #
# Глобальный экземпляр, используемый скрейпером
# ------------------------------------------------------------------------- #

proxy_pool = ProxyPool(get_settings().proxy_list)