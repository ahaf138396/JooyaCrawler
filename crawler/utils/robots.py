import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from loguru import logger


class RobotsHandler:
    """Fetch and cache robots.txt directives per domain."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        user_agent: str,
        cache_ttl: timedelta = timedelta(hours=12),
    ) -> None:
        self.client = client
        self.user_agent = user_agent
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[datetime, Optional[RobotFileParser]]] = {}
        self._lock = asyncio.Lock()

    async def is_allowed(self, url: str) -> bool:
        """Return True if URL is allowed for the configured user-agent."""

        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        scheme = parsed.scheme or "http"

        parser = await self._get_parser(domain, f"{scheme}://{domain}/robots.txt")

        if parser is None:
            return True

        return parser.can_fetch(self.user_agent, url)

    async def _get_parser(
        self, domain: str, robots_url: str
    ) -> Optional[RobotFileParser]:
        now = datetime.now(timezone.utc)

        async with self._lock:
            cached = self._cache.get(domain)
            if cached:
                fetched_at, parser = cached
                if fetched_at + self.cache_ttl > now:
                    return parser

        parser = await self._fetch_robots(robots_url)

        async with self._lock:
            self._cache[domain] = (now, parser)

        return parser

    async def _fetch_robots(self, robots_url: str) -> Optional[RobotFileParser]:
        try:
            response = await self.client.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
            )

            # If robots.txt is missing, treat as allowed.
            if response.status_code == 404:
                return None

            if response.status_code >= 500:
                logger.debug(
                    f"Robots fetch returned {response.status_code} for {robots_url}, treating as allow."
                )
                return None

            parser = RobotFileParser()
            parser.parse((response.text or "").splitlines())
            parser.modified()
            return parser
        except Exception as exc:
            logger.debug(f"Failed to fetch robots.txt from {robots_url}: {exc}")
            return None
