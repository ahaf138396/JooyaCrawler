import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from loguru import logger


_ROBOTS_CACHE: Dict[str, Tuple[datetime, Optional[RobotFileParser]]] = {}
_ROBOTS_LOCK = asyncio.Lock()


class RobotsHandler:
    """Fetch and cache robots.txt rules (Test-friendly version)."""

    def __init__(self, client, user_agent: str, cache_ttl: timedelta = timedelta(hours=12)):
        self.client = client
        self.user_agent = user_agent
        self.cache_ttl = cache_ttl

    async def is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        robots_url = f"{parsed.scheme or 'http'}://{domain}/robots.txt"

        parser = await self._get_parser(domain, robots_url)
        if parser is None:
            return True

        return parser.can_fetch(self.user_agent, url)

    async def _get_parser(self, domain: str, robots_url: str) -> Optional[RobotFileParser]:
        now = datetime.now(timezone.utc)

        async with _ROBOTS_LOCK:
            cached = _ROBOTS_CACHE.get(domain)
            if cached:
                timestamp, parser = cached
                if timestamp + self.cache_ttl > now:
                    return parser

        # fetch fresh robots
        parser = await self._fetch_robots(robots_url)

        async with _ROBOTS_LOCK:
            _ROBOTS_CACHE[domain] = (now, parser)

        return parser

    async def _fetch_robots(self, robots_url: str) -> Optional[RobotFileParser]:
        try:
            response = await self.client.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
            )

            # missing robots = allow
            if response.status_code == 404:
                return None

            # server error = allow
            if response.status_code >= 500:
                return None

            text = getattr(response, "text", "")

            parser = RobotFileParser()
            parser.parse(text.splitlines())
            parser.modified()
            return parser

        except Exception as exc:
            logger.debug(f"Failed to fetch robots.txt from {robots_url}: {exc}")
            return None
