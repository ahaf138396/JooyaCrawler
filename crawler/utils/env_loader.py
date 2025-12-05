from __future__ import annotations

from pathlib import Path
from typing import Union

from dotenv import find_dotenv, load_dotenv


PathLike = Union[str, Path]


def load_environment(dotenv_path: PathLike | None = None, *, override: bool = False) -> bool:
    """Load environment variables from a .env file.

    Args:
        dotenv_path: Explicit path to the .env file. If omitted, the first
            discoverable .env in the current working directory tree is used.
        override: Whether to overwrite existing environment variables.

    Returns:
        True if an env file was found and loaded, otherwise False.
    """

    path = dotenv_path
    if path is None:
        path = find_dotenv(usecwd=True)

    if not path:
        return False

    return load_dotenv(dotenv_path=path, override=override)
