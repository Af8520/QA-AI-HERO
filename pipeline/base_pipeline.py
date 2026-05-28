"""Base utilities לניהול pipeline + emit progress."""

from __future__ import annotations

from typing import Awaitable, Callable

# notify(text: str) -> Awaitable[None]
NotifyFn = Callable[[str], Awaitable[None]]
