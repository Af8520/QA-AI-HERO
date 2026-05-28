"""Dispatcher: בוחר runner לפי RUNNER_MODE."""

from __future__ import annotations

from typing import Protocol

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)


class Runner(Protocol):
    name: str

    async def invoke_api(self, test_case, collection=None, env_vars=None): ...
    async def verify_kafka(self, test_case): ...
    async def verify_elastic(self, test_case): ...


def get_runner() -> Runner:
    mode = (settings.RUNNER_MODE or "mock").lower()
    if mode == "esb":
        from agents.runner.esb_runner import ESBRunner

        log.info("runner_selected", mode="esb")
        return ESBRunner()
    from agents.runner.mock_runner import MockRunner

    log.info("runner_selected", mode="mock")
    return MockRunner()
