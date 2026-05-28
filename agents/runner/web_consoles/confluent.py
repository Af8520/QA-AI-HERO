"""Confluent Control Center — login + topic message verification דרך Playwright."""

from __future__ import annotations

from typing import Any, Dict, Optional

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)


async def verify_kafka_message(
    topic: str,
    search_term: str,
    expected_value: Optional[str] = None,
    headless: bool = True,
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    """מחפש מסר ב-Kafka topic דרך Confluent Control Center web UI.

    מחזיר:
    {found: bool, topic: str, search_term: str, sample: str|None, reason: str|None}
    """
    if not settings.CONFLUENT_URL:
        return {"found": False, "reason": "CONFLUENT_URL חסר ב-.env"}

    try:
        from playwright.async_api import async_playwright  # type: ignore[import-not-found]
    except ImportError:
        return {"found": False, "reason": "playwright לא מותקן"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(ignore_https_errors=not settings.VERIFY_SSL)
        page = await context.new_page()
        page.set_default_timeout(timeout_seconds * 1000)
        try:
            await _login(page)
            return await _check_topic(page, topic, search_term, expected_value)
        except Exception as e:
            log.warning("confluent_check_failed", error=str(e))
            return {"found": False, "reason": str(e)}
        finally:
            await context.close()
            await browser.close()


async def _login(page) -> None:
    await page.goto(settings.CONFLUENT_URL, wait_until="domcontentloaded")
    if not settings.CONFLUENT_USERNAME:
        log.info("confluent_no_credentials")
        return
    # selectors גמישים — Confluent משנה לפעמים
    user_selectors = [
        'input[placeholder="Log in"]',
        'input[name="username"]',
        'input[type="text"]',
        'input[autocomplete="username"]',
    ]
    pw_selectors = ['input[type="password"]', 'input[name="password"]']
    submit_selectors = ['button[type="submit"]', 'button:has-text("Log in")', 'button:has-text("Sign in")']

    for sel in user_selectors:
        if await page.locator(sel).count():
            await page.fill(sel, settings.CONFLUENT_USERNAME)
            break
    for sel in pw_selectors:
        if await page.locator(sel).count():
            await page.fill(sel, settings.CONFLUENT_PASSWORD or "")
            break
    for sel in submit_selectors:
        if await page.locator(sel).count():
            await page.locator(sel).first.click()
            break
    await page.wait_for_load_state("networkidle")


async def _check_topic(page, topic: str, search_term: str, expected_value: Optional[str]) -> Dict[str, Any]:
    # Navigate to topic page (URL pattern נפוץ)
    topic_url = f"{settings.CONFLUENT_URL.rstrip('/')}/clusters/_local/topics/{topic}/message-viewer"
    await page.goto(topic_url, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")

    # ננסה למצוא את הטקסט בדף
    body = await page.content()
    found = search_term in body
    if expected_value:
        found = found and expected_value in body

    sample = None
    try:
        msgs = await page.locator(".cc-message, .message-row, [data-testid='message-content']").all_text_contents()
        for m in msgs:
            if search_term in m:
                sample = m[:300]
                break
    except Exception:
        pass

    return {
        "found": found,
        "topic": topic,
        "search_term": search_term,
        "sample": sample,
        "reason": None if found else "המסר לא אותר ב-topic",
    }
