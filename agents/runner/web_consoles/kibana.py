"""Kibana — login + Discover query verification דרך Playwright."""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import quote

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)


async def verify_log_entry(
    index: str,
    query: str,
    must_not_contain_level: Optional[str] = "ERROR",
    headless: bool = True,
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    """מחפש log entry ב-Kibana Discover. מוודא שיש לפחות hit אחד וש-level תקין.

    מחזיר: {hits: int, errors: int, sample: str|None, reason: str|None}
    """
    if not settings.KIBANA_URL:
        return {"hits": 0, "errors": 0, "reason": "KIBANA_URL חסר ב-.env"}

    try:
        from playwright.async_api import async_playwright  # type: ignore[import-not-found]
    except ImportError:
        return {"hits": 0, "errors": 0, "reason": "playwright לא מותקן"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(ignore_https_errors=not settings.VERIFY_SSL)
        page = await context.new_page()
        page.set_default_timeout(timeout_seconds * 1000)
        try:
            await _login(page)
            return await _query_discover(page, index, query, must_not_contain_level)
        except Exception as e:
            log.warning("kibana_check_failed", error=str(e))
            return {"hits": 0, "errors": 0, "reason": str(e)}
        finally:
            await context.close()
            await browser.close()


async def _login(page) -> None:
    await page.goto(settings.KIBANA_URL, wait_until="domcontentloaded")
    if not settings.KIBANA_USERNAME:
        return
    user_sel = ['input[name="username"]', 'input[data-test-subj="loginUsername"]']
    pw_sel = ['input[name="password"]', 'input[data-test-subj="loginPassword"]']
    submit_sel = ['button[data-test-subj="loginSubmit"]', 'button[type="submit"]']
    for s in user_sel:
        if await page.locator(s).count():
            await page.fill(s, settings.KIBANA_USERNAME)
            break
    for s in pw_sel:
        if await page.locator(s).count():
            await page.fill(s, settings.KIBANA_PASSWORD or "")
            break
    for s in submit_sel:
        if await page.locator(s).count():
            await page.locator(s).first.click()
            break
    await page.wait_for_load_state("networkidle")


async def _query_discover(page, index: str, query: str, must_not_contain_level: Optional[str]) -> Dict[str, Any]:
    # Discover URL — index pattern + KQL query.
    # שימוש ב-app/discover עם _a state פרמטר.
    discover_url = (
        f"{settings.KIBANA_URL.rstrip('/')}/app/discover#/?"
        f"_a=(index:'{quote(index)}',query:(language:kuery,query:'{quote(query)}'))"
    )
    await page.goto(discover_url, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")

    hits = 0
    sample = None
    try:
        # Kibana בדרך כלל מציג hit count
        hit_text = await page.locator('[data-test-subj="discoverQueryHits"]').first.text_content()
        if hit_text:
            hits = int("".join(c for c in hit_text if c.isdigit()) or 0)
    except Exception:
        pass

    errors = 0
    if must_not_contain_level:
        try:
            content = await page.content()
            errors = content.count(f'"level":"{must_not_contain_level}"')
            errors += content.count(f'level: {must_not_contain_level}')
        except Exception:
            pass

    try:
        first_row = await page.locator('[data-test-subj="docTableField"]').first.text_content()
        sample = (first_row or "")[:300]
    except Exception:
        pass

    reason = None
    if hits == 0:
        reason = "לא נמצאו לוגים תואמים"
    elif errors > 0:
        reason = f"נמצאו {errors} לוגי {must_not_contain_level}"

    return {"hits": hits, "errors": errors, "sample": sample, "reason": reason}
