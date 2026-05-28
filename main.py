"""Entry point — CLI ל-server / test-bridge / test-postman."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(prog="qa-ai-hero")
    parser.add_argument("--server", action="store_true", help="הרץ FastAPI server")
    parser.add_argument("--test-bridge", action="store_true", help="בדוק את CopilotBridge ישירות")
    parser.add_argument("--test-postman", help="טען וסקור Postman collection (path)")
    args = parser.parse_args()

    if args.server:
        _run_server()
    elif args.test_bridge:
        asyncio.run(_test_bridge())
    elif args.test_postman:
        _test_postman(args.test_postman)
    else:
        parser.print_help()


def _run_server() -> None:
    import uvicorn

    from config.settings import settings

    uvicorn.run(
        "server.app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
    )


async def _test_bridge() -> None:
    from agents.copilot_bridge import get_copilot_bridge

    bridge = get_copilot_bridge()
    sid = "test-session"
    print(">>> start_session")
    print(await bridge.start_session(sid))
    print("\n>>> send_document")
    print(await bridge.send_document(sid, "API להוספת מטופל. כותב ל-Kafka topic patient-events.", "spec.txt"))
    print("\n>>> send: 'תקין'")
    print(await bridge.send(sid, "תקין"))
    print("\n>>> send: 'US 123456'")
    msg = await bridge.send(sid, "123456")
    print(msg)
    completion = bridge.is_completion_message(msg)
    print(f"\n>>> completion detected: {completion}")


def _test_postman(path: str) -> None:
    from agents.postman.postman_loader import load_collection_from_file

    p = Path(path)
    if not p.exists():
        print(f"קובץ לא נמצא: {path}", file=sys.stderr)
        sys.exit(1)
    coll = load_collection_from_file(str(p))
    print(f"Collection: {coll.name}")
    print(f"Requests ({len(coll.requests)}):")
    for r in coll.requests:
        print(f"  - [{r.method}] {r.name}")
        print(f"      URL: {r.url_raw}")
        if r.body and r.body.raw:
            preview = r.body.raw[:80].replace("\n", " ")
            print(f"      body preview: {preview}")
    print("\nראשון מלא:")
    if coll.requests:
        print(json.dumps(coll.requests[0].dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
