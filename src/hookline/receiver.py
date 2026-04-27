"""A receiver for demos and tests: fails the first N tries of every event, then accepts.

    hookline receiver --fail 2 --secret whsec_...

With a secret it also verifies each request's signature and says so, which makes it a working
example of the receiving side.
"""

import json
from collections import Counter
from collections.abc import Awaitable, Callable

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from hookline.signing import HEADER, SignatureError, verify

Log = Callable[[str], None]


def make_receiver(
    fail: int = 2, secret: str | None = None, *, status_code: int = 503, log: Log = print
) -> Starlette:
    seen: Counter[str] = Counter()

    async def receive(request: Request) -> Response:
        body = await request.body()
        event_id = request.headers.get("hookline-event-id", "?")
        attempt = request.headers.get("hookline-attempt", "?")
        if secret is not None:
            try:
                verify(body, request.headers.get(HEADER), secret)
            except SignatureError as e:
                log(f"  {event_id[:8]} attempt {attempt}: rejected, {e}")
                return JSONResponse({"error": str(e)}, status_code=401)
        seen[event_id] += 1
        if seen[event_id] <= fail:
            log(f"  {event_id[:8]} attempt {attempt}: failing on purpose ({seen[event_id]}/{fail})")
            return JSONResponse({"error": "not today"}, status_code=status_code)
        kind = json.loads(body).get("type", "?") if body else "?"
        log(f"  {event_id[:8]} attempt {attempt}: accepted {kind}")
        return JSONResponse({"received": event_id})

    handler: Callable[[Request], Awaitable[Response]] = receive
    return Starlette(routes=[Route("/", handler, methods=["POST"])])
