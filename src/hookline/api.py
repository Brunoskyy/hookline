import hmac
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from hookline import service
from hookline.config import Settings, get_settings
from hookline.models import Delivery, DeliveryStatus, Endpoint, Event
from hookline.runtime import Runtime, build_runtime
from hookline.schemas import (
    AttemptOut,
    DeliveryDetail,
    DeliveryOut,
    EndpointCreated,
    EndpointIn,
    EndpointOut,
    EndpointPatch,
    EventIn,
    EventOut,
)
from hookline.urls import UnsafeURLError, check_url

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class BodyLimit:
    """Refuses request bodies over ``limit`` bytes, whether or not Content-Length is honest."""

    def __init__(self, app: ASGIApp, limit: int) -> None:
        self.app = app
        self.limit = limit

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers", []):
            if name == b"content-length" and value.isdigit() and int(value) > self.limit:
                await _too_large(self.limit)(scope, receive, send)
                return
        # Read the whole body first (it is small by definition), then hand it to the app.
        # Stopping mid-stream would leave the app to answer a half-read request itself.
        chunks: list[bytes] = []
        seen = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            chunk = message.get("body", b"")
            seen += len(chunk)
            if seen > self.limit:
                await _too_large(self.limit)(scope, receive, send)
                return
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        body = b"".join(chunks)
        delivered = False

        async def replay() -> Message:
            nonlocal delivered
            if delivered:
                return await receive()
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay, send)


def _too_large(limit: int) -> JSONResponse:
    return JSONResponse({"detail": f"request body over {limit} bytes"}, status_code=413)


def create_app(settings: Settings | None = None, runtime: Runtime | None = None) -> FastAPI:
    cfg = settings or (runtime.settings if runtime else get_settings())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = runtime is None
        rt = runtime or build_runtime(cfg)
        app.state.runtime = rt
        yield
        if owned:
            await rt.close()

    app = FastAPI(
        title="Hookline",
        version="0.1.0",
        summary="Webhook delivery with signatures, retries and a record of every attempt.",
        lifespan=lifespan,
    )
    app.add_middleware(BodyLimit, limit=cfg.max_payload_bytes)
    if runtime is not None:
        app.state.runtime = runtime

    def rt(request: Request) -> Runtime:
        value: Runtime = request.app.state.runtime
        return value

    async def db(request: Request) -> AsyncIterator[AsyncSession]:
        async with rt(request).sessionmaker() as session:
            yield session

    def auth(authorization: Annotated[str | None, Header()] = None) -> None:
        if not cfg.api_key:
            return
        expected = f"Bearer {cfg.api_key}"
        given = (authorization or "").encode()
        if not hmac.compare_digest(given, expected.encode()):
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "missing or wrong API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

    SessionDep = Annotated[AsyncSession, Depends(db)]  # noqa: N806 - a type alias
    guarded = [Depends(auth)]

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/dashboard")

    # Endpoints ---------------------------------------------------------------------------

    @app.post("/v1/endpoints", status_code=201, dependencies=guarded)
    async def create_endpoint(body: EndpointIn, session: SessionDep) -> EndpointCreated:
        try:
            await check_url(body.url, allow_private=cfg.allow_private_urls)
        except UnsafeURLError as e:
            raise HTTPException(422, str(e)) from e
        endpoint = Endpoint(
            url=body.url,
            description=body.description,
            event_types=body.event_types,
            secret=service.new_secret(),
        )
        session.add(endpoint)
        await session.commit()
        return EndpointCreated.model_validate(
            {**EndpointOut.model_validate(endpoint).model_dump(), "secret": endpoint.secret}
        )

    @app.get("/v1/endpoints", dependencies=guarded)
    async def list_endpoints(session: SessionDep) -> list[EndpointOut]:
        rows = await session.scalars(select(Endpoint).order_by(Endpoint.created_at))
        return [EndpointOut.model_validate(e) for e in rows]

    @app.patch("/v1/endpoints/{endpoint_id}", dependencies=guarded)
    async def patch_endpoint(
        endpoint_id: uuid.UUID, body: EndpointPatch, session: SessionDep
    ) -> EndpointOut:
        endpoint = await session.get(Endpoint, endpoint_id)
        if endpoint is None:
            raise HTTPException(404, "endpoint not found")
        if body.active is not None:
            endpoint.active = body.active
        if body.description is not None:
            endpoint.description = body.description
        await session.commit()
        return EndpointOut.model_validate(endpoint)

    # Events ------------------------------------------------------------------------------

    @app.post("/v1/events", status_code=201, dependencies=guarded)
    async def publish(
        body: EventIn,
        request: Request,
        response: Response,
        session: SessionDep,
        idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
    ) -> EventOut:
        if idempotency_key is not None and not idempotency_key.strip():
            raise HTTPException(400, "Idempotency-Key is empty; omit the header or give it a value")
        try:
            event, created = await service.publish(
                session,
                rt(request).queue,
                event_type=body.type,
                payload=body.data,
                idempotency_key=idempotency_key,
            )
        except service.IdempotencyConflictError as e:
            raise HTTPException(
                409, "this Idempotency-Key was already used with a different request"
            ) from e
        if not created:
            response.status_code = 200
            response.headers["Idempotent-Replayed"] = "true"
        return await _event_out(session, event.id)

    @app.get("/v1/events/{event_id}", dependencies=guarded)
    async def get_event(event_id: uuid.UUID, session: SessionDep) -> EventOut:
        return await _event_out(session, event_id)

    async def _event_out(session: AsyncSession, event_id: uuid.UUID) -> EventOut:
        event = await session.scalar(
            select(Event).where(Event.id == event_id).options(selectinload(Event.deliveries))
        )
        if event is None:
            raise HTTPException(404, "event not found")
        return EventOut(
            id=event.id,
            type=event.type,
            created_at=event.created_at,
            data=event.payload,
            deliveries=[d.id for d in event.deliveries],
        )

    # Deliveries --------------------------------------------------------------------------

    @app.get("/v1/deliveries", dependencies=guarded)
    async def list_deliveries(
        session: SessionDep,
        status_: Annotated[DeliveryStatus | None, Query(alias="status")] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[DeliveryOut]:
        query = select(Delivery).order_by(Delivery.created_at.desc()).limit(limit)
        if status_ is not None:
            query = query.where(Delivery.status == status_)
        return [DeliveryOut.model_validate(d) for d in await session.scalars(query)]

    @app.get("/v1/deliveries/{delivery_id}", dependencies=guarded)
    async def get_delivery(delivery_id: uuid.UUID, session: SessionDep) -> DeliveryDetail:
        delivery = await load_delivery(session, delivery_id)
        if delivery is None:
            raise HTTPException(404, "delivery not found")
        return detail(delivery)

    @app.post("/v1/deliveries/{delivery_id}/replay", dependencies=guarded)
    async def replay(delivery_id: uuid.UUID, request: Request, session: SessionDep) -> DeliveryOut:
        try:
            delivery = await service.replay(session, rt(request).queue, delivery_id)
        except service.NotFoundError as e:
            raise HTTPException(404, "delivery not found") from e
        return DeliveryOut.model_validate(delivery)

    from hookline.dashboard import mount_dashboard

    mount_dashboard(app, cfg, Path(__file__).parent / "templates")
    return app


async def load_delivery(session: AsyncSession, delivery_id: uuid.UUID) -> Delivery | None:
    return await session.scalar(
        select(Delivery)
        .where(Delivery.id == delivery_id)
        .options(
            selectinload(Delivery.attempts),
            selectinload(Delivery.event),
            selectinload(Delivery.endpoint),
        )
    )


def detail(delivery: Delivery) -> DeliveryDetail:
    base = DeliveryOut.model_validate(delivery).model_dump()
    return DeliveryDetail(
        **base,
        event_type=delivery.event.type,
        endpoint_url=delivery.endpoint.url,
        attempts=[AttemptOut.model_validate(a) for a in delivery.attempts],
    )
