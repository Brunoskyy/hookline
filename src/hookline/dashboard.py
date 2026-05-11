"""A small server-rendered dashboard: what is pending, what failed, and why."""

import base64
import hmac
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from hookline import service
from hookline.config import Settings
from hookline.models import Delivery, DeliveryStatus, Endpoint


def ago(value: datetime | None, now: datetime | None = None) -> str:
    """'3m ago', 'in 40s'. Short on purpose; the full time is in the title attribute."""
    if value is None:
        return ""
    current = now or datetime.now(UTC)
    seconds = int((value - current).total_seconds())
    future = seconds > 0
    s = abs(seconds)
    for size, unit in ((86400, "d"), (3600, "h"), (60, "m")):
        if s >= size:
            text = f"{s // size}{unit}"
            break
    else:
        text = f"{s}s"
    if s < 2:
        return "now"
    return f"in {text}" if future else f"{text} ago"


def span(start: datetime, end: datetime) -> str:
    """'6s', '2m 30s': the gap between an attempt and the retry it scheduled."""
    s = max(0, int((end - start).total_seconds()))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m" + (f" {s % 60}s" if s % 60 else "")
    return f"{s // 3600}h" + (f" {s % 3600 // 60}m" if s % 3600 // 60 else "")


def host(url: str) -> str:
    parts = urlsplit(url)
    return (parts.netloc or url) + (parts.path if parts.path not in ("", "/") else "")


def mount_dashboard(app: FastAPI, settings: Settings, templates_dir: Path) -> None:
    templates = Jinja2Templates(directory=templates_dir)
    templates.env.filters["ago"] = ago
    templates.env.filters["host"] = host
    templates.env.filters["span"] = span
    templates.env.globals["DeliveryStatus"] = DeliveryStatus
    app.mount(
        "/dashboard/static",
        StaticFiles(directory=templates_dir.parent / "static"),
        name="dashboard-static",
    )

    def basic_auth(authorization: Annotated[str | None, Header()] = None) -> None:
        if not settings.api_key:
            return
        ok = False
        if authorization and authorization.startswith("Basic "):
            try:
                decoded = base64.b64decode(authorization[6:]).decode()
            except ValueError:
                decoded = ""
            _, _, password = decoded.partition(":")
            ok = hmac.compare_digest(password.encode(), settings.api_key.encode())
        if not ok:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "dashboard password required",
                headers={"WWW-Authenticate": 'Basic realm="hookline"'},
            )

    guarded = [Depends(basic_auth)]

    async def counts(request: Request) -> dict[str, int]:
        async with request.app.state.runtime.sessionmaker() as session:
            rows = await session.execute(
                select(Delivery.status, func.count()).group_by(Delivery.status)
            )
            by_status = {s.value: 0 for s in DeliveryStatus}
            for status_value, n in rows.all():
                by_status[DeliveryStatus(status_value).value] = n
            since = datetime.now(UTC) - timedelta(hours=24)
            finished = await session.execute(
                select(Delivery.status, func.count())
                .where(Delivery.finished_at >= since)
                .group_by(Delivery.status)
            )
            done = {DeliveryStatus(s).value: n for s, n in finished.all()}
        total = done.get("delivered", 0) + done.get("dead", 0)
        by_status["rate"] = round(100 * done.get("delivered", 0) / total) if total else -1
        return by_status

    async def rows(request: Request, status_filter: DeliveryStatus | None) -> list[Delivery]:
        async with request.app.state.runtime.sessionmaker() as session:
            query = (
                select(Delivery)
                .options(selectinload(Delivery.event), selectinload(Delivery.endpoint))
                .order_by(Delivery.created_at.desc())
                .limit(100)
            )
            if status_filter is not None:
                query = query.where(Delivery.status == status_filter)
            return list(await session.scalars(query))

    @app.get("/dashboard", response_class=HTMLResponse, dependencies=guarded)
    async def deliveries(
        request: Request,
        status_filter: Annotated[DeliveryStatus | None, Query(alias="status")] = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "deliveries.html",
            {
                "deliveries": await rows(request, status_filter),
                "counts": await counts(request),
                "status_filter": status_filter,
                "now": datetime.now(UTC),
                "page": "deliveries",
            },
        )

    @app.get("/dashboard/rows", response_class=HTMLResponse, dependencies=guarded)
    async def deliveries_rows(
        request: Request,
        status_filter: Annotated[DeliveryStatus | None, Query(alias="status")] = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "_live.html",
            {
                "deliveries": await rows(request, status_filter),
                "counts": await counts(request),
                "status_filter": status_filter,
                "now": datetime.now(UTC),
            },
        )

    @app.get(
        "/dashboard/deliveries/{delivery_id}", response_class=HTMLResponse, dependencies=guarded
    )
    async def delivery_detail(request: Request, delivery_id: uuid.UUID) -> HTMLResponse:
        from hookline.api import load_delivery
        from hookline.delivery import envelope

        async with request.app.state.runtime.sessionmaker() as session:
            delivery = await load_delivery(session, delivery_id)
        if delivery is None:
            raise HTTPException(404, "delivery not found")
        return templates.TemplateResponse(
            request,
            "delivery.html",
            {
                "d": delivery,
                "body": json.dumps(json.loads(envelope(delivery.event)), indent=2),
                "now": datetime.now(UTC),
                "page": "deliveries",
            },
        )

    @app.post(
        "/dashboard/deliveries/{delivery_id}/replay",
        response_class=HTMLResponse,
        dependencies=guarded,
    )
    async def delivery_replay(
        request: Request,
        delivery_id: uuid.UUID,
        hx_request: Annotated[str | None, Header()] = None,
    ) -> HTMLResponse:
        # A custom header a cross-site form cannot set: cheap protection against CSRF.
        if hx_request != "true":
            raise HTTPException(400, "replays come from the dashboard")
        async with request.app.state.runtime.sessionmaker() as session:
            try:
                await service.replay(session, request.app.state.runtime.queue, delivery_id)
            except service.NotFoundError as e:
                raise HTTPException(404, "delivery not found") from e
        return HTMLResponse("", headers={"HX-Refresh": "true"})

    @app.get("/dashboard/endpoints", response_class=HTMLResponse, dependencies=guarded)
    async def endpoints(request: Request) -> HTMLResponse:
        async with request.app.state.runtime.sessionmaker() as session:
            eps = list(await session.scalars(select(Endpoint).order_by(Endpoint.created_at)))
            stats = dict(
                (
                    await session.execute(
                        select(Delivery.endpoint_id, func.count()).group_by(Delivery.endpoint_id)
                    )
                ).all()
            )
        return templates.TemplateResponse(
            request,
            "endpoints.html",
            {"endpoints": eps, "stats": stats, "now": datetime.now(UTC), "page": "endpoints"},
        )
