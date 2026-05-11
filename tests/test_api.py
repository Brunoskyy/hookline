import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy import select

from hookline.models import Delivery
from hookline.runtime import Runtime

from .conftest import FakeReceiver, drain


async def add_endpoint(api: httpx.AsyncClient, **body: object) -> dict[str, object]:
    payload = {"url": "http://127.0.0.1:9000/hook", **body}
    r = await api.post("/v1/endpoints", json=payload)
    assert r.status_code == 201, r.text
    data: dict[str, object] = r.json()
    return data


async def test_requires_the_api_key(api: httpx.AsyncClient) -> None:
    r = await api.get("/v1/endpoints", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401
    r = await api.get("/v1/endpoints", headers={"Authorization": ""})
    assert r.status_code == 401


async def test_endpoint_secret_is_shown_once(api: httpx.AsyncClient) -> None:
    created = await add_endpoint(api, event_types=["order.paid", "order.refunded"])
    assert str(created["secret"]).startswith("whsec_")
    listed = (await api.get("/v1/endpoints")).json()
    assert listed[0]["id"] == created["id"]
    assert "secret" not in listed[0]
    assert listed[0]["event_types"] == ["order.paid", "order.refunded"]


async def test_rejects_private_urls_unless_allowed(
    api: httpx.AsyncClient, runtime: Runtime
) -> None:
    runtime.settings.allow_private_urls = False
    try:
        r = await api.post("/v1/endpoints", json={"url": "http://169.254.169.254/"})
        assert r.status_code == 422
        assert "non-public" in r.json()["detail"]
    finally:
        runtime.settings.allow_private_urls = True


async def test_fans_out_to_subscribed_endpoints_only(
    api: httpx.AsyncClient, runtime: Runtime
) -> None:
    await add_endpoint(api, event_types=["order.paid"])
    await add_endpoint(api, event_types=["*"])
    await add_endpoint(api, event_types=["user.created"])
    r = await api.post("/v1/events", json={"type": "order.paid", "data": {"order": 42}})
    assert r.status_code == 201
    assert len(r.json()["deliveries"]) == 2


async def test_idempotency_key(api: httpx.AsyncClient) -> None:
    await add_endpoint(api)
    body = {"type": "order.paid", "data": {"order": 1}}
    first = await api.post("/v1/events", json=body, headers={"Idempotency-Key": "k1"})
    again = await api.post("/v1/events", json=body, headers={"Idempotency-Key": "k1"})
    assert first.status_code == 201
    assert again.status_code == 200
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert again.json()["deliveries"] == first.json()["deliveries"]
    other = await api.post(
        "/v1/events", json={**body, "data": {"order": 2}}, headers={"Idempotency-Key": "k1"}
    )
    assert other.status_code == 409


@pytest.mark.parametrize(
    "body",
    [
        {"type": "Order Paid", "data": {}},
        {"type": "order.paid"},
        {"type": "order.paid", "data": [1, 2]},
    ],
)
async def test_validates_events(api: httpx.AsyncClient, body: dict[str, object]) -> None:
    assert (await api.post("/v1/events", json=body)).status_code == 422


async def test_refuses_large_bodies(api: httpx.AsyncClient, runtime: Runtime) -> None:
    big = {"type": "blob", "data": {"x": "a" * (runtime.settings.max_payload_bytes + 1)}}
    assert (await api.post("/v1/events", json=big)).status_code == 413


async def test_refuses_large_chunked_bodies(api: httpx.AsyncClient, runtime: Runtime) -> None:
    limit = runtime.settings.max_payload_bytes

    async def chunks() -> AsyncIterator[bytes]:
        yield b'{"type":"blob","data":{"x":"'
        for _ in range(limit // 1000 + 2):
            yield b"a" * 1000
        yield b'"}}'

    r = await api.post("/v1/events", content=chunks(), headers={"Content-Type": "application/json"})
    assert r.status_code == 413


async def test_delivery_timeline_and_replay(
    api: httpx.AsyncClient, runtime: Runtime, receiver: FakeReceiver
) -> None:
    await add_endpoint(api)
    event = (await api.post("/v1/events", json={"type": "order.paid", "data": {}})).json()
    delivery_id = event["deliveries"][0]
    await drain(runtime)

    detail = (await api.get(f"/v1/deliveries/{delivery_id}")).json()
    assert detail["status"] == "delivered"
    assert detail["event_type"] == "order.paid"
    assert [a["status_code"] for a in detail["attempts"]] == [200]

    replayed = await api.post(f"/v1/deliveries/{delivery_id}/replay")
    assert replayed.json()["status"] == "pending"
    assert replayed.json()["run_attempts"] == 0
    await drain(runtime)
    detail = (await api.get(f"/v1/deliveries/{delivery_id}")).json()
    assert [a["number"] for a in detail["attempts"]] == [1, 2]
    assert detail["total_attempts"] == 2
    assert len(receiver.requests) == 2

    listed = (await api.get("/v1/deliveries", params={"status": "delivered"})).json()
    assert [d["id"] for d in listed] == [delivery_id]
    assert (await api.get(f"/v1/deliveries/{uuid.uuid4()}")).status_code == 404
    assert (await api.post(f"/v1/deliveries/{uuid.uuid4()}/replay")).status_code == 404


async def test_disabled_endpoints_get_nothing_new(api: httpx.AsyncClient, runtime: Runtime) -> None:
    ep = await add_endpoint(api)
    await api.patch(f"/v1/endpoints/{ep['id']}", json={"active": False})
    r = await api.post("/v1/events", json={"type": "order.paid", "data": {}})
    assert r.json()["deliveries"] == []
    async with runtime.sessionmaker() as session:
        assert (await session.scalars(select(Delivery))).all() == []


async def test_dashboard_renders_and_guards_replay(
    api: httpx.AsyncClient, runtime: Runtime
) -> None:
    await add_endpoint(api)
    event = (await api.post("/v1/events", json={"type": "order.paid", "data": {"n": 1}})).json()
    await drain(runtime)
    auth = httpx.BasicAuth("admin", "test-key")
    assert (await api.get("/dashboard", headers={"Authorization": ""})).status_code == 401
    page = await api.get("/dashboard", auth=auth)
    assert page.status_code == 200
    assert "order.paid" in page.text
    detail = await api.get(f"/dashboard/deliveries/{event['deliveries'][0]}", auth=auth)
    assert "#1" in detail.text and "200" in detail.text
    replay = f"/dashboard/deliveries/{event['deliveries'][0]}/replay"
    assert (await api.post(replay, auth=auth)).status_code == 400
    ok = await api.post(replay, auth=auth, headers={"HX-Request": "true"})
    assert ok.status_code == 200
    assert (await api.get("/dashboard/endpoints", auth=auth)).status_code == 200
    assert (await api.get("/dashboard/rows", auth=auth)).status_code == 200


async def test_non_ascii_credentials_are_just_wrong(api: httpx.AsyncClient) -> None:
    r = await api.get("/v1/endpoints", headers=[(b"Authorization", "Bearer chave-ç".encode())])
    assert r.status_code == 401
    basic = httpx.BasicAuth("admin", "senha-ç")
    assert (await api.get("/dashboard", auth=basic)).status_code == 401


def test_span_formatting() -> None:
    from datetime import UTC, datetime, timedelta

    from hookline.dashboard import span

    t = datetime(2026, 1, 1, tzinfo=UTC)
    assert span(t, t + timedelta(seconds=6)) == "6s"
    assert span(t, t + timedelta(seconds=150)) == "2m 30s"
    assert span(t, t + timedelta(hours=3)) == "3h"
