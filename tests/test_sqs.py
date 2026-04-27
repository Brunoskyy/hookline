import uuid
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from hookline.queues import Claim, SQSQueue


@pytest.fixture
def sqs() -> Iterator[tuple[Any, str]]:
    with mock_aws():
        client = boto3.client("sqs", region_name="us-east-1")
        url = client.create_queue(QueueName="hookline")["QueueUrl"]
        yield client, url


async def test_schedule_claim_ack(sqs: tuple[Any, str]) -> None:
    client, url = sqs
    queue = SQSQueue(client, url, wait_seconds=0)
    ids = [uuid.uuid4() for _ in range(3)]
    for i in ids:
        await queue.schedule(i, 0)
    claims = await queue.claim(10)
    assert sorted(c.delivery_id for c in claims) == sorted(ids)
    assert all(c.receipt and c.lease_token is None for c in claims)
    for c in claims:
        await queue.ack(c)
    attrs = client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    assert attrs["ApproximateNumberOfMessages"] == "0"


async def test_long_delays_are_sent_in_hops(sqs: tuple[Any, str]) -> None:
    client, url = sqs
    sent: list[dict[str, Any]] = []
    original = client.send_message

    def spy(**kwargs: Any) -> Any:
        sent.append(kwargs)
        return original(**kwargs)

    client.send_message = spy
    queue = SQSQueue(client, url, wait_seconds=0)
    await queue.schedule(uuid.uuid4(), 6 * 3600)
    await queue.schedule(uuid.uuid4(), -5)
    assert [m["DelaySeconds"] for m in sent] == [900, 0]


async def test_drops_foreign_messages(sqs: tuple[Any, str]) -> None:
    client, url = sqs
    client.send_message(QueueUrl=url, MessageBody="not-a-uuid")
    queue = SQSQueue(client, url, wait_seconds=0)
    assert await queue.claim(10) == []
    assert await queue.claim(10) == []


async def test_ack_without_receipt_is_a_noop(sqs: tuple[Any, str]) -> None:
    client, url = sqs
    await SQSQueue(client, url, wait_seconds=0).ack(Claim(delivery_id=uuid.uuid4()))


async def test_lambda_worker_runs_each_record_and_reports_failures(
    runtime: Any, receiver: Any, sqs: tuple[Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlalchemy import select

    from hookline import aws, service
    from hookline.models import Delivery, DeliveryStatus, Endpoint

    client, url = sqs
    runtime.queue = SQSQueue(client, url, wait_seconds=0)
    monkeypatch.setattr(aws, "_runtime", runtime)
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_l"))
        await session.commit()
        await service.publish(
            session, runtime.queue, event_type="t", payload={}, idempotency_key=None
        )
        [delivery] = await session.scalars(select(Delivery))
    records = [
        {"messageId": "m1", "body": str(delivery.id)},
        {"messageId": "m2", "body": "garbage"},
    ]
    result = await aws._handle(records)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
    async with runtime.sessionmaker() as session:
        done = await session.get(Delivery, delivery.id)
    assert done is not None
    assert done.status is DeliveryStatus.delivered
    # A duplicate message for a finished delivery is a no-op, not an error.
    assert await aws._handle(records[:1]) == {"batchItemFailures": []}
