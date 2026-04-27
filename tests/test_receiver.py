import httpx

from hookline.receiver import make_receiver
from hookline.signing import sign


async def test_fails_n_times_per_event_then_accepts() -> None:
    lines: list[str] = []
    app = make_receiver(fail=2, secret="whsec_demo", log=lines.append)
    body = b'{"type":"order.paid"}'
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://r"
    ) as client:
        codes = []
        for attempt in range(1, 4):
            headers = {
                "Hookline-Signature": sign(body, "whsec_demo"),
                "Hookline-Event-Id": "evt_1",
                "Hookline-Attempt": str(attempt),
            }
            codes.append((await client.post("/", content=body, headers=headers)).status_code)
        bad = await client.post("/", content=body, headers={"Hookline-Signature": "t=1,v1=x"})
    assert codes == [503, 503, 200]
    assert bad.status_code == 401
    assert "accepted order.paid" in lines[2]
