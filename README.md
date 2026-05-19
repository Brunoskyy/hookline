<p align="center">
  <img src="docs/logo.svg" width="76" alt="">
</p>

<h1 align="center">Hookline</h1>

<p align="center">
  Webhook delivery with signatures, retries, a dead-letter queue and a record of every attempt.<br>
  <sub>Python 3.13 · FastAPI · SQLAlchemy 2 · Postgres · SQS · AWS Lambda · Terraform</sub>
</p>

<br>

Sending a webhook is one HTTP request. Sending it reliably is everything around that request:
signing it so the receiver knows it came from you, trying again when the other side is down,
not trying forever, not hammering an endpoint that is clearly broken, and being able to answer
"did customer X get event Y, and what did their server say?" a week later.

Hookline is that part. You post an event; it fans out to every endpoint subscribed to that
type, signs each request, retries with backoff, parks what never succeeds in a dead-letter
state you can replay, and keeps every attempt with its status code, timing and response.

<p align="center">
  <img src="docs/screenshots/deliveries-light.jpg" width="49%" alt="Delivery list with pending, delivered and dead-lettered rows">
  <img src="docs/screenshots/timeline-dark.jpg" width="49%" alt="One delivery's attempts: two 503s with the backoff gap between them, then a 200">
</p>

## Running it

Python 3.13 and [uv](https://docs.astral.sh/uv/). Postgres for the real thing; SQLite works for
a quick look.

```bash
uv sync
cp .env.example .env                 # point HOOKLINE_DATABASE_URL at your Postgres
uv run alembic upgrade head
uv run hookline serve                # API + dashboard on http://127.0.0.1:8000
uv run hookline worker               # in another terminal
```

Or everything at once, including a receiver that fails twice per event before accepting:

```bash
docker compose up
```

Then, with `HOOKLINE_ALLOW_PRIVATE_URLS=true` so a local receiver is allowed:

```bash
curl -X POST localhost:8000/v1/endpoints -H 'Authorization: Bearer dev-key' \
  -H 'content-type: application/json' \
  -d '{"url": "http://127.0.0.1:9000/", "event_types": ["invoice.paid"]}'
# -> {"id": "...", "secret": "whsec_...", ...}   the secret is shown only here

uv run hookline receiver --fail 2 --secret whsec_...

curl -X POST localhost:8000/v1/events -H 'Authorization: Bearer dev-key' \
  -H 'Idempotency-Key: inv_1042-paid' -H 'content-type: application/json' \
  -d '{"type": "invoice.paid", "data": {"invoice": "inv_1042", "amount": 12900}}'
```

The receiver prints two refusals and an acceptance; the dashboard shows the three attempts and
the gaps between them. Sending the same event again with the same `Idempotency-Key` returns the
first one instead of creating a second.

| Command | |
| --- | --- |
| `uv run pytest` | the test suite; set `HOOKLINE_TEST_DATABASE_URL` to include the Postgres tests |
| `uv run ruff check && uv run mypy` | lint and strict type checking |
| `uv run hookline receiver --fail N` | a demo endpoint that fails N times per event |
| `./infra/build.sh` | the Lambda package, `dist/lambda.zip` |

## How delivery works

```
POST /v1/events ──> event + one delivery per subscribed endpoint
                         │
          worker claims due rows  (FOR UPDATE SKIP LOCKED, with a lease)
                         │
            sign, POST, read ≤ 2 KB of the response
              │                 │                     │
             2xx        failure, attempts left     last attempt failed
              │                 │                     │
          delivered     next try = backoff         dead-lettered
                        (or Retry-After)           (replayable)
```

- **The deliveries table is the queue.** A worker claims due rows with
  `SELECT ... FOR UPDATE SKIP LOCKED` and takes a lease on them. Any number of workers can poll
  without two of them getting the same row. If a worker dies mid-request its lease runs out and
  someone else picks the delivery up, so delivery is at-least-once, never stuck. Receivers
  dedupe on `Hookline-Event-Id`.
- **Backoff is exponential with jitter**: 30 s, 1 min, 2 min... capped at six hours, with half
  of each delay random so a batch that failed together does not come back together. A
  `Retry-After` header wins when there is one. Eight attempts by default, then the delivery is
  dead-lettered with its last error, and a replay starts a fresh run.
- **A circuit breaker per endpoint.** After five consecutive failures, across all its
  deliveries, the endpoint rests for a minute. Deliveries that come due meanwhile are postponed
  without spending an attempt. The first try after the rest is a probe: if it fails the rest
  doubles, if it succeeds everything resets.
- **The database's clock decides.** Due times, leases and circuits all compare against
  `now()` in Postgres, not the worker's clock, so workers on skewed hosts still agree.
- **No redirects, no private addresses.** Endpoint URLs that resolve to loopback, private,
  link-local (the cloud metadata service) or other non-public space are refused, when the
  endpoint is registered and again before every attempt. Redirects are not followed, since a
  public URL could redirect somewhere private.

## Signatures

Every request carries a header like:

```
Hookline-Signature: t=1727712000,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd
```

`v1` is the hex HMAC-SHA256 of `"{t}.{body}"` under the endpoint's secret. Signing the timestamp
with the body lets the receiver refuse a replay of an old request. During a secret rotation the
header carries one `v1` per secret, and any match is enough.

Verifying on the receiving side, with the helper this package ships:

```python
from hookline import verify, SignatureError

@app.post("/webhooks")
async def receive(request: Request):
    body = await request.body()
    try:
        verify(body, request.headers.get("Hookline-Signature"), secret=WEBHOOK_SECRET)
    except SignatureError:
        return Response(status_code=401)
    ...
```

Or in any language: split the header on commas, check `t` is within five minutes of now,
compute the HMAC, and compare in constant time.

## Deploying to AWS

`infra/` has the Terraform (checked with `tofu validate`, not applied from this repository):

- **API Gateway (HTTP API) → Lambda** running the FastAPI app through Mangum.
- **SQS → Lambda** for the worker. With `HOOKLINE_QUEUE=sqs` the same queue interface sends
  delivery ids to SQS instead of polling Postgres. Messages only say "look at delivery X"; the
  database still decides whether it is due, so a duplicate or early message is harmless.
  Delays over SQS's 15-minute limit are sent in hops. The worker reports per-message failures,
  and a poison-message queue catches anything that cannot be processed at all.
- **RDS Postgres** in private subnets, reachable only from the functions.
- A concurrency cap on the worker so a burst of events cannot exhaust the database's
  connections.

```bash
./infra/build.sh
cd infra && tofu init && tofu apply \
  -var vpc_id=vpc-... -var 'private_subnet_ids=["subnet-a","subnet-b"]' \
  -var api_key=... -var db_password=...
alembic upgrade head   # against the new database, from a host that can reach it
```

The subnets need a NAT gateway: the worker has to reach the internet to deliver anything.

## Things worth opening

**`src/hookline/delivery.py`.** One attempt, start to finish, and every decision taken from its
outcome: delivered, retry when, or give up. The session is closed while the request is in
flight; the lease is checked again before anything is written, so a worker that was merely
slow records what happened without overruling the worker that took over.

**`src/hookline/queues.py`.** The two queues behind one three-method interface. The Postgres
claim is a single `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING id`.

**`src/hookline/signing.py`.** Forty lines, and the part other people's code depends on, so it
has a test that pins the exact bytes being signed.

**`src/hookline/urls.py`.** Why a webhook service is an SSRF tool unless it is careful, and
what "careful" covers.

**`src/hookline/service.py`.** Idempotency keys: two concurrent requests with the same key race
on a unique index, and the loser reads the winner's event. A reused key with a different body
is a 409, not a silent no-op.

## Tests

```bash
uv run pytest
HOOKLINE_TEST_DATABASE_URL=postgresql+asyncpg://localhost/hookline_test uv run pytest
```

77 tests. Without a database they run on SQLite: signatures (including a pinned vector and
rotation), the backoff schedule and `Retry-After`, the URL checks, every API route, body limits
that hold even when `Content-Length` lies, the dashboard, and the delivery engine against a
scripted fake receiver: retry and deliver, dead-letter after the last attempt, redirects,
blocked addresses, the circuit opening, postponing, reopening for longer and resetting, and a
worker that loses its lease mid-request. With Postgres, three more: eight workers claiming at
once never get the same row, ten concurrent publishes with one idempotency key make one event,
and concurrent failures to one endpoint are all counted. The SQS queue and the Lambda handler
run against moto.

## Layout

```
src/hookline/
  signing.py     sign() and verify()
  retry.py       backoff and Retry-After
  urls.py        which addresses may be delivered to
  models.py      endpoints, events, deliveries, attempts
  queues.py      Postgres SKIP LOCKED queue and SQS queue
  delivery.py    one attempt and its consequences
  service.py     publish with idempotency, replay
  worker.py      the claim-and-deliver loop
  api.py         the HTTP API
  dashboard.py   server-rendered dashboard (Jinja + htmx)
  aws.py         Lambda handlers for API Gateway and SQS
  receiver.py    the demo receiver
migrations/      Alembic
infra/           Terraform for API Gateway, Lambda, SQS, RDS
```

## What's missing

- One API key for everything. A real multi-tenant service needs per-tenant keys and endpoints
  scoped to them.
- The address check and the request resolve DNS separately, so a name that changes answer in
  between (DNS rebinding) could still slip through. Closing that means connecting to the
  address that was checked, which needs a custom transport.
- Secrets are stored as given. They should be encrypted at rest, and the Terraform passes the
  database password through a variable where Secrets Manager would be better.
- No rate limit per endpoint beyond the circuit breaker.
