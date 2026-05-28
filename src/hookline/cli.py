import argparse
import asyncio
import logging
import signal
import sys

from hookline.config import get_settings


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("hookline.api:create_app", factory=True, host=args.host, port=args.port)


async def _worker() -> None:
    from hookline import worker
    from hookline.queues import reconcile
    from hookline.runtime import build_runtime

    settings = get_settings()
    runtime = build_runtime(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    logging.getLogger("hookline").info("worker started, queue=%s", settings.queue)
    try:
        await worker.run(
            runtime.queue,
            runtime.deliverer(),
            batch_size=settings.worker_batch_size,
            concurrency=settings.worker_concurrency,
            idle_seconds=settings.worker_idle_seconds,
            stop=stop,
            reconcile=lambda: reconcile(
                runtime.sessionmaker,
                runtime.queue,
                grace_seconds=settings.reconcile_grace_seconds,
                limit=settings.reconcile_batch_size,
            ),
            reconcile_interval=settings.reconcile_interval_seconds,
        )
    finally:
        await runtime.close()


async def _init_db() -> None:
    from hookline.db import create_all, make_engine

    engine = make_engine(get_settings().database_url)
    await create_all(engine)
    await engine.dispose()


def _receiver(args: argparse.Namespace) -> None:
    import uvicorn

    from hookline.receiver import make_receiver

    print(f"receiver on http://{args.host}:{args.port}/, failing the first {args.fail} tries")
    app = make_receiver(args.fail, args.secret, status_code=args.status)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="hookline")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the API and the dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("worker", help="deliver due webhooks until stopped")
    sub.add_parser("init-db", help="create the tables (Alembic does this in production)")

    receiver = sub.add_parser("receiver", help="a demo endpoint that fails a few times first")
    receiver.add_argument("--host", default="127.0.0.1")
    receiver.add_argument("--port", type=int, default=9000)
    receiver.add_argument("--fail", type=int, default=2)
    receiver.add_argument("--status", type=int, default=503)
    receiver.add_argument("--secret", default=None, help="verify signatures with this secret")

    args = parser.parse_args(argv)
    if args.command == "serve":
        _serve(args)
    elif args.command == "worker":
        asyncio.run(_worker())
    elif args.command == "init-db":
        asyncio.run(_init_db())
    elif args.command == "receiver":
        _receiver(args)
    else:  # pragma: no cover
        parser.print_help()
        sys.exit(2)
