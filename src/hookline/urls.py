"""Subscriber URL checks.

A webhook service makes HTTP requests to addresses its users type in, which makes it an SSRF
tool unless it refuses the addresses nobody outside should reach: loopback, private ranges,
link-local (including the cloud metadata service at 169.254.169.254), and the rest of the
reserved space. URLs are checked when an endpoint is registered and again, after DNS, right
before every attempt, because a name can resolve somewhere else by then.

Checking is not enough on its own: if the HTTP client resolved the name again when it
connects, a DNS server answering with a TTL of zero could hand the check a public address and
the connection a private one. So the addresses that passed are pinned for the request (see
:class:`PinnedBackend`) and the connection goes to one of them.
"""

import asyncio
import contextvars
import ipaddress
import socket
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit

import httpcore

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

_NAT64 = ipaddress.IPv6Network("64:ff9b::/96")


class UnsafeURLError(ValueError):
    pass


class ResolutionError(UnsafeURLError):
    """The name did not resolve. Unlike a blocked address this may fix itself, so a delivery
    that hits it is retried rather than given up on."""


def embedded_ipv4(ip: ipaddress.IPv6Address) -> ipaddress.IPv4Address | None:
    """The IPv4 address an IPv6 address stands for, when it is one of the forms that route
    to IPv4: mapped (::ffff:a.b.c.d), compatible (::a.b.c.d), NAT64 (64:ff9b::a.b.c.d),
    6to4 and Teredo."""
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip in _NAT64:
        return ipaddress.IPv4Address(ip.packed[12:])
    if ip.packed[:12] == bytes(12) and int(ip) > 1:
        return ipaddress.IPv4Address(ip.packed[12:])
    if ip.sixtofour is not None:
        return ip.sixtofour
    if ip.teredo is not None:
        return ip.teredo[1]
    return None


def _is_public(ip: IPAddress) -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        inner = embedded_ipv4(ip)
        if inner is not None:
            return _is_public(inner)
    return ip.is_global and not ip.is_multicast


def check_syntax(url: str) -> tuple[str, int]:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise UnsafeURLError("only http and https URLs are allowed")
    if not parts.hostname:
        raise UnsafeURLError("URL has no host")
    if parts.username or parts.password:
        raise UnsafeURLError("credentials in the URL are not allowed; use the secret instead")
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError as e:
        raise UnsafeURLError("invalid port") from e
    return parts.hostname, port


async def resolve(host: str, port: int) -> list[IPAddress]:
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise ResolutionError(f"cannot resolve {host}") from e
    return [ipaddress.ip_address(info[4][0]) for info in infos]


async def check_url(url: str, *, allow_private: bool) -> tuple[str, list[IPAddress]]:
    """Raises :class:`UnsafeURLError` if ``url`` may not be delivered to.

    Returns the host and the addresses that passed, for :func:`pinned`. With
    ``allow_private`` nothing is resolved and the list is empty.
    """
    host, port = check_syntax(url)
    if allow_private:
        return host, []
    addresses = await resolve(host, port)
    if not addresses or not all(_is_public(a) for a in addresses):
        raise UnsafeURLError(f"{host} resolves to a non-public address")
    return host, addresses


_pins: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar(
    "hookline_pins", default=None
)


@contextmanager
def pinned(host: str, addresses: Iterable[IPAddress]) -> Iterator[None]:
    """Within this block, connections to ``host`` go to the first of ``addresses``."""
    ips = [str(a) for a in addresses]
    if not ips:
        yield
        return
    current = dict(_pins.get() or {})
    current[host.lower()] = ips[0]
    token = _pins.set(current)
    try:
        yield
    finally:
        _pins.reset(token)


class PinnedBackend(httpcore.AsyncNetworkBackend):
    """Wraps httpcore's network backend so the TCP connection uses the pinned address.

    Only the address changes: the request's Host header and the TLS server name still come
    from the URL, so certificates are checked against the name the user registered.
    """

    def __init__(self, inner: httpcore.AsyncNetworkBackend | None = None) -> None:
        self.inner = inner or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 - httpcore's signature
        local_address: str | None = None,
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        target = (_pins.get() or {}).get(host.lower(), host)
        return await self.inner.connect_tcp(
            target,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 - httpcore's signature
        socket_options: Iterable[Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        return await self.inner.connect_unix_socket(
            path, timeout=timeout, socket_options=socket_options
        )

    async def sleep(self, seconds: float) -> None:
        await self.inner.sleep(seconds)
