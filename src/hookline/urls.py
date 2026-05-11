"""Subscriber URL checks.

A webhook service makes HTTP requests to addresses its users type in, which makes it an SSRF
tool unless it refuses the addresses nobody outside should reach: loopback, private ranges,
link-local (including the cloud metadata service at 169.254.169.254), and the rest of the
reserved space. URLs are checked when an endpoint is registered and again, after DNS, right
before every attempt, because a name can resolve somewhere else by then.
"""

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit


class UnsafeURLError(ValueError):
    pass


class ResolutionError(UnsafeURLError):
    """The name did not resolve. Unlike a blocked address this may fix itself, so a delivery
    that hits it is retried rather than given up on."""


def _is_public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return _is_public(ip.ipv4_mapped)
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


async def resolve(host: str, port: int) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
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


async def check_url(url: str, *, allow_private: bool) -> None:
    """Raises :class:`UnsafeURLError` if ``url`` may not be delivered to."""
    host, port = check_syntax(url)
    if allow_private:
        return
    addresses = await resolve(host, port)
    if not addresses or not all(_is_public(a) for a in addresses):
        raise UnsafeURLError(f"{host} resolves to a non-public address")
