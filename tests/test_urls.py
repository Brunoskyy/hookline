import pytest

from hookline.urls import UnsafeURLError, check_url


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://localhost:8080/hook",
        "http://10.0.0.5/",
        "http://172.16.3.4/",
        "http://192.168.1.10/",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/",
        "http://[::ffff:127.0.0.1]/",
        "http://[fd00::1]/",
        "http://0.0.0.0/",
        "http://100.64.0.1/",
    ],
)
async def test_blocks_non_public_addresses(url: str) -> None:
    with pytest.raises(UnsafeURLError):
        await check_url(url, allow_private=False)


@pytest.mark.parametrize(
    "url",
    ["ftp://example.com/", "file:///etc/passwd", "http:///nohost", "https://u:p@93.184.215.14/"],
)
async def test_rejects_bad_urls(url: str) -> None:
    with pytest.raises(UnsafeURLError):
        await check_url(url, allow_private=True)


async def test_public_literal_passes() -> None:
    await check_url("https://93.184.215.14/hooks", allow_private=False)


async def test_allow_flag_lets_local_through() -> None:
    await check_url("http://127.0.0.1:9000/", allow_private=True)
