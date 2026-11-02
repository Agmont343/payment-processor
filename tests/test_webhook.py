from http import HTTPStatus
from typing import Any

import httpx
import pytest

from app.config import settings
from app.consumer.webhook import HttpxWebhookSender, WebhookResult


def _sender(status: int) -> HttpxWebhookSender:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, headers={"Location": "https://example.com/redirected"}
        )

    return HttpxWebhookSender(transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("status", [200, 204, 299])
async def test_2xx_is_success(status: int) -> None:
    assert await _sender(status).send("https://example.com/hook", {}) is (
        WebhookResult.SUCCESS
    )


@pytest.mark.parametrize("status", [429, 500, 502, 503])
async def test_429_and_5xx_are_transient(status: int) -> None:
    assert await _sender(status).send("https://example.com/hook", {}) is (
        WebhookResult.TRANSIENT
    )


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_other_4xx_are_permanent(status: int) -> None:
    assert await _sender(status).send("https://example.com/hook", {}) is (
        WebhookResult.PERMANENT
    )


async def test_3xx_without_redirect_is_permanent() -> None:
    assert await _sender(HTTPStatus.FOUND).send("https://example.com/hook", {}) is (
        WebhookResult.PERMANENT
    )


def _failing_transport(exc: Exception) -> httpx.MockTransport:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise exc

    return httpx.MockTransport(handler)


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectTimeout("timeout"),
        httpx.ReadTimeout("timeout"),
        httpx.ConnectError("network"),
    ],
)
async def test_timeout_and_network_errors_are_transient(exc: Exception) -> None:
    sender = HttpxWebhookSender(transport=_failing_transport(exc))
    assert await sender.send("https://example.com/hook", {}) is (
        WebhookResult.TRANSIENT
    )


async def test_payload_is_sent_as_json_body() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.read()
        return httpx.Response(HTTPStatus.OK)

    sender = HttpxWebhookSender(transport=httpx.MockTransport(handler))
    await sender.send("https://example.com/hook", {"event_id": "abc"})

    assert seen["url"] == "https://example.com/hook"
    assert b"abc" in seen["body"]


def _recording_sender(
    status: int = HTTPStatus.OK,
) -> tuple[HttpxWebhookSender, list[str]]:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(status)

    return HttpxWebhookSender(transport=httpx.MockTransport(handler)), seen


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8081/hook",
        "http://10.0.0.5/hook",
        "http://169.254.169.254/latest/meta-data/",
        "https://localhost:8081/hook",
        "ftp://example.com/hook",
    ],
)
async def test_forbidden_url_is_permanent_without_http_call(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # HttpxWebhookSender читает production settings из .env; для проверки
    # запрета приватных URL явно выключаем флаг.
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", False)
    sender, seen = _recording_sender(status=HTTPStatus.OK)

    assert await sender.send(url, {}) is WebhookResult.PERMANENT
    assert seen == []


async def test_allowed_url_still_reaches_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", False)
    sender, seen = _recording_sender(status=HTTPStatus.OK)

    assert await sender.send("https://example.com/hook", {}) is (WebhookResult.SUCCESS)
    assert seen == ["https://example.com/hook"]


async def test_private_url_allowed_only_with_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender, seen = _recording_sender(status=HTTPStatus.OK)

    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", False)
    assert await sender.send("http://127.0.0.1:8081/hook", {}) is (
        WebhookResult.PERMANENT
    )

    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)
    assert await sender.send("http://127.0.0.1:8081/hook", {}) is (
        WebhookResult.SUCCESS
    )
    assert seen == ["http://127.0.0.1:8081/hook"]
