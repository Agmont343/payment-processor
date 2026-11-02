from enum import Enum
from http import HTTPStatus
from typing import Any, Protocol

import httpx

from app.common.webhook_url import InvalidWebhookUrl, validate_webhook_url
from app.config import settings

WEBHOOK_TIMEOUT_SECONDS = 10.0


class WebhookResult(Enum):
    SUCCESS = "success"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict[str, Any]) -> WebhookResult: ...


class HttpxWebhookSender:
    """Классификация HTTP-исходов webhook (план, этап 5).

    2xx -> SUCCESS; 429, 5xx, таймаут и сетевые ошибки -> TRANSIENT;
    прочие 4xx и 3xx (редиректы отключены) -> PERMANENT.

    URL проверяется тем же фильтром, что и при создании платежа: адрес
    мог стать запрещённым после сохранения, а URL-фильтр упрощённый и не
    защищает от DNS rebinding. Запретный URL -> PERMANENT, запрос не
    выполняется и повторов нет.
    """

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport

    async def send(self, url: str, payload: dict[str, Any]) -> WebhookResult:
        try:
            validate_webhook_url(url, allow_private=settings.WEBHOOK_ALLOW_PRIVATE)
        except InvalidWebhookUrl:
            return WebhookResult.PERMANENT
        try:
            async with httpx.AsyncClient(
                timeout=WEBHOOK_TIMEOUT_SECONDS,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                response = await client.post(url, json=payload)
        except httpx.TransportError:
            return WebhookResult.TRANSIENT
        if HTTPStatus.OK <= response.status_code < 300:
            return WebhookResult.SUCCESS
        if (
            response.status_code == HTTPStatus.TOO_MANY_REQUESTS
            or response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            return WebhookResult.TRANSIENT
        return WebhookResult.PERMANENT
