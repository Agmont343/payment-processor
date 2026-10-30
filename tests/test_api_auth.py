from uuid import uuid4

from httpx import AsyncClient

from app.config import settings


async def test_health_does_not_require_api_key(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_post_without_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "RUB",
            "description": "x",
            "webhook_url": "https://example.com/hook",
        },
        headers={"Idempotency-Key": "k"},
    )
    assert response.status_code == 401
    assert "API_KEY" not in response.text
    assert settings.API_KEY not in response.text


async def test_post_with_wrong_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "RUB",
            "description": "x",
            "webhook_url": "https://example.com/hook",
        },
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": "k"},
    )
    assert response.status_code == 401
    assert "wrong-key" not in response.text


async def test_get_without_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid4()}")
    assert response.status_code == 401
