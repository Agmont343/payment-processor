import hmac
from http import HTTPStatus
from typing import Annotated

from fastapi import Header, HTTPException

from app.config import settings


async def require_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    if x_api_key is None:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Missing API key",
        )
    if not hmac.compare_digest(x_api_key, settings.API_KEY):
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Invalid API key",
        )
