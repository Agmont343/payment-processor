import asyncio
import hashlib
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

# Бизнес-исход и задержка детерминированы от payment_id (план, этап 4):
# digest % 10 == 0 -> отказ (10%), задержка 2-5 с из того же digest.
MIN_DELAY_SECONDS = 2
MAX_DELAY_SECONDS = 5


@dataclass(frozen=True, slots=True)
class GatewayResult:
    success: bool


@dataclass(frozen=True, slots=True)
class GatewayOutcome:
    success: bool
    delay_seconds: float


def gateway_outcome(payment_id: UUID) -> GatewayOutcome:
    digest = int.from_bytes(
        hashlib.sha256(str(payment_id).encode("utf-8")).digest(),
        "big",
    )
    return GatewayOutcome(
        success=digest % 10 != 0,
        delay_seconds=MIN_DELAY_SECONDS
        + (digest >> 8) % (MAX_DELAY_SECONDS - MIN_DELAY_SECONDS + 1),
    )


class PaymentGateway(Protocol):
    async def process(self, payment_id: UUID) -> GatewayResult: ...


class DeterministicGateway:
    """Эмулятор эквайринга: тот же payment_id -> тот же исход.

    Повторный вызов при незаписанном статусе вернёт тот же результат,
    но это свойство эмулятора, а не идемпотентность реального провайдера.
    """

    async def process(self, payment_id: UUID) -> GatewayResult:
        outcome = gateway_outcome(payment_id)
        await asyncio.sleep(outcome.delay_seconds)
        return GatewayResult(success=outcome.success)
