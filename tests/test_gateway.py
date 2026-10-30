from uuid import UUID

from app.consumer.gateway import DeterministicGateway, GatewayResult, gateway_outcome

EMULATION_IDS = [UUID(int=i) for i in range(1000)]


def test_outcome_is_stable_for_same_payment_id() -> None:
    payment_id = UUID(int=42)
    assert gateway_outcome(payment_id) == gateway_outcome(payment_id)


def test_delay_is_within_emulation_window() -> None:
    for payment_id in EMULATION_IDS:
        delay = gateway_outcome(payment_id).delay_seconds
        assert 2 <= delay <= 5


def test_business_failure_share_is_about_10_percent() -> None:
    failures = sum(
        1 for payment_id in EMULATION_IDS if not gateway_outcome(payment_id).success
    )
    assert 85 <= failures <= 115


async def test_gateway_process_sleeps_and_returns_outcome(monkeypatch) -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("app.consumer.gateway.asyncio.sleep", fake_sleep)

    payment_id = UUID(int=7)
    outcome = gateway_outcome(payment_id)

    result = await DeterministicGateway().process(payment_id)

    assert isinstance(result, GatewayResult)
    assert result.success is outcome.success
    assert slept == [outcome.delay_seconds]
